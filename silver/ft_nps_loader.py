import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# ==============================================================================
# 0. Configuração de Variáveis
# ==============================================================================
BUCKET_NAME = "weedle"
TENANCY_NAMESPACE = "gr7jlaomnxx1" 

SILVER_PREFIX = f"oci://{BUCKET_NAME}@{TENANCY_NAMESPACE}/silver"
GOLD_PREFIX = f"oci://{BUCKET_NAME}@{TENANCY_NAMESPACE}/gold"

# Caminhos de entrada para todos os tipos de NPS e tickets
silver_path_nps_aquisicao = f"{SILVER_PREFIX}/nps_transacional_aquisicao"
silver_path_nps_onboarding = f"{SILVER_PREFIX}/nps_transacional_onboarding"
silver_path_nps_produto = f"{SILVER_PREFIX}/nps_transacional_produto"
silver_path_nps_relacional = f"{SILVER_PREFIX}/nps_relacional"
silver_path_nps_suporte = f"{SILVER_PREFIX}/nps_transacional_suporte"
silver_path_tickets = f"{SILVER_PREFIX}/tickets" # <-- Caminho para os tickets

# Caminhos para as dimensões
gold_path_dim_cliente = f"{GOLD_PREFIX}/dim_cliente"
gold_path_dim_produto = f"{GOLD_PREFIX}/dim_produto"
gold_path_dim_tempo = f"{GOLD_PREFIX}/dim_tempo"

spark = SparkSession.builder.appName("SilverToGoldFatoNPS_Atualizado").getOrCreate()

# ==============================================================================
# 1. Parâmetros de Conexão JDBC
# ==============================================================================
jdbc_url = "jdbc:oracle:thin:@oracle.fiap.com.br:1521:orcl"
properties = { "user": "rm555625", "password": "100203", "driver": "oracle.jdbc.OracleDriver" }

print("Lendo dados de NPS da camada Silver e dimensões da camada Gold...")

# ==============================================================================
# 2. Leitura e Processamento para a Fato (DataFrame)
# ==============================================================================
try:
    # Leitura dos DataFrames de NPS e Tickets
    df_nps_aquisicao = spark.read.parquet(silver_path_nps_aquisicao)
    df_nps_onboarding = spark.read.parquet(silver_path_nps_onboarding)
    df_nps_produto_raw = spark.read.parquet(silver_path_nps_produto)
    df_nps_relacional = spark.read.parquet(silver_path_nps_relacional)
    df_nps_suporte_raw = spark.read.parquet(silver_path_nps_suporte) # Lendo como dados brutos
    df_silver_tickets = spark.read.parquet(silver_path_tickets)     # Lendo os tickets

    # Leitura das Dimensões
    df_dim_cliente = spark.read.parquet(gold_path_dim_cliente)
    df_dim_produto = spark.read.parquet(gold_path_dim_produto)
    df_dim_tempo = spark.read.parquet(gold_path_dim_tempo)

    # --- LÓGICA ATUALIZADA ---
    # Passo 1: Enriquecer NPS de Suporte com a data do ticket
    df_mapa_ticket_data = df_silver_tickets.select(
        F.col("bk_ticket"),
        F.to_date(F.col("dt_criacao")).alias("dt_resposta") # Padroniza nome da coluna de data
    )
    df_nps_suporte_enriquecido = df_nps_suporte_raw.join(
        df_mapa_ticket_data,
        "bk_ticket",
        "inner"
    ).select(
        "cd_cliente", "dt_resposta", 
        F.col("nota_nps").alias("ds_nota"), # Padroniza nome da coluna de nota
        "tipo_nps", F.lit(None).alias("cd_prod")
    )
    print("Dados de NPS de Suporte enriquecidos com a data do ticket.")
    
    # Passo 2: Tratar NPS de Produto (lógica anterior)
    df_nps_produto_enriquecido = df_nps_produto_raw.join(
        df_dim_produto,
        df_nps_produto_raw.nome_produto == df_dim_produto.DS_PRODUTO,
        "inner"
    ).select(
        "cd_cliente", "dt_resposta",
        F.col("nota_nps").alias("ds_nota"),
        "tipo_nps",
        F.col("CD_PRODUTO").alias("cd_prod")
    )

    # Passo 3: Unificar todos os DataFrames de NPS
    df_nps_unificado = df_nps_aquisicao.select(F.col("cd_cliente"), F.col("dt_resposta"), F.col("nota_nps").alias("ds_nota"), "tipo_nps", F.lit(None).alias("cd_prod")) \
        .unionByName(df_nps_onboarding.select(F.col("cd_cliente"), F.col("dt_resposta"), F.col("nota_nps").alias("ds_nota"), "tipo_nps", F.lit(None).alias("cd_prod"))) \
        .unionByName(df_nps_produto_enriquecido) \
        .unionByName(df_nps_relacional.select(F.col("cd_cliente"), F.col("dt_resposta"), F.col("nota_nps").alias("ds_nota"), "tipo_nps", F.lit(None).alias("cd_prod"))) \
        .unionByName(df_nps_suporte_enriquecido) # Usando a versão enriquecida

    print("Todos os arquivos de NPS foram unificados com sucesso.")

    # O resto do script continua normalmente...
    df_base = df_nps_unificado.join(df_dim_cliente, df_nps_unificado.cd_cliente == df_dim_cliente.CD_CLIENTE, "inner")
    df_base = df_base.join(df_dim_produto, df_base.cd_prod == df_dim_produto.CD_PRODUTO, "left")
    df_base = df_base.withColumn("data_para_join", F.to_date(F.col("dt_resposta")))
    df_final = df_base.join(df_dim_tempo, df_base.data_para_join == df_dim_tempo.DATA, "inner")
    
    window_spec = Window.orderBy(F.monotonically_increasing_id())
    df_gold_ft_nps = df_final.select(
        "SK_CLIENTE", "SK_TEMPO",
        F.col("ds_nota").alias("NOTA_NPS"),
        F.col("tipo_nps").alias("TIPO_NPS")
    )
    df_gold_ft_nps = df_gold_ft_nps.withColumn("SK_FATO_NPS", F.row_number().over(window_spec).cast(IntegerType())) \
                                  .select("SK_FATO_NPS", "SK_CLIENTE", "SK_TEMPO", "NOTA_NPS", "TIPO_NPS")
    
    print("\nDataFrame para a FATO_NPS criado com sucesso.")
    print(f"Total de registros de NPS: {df_gold_ft_nps.count()}")

except Exception as e:
    print(f"Erro no processamento: {e}")
    if 'spark' in locals():
        spark.stop()

# ==============================================================================
# 3. Carregamento dos Dados na Camada Gold
# ==============================================================================
if 'df_gold_ft_nps' in locals() and df_gold_ft_nps is not None:
    try:
        df_gold_ft_nps.write.jdbc(url=jdbc_url, table="FATO_NPS", mode="overwrite", properties=properties)
        print("\nDados da FATO_NPS carregados com sucesso no banco de dados!")
    except Exception as e:
        print(f"Erro ao carregar dados no banco via JDBC: {e}")
else:
    print("Processamento interrompido.")

spark.stop()