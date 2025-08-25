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

# Caminhos de entrada
silver_path_historico = f"{SILVER_PREFIX}/historico"
silver_path_contratacoes_12m = f"{SILVER_PREFIX}/contratacoes" # <-- Adicionado caminho
gold_path_dim_cliente = f"{GOLD_PREFIX}/dim_cliente"
gold_path_dim_produto = f"{GOLD_PREFIX}/dim_produto"
gold_path_dim_tempo = f"{GOLD_PREFIX}/dim_tempo"

spark = SparkSession.builder.appName("SilverToGoldFatoContratacoes_Final_Ajustado").getOrCreate()

# ==============================================================================
# 1. Parâmetros de Conexão JDBC
# ==============================================================================
jdbc_url = "jdbc:oracle:thin:@oracle.fiap.com.br:1521:orcl"
properties = { "user": "rm555625", "password": "100203", "driver": "oracle.jdbc.OracleDriver" }

print("Lendo dados de origem e dimensões...")

df_gold_ft_contratacoes = None

# ==============================================================================
# 2. Leitura e Processamento (FLUXO AJUSTADO)
# ==============================================================================
try:
    # Leitura da tabela transacional e das dimensões
    df_silver_historico = spark.read.parquet(silver_path_historico)
    df_silver_contratacoes_12m = spark.read.parquet(silver_path_contratacoes_12m) # <-- Adicionada leitura
    df_dim_cliente = spark.read.parquet(gold_path_dim_cliente)
    df_dim_produto = spark.read.parquet(gold_path_dim_produto)
    df_dim_tempo = spark.read.parquet(gold_path_dim_tempo)

    # Passo 1: Join do histórico com a DIM_CLIENTE
    df_base = df_silver_historico.join(
        df_dim_cliente,
        df_silver_historico.cd_cliente == df_dim_cliente.CD_CLIENTE,
        "inner"
    )
    
    # Passo 2: Join com a DIM_PRODUTO
    df_base = df_base.join(
        df_dim_produto,
        df_base.cd_prod == df_dim_produto.CD_PRODUTO,
        "inner"
    )
    
    # Passo 3: Join com a DIM_TEMPO
    df_base = df_base.withColumn("data_para_join", F.to_date(F.col("dt_upload")))
    df_base = df_base.join(
        df_dim_tempo,
        df_base.data_para_join == df_dim_tempo.DATA,
        "inner"
    )

    # Passo 4: Join com os dados de contratações dos últimos 12 meses
    df_final = df_base.join(
        df_silver_contratacoes_12m,
        "cd_cliente", # Junta pela chave do cliente
        "left"
    )

    window_spec = Window.orderBy(F.monotonically_increasing_id())
    
    # Seleciona as colunas finais para a fato (apenas SKs e métricas)
    df_gold_ft_contratacoes = df_final.select(
        F.col("SK_CLIENTE"),
        F.col("SK_PRODUTO"),
        F.col("SK_TEMPO"),
        F.col("vl_total").alias("VL_TOTAL_CONTRATO"),
        F.col("vl_desconto").alias("VL_DESCONTO"),
        F.col("vlr_contratacoes_12m").alias("VL_CONTRATACOES_12M") # <-- Coluna adicionada
    ).na.fill(0, ["VL_CONTRATACOES_12M"]) # Garante que não haja nulos
    
    # Adiciona e renomeia a chave primária da fato
    df_gold_ft_contratacoes = df_gold_ft_contratacoes.withColumn(
        "SK_CONTRATACAO", # <-- Nome da chave alterado
        F.row_number().over(window_spec).cast(IntegerType())
    )
    
    # Reordena para a ordem final desejada
    df_gold_ft_contratacoes = df_gold_ft_contratacoes.select(
        "SK_CONTRATACAO", "SK_CLIENTE", "SK_PRODUTO", "SK_TEMPO",
        "VL_TOTAL_CONTRATO", "VL_DESCONTO", "VL_CONTRATACOES_12M"
    )

    print("\nDataFrame para a FATO_CONTRATACOES (ajustado) criado com sucesso.")
    print(f"Tamanho final do DataFrame: {df_gold_ft_contratacoes.count()} linhas.")
    df_gold_ft_contratacoes.printSchema()

except Exception as e:
    print(f"Erro no processamento: {e}")
    if 'spark' in locals():
        spark.stop()

# ==============================================================================
# 3. Carregamento dos Dados na Camada Gold
# ==============================================================================
if df_gold_ft_contratacoes is not None:
    try:
        df_gold_ft_contratacoes.write.jdbc(url=jdbc_url, table="FATO_CONTRATACOES", mode="overwrite", properties=properties)
        print("\nDados da FATO_CONTRATACOES carregados com sucesso no banco de dados!")
    except Exception as e:
        print(f"Erro ao carregar dados no banco via JDBC: {e}")
else:
    print("Processamento interrompido.")

spark.stop()