import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("SilverToGoldFatoNPS").getOrCreate()

# ==============================================================================
# 1. Parâmetros de Conexão JDBC
# ==============================================================================
# A conexão JDBC será usada apenas para a escrita final da Fato
jdbc_url = "jdbc:oracle:thin:@oracle.fiap.com.br:1521:orcl" 

properties = {
    "user": "rm555625",
    "password": "100203",
    "driver": "oracle.jdbc.OracleDriver"
}

# Caminhos dos dados limpos da camada Silver e Gold
BUCKET_NAME = "weedle"
TENANCY_NAMESPACE = "rm555625"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"
GOLD_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/gold"
silver_path_nps_relacional = f"{SILVER_PREFIX}/nps_relacional"
silver_path_nps_produto = f"{SILVER_PREFIX}/nps_transacional_produto"
silver_path_nps_suporte = f"{SILVER_PREFIX}/nps_transacional_suporte"
silver_path_nps_onboarding = f"{SILVER_PREFIX}/nps_transacional_onboarding"
silver_path_nps_aquisicao = f"{SILVER_PREFIX}/nps_transacional_aquisicao"
silver_path_tickets = f"{SILVER_PREFIX}/tickets"

print("Lendo dados da camada Silver...")

df_silver_tickets = None

# ==============================================================================
# 2. Leitura dos Dados da Camada Silver e Dimensões da Camada GOLD
# ==============================================================================
try:
    df_silver_nps_relacional = spark.read.parquet(silver_path_nps_relacional).cache()
    df_silver_nps_produto = spark.read.parquet(silver_path_nps_produto).cache()
    df_silver_nps_suporte = spark.read.parquet(silver_path_nps_suporte).cache()
    df_silver_nps_onboarding = spark.read.parquet(silver_path_nps_onboarding).cache()
    df_silver_nps_aquisicao = spark.read.parquet(silver_path_nps_aquisicao).cache()
    df_silver_tickets = spark.read.parquet(silver_path_tickets).cache() # Lendo a tabela de tickets para a correção

    # Lendo as dimensões dos arquivos Parquet da CAMADA GOLD
    df_dim_cliente = spark.read.parquet(f"{GOLD_PREFIX}/dim_cliente").select("SK_CLIENTE", "CD_CLIENTE").cache()
    df_dim_produto = spark.read.parquet(f"{GOLD_PREFIX}/dim_produto").select("SK_PRODUTO", "CD_PRODUTO", "NM_PRODUTO").cache()
    df_dim_tempo = spark.read.parquet(f"{GOLD_PREFIX}/dim_tempo").select("SK_TEMPO", "DATA").cache()

    print("Dados das camadas Silver e Gold lidos com sucesso.")

except Exception as e:
    print(f"Erro ao ler arquivos ou tabelas. Verifique a existência dos dados: {e}")
    spark.stop()


# ==============================================================================
# 3. Processamento para a Tabela Fato (Lógica Corrigida)
# ==============================================================================
# Adicionar a data de criação do ticket à tabela de NPS de suporte
df_nps_suporte_com_data = df_silver_nps_suporte.join(
    df_silver_tickets.select("bk_ticket", "dt_criacao"),
    on="bk_ticket",
    how="left"
).select(
    F.col("dt_criacao").alias("dt_resposta"),
    F.col("cd_cliente"),
    F.col("nota_nps"),
    F.col("bk_ticket")
)

# Unir as métricas de NPS em um único DataFrame, seguindo a estrutura da FATO_NPS
# Esta é a lógica que alinha os schemas para a união
df_nps_relacional_formatado = df_silver_nps_relacional.select(
    F.col("dt_resposta"), F.col("cd_cliente"), F.col("nota_nps").alias("RESPOSTA_NPS"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_PRODUTO"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_SUPORTE"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_ONBOARDING"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_AQUISICAO"),
    F.lit(None).cast(IntegerType()).alias("BK_TICKET"),
    F.lit(None).cast(StringType()).alias("NM_PRODUTO")
)

df_nps_produto_formatado = df_silver_nps_produto.select(
    F.col("dt_resposta"), F.col("cd_cliente"), F.col("nota_nps").alias("RESPOSTA_NPS"),
    F.col("nota_nps").alias("NOTA_SATISFACAO_PRODUTO"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_SUPORTE"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_ONBOARDING"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_AQUISICAO"),
    F.lit(None).cast(IntegerType()).alias("BK_TICKET"),
    F.col("nome_produto").alias("NM_PRODUTO")
)

df_nps_suporte_formatado = df_nps_suporte_com_data.select(
    F.col("dt_resposta"), F.col("cd_cliente"), F.col("nota_nps").alias("RESPOSTA_NPS"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_PRODUTO"),
    F.col("nota_nps").alias("NOTA_SATISFACAO_SUPORTE"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_ONBOARDING"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_AQUISICAO"),
    F.col("bk_ticket").alias("BK_TICKET"),
    F.lit(None).cast(StringType()).alias("NM_PRODUTO")
)

df_nps_onboarding_formatado = df_silver_nps_onboarding.select(
    F.col("dt_resposta"), F.col("cd_cliente"), F.col("nota_nps_onboarding").alias("RESPOSTA_NPS"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_PRODUTO"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_SUPORTE"),
    F.col("nota_nps_onboarding").alias("NOTA_SATISFACAO_ONBOARDING"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_AQUISICAO"),
    F.lit(None).cast(IntegerType()).alias("BK_TICKET"),
    F.lit(None).cast(StringType()).alias("NM_PRODUTO")
)

df_nps_aquisicao_formatado = df_silver_nps_aquisicao.select(
    F.col("dt_resposta"), F.col("cd_cliente"), F.col("nota_nps").alias("RESPOSTA_NPS"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_PRODUTO"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_SUPORTE"),
    F.lit(None).cast(DoubleType()).alias("NOTA_SATISFACAO_ONBOARDING"),
    F.col("nota_nps").alias("NOTA_SATISFACAO_AQUISICAO"),
    F.lit(None).cast(IntegerType()).alias("BK_TICKET"),
    F.lit(None).cast(StringType()).alias("NM_PRODUTO")
)

# União de todos os DataFrames formatados
df_nps_consolidado = df_nps_relacional_formatado.unionByName(df_nps_produto_formatado)\
    .unionByName(df_nps_suporte_formatado)\
    .unionByName(df_nps_onboarding_formatado)\
    .unionByName(df_nps_aquisicao_formatado)

# Juntar com as dimensões para obter as SKs
df_fato = df_nps_consolidado.join(
    F.broadcast(df_dim_cliente), on="cd_cliente", how="left"
).join(
    F.broadcast(df_dim_tempo), df_nps_consolidado.dt_resposta == df_dim_tempo.DATA, how="left"
).join(
    F.broadcast(df_dim_produto), df_nps_consolidado.NM_PRODUTO == df_dim_produto.NM_PRODUTO, how="left"
)

# Selecionar as colunas finais para a FATO_NPS
df_gold_fato_nps = df_fato.select(
    F.col("sk_cliente").alias("SK_CLIENTE"),
    F.col("sk_produto").alias("SK_PRODUTO"),
    F.col("sk_tempo").alias("SK_TEMPO"),
    F.col("RESPOSTA_NPS"),
    F.coalesce(F.col("NOTA_SATISFACAO_PRODUTO"), F.lit(-1)).alias("NOTA_SATISFACAO_PRODUTO"),
    F.coalesce(F.col("NOTA_SATISFACAO_SUPORTE"), F.lit(-1)).alias("NOTA_SATISFACAO_SUPORTE"),
    F.coalesce(F.col("NOTA_SATISFACAO_ONBOARDING"), F.lit(-1)).alias("NOTA_SATISFACAO_ONBOARDING"),
    F.coalesce(F.col("NOTA_SATISFACAO_AQUISICAO"), F.lit(-1)).alias("NOTA_SATISFACAO_AQUISICAO"),
    F.col("BK_TICKET")
)

# Adicionar a Surrogate Key da tabela fato
df_gold_fato_nps = df_gold_fato_nps.withColumn(
    "SK_FATO_NPS",
    F.monotonically_increasing_id().cast(IntegerType())
)

print("\nDataFrame para a FATO_NPS criado com sucesso.")
df_gold_fato_nps.printSchema()

# ==============================================================================
# 4. Carregamento dos Dados no Banco de Dados (Sua Camada Gold)
# ==============================================================================
if df_gold_fato_nps is not None:
    try:
        df_gold_fato_nps.write.jdbc(
            url=jdbc_url,
            table="FATO_NPS",
            mode="overwrite",
            properties=properties
        )
        print("\nDados da FATO_NPS carregados com sucesso no banco de dados!")
    except Exception as e:
        print(f"Erro ao carregar dados no banco via JDBC: {e}")
        print("Verifique a URL, credenciais e regras de firewall.")
else:
    print("Processamento interrompido.")

spark.stop()