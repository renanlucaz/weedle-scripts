import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("FatoContratacoes_Consolidated").getOrCreate()

# ==============================================================================
# 1. Parâmetros de Conexão JDBC
# ==============================================================================
jdbc_url = "jdbc:oracle:thin:@oracle.fiap.com.br:1521:orcl" 

properties = {
    "user": "rm555625",
    "password": "100203",
    "driver": "oracle.jdbc.OracleDriver"
}

# Caminhos dos dados limpos da camada Silver e Gold
BUCKET_NAME = "weedle"
TENANCY_NAMESPACE = "rm555625"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@{TENANCY_NAMESPACE}/silver"
GOLD_PREFIX = f"oci://{BUCKET_NAME}@{TENANCY_NAMESPACE}/gold"
silver_path_historico = f"{SILVER_PREFIX}/historico"
silver_path_contratacoes = f"{SILVER_PREFIX}/contratacoes"
silver_path_clientes = f"{SILVER_PREFIX}/clientes"

print("Lendo dados da camada Silver...")

df_silver_historico = None
df_silver_contratacoes = None
df_silver_clientes = None

# ==============================================================================
# 2. Leitura dos Dados da Camada Silver e Dimensões da Camada GOLD
# ==============================================================================
try:
    df_silver_historico = spark.read.parquet(silver_path_historico).cache()
    df_silver_contratacoes = spark.read.parquet(silver_path_contratacoes).cache()
    df_silver_clientes = spark.read.parquet(silver_path_clientes).cache()

    # Lendo as dimensões dos arquivos Parquet da CAMADA GOLD
    df_dim_cliente = spark.read.parquet(f"{GOLD_PREFIX}/dim_cliente").select("SK_CLIENTE", "CD_CLIENTE").cache()
    df_dim_tempo = spark.read.parquet(f"{GOLD_PREFIX}/dim_tempo").select("SK_TEMPO", "DATA").cache()
    df_dim_produto = spark.read.parquet(f"{GOLD_PREFIX}/dim_produto").select("SK_PRODUTO", "CD_PRODUTO").cache()
    df_dim_modalidade = spark.read.parquet(f"{GOLD_PREFIX}/dim_modalidade_comercial").select("SK_MODALIDADE", "TIPO_MODALIDADE").cache()
    df_dim_hospedagem = spark.read.parquet(f"{GOLD_PREFIX}/dim_hospedagem").select("SK_HOSPEDAGEM", "TIPO_HOSPEDAGEM").cache()

    print("Dados das camadas Silver e Gold lidos com sucesso.")

except Exception as e:
    print(f"Erro ao ler arquivos ou tabelas. Verifique a existência dos dados: {e}")
    spark.stop()

# ==============================================================================
# 3. Processamento para a Tabela Fato
# ==============================================================================
# Agregando os dados do histórico de propostas na granularidade da FATO
df_fato_base_agregado = df_silver_historico.join(
    df_silver_clientes.select("cd_cliente", "modal_comerc", "vl_total_contrato", "hospedagem"), on="cd_cliente", how="left"
).join(
    df_silver_contratacoes, on="cd_cliente", how="left"
).groupBy(
    "cd_cliente", "cd_prod", "dt_upload", "modal_comerc", "hospedagem"
).agg(
    F.sum("vl_total_contrato").alias("VL_TOTAL_CONTRATO"),
    F.sum("vl_desconto").alias("VL_DESCONTO"),
    F.max("qtd_contratacoes_12m").alias("QTD_CONTRATACOES_12M"),
    F.max("vlr_contratacoes_12m").alias("VL_CONTRATACOES_12M")
)


# Adicionar as SKs das dimensões
df_fato = df_fato_base_agregado.join(
    F.broadcast(df_dim_cliente), on=F.col("cd_cliente") == F.col("CD_CLIENTE"), how="left"
).join(
    F.broadcast(df_dim_produto), on=F.col("cd_prod") == F.col("CD_PRODUTO"), how="left"
).join(
    F.broadcast(df_dim_tempo), on=F.col("dt_upload") == F.col("DATA"), how="left"
).join(
    F.broadcast(df_dim_modalidade), on=F.col("modal_comerc") == F.col("TIPO_MODALIDADE"), how="left"
).join(
    F.broadcast(df_dim_hospedagem), on=F.col("hospedagem") == F.col("TIPO_HOSPEDAGEM"), how="left"
)

# Selecionar as colunas finais para a FATO_CONTRATACOES
df_gold_fato_contratacoes = df_fato.select(
    F.col("sk_cliente").alias("SK_CLIENTE"),
    F.col("sk_tempo").alias("SK_TEMPO"),
    F.col("sk_produto").alias("SK_PRODUTO"),
    F.col("sk_modalidade").alias("SK_MODALIDADE"),
    F.col("sk_hospedagem").alias("SK_HOSPEDAGEM"),
    F.col("VL_TOTAL_CONTRATO"),
    F.col("VL_DESCONTO"),
    F.col("QTD_CONTRATACOES_12M"),
    F.col("VL_CONTRATACOES_12M"),
    F.col("cd_prod").alias("DS_PRODUTO")
)

# Adicionar a Surrogate Key da tabela fato
df_gold_fato_contratacoes = df_gold_fato_contratacoes.withColumn(
    "SK_CONTRATACOES",
    F.monotonically_increasing_id().cast(IntegerType())
)

print("\nDataFrame para a FATO_CONTRATACOES criado com sucesso.")
df_gold_fato_contratacoes.printSchema()

# ==============================================================================
# 4. Carregamento dos Dados no Banco de Dados (Sua Camada Gold)
# ==============================================================================
if df_gold_fato_contratacoes is not None:
    try:
        df_gold_fato_contratacoes.write.jdbc(
            url=jdbc_url,
            table="FATO_CONTRATACOES",
            mode="overwrite",
            properties=properties
        )
        print("\nDados da FATO_CONTRATACOES carregados com sucesso no banco de dados!")
    except Exception as e:
        print(f"Erro ao carregar dados no banco via JDBC: {e}")
        print("Verifique a URL, credenciais e regras de firewall.")
else:
    print("Processamento interrompido.")

spark.stop()