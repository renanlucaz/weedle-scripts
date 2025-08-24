import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, DateType
from pyspark.sql import SparkSession

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# ==============================================================================
# 1. Parâmetros e Caminhos
# ==============================================================================
# Defina o nome do seu bucket no OCI Object Storage
BUCKET_NAME = "weedle"
BRONZE_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/bronze"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"

# Caminhos dos arquivos
input_path_clientes = f"{BRONZE_PREFIX}/dados_clientes.csv"
output_path_clientes = f"{SILVER_PREFIX}/clientes"

print(f"Lendo dados brutos de: {input_path_clientes}")

df_bronze_clientes = {}

# ==============================================================================
# 2. Leitura dos Dados da Camada Bronze
# ==============================================================================
try:
    df_bronze_clientes = spark.read.csv(
        input_path_clientes, 
        header=True, 
        inferSchema=True, 
        sep=';'
    )
    print("Schema da tabela de clientes na camada Bronze:")
    df_bronze_clientes.printSchema()

except Exception as e:
    print(f"Erro ao ler o arquivo CSV: {e}")
    spark.stop()

# ==============================================================================
# 3. Transformações para a Camada Silver (Agregado por Cliente)
# ==============================================================================
# Padronizar nomes e converter tipos antes de agrupar
df_temp = df_bronze_clientes.select(
    F.col("CD_CLIENTE").alias("cd_cliente"),
    F.col("CIDADE").alias("cidade"),
    F.col("DS_CNAE").alias("ds_cnae"),
    F.col("DS_SEGMENTO").alias("ds_segmento"),
    F.col("DS_SUBSEGMENTO").alias("ds_subsegmento"),
    F.col("DS_PROD").alias("ds_prod"),
    F.col("DS_LIN_REC").alias("ds_lin_rec"),
    F.col("FAT_FAIXA").alias("fat_faixa"),
    F.col("MARCA_TOTVS").alias("marca_totvs"),
    F.col("MODAL_COMERC").alias("modal_comerc"),
    F.col("SITUACAO_CONTRATO").alias("situacao_contrato"),
    F.col("UF").alias("uf"),
    F.regexp_replace(F.col("VL_TOTAL_CONTRATO"), ',', '.').cast(DoubleType()).alias("vl_total_contrato"),
    F.to_date(F.col("DT_ASSINATURA_CONTRATO"), "yyyy-MM-dd").alias("dt_assinatura_contrato")
).na.fill({
    "ds_subsegmento": "Nao Informado",
    "marca_totvs": "Nao Informado",
    "modal_comerc": "Nao Informado"
})

# Agrupando por cliente e agregando as métricas e datas
df_silver_clientes = df_temp.select(
    F.col("cd_cliente").alias("cd_cliente"),
    F.col("dt_assinatura_contrato").alias("dt_assinatura_contrato"),
    F.col("vl_total_contrato").alias("vl_total_contrato"),
    F.col("cidade").alias("cidade"),
    F.col("ds_cnae").alias("ds_cnae"),
    F.col("ds_segmento").alias("ds_segmento"),
    F.col("ds_subsegmento").alias("ds_subsegmento"),
    F.col("ds_prod").alias("ds_prod"),
    F.col("ds_lin_rec").alias("ds_lin_rec"),
    F.col("fat_faixa").alias("fat_faixa"),
    F.col("marca_totvs").alias("marca_totvs"),
    F.col("modal_comerc").alias("modal_comerc"),
    F.col("uf").alias("uf"),
    F.col("situacao_contrato").alias("situacao_contrato")
)

print("\nSchema da tabela de clientes na camada Silver:")
df_silver_clientes.printSchema()

# ==============================================================================
# 4. Salvando os Dados na Camada Silver (Formato Parquet)
# ==============================================================================
print(f"\nSalvando dados agregados em: {output_path_clientes}")
df_silver_clientes.write.mode("overwrite").parquet(output_path_clientes)

print("Processamento concluído com sucesso!")