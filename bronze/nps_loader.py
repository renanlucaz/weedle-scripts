import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import SparkSession

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("NPSRelacionalBronzeToSilver").getOrCreate()

# ==============================================================================
# 1. Parâmetros e Caminhos (VERIFIQUE E CORRIJA)
# ==============================================================================
# Defina o nome do seu bucket no OCI Object Storage e o namespace da sua tenancy
BUCKET_NAME = "weedle"
BRONZE_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/bronze"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"  

# Caminhos dos arquivos
input_path_nps_relacional = f"{BRONZE_PREFIX}/nps_relacional.csv"
output_path_nps_relacional = f"{SILVER_PREFIX}/nps_relacional"

print(f"Lendo dados brutos de: {input_path_nps_relacional}")

# ==============================================================================
# 2. Leitura dos Dados da Camada Bronze
# ==============================================================================
try:
    df_bronze_nps = spark.read.csv(
        input_path_nps_relacional, 
        header=True, 
        inferSchema=True, 
        sep=';'
    )
    print("Schema da tabela NPS Relacional na camada Bronze:")
    df_bronze_nps.printSchema()

except Exception as e:
    print(f"Erro fatal ao ler o arquivo CSV. Verifique o caminho e as permissões: {e}")
    spark.stop()

# ==============================================================================
# 3. Transformações para a Camada Silver
# ==============================================================================
# As transformações para a camada Silver incluem:
# - Padronização dos nomes das colunas
# - Conversão de tipos de data
# - Tratamento de valores nulos (NaN) para as notas
df_silver_nps = df_bronze_nps.select(
    F.to_date(F.col("respondedAt")).alias("dt_resposta"),
    F.col("metadata_codcliente").alias("cd_cliente"),
    F.col("resposta_NPS").alias("nota_nps"),
    F.col("resposta_unidade").alias("nota_unidade"),
    F.col("Nota_SupTec_Agilidade").alias("nota_suporte_agilidade"),
    F.col("Nota_SupTec_Atendimento").alias("nota_suporte_atendimento"),
    F.col("Nota_Comercial").alias("nota_comercial"),
    F.col("Nota_Custos").alias("nota_custos"),
    F.col("Nota_AdmFin_Atendimento").alias("nota_admfin_atendimento"),
    F.col("Nota_Software").alias("nota_software"),
    F.col("Nota_Software_Atualizacao").alias("nota_software_atualizacao")
)

df_silver_nps = df_silver_nps.withColumn("tipo_nps", F.lit("relacional"))

# Tratamento de valores nulos, preenchendo as notas com -1 para indicar ausência de resposta
# A nota NPS principal (nota_nps) não tem nulos, então preenchemos as notas secundárias
df_silver_nps = df_silver_nps.na.fill(-1, subset=[
    "nota_unidade",
    "nota_suporte_agilidade",
    "nota_suporte_atendimento",
    "nota_comercial",
    "nota_custos",
    "nota_admfin_atendimento",
    "nota_software",
    "nota_software_atualizacao"
])

print("\nSchema da tabela NPS Relacional na camada Silver:")
df_silver_nps.printSchema()

# ==============================================================================
# 4. Salvando os Dados na Camada Silver (Formato Delta Lake)
# ==============================================================================
print(f"\nSalvando dados limpos em: {output_path_nps_relacional}")
df_silver_nps.write.mode("overwrite").parquet(output_path_nps_relacional)

print("Processamento concluído com sucesso!")