import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql import SparkSession

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("Contratacoes12mBronzeToSilver").getOrCreate()

# ==============================================================================
# 1. Parâmetros e Caminhos (VERIFIQUE E CORRIJA)
# ==============================================================================
# Defina o nome do seu bucket e o namespace da sua tenancy
BUCKET_NAME = "weedle"
BRONZE_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/bronze"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"  

# Caminhos dos arquivos
input_path_contratacoes = f"{BRONZE_PREFIX}/contratacoes_ultimos_12_meses.csv"
output_path_contratacoes = f"{SILVER_PREFIX}/contratacoes"


print(f"Lendo dados brutos de: {input_path_contratacoes}")

# ==============================================================================
# 2. Leitura dos Dados da Camada Bronze
# ==============================================================================
try:
    df_bronze_contratacoes = spark.read.csv(
        input_path_contratacoes, 
        header=True, 
        inferSchema=True, 
        sep=';'
    )
    print("Schema da tabela de contratações 12m na camada Bronze:")
    df_bronze_contratacoes.printSchema()

except Exception as e:
    print(f"Erro fatal ao ler o arquivo CSV. Verifique o caminho e as permissões: {e}")
    spark.stop()

# ==============================================================================
# 3. Transformações para a Camada Silver
# ==============================================================================
# As transformações para a camada Silver incluem:
# - Padronização dos nomes das colunas
# - Conversão de tipos numéricos com tratamento de vírgulas
if df_bronze_contratacoes is not None:
    df_silver_contratacoes = df_bronze_contratacoes.select(
        F.col("CD_CLIENTE").alias("cd_cliente"),
        F.col("QTD_CONTRATACOES_12M").alias("qtd_contratacoes_12m").cast(IntegerType()),
        # Conversão de valores numéricos que podem estar como string com vírgula
        F.regexp_replace(F.col("VLR_CONTRATACOES_12M"), ',', '.').cast(DoubleType()).alias("vlr_contratacoes_12m")
    )

    print("\nSchema da tabela de contratações 12m na camada Silver:")
    df_silver_contratacoes.printSchema()

    # ==============================================================================
    # 4. Salvando os Dados na Camada Silver (Formato Delta Lake)
    # ==============================================================================
    print(f"\nSalvando dados limpos em: {output_path_contratacoes}")
    df_silver_contratacoes.write.mode("overwrite").parquet(output_path_contratacoes)

    print("Processamento concluído com sucesso!")
else:
    print("Processamento interrompido devido a erro na leitura da camada Bronze.")

spark.stop()