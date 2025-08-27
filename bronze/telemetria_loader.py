import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType

# ==============================================================================
# 0. Configuração de Variáveis
# ==============================================================================
BUCKET_NAME = "weedle"
TENANCY_NAMESPACE = "gr7jlaomnxx1" 

BRONZE_PREFIX = f"oci://{BUCKET_NAME}@{TENANCY_NAMESPACE}/bronze"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@{TENANCY_NAMESPACE}/silver"

# --- CORREÇÃO APLICADA AQUI ---
# Usamos um curinga (*) para ler de todas as pastas que começam com "telemetria_"
input_path_telemetria_dir = f"{BRONZE_PREFIX}/telemetria_*"
output_path_telemetria = f"{SILVER_PREFIX}/telemetria"

spark = SparkSession.builder.appName("BronzeToSilverTelemetria").getOrCreate()

print(f"Lendo todos os arquivos CSV dos diretórios em: {input_path_telemetria_dir}")
print(f"Salvando dados processados em: {output_path_telemetria}")

# ==============================================================================
# 1. Leitura dos Dados da Camada Bronze
# ==============================================================================
try:
    # Spark lê todos os arquivos .csv de todos os diretórios que correspondem ao padrão
    df_bronze_telemetria = spark.read.csv(
        input_path_telemetria_dir, 
        header=True, 
        inferSchema=True, 
        sep=','
    )
    
    print(f"Leitura concluída. Total de registros lidos de todos os arquivos: {df_bronze_telemetria.count()}")

except Exception as e:
    print(f"Erro ao ler os arquivos CSV: {e}")
    if 'spark' in locals():
        spark.stop()

# ==============================================================================
# 2. Transformações para a Camada Silver
# ==============================================================================
if 'df_bronze_telemetria' in locals():
    print("Iniciando transformações: padronizando colunas e tipos...")

    # Seleciona, renomeia e converte os tipos das colunas
    df_silver_telemetria = df_bronze_telemetria.select(
        F.col("clienteid").alias("cd_cliente"),
        F.col("eventduration").alias("duracao_evento"),
        F.col("moduloid").alias("id_modulo"),
        F.col("productlineid").alias("id_linha_produto"),
        F.to_date(F.col("referencedatestart")).alias("dt_referencia"),
        F.col("statuslicenca").alias("status_licenca")
    )
    
    print("\nSchema final da tabela na camada Silver:")
    df_silver_telemetria.printSchema()

    # ==============================================================================
    # 3. Salvando os Dados na Camada Silver (Formato Parquet)
    # ==============================================================================
    try:
        print(f"\nSalvando dados em formato Parquet em: {output_path_telemetria}")
        df_silver_telemetria.write.mode("overwrite").parquet(output_path_telemetria)
        print("Processamento concluído com sucesso!")

    except Exception as e:
        print(f"Erro ao salvar o arquivo Parquet: {e}")

if 'spark' in locals():
    spark.stop()