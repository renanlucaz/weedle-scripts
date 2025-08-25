import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import SparkSession

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("NPSAquisicaoBronzeToSilver").getOrCreate()

# ==============================================================================
# 1. Parâmetros e Caminhos (VERIFIQUE E CORRIJA)
# ==============================================================================
# Defina o nome do seu bucket e o namespace da sua tenancy
BUCKET_NAME = "weedle"
BRONZE_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/bronze"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"  

# Caminhos dos arquivos
input_path_nps = f"{BRONZE_PREFIX}/nps_transacional_aquisicao.csv"
output_path_nps = f"{SILVER_PREFIX}/nps_transacional_aquisicao"

print(f"Lendo dados brutos de: {input_path_nps}")

# ==============================================================================
# 2. Leitura dos Dados da Camada Bronze
# ==============================================================================
try:
    df_bronze_nps = spark.read.csv(
        input_path_nps, 
        header=True, 
        inferSchema=True, 
        sep=';'
    )
    print("Schema da tabela NPS de Aquisição na camada Bronze:")
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
# - Tratamento de valores nulos
if df_bronze_nps is not None:
    df_silver_nps = df_bronze_nps.select(
        F.col("Cod Cliente").alias("cd_cliente"),
        F.to_date(F.col("Data da Resposta"), "yyyy-MM-dd").alias("dt_resposta"),
        F.col("Nota NPS").alias("nota_nps").cast(IntegerType()),
        F.col("Nota Agilidade").alias("nota_agilidade"),
        F.col("Nota Conhecimento").alias("nota_conhecimento"),
        F.col("Nota Custo").alias("nota_custo"),
        F.col("Nota Facilidade").alias("nota_facilidade"),
        F.col("Nota Flexibilidade").alias("nota_flexibilidade")
    )

    df_silver_nps = df_silver_nps.withColumn("tipo_nps", F.lit("aquisicao"))
    
    # Tratamento de valores nulos, preenchendo as notas com -1 para indicar ausência de resposta
    df_silver_nps = df_silver_nps.na.fill(-1, subset=[
        "nota_agilidade",
        "nota_conhecimento",
        "nota_custo",
        "nota_facilidade",
        "nota_flexibilidade",
    ])

    print("\nSchema da tabela NPS de Aquisição na camada Silver:")
    df_silver_nps.printSchema()

    # ==============================================================================
    # 4. Salvando os Dados na Camada Silver (Formato Delta Lake)
    # ==============================================================================
    print(f"\nSalvando dados limpos em: {output_path_nps}")
    df_silver_nps.write.mode("overwrite").parquet(output_path_nps)

    print("Processamento concluído com sucesso!")
else:
    print("Processamento interrompido devido a erro na leitura da camada Bronze.")

spark.stop()