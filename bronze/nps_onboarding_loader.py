import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import SparkSession

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("NPSOnboardingBronzeToSilver").getOrCreate()

# ==============================================================================
# 1. Parâmetros e Caminhos
# ==============================================================================
BUCKET_NAME = "weedle"
BRONZE_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/bronze"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"  

input_path_nps = f"{BRONZE_PREFIX}/nps_transacional_onboarding.csv"
output_path_nps = f"{SILVER_PREFIX}/nps_transacional_onboarding"

print(f"Lendo dados brutos de: {input_path_nps}")

# ==============================================================================
# 2. Leitura dos Dados
# ==============================================================================
try:
    df_bronze_nps = spark.read.csv(
        input_path_nps, 
        header=True, 
        inferSchema=True, 
        sep=';'
    )
    print("Schema da tabela NPS de Onboarding na camada Bronze:")
    df_bronze_nps.printSchema()

except Exception as e:
    print(f"Erro ao ler o CSV: {e}")
    spark.stop()
    raise

# ==============================================================================
# 3. Transformações para a Camada Silver
# ==============================================================================
if df_bronze_nps is not None:
    df_silver_nps = df_bronze_nps.select(
        F.to_date(F.to_timestamp(F.col("`Data de resposta`"), "yyyy-MM-dd'T'HH:mm:ss")).alias("dt_resposta"),
        F.col("`Cod Cliente`").alias("cd_cliente"),
        F.col("`Nota NPS onboarding`").cast(IntegerType()).alias("nota_nps_onboarding"),
        F.col("`Nota atendimento CS`").alias("nota_atendimento_cs"),
        F.col("`Nota duracao reuniao`").alias("nota_duracao_reuniao"),
        F.col("`Nota clareza acesso`").alias("nota_clareza_acesso"),
        F.col("`Nota clareza informacoes`").alias("nota_clareza_informacoes"),
        F.col("`Nota expectativas`").alias("nota_expectativas")
    )

    # Preenche nulos com -1 para as notas
    df_silver_nps = df_silver_nps.na.fill(-1, subset=[
        "nota_atendimento_cs",
        "nota_duracao_reuniao",
        "nota_clareza_acesso",
        "nota_clareza_informacoes",
        "nota_expectativas"
    ])

    print("\nSchema da tabela NPS de Onboarding na camada Silver:")
    df_silver_nps.printSchema()

    # ==============================================================================
    # 4. Salvando em Delta Lake
    # ==============================================================================
    print(f"\nSalvando dados limpos em: {output_path_nps}")
    df_silver_nps.write.mode("overwrite").parquet(output_path_nps)

    print("Processamento concluído com sucesso!")
else:
    print("Processamento interrompido devido a erro na leitura.")

spark.stop()
