import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType
from pyspark.sql import SparkSession

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("HistoricoBronzeToSilver").getOrCreate()

# ==============================================================================
# 1. Parâmetros e Caminhos (VERIFIQUE E CORRIJA)
# ==============================================================================
# Defina o nome do seu bucket e o namespace da sua tenancy
BUCKET_NAME = "weedle"
BRONZE_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/bronze"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"  

# Caminhos dos arquivos
input_path_historico = f"{BRONZE_PREFIX}/historico.csv"
output_path_historico = f"{SILVER_PREFIX}/historico"

print(f"Lendo dados brutos de: {input_path_historico}")

# ==============================================================================
# 2. Leitura dos Dados da Camada Bronze
# ==============================================================================
try:
    df_bronze_historico = spark.read.csv(
        input_path_historico, 
        header=True, 
        inferSchema=True, 
        sep=';'
    )
    print("Schema da tabela histórico na camada Bronze:")
    df_bronze_historico.printSchema()

except Exception as e:
    print(f"Erro fatal ao ler o arquivo CSV. Verifique o caminho e as permissões: {e}")
    spark.stop()

# ==============================================================================
# 3. Transformações para a Camada Silver
# ==============================================================================
# As transformações para a camada Silver incluem:
# - Padronização dos nomes das colunas
# - Conversão de tipos de data e numéricos com tratamento de vírgulas
# - Manter as colunas de interesse para a Fato_Contratacoes
if df_bronze_historico is not None:
    df_silver_historico = df_bronze_historico.select(
        F.col("NR_PROPOSTA").alias("nr_proposta"),
        F.col("ITEM_PROPOSTA").alias("item_proposta").cast(IntegerType()),
        F.to_date(F.col("DT_UPLOAD"), "yyyy-MM-dd").alias("dt_upload"),
        F.col("HOSPEDAGEM").alias("hospedagem"),
        F.col("CD_CLI").alias("cd_cliente"),
        F.col("FAT_FAIXA").alias("fat_faixa"),
        F.col("CD_PROD").alias("cd_prod"),
        # Conversão de valores numéricos que podem estar como string com vírgula
        F.regexp_replace(F.col("QTD"), ',', '.').cast(DoubleType()).alias("qtd"),
        F.col("MESES_BONIF").alias("meses_bonif").cast(IntegerType()),
        F.regexp_replace(F.col("VL_TOTAL"), ',', '.').cast(DoubleType()).alias("vl_total"),
        F.regexp_replace(F.col("VL_FULL"), ',', '.').cast(DoubleType()).alias("vl_full"),
        F.regexp_replace(F.col("VL_DESCONTO"), ',', '.').cast(DoubleType()).alias("vl_desconto")
    )

    # Tratamento de valores nulos
    df_silver_historico = df_silver_historico.na.fill({
        "hospedagem": "Nao Informado"
    })

    print("\nSchema da tabela histórico na camada Silver:")
    df_silver_historico.printSchema()

    # ==============================================================================
    # 4. Salvando os Dados na Camada Silver (Formato Delta Lake)
    # ==============================================================================
    print(f"\nSalvando dados limpos em: {output_path_historico}")
    df_silver_historico.write.mode("overwrite").parquet(output_path_historico)

    print("Processamento concluído com sucesso!")
else:
    print("Processamento interrompido devido a erro na leitura da camada Bronze.")

spark.stop()