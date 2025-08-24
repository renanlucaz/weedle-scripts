import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql import SparkSession

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("NPSProdutoBronzeToSilver").getOrCreate()

# ==============================================================================
# 1. Parâmetros e Caminhos (VERIFIQUE E CORRIJA)
# ==============================================================================
# Defina o nome do seu bucket e o namespace da sua tenancy
BUCKET_NAME = "weedle"
BRONZE_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/bronze"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"  

# Caminhos dos arquivos
input_path_nps_produto = f"{BRONZE_PREFIX}/nps_transacional_produto.csv"
output_path_nps_produto = f"{SILVER_PREFIX}/nps_transacional_produto"


print(f"Lendo dados brutos de: {input_path_nps_produto}")

# ==============================================================================
# 2. Leitura dos Dados da Camada Bronze
# ==============================================================================
try:
    df_bronze_nps = spark.read.csv(
        input_path_nps_produto, 
        header=True, 
        inferSchema=True, 
        sep=';'
    )
    print("Schema da tabela NPS de Produto na camada Bronze:")
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
        F.to_date(F.col("Data da Resposta"), "M/d/yyyy H:mm:ss a").alias("dt_resposta"),
        F.col("Linha de Produto").alias("linha_produto"),
        F.col("Nome do Produto").alias("nome_produto"),
        F.col("Nota").alias("nota_nps").cast(IntegerType()),
        F.col("Cod Cliente").alias("cd_cliente")
    )
    
    # Tratamento de valores nulos, preenchendo com uma string 'Nao Informado'
    df_silver_nps = df_silver_nps.na.fill({
        "nome_produto": "Nao Informado"
    })
    
    print("\nSchema da tabela NPS de Produto na camada Silver:")
    df_silver_nps.printSchema()

    # ==============================================================================
    # 4. Salvando os Dados na Camada Silver (Formato Delta Lake)
    # ==============================================================================
    print(f"\nSalvando dados limpos em: {output_path_nps_produto}")
    df_silver_nps.write.mode("overwrite").parquet(output_path_nps_produto)

    print("Processamento concluído com sucesso!")
else:
    print("Processamento interrompido devido a erro na leitura da camada Bronze.")

spark.stop()