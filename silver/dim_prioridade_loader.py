import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

BUCKET_NAME = "weedle"
TENANCY_NAMESPACE = "rm555625"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"
GOLD_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/gold"
silver_path_tickets = f"{SILVER_PREFIX}/tickets"

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("SilverToGoldDimCliente").getOrCreate()

# ==============================================================================
# 1. Parâmetros e Conexão JDBC
# ==============================================================================
# Configurações do banco de dados
jdbc_url = "jdbc:oracle:thin:@oracle.fiap.com.br:1521:orcl" # Exemplo Oracle
# jdbc_url = "jdbc:sqlserver://<server_name>.database.windows.net:1433;databaseName=<db_name>" # Exemplo Azure SQL

properties = {
    "user": "rm555625",
    "password": "100203",
    "driver": "oracle.jdbc.OracleDriver" # Para Azure SQL, use "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

print(f"Lendo dados da camada Silver em: {silver_path_tickets}")

df_gold_dim_prioridade = None

# ==============================================================================
# 2. Leitura e Processamento para a Camada Gold (DataFrame)
# ==============================================================================
try:
    df_silver_tickets = spark.read.parquet(silver_path_tickets)
    
    # Extrai os valores únicos da prioridade e cria a Surrogate Key
    window_spec = Window.orderBy("TIPO_PRIORIDADE")
    df_gold_dim_prioridade = df_silver_tickets.select(
        F.col("prioridade_ticket").alias("TIPO_PRIORIDADE")
    ).dropDuplicates().withColumn(
        "SK_PRIORIDADE",
        F.row_number().over(window_spec).cast(IntegerType())
    ).select(
        "SK_PRIORIDADE",
        "TIPO_PRIORIDADE"
    )

    print("\nDataFrame para a DIM_PRIORIDADE criado com sucesso.")
    df_gold_dim_prioridade.printSchema()

except Exception as e:
    print(f"Erro no processamento. Verifique a camada Silver: {e}")
    spark.stop()


# ==============================================================================
# 3. Carregamento dos Dados no Banco de Dados (Sua Camada Gold)
# ==============================================================================
if df_gold_dim_prioridade is not None:
    try:
        df_gold_dim_prioridade.write.jdbc(
            url=jdbc_url,
            table="DIM_PRIORIDADE",
            mode="append",
            properties=properties
        )
        print("\nDados da DIM_PRIORIDADE carregados com sucesso no banco de dados!")

    except Exception as e:
        print(f"Erro ao carregar dados no banco via JDBC: {e}")
        print("Verifique a URL, credenciais e regras de firewall.")

    try:
        gold_path_dim_prioridade = f"{GOLD_PREFIX}/dim_prioridade"
        df_gold_dim_prioridade.write.parquet(
            gold_path_dim_prioridade,
            mode="overwrite"
        )
        print(f"\nDados da DIM_PRIORIDADE salvos no Object Storage em: {gold_path_dim_prioridade}")
    except Exception as e:
        print(f"Erro ao salvar dados no Object Storage: {e}")
else:
    print("Processamento interrompido.")

spark.stop()