import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

BUCKET_NAME = "weedle"
TENANCY_NAMESPACE = "rm555625"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"
GOLD_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/gold"
silver_path_clientes = f"{SILVER_PREFIX}/clientes"

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

# Caminhos dos dados limpos da camada Silver


print(f"Lendo dados da camada Silver em: {silver_path_clientes}")

df_gold_dim_cliente = None

try:
    df_silver_clientes = spark.read.parquet(silver_path_clientes)
    
    # A diferença principal é que NÃO vamos remover duplicatas aqui.
    # Cada registro da camada Silver será uma linha na nossa dimensão final.
    window_spec = Window.orderBy("cd_cliente")
    df_gold_dim_cliente = df_silver_clientes.withColumn(
        "SK_CLIENTE",
        F.row_number().over(window_spec).cast(IntegerType())
    ).select(
        "SK_CLIENTE",
        "CD_CLIENTE",
        "DS_SEGMENTO",
        "DS_SUBSEGMENTO",
        "DS_PROD",
        "FAT_FAIXA",
        "UF",
        "CIDADE",
        "MARCA_TOTVS",
        "SITUACAO_CONTRATO",
        "DT_ULTIMO_CONTRATO",
        "DT_INICIO_CONTRATO"
    )

    print("\nDataFrame para a DIM_CLIENTE criado com sucesso.")
    df_gold_dim_cliente.printSchema()

except Exception as e:
    print(f"Erro no processamento. Verifique a camada Silver: {e}")
    spark.stop()


# ==============================================================================
# 3. Carregamento dos Dados no Banco de Dados (Sua Camada Gold)
# ==============================================================================
if df_gold_dim_cliente is not None:
    try:
        df_gold_dim_cliente.write.jdbc(
            url=jdbc_url,
            table="DIM_CLIENTE",
            mode="append",
            properties=properties
        )
        print("\nDados da DIM_CLIENTE carregados com sucesso no banco de dados!")

    except Exception as e:
        print(f"Erro ao carregar dados no banco via JDBC: {e}")
        print("Verifique a URL, credenciais e regras de firewall.")

    try:
        gold_path_dim_cliente = f"{GOLD_PREFIX}/dim_cliente"
        df_gold_dim_cliente.write.parquet(
            gold_path_dim_cliente,
            mode="append"
        )
        print(f"\nDados da DIM_CLIENTE salvos no Object Storage em: {gold_path_dim_cliente}")
    except Exception as e:
        print(f"Erro ao salvar dados no Object Storage: {e}")
else:
    print("Processamento interrompido.")

spark.stop()