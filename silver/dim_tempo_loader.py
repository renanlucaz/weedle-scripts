import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql import SparkSession

BUCKET_NAME = "weedle"
TENANCY_NAMESPACE = "rm555625"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"
GOLD_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/gold"
silver_path_clientes = f"{SILVER_PREFIX}/clientes"
silver_path_tickets = f"{SILVER_PREFIX}/tickets"
silver_path_historico = f"{SILVER_PREFIX}/historico"

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("SilverToGoldDimTempo").getOrCreate()

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


print("Lendo dados da camada Silver para extrair datas...")

df_gold_dim_tempo = None

# ==============================================================================
# 2. Leitura e Processamento para a Camada Gold (DataFrame)
# ==============================================================================
try:
    df_silver_clientes = spark.read.parquet(silver_path_clientes)
    df_silver_tickets = spark.read.parquet(silver_path_tickets)
    df_silver_historico = spark.read.parquet(silver_path_historico)

    # Extrai todas as datas únicas das tabelas Silver
    df_datas_clientes = df_silver_clientes.select(F.col("dt_ultimo_contrato").alias("DATA"))
    df_datas_tickets = df_silver_tickets.select(F.col("dt_criacao").alias("DATA"))
    df_datas_historico = df_silver_historico.select(F.col("dt_upload").alias("DATA"))

    # Unir todas as datas e remover duplicatas para criar a base da dimensão
    df_datas_unificadas = df_datas_clientes.union(df_datas_tickets).union(df_datas_historico).distinct()

    # Gerar os atributos de tempo
    df_gold_dim_tempo = df_datas_unificadas.filter(F.col("DATA").isNotNull()).withColumn(
        "SK_TEMPO",
        F.date_format(F.col("DATA"), "yyyyMMdd").cast(IntegerType())
    ).withColumn(
        "ANO",
        F.year(F.col("DATA"))
    ).withColumn(
        "MES",
        F.month(F.col("DATA"))
    ).withColumn(
        "NOME_MES",
        F.date_format(F.col("DATA"), "MMMM")
    ).withColumn(
        "TRIMESTRE",
        F.concat(F.lit("Q"), F.quarter(F.col("DATA")).cast("string"))
    ).select(
        "SK_TEMPO",
        "DATA",
        "ANO",
        "MES",
        "NOME_MES",
        "TRIMESTRE"
    ).orderBy("SK_TEMPO")

    print("\nDataFrame para a DIM_TEMPO criado com sucesso.")
    df_gold_dim_tempo.printSchema()

except Exception as e:
    print(f"Erro no processamento. Verifique a camada Silver: {e}")
    spark.stop()


# ==============================================================================
# 3. Carregamento dos Dados no Banco de Dados (Sua Camada Gold)
# ==============================================================================
if df_gold_dim_tempo is not None:
    try:
        df_gold_dim_tempo.write.jdbc(
            url=jdbc_url,
            table="DIM_TEMPO",
            mode="append",
            properties=properties
        )
        print("\nDados da DIM_TEMPO carregados com sucesso no banco de dados!")

    except Exception as e:
        print(f"Erro ao carregar dados no banco via JDBC: {e}")
        print("Verifique a URL, credenciais e regras de firewall.")

    try:
        gold_path_dim_tempo = f"{GOLD_PREFIX}/dim_tempo"
        df_gold_dim_tempo.write.parquet(
            gold_path_dim_tempo,
            mode="overwrite"
        )
        print(f"\nDados da DIM_TEMPO salvos no Object Storage em: {gold_path_dim_tempo}")
    except Exception as e:
        print(f"Erro ao salvar dados no Object Storage: {e}")
else:
    print("Processamento interrompido.")

spark.stop()