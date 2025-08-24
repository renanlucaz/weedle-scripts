import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

BUCKET_NAME = "weedle"
TENANCY_NAMESPACE = "rm555625"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"
GOLD_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/gold"
silver_path_historico = f"{SILVER_PREFIX}/historico"
silver_path_nps_produto = f"{SILVER_PREFIX}/nps_transacional_produto"
silver_path_clientes = f"{SILVER_PREFIX}/clientes"

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("SilverToGoldDimProduto").getOrCreate()

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


print("Lendo dados da camada Silver...")
df_gold_dim_produto = None

# ==============================================================================
# 2. Leitura e Processamento para a Camada Gold (DataFrame)
# ==============================================================================
try:
    df_silver_historico = spark.read.parquet(silver_path_historico)
    df_silver_nps_produto = spark.read.parquet(silver_path_nps_produto)
    df_silver_clientes = spark.read.parquet(silver_path_clientes)
    
    # 2.1. Extrai informações de produto de cada fonte
    df_produtos_from_historico = df_silver_historico.select(
        F.col("cd_prod").alias("CD_PRODUTO"),
        F.lit(None).cast("string").alias("NM_PRODUTO"),
        F.lit(None).cast("string").alias("LINHA_PRODUTO")
    ).dropDuplicates()

    df_produtos_from_nps = df_silver_nps_produto.select(
        F.lit(None).cast("string").alias("CD_PRODUTO"),
        F.col("nm_produto").alias("NM_PRODUTO"),
        F.col("linha_produto").alias("LINHA_PRODUTO")
    ).dropDuplicates()
    
    # Usando a tabela de clientes para obter descrições de produtos
    df_produtos_from_clientes = df_silver_clientes.select(
        F.lit(None).cast("string").alias("CD_PRODUTO"),
        F.col("nm_produto").alias("NM_PRODUTO"),
        F.col("ds_lin_rec").alias("LINHA_PRODUTO")
    ).dropDuplicates()

    # 2.2. Consolida todas as informações de produto em um único DataFrame
    df_produtos_consolidado = df_produtos_from_historico.unionByName(
        df_produtos_from_nps
    ).unionByName(
        df_produtos_from_clientes
    )

    # 2.3. Agrupa e consolida as informações por produto para reduzir nulos
    # A agregação usa F.first(..., ignoreNulls=True) para preencher valores ausentes
    df_produtos_agregado = df_produtos_consolidado.groupBy(
        "CD_PRODUTO", "NM_PRODUTO", "LINHA_PRODUTO"
    ).agg(
        F.max("CD_PRODUTO").alias("CD_PRODUTO"),
        F.max("NM_PRODUTO").alias("NM_PRODUTO"),
        F.max("LINHA_PRODUTO").alias("LINHA_PRODUTO")
    )
    
    # 2.4. Criação da tabela final DIM_PRODUTO com Surrogate Key
    df_produtos_deduplicado = df_produtos_agregado.filter(
        F.col("CD_PRODUTO").isNotNull() | F.col("NM_PRODUTO").isNotNull()
    ).dropDuplicates(["CD_PRODUTO", "NM_PRODUTO"])

    df_gold_dim_produto = df_produtos_deduplicado.withColumn(
        "SK_PRODUTO",
        F.monotonically_increasing_id().cast(IntegerType())
    ).select(
        "SK_PRODUTO",
        "CD_PRODUTO",
        "NM_PRODUTO",
        "LINHA_PRODUTO"
    )
    
    print("\nDataFrame para a DIM_PRODUTO criado com sucesso.")
    df_gold_dim_produto.printSchema()

except Exception as e:
    print(f"Erro no processamento. Verifique a camada Silver: {e}")
    spark.stop()

# ==============================================================================
# 3. Carregamento dos Dados no Banco de Dados (Sua Camada Gold)
# ==============================================================================
if df_gold_dim_produto is not None:
    try:
        df_gold_dim_produto.write.jdbc(
            url=jdbc_url,
            table="DIM_PRODUTO",
            mode="append",
            properties=properties
        )
        print("\nDados da DIM_PRODUTO carregados com sucesso no banco de dados!")

    except Exception as e:
        print(f"Erro ao carregar dados no banco via JDBC: {e}")
        print("Verifique a URL, credenciais e regras de firewall.")

    try:
        gold_path_dim_produto = f"{GOLD_PREFIX}/dim_produto"
        df_gold_dim_produto.write.parquet(
            gold_path_dim_produto,
            mode="overwrite"
        )
        print(f"\nDados da DIM_PRODUTO salvos no Object Storage em: {gold_path_dim_produto}")
    except Exception as e:
        print(f"Erro ao salvar dados no Object Storage: {e}")
else:
    print("Processamento interrompido.")

spark.stop()