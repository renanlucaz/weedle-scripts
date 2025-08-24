import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# ==============================================================================
# 0. Configuração de Variáveis
# ==============================================================================
BUCKET_NAME = "weedle"
# Substitua pelo seu tenancy namespace
TENANCY_NAMESPACE = "gr7jlaomnxx1" 

# Define os caminhos de leitura (Silver) e escrita (Gold)
SILVER_PREFIX = f"oci://{BUCKET_NAME}@{TENANCY_NAMESPACE}/silver"
GOLD_PREFIX = f"oci://{BUCKET_NAME}@{TENANCY_NAMESPACE}/gold"

# Caminhos específicos para os dados de entrada
silver_path_clientes = f"{SILVER_PREFIX}/clientes"
silver_path_historico = f"{SILVER_PREFIX}/historico"

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("SilverToGoldDimProduto").getOrCreate()

# ==============================================================================
# 1. Parâmetros de Conexão JDBC
# ==============================================================================
# Configurações do seu banco de dados Oracle
jdbc_url = "jdbc:oracle:thin:@oracle.fiap.com.br:1521:orcl"
properties = {
    "user": "RM555625",  # Substitua pelo seu usuário
    "password": "100203", # Substitua pela sua senha
    "driver": "oracle.jdbc.OracleDriver"
}

print(f"Lendo dados da camada Silver em: {silver_path_clientes} e {silver_path_historico}")

df_gold_dim_produto = None


# ==============================================================================
# 2. Leitura e Processamento para a Camada Gold (DataFrame)
# ==============================================================================
try:
    # Leitura dos dados da camada Silver
    df_silver_clientes = spark.read.parquet(silver_path_clientes)

    # Extrai as colunas de produto e remove duplicatas para criar o catálogo
    df_produtos_final = df_silver_clientes.select(
        F.col("DS_PROD").alias("DS_PRODUTO"),
        F.col("DS_LIN_REC").alias("DS_LINHA_RECEITA")
    ).dropDuplicates()

    # Trata os nulos na linha de receita
    df_produtos_final = df_produtos_final.withColumn(
        "DS_LINHA_RECEITA",
        F.when(F.col("DS_LINHA_RECEITA").isNull(), "NÃO INFORMADA")
         .otherwise(F.col("DS_LINHA_RECEITA"))
    )

    # Define a janela para gerar a Surrogate Key
    window_spec = Window.orderBy("DS_PRODUTO")

    # Gera a Surrogate Key (SK) e seleciona as colunas finais
    df_gold_dim_produto = df_produtos_final.withColumn(
        "SK_PRODUTO",
        F.row_number().over(window_spec).cast(IntegerType())
    ).select(
        "SK_PRODUTO",
        "DS_PRODUTO",
        "DS_LINHA_RECEITA"
    )

    print("\nDataFrame para a DIM_PRODUTO criado com sucesso.")
    df_gold_dim_produto.printSchema()
    df_gold_dim_produto.show(5, truncate=False)

except Exception as e:
    print(f"Erro no processamento. Verifique a camada Silver: {e}")
    spark.stop()



# ==============================================================================
# 3. Carregamento dos Dados no Banco de Dados e Object Storage (Camada Gold)
# ==============================================================================
if df_gold_dim_produto is not None:
    # Carregamento no Banco de Dados
    try:
        df_gold_dim_produto.write.jdbc(
            url=jdbc_url,
            table="DIM_PRODUTO",
            mode="append", # Use "overwrite" se quiser recriar a tabela
            properties=properties
        )
        print("\nDados da DIM_PRODUTO carregados com sucesso no banco de dados!")

    except Exception as e:
        print(f"Erro ao carregar dados no banco via JDBC: {e}")

    # Salvando no Object Storage
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
    print("Processamento interrompido. DataFrame de produto não foi criado.")

# Finaliza a sessão Spark
spark.stop()