import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# ==============================================================================
# 0. Configuração de Variáveis
# ==============================================================================
BUCKET_NAME = "weedle"
TENANCY_NAMESPACE = "gr7jlaomnxx1" 

SILVER_PREFIX = f"oci://{BUCKET_NAME}@{TENANCY_NAMESPACE}/silver"
GOLD_PREFIX = f"oci://{BUCKET_NAME}@{TENANCY_NAMESPACE}/gold"

# Caminhos de entrada para inferir a relação
silver_path_historico = f"{SILVER_PREFIX}/historico"
silver_path_clientes = f"{SILVER_PREFIX}/clientes"

spark = SparkSession.builder.appName("SilverToGoldDimProduto_Inferido").getOrCreate()

# ==============================================================================
# 1. Parâmetros de Conexão JDBC
# ==============================================================================
jdbc_url = "jdbc:oracle:thin:@oracle.fiap.com.br:1521:orcl"
properties = { "user": "rm555625", "password": "100203", "driver": "oracle.jdbc.OracleDriver" }

print("Inferindo a relação entre código e descrição do produto...")

# ==============================================================================
# 2. Leitura e Processamento para a Camada Gold (DataFrame)
# ==============================================================================
try:
    df_silver_historico = spark.read.parquet(silver_path_historico)
    df_silver_clientes = spark.read.parquet(silver_path_clientes)

    # Passo 1: Criar uma lista de (cliente, código_produto) do histórico
    df_rel_cliente_codigo = df_silver_historico.select("cd_cliente", "cd_prod").distinct()

    # Passo 2: Criar uma lista de (cliente, descricao_produto) da tabela clientes
    df_rel_cliente_descricao = df_silver_clientes.select(
        F.col("cd_cliente"),
        F.col("ds_prod").alias("DS_PRODUTO"),
        F.col("ds_lin_rec").alias("DS_LINHA_RECEITA")
    ).distinct()

    # Passo 3: Juntar as duas listas pelo cliente para inferir a relação (código -> descrição)
    df_mapa_inferido = df_rel_cliente_codigo.join(
        df_rel_cliente_descricao,
        "cd_cliente",
        "inner"
    )

    # Passo 4: Agora que temos o mapa, selecionamos o código e a descrição e removemos duplicatas
    # para criar nossa dimensão.
    df_produtos_final = df_mapa_inferido.select(
        F.col("cd_prod").alias("CD_PRODUTO"),
        F.col("DS_PRODUTO"),
        F.col("DS_LINHA_RECEITA")
    ).dropDuplicates(["CD_PRODUTO"])
    
    window_spec = Window.orderBy("CD_PRODUTO")

    df_gold_dim_produto = df_produtos_final.withColumn(
        "SK_PRODUTO",
        F.row_number().over(window_spec).cast(IntegerType())
    ).select("SK_PRODUTO", "CD_PRODUTO", "DS_PRODUTO", "DS_LINHA_RECEITA")

    print("\nDataFrame para a DIM_PRODUTO (inferido) criado com sucesso.")
    df_gold_dim_produto.printSchema()
    print(f"Total de produtos na dimensão: {df_gold_dim_produto.count()}")


except Exception as e:
    print(f"Erro no processamento da DIM_PRODUTO: {e}")
    if 'spark' in locals():
        spark.stop()

# ==============================================================================
# 3. Carregamento dos Dados na Camada Gold
# ==============================================================================
if 'df_gold_dim_produto' in locals() and df_gold_dim_produto is not None:
    try:
        df_gold_dim_produto.write.jdbc(url=jdbc_url, table="DIM_PRODUTO", mode="append", properties=properties)
        print("\nDados da DIM_PRODUTO carregados com sucesso no banco de dados!")
    except Exception as e:
        print(f"Erro ao carregar dados da DIM_PRODUTO no banco via JDBC: {e}")

        # Salvando no Object Storage (Camada Gold)
    try:
        gold_path_dim_produto = f"{GOLD_PREFIX}/dim_produto"
        df_gold_dim_produto.write.parquet(
            gold_path_dim_produto,
            mode="overwrite"
        )
        print(f"\nDados da DIM_PRODUTO salvos no Object Storage em: {gold_path_dim_produto}")
    except Exception as e:
        print(f"Erro ao salvar dados no Object Storage: {e}")
    # --- FIM DO BLOCO ADICIONADO ---
else:
    print("Processamento da DIM_PRODUTO interrompido.")

spark.stop()