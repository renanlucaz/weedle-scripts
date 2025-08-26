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

# Caminhos de entrada
silver_path_tickets = f"{SILVER_PREFIX}/tickets"
gold_path_dim_cliente = f"{GOLD_PREFIX}/dim_cliente"
gold_path_dim_produto = f"{GOLD_PREFIX}/dim_produto"
gold_path_dim_tempo = f"{GOLD_PREFIX}/dim_tempo"
gold_path_dim_prioridade = f"{GOLD_PREFIX}/dim_prioridade"
gold_path_dim_ticket = f"{GOLD_PREFIX}/dim_ticket"

spark = SparkSession.builder.appName("SilverToGoldFatoTickets_Corrigido").getOrCreate()

# ==============================================================================
# 1. Parâmetros de Conexão JDBC
# ==============================================================================
jdbc_url = "jdbc:oracle:thin:@oracle.fiap.com.br:1521:orcl"
properties = { "user": "RM555625", "password": "100203", "driver": "oracle.jdbc.OracleDriver" }

print("Lendo dados de origem e dimensões...")
df_gold_ft_tickets = None

# ==============================================================================
# 2. Leitura e Processamento para a Fato (DataFrame) - VERSÃO CORRIGIDA
# ==============================================================================
try:
    # Leitura da tabela transacional e das dimensões
    df_silver_tickets = spark.read.parquet(silver_path_tickets)
    df_dim_cliente = spark.read.parquet(gold_path_dim_cliente)
    df_dim_produto = spark.read.parquet(gold_path_dim_produto)
    df_dim_tempo = spark.read.parquet(gold_path_dim_tempo)
    df_dim_prioridade = spark.read.parquet(gold_path_dim_prioridade)
    df_dim_ticket = spark.read.parquet(gold_path_dim_ticket)

    df_dim_cliente = df_dim_cliente.dropDuplicates(["CD_CLIENTE"])

    df_tickets_com_metricas = df_silver_tickets.withColumn(
        "TEMPO_RESOLUCAO_DIAS",
        F.datediff(F.to_date(F.col("dt_atualizacao")), F.to_date(F.col("dt_criacao")))
    ).withColumn("QTD_TICKETS", F.lit(1))

    df_base = df_tickets_com_metricas.join(F.broadcast(df_dim_cliente), df_tickets_com_metricas.cd_cliente == df_dim_cliente.CD_CLIENTE, "inner")
    df_base = df_base.join(F.broadcast(df_dim_prioridade), df_base.prioridade_ticket == df_dim_prioridade.TIPO_PRIORIDADE, "inner")
    df_base = df_base.join(F.broadcast(df_dim_ticket), df_base.tipo_ticket == df_dim_ticket.tipo_ticket, "inner")
    df_base = df_base.join(F.broadcast(df_dim_produto), df_base.nome_grupo == df_dim_produto.DS_PRODUTO, "left")

    df_com_criacao = df_base.join(
        F.broadcast(df_dim_tempo), # Usamos a dimensão original
        F.to_date(df_base.dt_criacao) == df_dim_tempo.DATA,
        "inner"
    ).select(
        df_base["*"], # Seleciona TODAS as colunas originais de df_base
        F.col("SK_TEMPO").alias("SK_TEMPO_CRIACAO")
    )

    df_final = df_com_criacao.join(
        F.broadcast(df_dim_tempo), # Usamos a dimensão original de novo
        F.to_date(df_com_criacao.dt_atualizacao) == df_dim_tempo.DATA,
        "inner"
    ).select(
        df_com_criacao["*"], # Seleciona TODAS as colunas do passo anterior (incluindo SK_TEMPO_CRIACAO)
        F.col("SK_TEMPO").alias("SK_TEMPO_ATUALIZACAO")
    )

    window_spec = Window.orderBy(F.monotonically_increasing_id())

    df_gold_ft_tickets = df_final.select(
        "SK_CLIENTE", "SK_TEMPO_CRIACAO",
        "SK_PRIORIDADE", "SK_TICKET",
        "TEMPO_RESOLUCAO_DIAS"
    )

    df_gold_ft_tickets = df_gold_ft_tickets.withColumn(
        "SK_FATO_TICKET",
        F.row_number().over(window_spec).cast(IntegerType())
    ).select(
        "SK_FATO_TICKET", "SK_CLIENTE", F.col("SK_TEMPO_CRIACAO").alias("SK_TEMPO"),
        "SK_PRIORIDADE", "SK_TICKET", "TEMPO_RESOLUCAO_DIAS"
    )
    print(f"Tamanho final do DataFrame: {df_gold_ft_tickets.count()} linhas.")
    df_gold_ft_tickets.printSchema()

except Exception as e:
    print(f"Erro no processamento: {e}")
    if 'spark' in locals():
        spark.stop()

# ==============================================================================
# 3. Carregamento dos Dados na Camada Gold
# ==============================================================================
if df_gold_ft_tickets is not None:
    try:
        df_gold_ft_tickets.write.jdbc(url=jdbc_url, table="FATO_TICKETS", mode="overwrite", properties=properties)
        print("\nDados da FATO_TICKETS carregados com sucesso no banco de dados!")
    except Exception as e:
        print(f"Erro ao carregar dados no banco via JDBC: {e}")
else:
    print("Processamento interrompido.")

spark.stop()