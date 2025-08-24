import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("SilverToGoldFatoTickets").getOrCreate()

# ==============================================================================
# 1. Parâmetros de Conexão JDBC
# ==============================================================================
# A conexão JDBC será usada apenas para a escrita final da Fato
jdbc_url = "jdbc:oracle:thin:@oracle.fiap.com.br:1521:orcl" 

properties = {
    "user": "rm555625",
    "password": "100203",
    "driver": "oracle.jdbc.OracleDriver"
}

# Caminhos dos dados limpos da camada Silver e Gold
BUCKET_NAME = "weedle"
TENANCY_NAMESPACE = "rm555625"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"
GOLD_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/gold"
silver_path_tickets = f"{SILVER_PREFIX}/tickets"

print("Lendo dados da camada Silver...")

# ==============================================================================
# 2. Leitura dos Dados da Camada Silver e Dimensões da Camada GOLD
# ==============================================================================
try:
    df_silver_tickets = spark.read.parquet(silver_path_tickets).cache()

    # Lendo as dimensões dos arquivos Parquet da CAMADA GOLD
    df_dim_cliente = spark.read.parquet(f"{GOLD_PREFIX}/dim_cliente").select("SK_CLIENTE", "CD_CLIENTE").cache()
    df_dim_tempo = spark.read.parquet(f"{GOLD_PREFIX}/dim_tempo").select("SK_TEMPO", "DATA").cache()
    df_dim_ticket = spark.read.parquet(f"{GOLD_PREFIX}/dim_ticket").select("SK_TICKET", "TIPO_TICKET", "STATUS_TICKET").cache()
    df_dim_prioridade = spark.read.parquet(f"{GOLD_PREFIX}/dim_prioridade").select("SK_PRIORIDADE", "TIPO_PRIORIDADE").cache()

    print("Dados das camadas Silver e Gold lidos com sucesso.")

except Exception as e:
    print(f"Erro ao ler arquivos ou tabelas. Verifique a existência dos dados: {e}")
    spark.stop()


# ==============================================================================
# 3. Processamento para a Tabela Fato
# ==============================================================================
# Juntar com as tabelas de dimensão para obter as SKs
df_fato = df_silver_tickets.join(
    F.broadcast(df_dim_cliente), on="cd_cliente", how="left"
).join(
    F.broadcast(df_dim_tempo), df_silver_tickets.dt_criacao == df_dim_tempo.DATA, how="left"
).join(
    F.broadcast(df_dim_ticket), 
    (df_silver_tickets.tipo_ticket == df_dim_ticket.TIPO_TICKET) & (df_silver_tickets.status_ticket == df_dim_ticket.STATUS_TICKET), 
    how="left"
).join(
    F.broadcast(df_dim_prioridade), df_silver_tickets.prioridade_ticket == df_dim_prioridade.TIPO_PRIORIDADE, how="left"
)

# Selecionar as colunas finais para a FATO_TICKETS
df_gold_fato_tickets = df_fato.select(
    F.col("sk_cliente").alias("SK_CLIENTE"),
    F.col("sk_ticket").alias("SK_TICKET"),
    F.col("sk_tempo").alias("SK_TEMPO"),
    F.col("sk_prioridade").alias("SK_PRIORIDADE"),
    F.col("bk_ticket").alias("BK_TICKET"),
    F.col("tempo_resolucao_dias").alias("TEMPO_RESOLUCAO_DIAS")
)

# Adicionar a Surrogate Key da tabela fato
df_gold_fato_tickets = df_gold_fato_tickets.withColumn(
    "SK_FATO_TICKETS",
    F.monotonically_increasing_id().cast(IntegerType())
)

print("\nDataFrame para a FATO_TICKETS criado com sucesso.")
df_gold_fato_tickets.printSchema()

# ==============================================================================
# 4. Carregamento dos Dados no Banco de Dados (Sua Camada Gold)
# ==============================================================================
if df_gold_fato_tickets is not None:
    try:
        df_gold_fato_tickets.write.jdbc(
            url=jdbc_url,
            table="FATO_TICKETS",
            mode="append",
            properties=properties
        )
        print("\nDados da FATO_TICKETS carregados com sucesso no banco de dados!")
    except Exception as e:
        print(f"Erro ao carregar dados no banco via JDBC: {e}")
        print("Verifique a URL, credenciais e regras de firewall.")
else:
    print("Processamento interrompido.")

spark.stop()