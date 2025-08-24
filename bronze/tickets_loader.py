import pyspark.sql.functions as F
from pyspark.sql.types import DateType, IntegerType
from pyspark.sql import SparkSession

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("TicketsBronzeToSilver").getOrCreate()

# ==============================================================================
# 1. Parâmetros e Caminhos (VERIFIQUE E CORRIJA)
# ==============================================================================
# Defina o nome do seu bucket e o namespace da sua tenancy
BUCKET_NAME = "weedle"
BRONZE_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/bronze"
SILVER_PREFIX = f"oci://{BUCKET_NAME}@gr7jlaomnxx1/silver"  

# Caminhos dos arquivos
input_path_tickets = f"{BRONZE_PREFIX}/tickets.csv"
output_path_tickets = f"{SILVER_PREFIX}/tickets"

print(f"Lendo dados brutos de: {input_path_tickets}")

# ==============================================================================
# 2. Leitura dos Dados da Camada Bronze
# ==============================================================================
try:
    df_bronze_tickets = spark.read.csv(
        input_path_tickets, 
        header=True, 
        inferSchema=True, 
        sep=';'
    )
    print("Schema da tabela de tickets na camada Bronze:")
    df_bronze_tickets.printSchema()

except Exception as e:
    print(f"Erro fatal ao ler o arquivo CSV. Verifique o caminho e as permissões: {e}")
    spark.stop()

# ==============================================================================
# 3. Transformações para a Camada Silver
# ==============================================================================
# As transformações para a camada Silver incluem:
# - Padronização dos nomes das colunas
# - Conversão de tipos para colunas de data
# - Cálculo da métrica de tempo de resolução
if df_bronze_tickets is not None:
    df_silver_tickets = df_bronze_tickets.select(
        F.col("CODIGO_ORGANIZACAO").alias("cd_cliente"),
        F.col("NOME_GRUPO").alias("nome_grupo"),
        F.col("TIPO_TICKET").alias("tipo_ticket"),
        F.col("STATUS_TICKET").alias("status_ticket"),
        F.col("PRIORIDADE_TICKET").alias("prioridade_ticket"),
        F.col("BK_TICKET").alias("bk_ticket"),
        F.to_date(F.col("DT_CRIACAO"), "yyyy-MM-dd").alias("dt_criacao"),
        F.to_date(F.col("DT_ATUALIZACAO"), "yyyy-MM-dd").alias("dt_atualizacao"),
        # Métrica: calcula a diferença em dias entre a criação e a atualização
        F.datediff(F.to_date(F.col("DT_ATUALIZACAO"), "yyyy-MM-dd"), F.to_date(F.col("DT_CRIACAO"), "yyyy-MM-dd")).alias("tempo_resolucao_dias")
    )
    
    # Tratamento de valores nulos
    df_silver_tickets = df_silver_tickets.na.fill({
        "nome_grupo": "Nao Informado"
    })
    
    print("\nSchema da tabela de tickets na camada Silver:")
    df_silver_tickets.printSchema()

    # ==============================================================================
    # 4. Salvando os Dados na Camada Silver (Formato Delta Lake)
    # ==============================================================================
    print(f"\nSalvando dados limpos em: {output_path_tickets}")
    df_silver_tickets.write.mode("overwrite").parquet(output_path_tickets)

    print("Processamento concluído com sucesso!")
else:
    print("Processamento interrompido devido a erro na leitura da camada Bronze.")

spark.stop()