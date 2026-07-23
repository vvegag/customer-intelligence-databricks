# Databricks notebook source
# DBTITLE 1,Gold Layer - Features para ML
# MAGIC %md
# MAGIC # Gold Layer - Feature Engineering
# MAGIC
# MAGIC Esta camada contém:
# MAGIC - **Features agregadas** por cliente
# MAGIC - **Métricas de comportamento** (RFM, engajamento, etc)
# MAGIC - **Features temporais** (tendências, sazonalidade)
# MAGIC - **Features de campanha** (histórico de respostas)
# MAGIC - **Targets** (churn, propensão)
# MAGIC
# MAGIC ## Feature Sets:
# MAGIC 1. **customer_features**: Features completas por cliente
# MAGIC 2. **rfm_features**: Recency, Frequency, Monetary
# MAGIC 3. **behavioral_features**: Engajamento e atividade
# MAGIC 4. **campaign_history_features**: Histórico de campanhas
# MAGIC 5. **churn_labels**: Target para modelo de churn

# COMMAND ----------

# DBTITLE 1,Configuração
# Configurações globais do projeto (inline - sem usar %run)
from datetime import timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Configurações de catálogo e schema
CATALOG = "customer_intelligence"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# Configurações MLflow (usuário detectado dinamicamente, funciona em qualquer conta)
CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]
MLFLOW_EXPERIMENT_PATH = f"/Users/{CURRENT_USER}/customer_intelligence_experiments"

# Configurações gerais
DATA_PATH = "/FileStore/customer_intelligence/data"
MODEL_REGISTRY_NAME_PREFIX = "customer_intelligence"

# Helper functions
def get_full_table_name(schema, table):
    """Retorna nome completo da tabela"""
    return f"{CATALOG}.{schema}.{table}"

def create_or_replace_table(df, schema, table, partition_by=None):
    """Salva DataFrame como tabela Delta"""
    full_name = get_full_table_name(schema, table)
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.saveAsTable(full_name)
    print(f"✓ Tabela criada: {full_name}")
    return full_name

print("✓ Configuração carregada")
print(f"  Catalog: {CATALOG}")
print(f"  Bronze: {SCHEMA_BRONZE}")
print(f"  Silver: {SCHEMA_SILVER}")
print(f"  Gold: {SCHEMA_GOLD}")

# COMMAND ----------

# DBTITLE 1,1. RFM Features (Recency, Frequency, Monetary)
# Calcular RFM - métricas fundamentais para churn e propensão
df_transactions = spark.table(get_full_table_name(SCHEMA_SILVER, "transactions"))
reference_date = df_transactions.agg(F.max("transaction_date")).collect()[0][0]

print(f"Data de referência: {reference_date}")

df_rfm = df_transactions.groupBy("customer_id").agg(
    # Recency: dias desde última compra
    F.datediff(F.lit(reference_date), F.max("transaction_date")).alias("recency_days"),
    
    # Frequency: número de transações
    F.count("transaction_id").alias("frequency"),
    
    # Monetary: valor total gasto
    F.sum("total_amount").alias("monetary_total"),
    F.avg("total_amount").alias("monetary_avg"),
    F.max("total_amount").alias("monetary_max"),
    F.min("total_amount").alias("monetary_min"),
    
    # Features adicionais
    F.min("transaction_date").alias("first_purchase_date"),
    F.max("transaction_date").alias("last_purchase_date"),
    F.countDistinct("product_id").alias("unique_products_purchased"),
    F.sum("quantity").alias("total_items_purchased")
)

# Calcular customer lifetime (dias desde primeira compra)
df_rfm = df_rfm.withColumn(
    "customer_lifetime_days",
    F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date"))
)

# Calcular frequência média (compras por dia)
df_rfm = df_rfm.withColumn(
    "purchase_frequency_per_day",
    F.when(F.col("customer_lifetime_days") > 0,
           F.col("frequency") / F.col("customer_lifetime_days"))
    .otherwise(0)
)

create_or_replace_table(df_rfm, SCHEMA_GOLD, "rfm_features")
print(f"✓ RFM Features: {df_rfm.count():,} clientes")
df_rfm.show(5)

# COMMAND ----------

# DBTITLE 1,2. Behavioral Features (Engajamento)
# Features comportamentais dos últimos 30, 60 e 90 dias
df_events = spark.table(get_full_table_name(SCHEMA_SILVER, "behavioral_events"))

# Definir janelas de tempo
max_event_date = df_events.agg(F.max("event_date_only")).collect()[0][0]
windows = {
    "30d": max_event_date - timedelta(days=30),
    "60d": max_event_date - timedelta(days=60),
    "90d": max_event_date - timedelta(days=90)
}

print(f"Janelas de tempo a partir de {max_event_date}")

# Features para cada janela
behavioral_features = None

for window_name, start_date in windows.items():
    df_window = df_events.filter(F.col("event_date_only") >= start_date)
    
    df_agg = df_window.groupBy("customer_id").agg(
        F.count("event_id").alias(f"event_count_{window_name}"),
        F.countDistinct("session_id").alias(f"session_count_{window_name}"),
        F.sum("event_value").alias(f"engagement_score_{window_name}"),
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias(f"page_views_{window_name}"),
        F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias(f"product_views_{window_name}"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(f"add_to_cart_{window_name}"),
        F.sum(F.when(F.col("is_mobile") == 1, 1).otherwise(0)).alias(f"mobile_events_{window_name}"),
        F.countDistinct(F.col("product_id")).alias(f"unique_products_viewed_{window_name}")
    )
    
    if behavioral_features is None:
        behavioral_features = df_agg
    else:
        behavioral_features = behavioral_features.join(df_agg, "customer_id", "outer")

# Preencher nulos com 0
for col in behavioral_features.columns:
    if col != "customer_id":
        behavioral_features = behavioral_features.fillna({col: 0})

create_or_replace_table(behavioral_features, SCHEMA_GOLD, "behavioral_features")
print(f"✓ Behavioral Features: {behavioral_features.count():,} clientes")
behavioral_features.show(5)

# COMMAND ----------

# DBTITLE 1,3. Campaign History Features
# Features de histórico de campanhas
df_exposures = spark.table(get_full_table_name(SCHEMA_SILVER, "campaign_exposures"))
df_responses = spark.table(get_full_table_name(SCHEMA_SILVER, "campaign_responses"))

# Features de exposição
df_campaign_features = df_exposures.groupBy("customer_id").agg(
    F.count("exposure_id").alias("total_campaigns_exposed"),
    # Contar campanhas em que o cliente foi do grupo de tratamento (não controle)
    F.sum(F.when(F.col("is_control_group") == False, 1).otherwise(0)).alias("treatment_campaigns_count"),
    F.sum(F.when(F.col("is_control_group") == True, 1).otherwise(0)).alias("control_campaigns_count"),
    F.countDistinct("campaign_id").alias("unique_campaigns_exposed")
)

# Features de resposta
df_response_features = df_responses.groupBy("customer_id").agg(
    F.count("response_id").alias("total_responses"),
    F.sum("is_conversion").alias("total_conversions"),
    F.sum("response_value").alias("total_response_value"),
    F.avg("response_value").alias("avg_response_value")
)

# Juntar features
df_campaign_history = df_campaign_features.join(
    df_response_features, 
    "customer_id", 
    "left"
).fillna(0)

# Calcular taxa de resposta
df_campaign_history = df_campaign_history.withColumn(
    "response_rate",
    F.when(F.col("total_campaigns_exposed") > 0,
           F.col("total_responses") / F.col("total_campaigns_exposed"))
    .otherwise(0)
)

df_campaign_history = df_campaign_history.withColumn(
    "conversion_rate",
    F.when(F.col("total_campaigns_exposed") > 0,
           F.col("total_conversions") / F.col("total_campaigns_exposed"))
    .otherwise(0)
)

create_or_replace_table(df_campaign_history, SCHEMA_GOLD, "campaign_history_features")
print(f"✓ Campaign History Features: {df_campaign_history.count():,} clientes")
df_campaign_history.show(5)

# COMMAND ----------

# DBTITLE 1,4. Customer Master Features (Feature Store Completo)
# Juntar todas as features em uma tabela master
df_customers =   spark.table(get_full_table_name(SCHEMA_SILVER, "customers"))
df_rfm =         spark.table(get_full_table_name(SCHEMA_GOLD, "rfm_features"))
df_behavioral =  spark.table(get_full_table_name(SCHEMA_GOLD, "behavioral_features"))
df_campaign =    spark.table(get_full_table_name(SCHEMA_GOLD, "campaign_history_features"))

# Começar com clientes
df_features = df_customers.select(
    "customer_id",
    "signup_date",
    "age",
    "age_group",
    "gender",
    "country",
    "segment",
    "customer_age_days",
    "is_active",
    "is_churned"
)

# Join RFM
df_features = df_features.join(df_rfm, "customer_id", "left")

# Join Behavioral
df_features = df_features.join(df_behavioral, "customer_id", "left")

# Join Campaign
df_features = df_features.join(df_campaign, "customer_id", "left")

# Preencher nulos
for col in df_features.columns:
    if col not in ["customer_id", "signup_date", "age_group", "gender", "country", "segment", "first_purchase_date", "last_purchase_date"]:
        # Preencher nulos com 0
        df_features = df_features.fillna({col: 0})

# Adicionar timestamp de criação
df_features = df_features.withColumn("feature_timestamp", F.current_timestamp())

create_or_replace_table(df_features, SCHEMA_GOLD, "customer_features")
print(f"✓ Customer Features (Master): {df_features.count():,} clientes")
print(f"   Total features: {len(df_features.columns)}")
df_features.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,5. Criar Target para Churn
# Criar labels de churn
# Definição: Cliente churn se ficou 90+ dias sem comprar (após ter comprado antes)

df_features = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_features"))

# Lógica de churn:
# - Se is_churned == 1 (status manual), é churn
# - Se recency > 90 dias E já comprou antes, é churn
# - Caso contrário, não é churn

df_churn_labels = df_features.select(
    "customer_id",
    "is_churned",
    "recency_days",
    "frequency"
).withColumn(
    "churn_label",
    F.when(
        (F.col("is_churned") == 1) | 
        ((F.col("recency_days") > 90) & (F.col("frequency") > 0)),
        1
    ).otherwise(0)
).withColumn(
    "churn_risk_category",
    F.when(F.col("churn_label") == 1, "Churned")
    .when(F.col("recency_days") > 60, "High Risk")
    .when(F.col("recency_days") > 30, "Medium Risk")
    .otherwise("Low Risk")
)

create_or_replace_table(df_churn_labels, SCHEMA_GOLD, "churn_labels")

churn_dist = df_churn_labels.groupBy("churn_label").count().orderBy("churn_label")
print("✓ Churn Labels criados:")
churn_dist.show()

risk_dist = df_churn_labels.groupBy("churn_risk_category").count().orderBy("count", ascending=False)
print("\nDistribuição de risco:")
risk_dist.show()

# COMMAND ----------

# DBTITLE 1,Resumo Gold Layer
# Resumo das tabelas Gold criadas
print("="*60)
print("GOLD LAYER - RESUMO")
print("="*60)

gold_tables = [
    "rfm_features",
    "behavioral_features",
    "campaign_history_features",
    "customer_features",
    "churn_labels"
]

for table in gold_tables:
    full_name = get_full_table_name(SCHEMA_GOLD, table)
    df = spark.table(full_name)
    count = df.count()
    cols = len(df.columns)
    print(f"\n{table}:")
    print(f"  - Registros: {count:,}")
    print(f"  - Colunas: {cols}")
    print(f"  - Tabela: {full_name}")

# Explicação: rfm_features pode ter mais registros que behavioral_features porque nem todo cliente realizou eventos comportamentais,
# mas pode ter feito transações (compras). Clientes sem eventos comportamentais ainda aparecem em rfm_features se compraram algo.
print("\n" + "="*60)
print("✓ GOLD LAYER COMPLETA - PRONTA PARA MODELAGEM")
print("="*60)

# COMMAND ----------

# DBTITLE 1,Data Quality Gate - Gold
# customer_features e churn_labels partem de silver.customers com apenas LEFT
# JOINs/select (ver células "4. Customer Master Features" e "5. Criar Target
# para Churn" acima) — nenhuma delas filtra nem faz join que multiplique
# linhas, então o row count deve bater EXATAMENTE com silver.customers.
# Qualquer divergência aqui indica um join que virou inner por engano, ou uma
# chave duplicada gerando fan-out. Levanta erro de propósito (mesmo raciocínio
# do gate de Silver): um gate que só avisa é o mesmo que não ter gate.

silver_customers_count = spark.table(get_full_table_name(SCHEMA_SILVER, "customers")).count()

# (gold_table, expected_count)
GOLD_EXACT_MATCH_CHECKS = [
    ("customer_features", silver_customers_count),
    ("churn_labels", silver_customers_count),
]

# (gold_table, critical_column)
GOLD_NULL_CHECKS = [
    ("customer_features", "customer_id"),
    ("churn_labels", "customer_id"),
    ("churn_labels", "churn_label"),
]

print("="*60)
print("DATA QUALITY GATE - GOLD")
print("="*60)

dq_errors = []

for gold_tbl, expected_count in GOLD_EXACT_MATCH_CHECKS:
    actual_count = spark.table(get_full_table_name(SCHEMA_GOLD, gold_tbl)).count()
    status = "OK" if actual_count == expected_count else "FALHOU"
    print(f"  [{status}] {gold_tbl}: {actual_count:,} (esperado exatamente {expected_count:,})")
    if actual_count != expected_count:
        dq_errors.append(f"{gold_tbl}: {actual_count:,} linhas, esperado {expected_count:,} (== silver.customers)")

for gold_tbl, col in GOLD_NULL_CHECKS:
    null_count = spark.table(get_full_table_name(SCHEMA_GOLD, gold_tbl)).filter(F.col(col).isNull()).count()
    status = "OK" if null_count == 0 else "FALHOU"
    print(f"  [{status}] {gold_tbl}.{col}: {null_count} nulos (esperado 0)")
    if null_count > 0:
        dq_errors.append(f"{gold_tbl}.{col}: {null_count} valores nulos em coluna crítica")

if dq_errors:
    raise ValueError(
        "Data Quality Gate (Gold) falhou:\n" + "\n".join(f"  - {e}" for e in dq_errors)
    )
print("\n✓ Data Quality Gate (Gold) passou")

# COMMAND ----------


