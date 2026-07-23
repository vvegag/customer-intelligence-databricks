# Databricks notebook source
# DBTITLE 1,Bronze Layer - Ingestão de Dados Raw
# MAGIC %md
# MAGIC # Bronze Layer - Dados Raw
# MAGIC
# MAGIC Esta camada contém os dados na forma mais próxima possível da origem.
# MAGIC
# MAGIC ## Tabelas Bronze:
# MAGIC 1. **customers_raw**: Dados cadastrais de clientes
# MAGIC 2. **products_raw**: Catálogo de produtos
# MAGIC 3. **transactions_raw**: Histórico de transações
# MAGIC 4. **campaigns_raw**: Campanhas de marketing
# MAGIC 5. **campaign_exposures_raw**: Exposição de clientes a campanhas
# MAGIC 6. **campaign_responses_raw**: Respostas de clientes a campanhas
# MAGIC 7. **behavioral_events_raw**: Eventos comportamentais (cliques, visualizações, etc)
# MAGIC
# MAGIC ## Simulação de Dados
# MAGIC Neste notebook, vamos **simular dados realistas** para demonstração. 
# MAGIC Em produção, estes dados viriam de APIs, bancos de dados, arquivos, etc.

# COMMAND ----------

# DBTITLE 1,Configuração
# Configurações globais do projeto (inline - sem usar %run)
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
import random

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

# DBTITLE 1,1. Clientes (customers_raw)
# Simular base de clientes
np.random.seed(42)
random.seed(42)

N_CUSTOMERS = 10000

# Gerar dados de clientes
customer_data = []
for i in range(N_CUSTOMERS):
    customer_id = f"CUST_{i+1:06d}"
    signup_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))
    
    # Segmentos com distribuição realista
    segment_prob = random.random()
    if segment_prob < 0.15:
        segment = "VIP"
        avg_purchase = random.uniform(500, 2000)
    elif segment_prob < 0.40:
        segment = "High Value"
        avg_purchase = random.uniform(200, 500)
    elif segment_prob < 0.75:
        segment = "Medium Value"
        avg_purchase = random.uniform(50, 200)
    else:
        segment = "Low Value"
        avg_purchase = random.uniform(10, 50)
    
    customer_data.append({
        "customer_id": customer_id,
        "signup_date": signup_date,
        "age": random.randint(18, 75),
        "gender": random.choice(["M", "F", "Other"]),
        "country": random.choices(["BR", "US", "UK", "CA", "AU"], weights=[70, 10, 8, 7, 5], k=1)[0],
        "segment": segment,
        "avg_purchase_value": round(avg_purchase, 2),
        "account_status": random.choice(["Active", "Active", "Active", "Inactive", "Churned"]),
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    })

df_customers = spark.createDataFrame(pd.DataFrame(customer_data))
create_or_replace_table(df_customers, SCHEMA_BRONZE, "customers_raw")

print(f"✓ Criados {N_CUSTOMERS} clientes")
df_customers.show(5)

# COMMAND ----------

# DBTITLE 1,2. Produtos (products_raw)
# Simular catálogo de produtos
N_PRODUCTS = 500

categories = ["Electronics", "Clothing", "Home", "Books", "Sports", "Beauty", "Toys"]
product_data = []

for i in range(N_PRODUCTS):
    product_id = f"PROD_{i+1:04d}"
    category = random.choice(categories)
    
    # Preço varia por categoria
    if category == "Electronics":
        price = random.uniform(100, 2000)
    elif category == "Home":
        price = random.uniform(50, 500)
    else:
        price = random.uniform(10, 200)
    
    product_data.append({
        "product_id": product_id,
        "product_name": f"{category} Item {i+1}",
        "category": category,
        "price": round(price, 2),
        "cost": round(price * random.uniform(0.3, 0.6), 2),
        "stock_quantity": random.randint(0, 1000),
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    })

df_products = spark.createDataFrame(pd.DataFrame(product_data))
create_or_replace_table(df_products, SCHEMA_BRONZE, "products_raw")

print(f"✓ Criados {N_PRODUCTS} produtos")
df_products.show(5)

# COMMAND ----------

# DBTITLE 1,3. Transações (transactions_raw)
# Simular transações
N_TRANSACTIONS = 50000

customer_ids = [row.customer_id for row in df_customers.select("customer_id").collect()]
product_ids = [row.product_id for row in df_products.select("product_id").collect()]
product_prices = {row.product_id: row.price for row in df_products.select("product_id", "price").collect()}

transaction_data = []
for i in range(N_TRANSACTIONS):
    customer_id = random.choice(customer_ids)
    product_id = random.choice(product_ids)
    transaction_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))
    quantity = random.randint(1, 5)
    unit_price = product_prices[product_id]
    
    transaction_data.append({
        "transaction_id": f"TXN_{i+1:08d}",
        "customer_id": customer_id,
        "product_id": product_id,
        "transaction_date": transaction_date,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": round(quantity * unit_price, 2),
        "payment_method": random.choice(["Credit Card", "Debit Card", "PayPal", "Bank Transfer"]),
        "status": random.choice(["Completed", "Completed", "Completed", "Pending", "Cancelled"]),
        "created_at": datetime.now()
    })

df_transactions = spark.createDataFrame(pd.DataFrame(transaction_data))
create_or_replace_table(df_transactions, SCHEMA_BRONZE, "transactions_raw", partition_by="transaction_date")

print(f"✓ Criadas {N_TRANSACTIONS} transações")
df_transactions.show(5)

# COMMAND ----------

# DBTITLE 1,4. Campanhas (campaigns_raw)
# Simular campanhas de marketing
N_CAMPAIGNS = 20

campaign_data = []
for i in range(N_CAMPAIGNS):
    start_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 600))
    end_date = start_date + timedelta(days=random.randint(7, 60))
    
    campaign_data.append({
        "campaign_id": f"CAMP_{i+1:03d}",
        "campaign_name": f"Campaign {i+1} - {random.choice(['Summer Sale', 'Black Friday', 'New Year', 'Spring Offer', 'VIP Exclusive'])}",
        "campaign_type": random.choice(["Email", "SMS", "Push", "Display Ad"]),
        "start_date": start_date,
        "end_date": end_date,
        "budget": round(random.uniform(5000, 100000), 2),
        "target_segment": random.choice(["All", "VIP", "High Value", "At Risk", "New Customers"]),
        "discount_pct": random.choice([0, 10, 15, 20, 25, 30]),
        "created_at": datetime.now()
    })

df_campaigns = spark.createDataFrame(pd.DataFrame(campaign_data))
create_or_replace_table(df_campaigns, SCHEMA_BRONZE, "campaigns_raw")

print(f"✓ Criadas {N_CAMPAIGNS} campanhas")
df_campaigns.show(5)

# COMMAND ----------

# DBTITLE 1,5. Exposições a Campanhas (campaign_exposures_raw)
# Simular exposições a campanhas (quem viu cada campanha) — 100% vetorizado em
# PySpark, sem loop Python e sem ação Spark repetida (a versão anterior chamava
# .filter().first() 30.000 vezes dentro do loop, o que sozinho já custava minutos
# de overhead de agendamento de job por iteração).
N_EXPOSURES = 30000

# Índice numérico sequencial para clientes e campanhas, para permitir "escolher
# um valor aleatório" via join em vez de random.choice() célula a célula
customers_indexed = (
    df_customers.select("customer_id")
    .withColumn("idx", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1)
)
n_customers = customers_indexed.count()

campaigns_indexed = (
    df_campaigns.select("campaign_id", "campaign_type", "start_date", "end_date")
    .withColumn("idx", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1)
)
n_campaigns = campaigns_indexed.count()

df_exposures = (
    spark.range(N_EXPOSURES)
    .withColumnRenamed("id", "row_id")
    .withColumn("customer_idx", (F.rand(seed=42) * n_customers).cast("int"))
    .withColumn("campaign_idx", (F.rand(seed=43) * n_campaigns).cast("int"))
    .join(F.broadcast(customers_indexed), F.col("customer_idx") == customers_indexed.idx)
    .join(F.broadcast(campaigns_indexed), F.col("campaign_idx") == campaigns_indexed.idx)
    .withColumn("exposure_id", F.concat(F.lit("EXP_"), F.lpad((F.col("row_id") + 1).cast("string"), 8, "0")))
    .withColumn("days_range", F.greatest(F.datediff("end_date", "start_date"), F.lit(0)))
    .withColumn("exposure_date", F.expr("date_add(start_date, cast(rand(44) * days_range as int))"))
    .withColumn("is_control_group", F.rand(seed=45) < 0.3)  # 30% controle
    .withColumn("channel", F.col("campaign_type"))
    .withColumn("created_at", F.current_timestamp())
    .select("exposure_id", "campaign_id", "customer_id", "exposure_date", "is_control_group", "channel", "created_at")
)

create_or_replace_table(df_exposures, SCHEMA_BRONZE, "campaign_exposures_raw")

print(f"✓ Criadas {N_EXPOSURES} exposições")
df_exposures.show(5)

# COMMAND ----------

# DBTITLE 1,6. Respostas a Campanhas (campaign_responses_raw)
# Simular respostas a campanhas — também vetorizado: taxa de resposta diferente
# por grupo controle/tratamento, sem iterar linha a linha em Python.
# Taxa de resposta realista: ~5-15% dependendo do grupo

df_responses = (
    df_exposures
    .withColumn("response_rate", F.when(F.col("is_control_group"), 0.05).otherwise(0.12))
    .withColumn("responded", F.rand(seed=46) < F.col("response_rate"))
    .filter(F.col("responded"))
    .withColumn("response_date", F.expr("date_add(exposure_date, cast(rand(47) * 7 as int))"))
    .withColumn(
        "response_type",
        F.element_at(F.array(F.lit("Purchase"), F.lit("Click"), F.lit("Sign Up")), (F.floor(F.rand(seed=48) * 3) + 1).cast("int"))
    )
    .withColumn(
        "response_value",
        F.when(F.rand(seed=49) < 0.7, F.round(F.rand(seed=50) * 490 + 10, 2)).otherwise(F.lit(0.0))
    )
    .withColumn("created_at", F.current_timestamp())
    .withColumn("response_id", F.concat(F.lit("RESP_"), F.lpad(F.row_number().over(Window.orderBy(F.monotonically_increasing_id())).cast("string"), 8, "0")))
    .select("response_id", "exposure_id", "campaign_id", "customer_id", "response_date", "response_type", "response_value", "created_at")
)

create_or_replace_table(df_responses, SCHEMA_BRONZE, "campaign_responses_raw")

n_responses = df_responses.count()
print(f"✓ Criadas {n_responses} respostas")
df_responses.show(5)

# COMMAND ----------

# DBTITLE 1,7. Eventos Comportamentais (behavioral_events_raw)
# Simular eventos comportamentais
N_EVENTS = 100000

event_types = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "search", "login", "logout"]
event_data = []

for i in range(N_EVENTS):
    customer_id = random.choice(customer_ids)
    event_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730), 
                                                   hours=random.randint(0, 23),
                                                   minutes=random.randint(0, 59))
    event_type = random.choice(event_types)
    
    event_data.append({
        "event_id": f"EVT_{i+1:08d}",
        "customer_id": customer_id,
        "event_type": event_type,
        "event_date": event_date,
        "product_id": random.choice(product_ids) if "product" in event_type or "cart" in event_type else None,
        "session_id": f"SESSION_{random.randint(1, 50000):06d}",
        "device_type": random.choice(["Mobile", "Desktop", "Tablet"]),
        "created_at": datetime.now()
    })

df_events = spark.createDataFrame(pd.DataFrame(event_data))
# Sem partition_by: event_date tem granularidade de minuto, então seria uma
# coluna de altíssima cardinalidade (quase 1 valor distinto por linha em 100k
# linhas) — particionar por ela criaria dezenas de milhares de arquivos minúsculos
# e travaria a escrita. Numa tabela desse tamanho (100k linhas), particionamento
# nem compensa; se precisar no futuro, particionar por uma coluna derivada de
# granularidade grosseira (ex: ano-mês), não pelo timestamp completo.
create_or_replace_table(df_events, SCHEMA_BRONZE, "behavioral_events_raw")

print(f"✓ Criados {N_EVENTS} eventos")
df_events.show(5)

# COMMAND ----------

# DBTITLE 1,Resumo Bronze Layer
# Resumo das tabelas Bronze criadas
print("="*60)
print("BRONZE LAYER - RESUMO")
print("="*60)

bronze_tables = [
    "customers_raw",
    "products_raw",
    "transactions_raw",
    "campaigns_raw",
    "campaign_exposures_raw",
    "campaign_responses_raw",
    "behavioral_events_raw"
]

for table in bronze_tables:
    full_name = get_full_table_name(SCHEMA_BRONZE, table)
    count = spark.table(full_name).count()
    print(f"\n{table}:")
    print(f"  - Registros: {count:,}")
    print(f"  - Tabela: {full_name}")

print("\n" + "="*60)
print("✓ BRONZE LAYER COMPLETA")
print("="*60)

# COMMAND ----------


