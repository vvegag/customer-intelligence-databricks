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
import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random

# Configurações de catálogo e schema
CATALOG = "customer_intelligence"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# Configurações MLflow
MLFLOW_EXPERIMENT_PATH = "/Users/valdomirovega@hotmail.com/customer_intelligence_experiments"

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
# Simular exposições a campanhas (quem viu cada campanha)
N_EXPOSURES = 30000

# Extrai todos os IDs de campanhas da tabela campaigns_raw para uma lista
# 'row.campaign_id' refere-se ao valor da coluna 'campaign_id' em cada linha (row) retornada pelo método .collect() do DataFrame 'df_campaigns'
campaign_ids = [row.campaign_id for row in df_campaigns.select("campaign_id").collect()]

exposure_data = []
for i in range(N_EXPOSURES):
    campaign_id = random.choice(campaign_ids)
    customer_id = random.choice(customer_ids)
    
    # Buscar datas da campanha
    campaign_info = df_campaigns.filter(F.col("campaign_id") == campaign_id).first()
    exposure_date = campaign_info.start_date + timedelta(days=random.randint(0, (campaign_info.end_date - campaign_info.start_date).days))
    
    # Randomizar grupo de controle vs tratamento
    is_control = random.random() < 0.3  # 30% controle
    
    exposure_data.append({
        "exposure_id": f"EXP_{i+1:08d}",
        "campaign_id": campaign_id,
        "customer_id": customer_id,
        "exposure_date": exposure_date,
        "is_control_group": is_control,
        "channel": campaign_info.campaign_type,
        "created_at": datetime.now()
    })

df_exposures = spark.createDataFrame(pd.DataFrame(exposure_data))
create_or_replace_table(df_exposures, SCHEMA_BRONZE, "campaign_exposures_raw")

print(f"✓ Criadas {N_EXPOSURES} exposições")
df_exposures.show(5)

# COMMAND ----------

# DBTITLE 1,6. Respostas a Campanhas (campaign_responses_raw)
# Simular respostas a campanhas
# Taxa de resposta realista: ~5-15% dependendo do grupo

response_data = []
exposures_list = df_exposures.collect()

for exposure in exposures_list:
    # Taxa de resposta diferente para controle vs tratamento
    if exposure.is_control_group:
        response_rate = 0.05  # 5% taxa base
    else:
        response_rate = 0.12  # 12% com campanha
    
    if random.random() < response_rate:
        response_date = exposure.exposure_date + timedelta(days=random.randint(0, 7))
        
        response_data.append({
            "response_id": f"RESP_{len(response_data)+1:08d}",
            "exposure_id": exposure.exposure_id,
            "campaign_id": exposure.campaign_id,
            "customer_id": exposure.customer_id,
            "response_date": response_date,
            "response_type": random.choice(["Purchase", "Click", "Sign Up"]),
            "response_value": round(random.uniform(10, 500), 2) if random.random() < 0.7 else 0,
            "created_at": datetime.now()
        })

df_responses = spark.createDataFrame(pd.DataFrame(response_data))
create_or_replace_table(df_responses, SCHEMA_BRONZE, "campaign_responses_raw")

print(f"✓ Criadas {len(response_data)} respostas")
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
create_or_replace_table(df_events, SCHEMA_BRONZE, "behavioral_events_raw", partition_by="event_date")

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


