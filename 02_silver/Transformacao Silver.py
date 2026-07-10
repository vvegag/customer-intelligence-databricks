# Databricks notebook source
# DBTITLE 1,Silver Layer - Dados Limpos
# MAGIC %md
# MAGIC # Silver Layer - Dados Limpos e Transformados
# MAGIC
# MAGIC Esta camada contém dados:
# MAGIC - **Limpos**: sem duplicatas, nulos tratados, tipos corretos
# MAGIC - **Padronizados**: formato consistente
# MAGIC - **Enriquecidos**: cálculos básicos e flags
# MAGIC - **Validados**: regras de negócio aplicadas
# MAGIC
# MAGIC ## Transformações Aplicadas:
# MAGIC 1. Deduplication
# MAGIC 2. Type casting e conversões
# MAGIC 3. Tratamento de nulos
# MAGIC 4. Padronização de strings
# MAGIC 5. Cálculos derivados
# MAGIC 6. Data quality flags

# COMMAND ----------

# DBTITLE 1,Configuração
# Configurações globais do projeto (inline - sem usar %run)
import os
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

# DBTITLE 1,1. Clientes Silver
# Transformar clientes
df_customers_bronze = spark.table(get_full_table_name(SCHEMA_BRONZE, "customers_raw"))

df_customers_silver = df_customers_bronze \
    .dropDuplicates(["customer_id"]) \
    .filter(F.col("customer_id").isNotNull()) \
    .withColumn("signup_date", F.to_date("signup_date")) \
    .withColumn("age_group", 
        F.when(F.col("age") < 25, "18-24")
        .when(F.col("age") < 35, "25-34")
        .when(F.col("age") < 50, "35-49")
        .when(F.col("age") < 65, "50-64")
        .otherwise("65+")
    ) \
    .withColumn("customer_age_days", F.datediff(F.current_date(), F.col("signup_date"))) \
    .withColumn("is_active", F.when(F.col("account_status") == "Active", 1).otherwise(0)) \
    .withColumn("is_churned", F.when(F.col("account_status") == "Churned", 1).otherwise(0)) \
    .withColumn("processed_at", F.current_timestamp())

create_or_replace_table(df_customers_silver, SCHEMA_SILVER, "customers")
print(f"✓ Clientes Silver: {df_customers_silver.count():,} registros")
df_customers_silver.show(5)

# COMMAND ----------

# DBTITLE 1,2. Produtos Silver
# Transformar produtos
df_products_bronze = spark.table(get_full_table_name(SCHEMA_BRONZE, "products_raw"))

df_products_silver = df_products_bronze \
    .dropDuplicates(["product_id"]) \
    .filter(F.col("product_id").isNotNull()) \
    .withColumn("margin", F.col("price") - F.col("cost")) \
    .withColumn("margin_pct", F.round((F.col("margin") / F.col("price")) * 100, 2)) \
    .withColumn("is_in_stock", F.when(F.col("stock_quantity") > 0, 1).otherwise(0)) \
    .withColumn("price_tier",
        F.when(F.col("price") < 50, "Budget")
        .when(F.col("price") < 200, "Mid-Range")
        .when(F.col("price") < 500, "Premium")
        .otherwise("Luxury")
    ) \
    .withColumn("processed_at", F.current_timestamp())

create_or_replace_table(df_products_silver, SCHEMA_SILVER, "products")
print(f"✓ Produtos Silver: {df_products_silver.count():,} registros")
df_products_silver.show(5)

# COMMAND ----------

# DBTITLE 1,3. Transações Silver
# Transformar transações
df_transactions_bronze = spark.table(get_full_table_name(SCHEMA_BRONZE, "transactions_raw"))

df_transactions_silver = df_transactions_bronze \
    .dropDuplicates(["transaction_id"]) \
    .filter(F.col("transaction_id").isNotNull()) \
    .filter(F.col("customer_id").isNotNull()) \
    .filter(F.col("status") == "Completed") \
    .withColumn("transaction_date", F.to_date("transaction_date")) \
    .withColumn("year", F.year("transaction_date")) \
    .withColumn("month", F.month("transaction_date")) \
    .withColumn("quarter", F.quarter("transaction_date")) \
    .withColumn("day_of_week", F.dayofweek("transaction_date")) \
    .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0)) \
    .withColumn("processed_at", F.current_timestamp())

create_or_replace_table(df_transactions_silver, SCHEMA_SILVER, "transactions", partition_by="transaction_date")
print(f"✓ Transações Silver: {df_transactions_silver.count():,} registros")
df_transactions_silver.show(5)

# COMMAND ----------

# DBTITLE 1,4. Campanhas Silver
# Transformar campanhas
df_campaigns_bronze = spark.table(get_full_table_name(SCHEMA_BRONZE, "campaigns_raw"))

df_campaigns_silver = df_campaigns_bronze \
    .dropDuplicates(["campaign_id"]) \
    .filter(F.col("campaign_id").isNotNull()) \
    .withColumn("start_date", F.to_date("start_date")) \
    .withColumn("end_date", F.to_date("end_date")) \
    .withColumn("duration_days", F.datediff(F.col("end_date"), F.col("start_date"))) \
    .withColumn("is_active", 
        F.when(
            (F.col("start_date") <= F.current_date()) & 
            (F.col("end_date") >= F.current_date()), 1
        ).otherwise(0)
    ) \
    .withColumn("processed_at", F.current_timestamp())

create_or_replace_table(df_campaigns_silver, SCHEMA_SILVER, "campaigns")
print(f"✓ Campanhas Silver: {df_campaigns_silver.count():,} registros")
df_campaigns_silver.show(5)

# COMMAND ----------

# DBTITLE 1,5. Exposições Silver
# Transformar exposições
df_exposures_bronze = spark.table(get_full_table_name(SCHEMA_BRONZE, "campaign_exposures_raw"))

df_exposures_silver = df_exposures_bronze \
    .dropDuplicates(["exposure_id"]) \
    .filter(F.col("exposure_id").isNotNull()) \
    .filter(F.col("campaign_id").isNotNull()) \
    .filter(F.col("customer_id").isNotNull()) \
    .withColumn("exposure_date", F.to_date("exposure_date")) \
    .withColumn("treatment_group", 
        F.when(F.col("is_control_group") == True, "Control")
        .otherwise("Treatment")
    ) \
    .withColumn("processed_at", F.current_timestamp())

create_or_replace_table(df_exposures_silver, SCHEMA_SILVER, "campaign_exposures")
print(f"✓ Exposições Silver: {df_exposures_silver.count():,} registros")
df_exposures_silver.show(5)

# COMMAND ----------

# DBTITLE 1,6. Respostas Silver
# Transformar respostas
df_responses_bronze = spark.table(get_full_table_name(SCHEMA_BRONZE, "campaign_responses_raw"))

df_responses_silver = df_responses_bronze \
    .dropDuplicates(["response_id"]) \
    .filter(F.col("response_id").isNotNull()) \
    .withColumn("response_date", F.to_date("response_date")) \
    .withColumn("is_conversion", F.when(F.col("response_type") == "Purchase", 1).otherwise(0)) \
    .withColumn("processed_at", F.current_timestamp())

create_or_replace_table(df_responses_silver, SCHEMA_SILVER, "campaign_responses")
print(f"✓ Respostas Silver: {df_responses_silver.count():,} registros")
df_responses_silver.show(5)

# COMMAND ----------

# DBTITLE 1,7. Eventos Silver
# Transformar eventos comportamentais
df_events_bronze = spark.table(get_full_table_name(SCHEMA_BRONZE, "behavioral_events_raw"))

df_events_silver = df_events_bronze \
    .dropDuplicates(["event_id"]) \
    .filter(F.col("event_id").isNotNull()) \
    .filter(F.col("customer_id").isNotNull()) \
    .withColumn("event_date", F.to_timestamp("event_date")) \
    .withColumn("event_date_only", F.to_date("event_date")) \
    .withColumn("event_hour", F.hour("event_date")) \
    .withColumn("is_mobile", F.when(F.col("device_type") == "Mobile", 1).otherwise(0)) \
    .withColumn("event_value",
        F.when(F.col("event_type") == "add_to_cart", 10)
        .when(F.col("event_type") == "product_view", 5)
        .when(F.col("event_type") == "search", 3)
        .otherwise(1)
    ) \
    .withColumn("processed_at", F.current_timestamp())

create_or_replace_table(df_events_silver, SCHEMA_SILVER, "behavioral_events", partition_by="event_date_only")
print(f"✓ Eventos Silver: {df_events_silver.count():,} registros")
df_events_silver.show(5)

# COMMAND ----------

# DBTITLE 1,Resumo Silver Layer
# Resumo das tabelas Silver criadas
print("="*60)
print("SILVER LAYER - RESUMO")
print("="*60)

silver_tables = [
    "customers",
    "products",
    "transactions",
    "campaigns",
    "campaign_exposures",
    "campaign_responses",
    "behavioral_events"
]

for table in silver_tables:
    full_name = get_full_table_name(SCHEMA_SILVER, table)
    count = spark.table(full_name).count()
    print(f"\n{table}:")
    print(f"  - Registros: {count:,}")
    print(f"  - Tabela: {full_name}")

print("\n" + "="*60)
print("✓ SILVER LAYER COMPLETA")
print("="*60)

# COMMAND ----------


