# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Configuração do Projeto Customer Intelligence
# MAGIC %md
# MAGIC # Customer Intelligence & Growth Project
# MAGIC
# MAGIC ## Objetivo
# MAGIC Projeto completo de Customer Intelligence focado em:
# MAGIC - **Churn Prediction**: identificar clientes com risco de cancelamento
# MAGIC - **Propensity Modeling**: prever probabilidade de compra/renovação
# MAGIC - **Recommendation**: sugerir próxima melhor ação
# MAGIC - **Segmentation**: agrupar clientes por comportamento
# MAGIC - **A/B Testing**: medir efetividade de campanhas
# MAGIC - **Causal Inference**: entender impacto causal de ações
# MAGIC
# MAGIC ## Arquitetura
# MAGIC ```
# MAGIC Bronze → Silver → Gold → Models → Scoring
# MAGIC    ↓        ↓       ↓        ↓        ↓
# MAGIC  Raw    Clean   Features  ML     Predictions
# MAGIC ```
# MAGIC
# MAGIC ## Estrutura de Pastas
# MAGIC - `00_setup/`: Configuração inicial
# MAGIC - `01_bronze/`: Ingestão de dados raw
# MAGIC - `02_silver/`: Limpeza e transformação
# MAGIC - `03_gold/`: Feature engineering
# MAGIC - `04_models/`: Treinamento de modelos
# MAGIC - `05_scoring/`: Batch scoring
# MAGIC - `06_experimentation/`: A/B testing e causalidade
# MAGIC - `07_monitoring/`: Drift e performance
# MAGIC - `08_dashboards/`: SQL queries para visualização

# COMMAND ----------

# DBTITLE 1,Configurações Globais
# Configurações globais do projeto
import os
from datetime import datetime

# Configurações de catálogo e schema
CATALOG = "workspace"  # Catálogo padrão do workspace
SCHEMA_BRONZE = "customer_intelligence_bronze"
SCHEMA_SILVER = "customer_intelligence_silver"
SCHEMA_GOLD = "customer_intelligence_gold"

# Configurações MLflow
MLFLOW_EXPERIMENT_PATH = "/Users/valdomirovega@hotmail.com/customer_intelligence_experiments"

# Configurações gerais
DATA_PATH = "/FileStore/customer_intelligence/data"
MODEL_REGISTRY_NAME_PREFIX = "customer_intelligence"

# Armazenar em widgets para reutilização
dbutils.widgets.text("catalog", CATALOG, "Catalog")
dbutils.widgets.text("schema_bronze", SCHEMA_BRONZE, "Bronze Schema")
dbutils.widgets.text("schema_silver", SCHEMA_SILVER, "Silver Schema")
dbutils.widgets.text("schema_gold", SCHEMA_GOLD, "Gold Schema")

print(f"✓ Configurações carregadas")
print(f"  Catalog: {CATALOG}")
print(f"  Bronze: {SCHEMA_BRONZE}")
print(f"  Silver: {SCHEMA_SILVER}")
print(f"  Gold: {SCHEMA_GOLD}")

# COMMAND ----------

# DBTITLE 1,Criar Schemas
# MAGIC %sql
# MAGIC -- Criar schemas para o projeto
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.customer_intelligence_bronze
# MAGIC   COMMENT 'Raw data - camada bronze para ingestão inicial';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.customer_intelligence_silver
# MAGIC   COMMENT 'Clean data - camada silver com dados limpos e transformados';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.customer_intelligence_gold
# MAGIC   COMMENT 'Feature store - camada gold com features agregadas e scores';
# MAGIC
# MAGIC SHOW SCHEMAS IN workspace LIKE 'customer_intelligence%';

# COMMAND ----------

# DBTITLE 1,Helper Functions
# Funções auxiliares para o projeto

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

def get_latest_model_version(model_name):
    """Retorna a última versão de um modelo no MLflow"""
    from mlflow.tracking import MlflowClient
    client = MlflowClient()
    try:
        versions = client.search_model_versions(f"name='{model_name}'")
        if versions:
            latest = max([int(v.version) for v in versions])
            return latest
    except:
        pass
    return None

def log_metrics_to_mlflow(metrics_dict, step=None):
    """Log múltiplas métricas no MLflow"""
    import mlflow
    for key, value in metrics_dict.items():
        mlflow.log_metric(key, value, step=step)

print("✓ Helper functions carregadas")

# COMMAND ----------

# DBTITLE 1,Verificação de Setup
# Verificar se tudo está configurado
import mlflow

print("="*60)
print("VERIFICAÇÃO DE SETUP")
print("="*60)

# 1. Verificar schemas
try:
    schemas = spark.sql(f"SHOW SCHEMAS IN {CATALOG} LIKE 'customer_intelligence%'").collect()
    print(f"\n✓ Schemas criados: {len(schemas)}")
    for schema in schemas:
        print(f"  - {schema.databaseName}")
except Exception as e:
    print(f"✗ Erro ao verificar schemas: {e}")

# 2. Verificar MLflow
try:
    mlflow.set_experiment(MLFLOW_EXPERIMENT_PATH)
    print(f"\n✓ MLflow experiment configurado: {MLFLOW_EXPERIMENT_PATH}")
except Exception as e:
    print(f"✗ Erro ao configurar MLflow: {e}")

# 3. Resumo
print("\n" + "="*60)
print("SETUP COMPLETO ✓")
print("="*60)
print("\nPróximos passos:")
print("1. Execute notebooks em 01_bronze/ para ingerir dados")
print("2. Execute notebooks em 02_silver/ para limpar dados")
print("3. Execute notebooks em 03_gold/ para criar features")
print("4. Execute notebooks em 04_models/ para treinar modelos")
print("5. Execute notebooks em 05_scoring/ para gerar previsões")

# COMMAND ----------

