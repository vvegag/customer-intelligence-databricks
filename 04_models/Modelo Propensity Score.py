# Databricks notebook source
# DBTITLE 1,Modelo de Propensão de Compra
# MAGIC %md
# MAGIC # Propensity to Buy Model
# MAGIC
# MAGIC ## Objetivo
# MAGIC Prever a probabilidade de um cliente realizar uma compra nos próximos 30 dias.
# MAGIC
# MAGIC ## Features
# MAGIC - RFM (recency, frequency, monetary)
# MAGIC - Engajamento recente
# MAGIC - Histórico de campanhas
# MAGIC - Comportamento de navegação
# MAGIC
# MAGIC ## Target
# MAGIC Cliente comprou nos últimos 30 dias? (0/1)

# COMMAND ----------

# DBTITLE 1,Instalar XGBoost
# MAGIC %pip install xgboost --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Setup
# Configurações globais do projeto (inline)
import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import pandas as pd
import numpy as np

# Configs
CATALOG = "customer_intelligence"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]
MLFLOW_EXPERIMENT_PATH = f"/Users/{CURRENT_USER}/customer_intelligence_experiments"
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

def get_latest_model_version(model_name):
    from mlflow.tracking import MlflowClient
    client = MlflowClient()
    try:
        versions = client.search_model_versions(f"name='{model_name}'")
        if versions:
            return max([int(v.version) for v in versions])
    except:
        pass
    return None

# ML imports
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score
import warnings
warnings.filterwarnings('ignore')

print("✓ Setup OK")
print(f"  Catalog: {CATALOG}")

# COMMAND ----------

# DBTITLE 1,Criar Target - Compra nos Últimos 30 dias
# Criar target: comprou nos últimos 30 dias?
df_transactions = spark.table(get_full_table_name(SCHEMA_SILVER, "transactions"))
max_date = df_transactions.agg(F.max("transaction_date")).collect()[0][0]
cutoff_date = max_date - pd.Timedelta(days=30)

df_recent_buyers = df_transactions.filter(F.col("transaction_date") >= cutoff_date).select("customer_id").distinct()
# Adiciona uma coluna 'purchased_last_30d' com valor 1 para clientes que compraram nos últimos 30 dias
df_recent_buyers = df_recent_buyers.withColumn("purchased_last_30d", F.lit(1))

df_features = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_features"))
df_propensity = df_features.join(df_recent_buyers, "customer_id", "left").fillna({"purchased_last_30d": 0})

print(f"✓ Target criado: {df_propensity.filter(F.col('purchased_last_30d') == 1).count():,} compraram")

# COMMAND ----------

# DBTITLE 1,Preparar Dados
feature_cols = [
    "recency_days", "frequency", "monetary_total", "monetary_avg",
    "event_count_30d", "session_count_30d", "engagement_score_30d",
    "page_views_30d", "product_views_30d", "add_to_cart_30d",
    "total_campaigns_exposed", "response_rate", "conversion_rate"
]

df_pandas = df_propensity.select(["customer_id"] + feature_cols + ["purchased_last_30d"]).fillna(0).toPandas()
X = df_pandas[feature_cols]
y = df_pandas["purchased_last_30d"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
print(f"✓ Train: {X_train.shape}, Test: {X_test.shape}")

# COMMAND ----------

# DBTITLE 1,Treinar Modelo
# =============================================================================
# TIPO DE MODELO: CLASSIFICAÇÃO BINÁRIA
# =============================================================================
# Target: purchased_last_30d (0 ou 1)
# Output: Probabilidade de compra (0.0 a 1.0) - "propensity score"
# 
# NÃO é regressão! Embora o output seja contínuo (probabilidade), o problema
# é de classificação porque prevemos uma classe binária (comprou: sim/não).
# 
# XGBClassifier.predict_proba() retorna P(y=1|X), que interpretamos como
# "propensão de compra" - quanto maior, mais provável o cliente comprar.
# =============================================================================

mlflow.set_experiment(MLFLOW_EXPERIMENT_PATH)

with mlflow.start_run(run_name="propensity_xgboost_v1") as run:
    # XGBClassifier = Classificação Binária (não XGBRegressor)
    model = XGBClassifier(
        n_estimators=100,      # Número de árvores (boosting rounds)
        max_depth=6,           # Profundidade máxima de cada árvore
        learning_rate=0.1,     # Taxa de aprendizado (step size)
        random_state=42        # Semente para reprodução dos resultados
    )
    model.fit(X_train, y_train)
    
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)
    
    metrics = {
        "auc_roc": roc_auc_score(y_test, y_pred_proba),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1": f1_score(y_test, y_pred)
    }
    
    mlflow.log_params({"n_estimators": 100, "max_depth": 6})
    mlflow.log_metrics(metrics)
    
    # Criar signature e input_example para Unity Catalog
    from mlflow.models.signature import infer_signature
    signature = infer_signature(X_train, y_pred_proba)
    input_example = X_train.head(5)
    
    mlflow.sklearn.log_model(
        model, 
        "model", 
        registered_model_name=f"{MODEL_REGISTRY_NAME_PREFIX}_propensity",
        signature=signature,
        input_example=input_example
    )
    
print("\n✓ Modelo treinado")
for k, v in metrics.items():
    print(f"  {k}: {v:.4f}")

# COMMAND ----------

# DBTITLE 1,Salvar Scores
# Score todos os clientes
# predict_proba()[:, 1] = Probabilidade da classe positiva (comprou = 1)
# Isso é o "propensity score" - quanto maior, mais propensão de compra
X_all = df_pandas[feature_cols]
propensity_scores = model.predict_proba(X_all)[:, 1]  # Valores entre 0.0 e 1.0

df_pandas["propensity_score"] = propensity_scores
df_pandas["propensity_category"] = pd.cut(propensity_scores, bins=[0, 0.3, 0.7, 1.0], labels=["Low", "Medium", "High"])

df_scores = df_pandas[["customer_id", "propensity_score", "propensity_category"]]
df_scores_spark = spark.createDataFrame(df_scores)
create_or_replace_table(df_scores_spark, SCHEMA_GOLD, "propensity_scores")

print(f"\n✓ Scores salvos: {get_full_table_name(SCHEMA_GOLD, 'propensity_scores')}")
print(f"\nDistribuição:")
print(df_scores["propensity_category"].value_counts())
