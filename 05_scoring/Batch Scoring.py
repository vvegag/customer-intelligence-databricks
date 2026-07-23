# Databricks notebook source
# DBTITLE 1,Batch Scoring - Geração de Scores em Lote
# MAGIC %md
# MAGIC # Batch Scoring
# MAGIC
# MAGIC Geração de scores em lote para toda a base de clientes.
# MAGIC
# MAGIC ## Scores Gerados:
# MAGIC 1. **Churn Score**: Probabilidade de churn
# MAGIC 2. **Propensity Score**: Probabilidade de compra
# MAGIC 3. **Customer Value Score**: Valor estimado do cliente
# MAGIC 4. **Engagement Score**: Nível de engajamento
# MAGIC
# MAGIC ## Uso
# MAGIC Execute este notebook periodicamente (ex: semanalmente) para atualizar scores.

# COMMAND ----------

# DBTITLE 1,Instalar dependências
# MAGIC %pip install --upgrade numpy xgboost scikit-learn --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuração
# Configs inline
import pandas as pd
import mlflow
import mlflow.sklearn

CATALOG = "customer_intelligence"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]
MLFLOW_EXPERIMENT_PATH = f"/Users/{CURRENT_USER}/customer_intelligence_experiments"
MODEL_REGISTRY_NAME_PREFIX = "customer_intelligence"

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

import warnings
warnings.filterwarnings('ignore')

print("✓ Configuração carregada")
print(f"  Catalog: {CATALOG}")

# COMMAND ----------

# DBTITLE 1,1. Carregar Modelo de Churn do Model Registry
# Carrega o modelo Champion direto do Unity Catalog Model Registry (não mais um
# pickle num Volume — esse caminho nunca funcionou de forma confiável, ver
# docs/05_MIGRATION.md para o histórico). O nome/alias tem que bater exatamente
# com o que "Modelo Churn Prediction.py" registra.
from mlflow.tracking import MlflowClient

mlflow.set_registry_uri("databricks-uc")

model_name = f"{CATALOG}.{SCHEMA_GOLD}.churn_model"
model_uri = f"models:/{model_name}@champion"

print(f"Carregando modelo: {model_uri}")
model = mlflow.sklearn.load_model(model_uri)

client = MlflowClient()
champion_version = client.get_model_version_by_alias(model_name, "champion")
champion_metrics = client.get_run(champion_version.run_id).data.metrics
model_version = champion_version.version

print(f"✓ Modelo carregado: {model_uri} (v{model_version})")
print(f"  AUC-ROC: {champion_metrics.get('auc_roc', float('nan')):.4f}")

# COMMAND ----------

# DBTITLE 1,2. Preparar Features para Scoring
# Carregar features
df_features = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_features"))

# Features do modelo
feature_cols = [
    "age", "customer_age_days",
    "recency_days", "frequency", "monetary_total", "monetary_avg",
    "customer_lifetime_days", "purchase_frequency_per_day",
    "unique_products_purchased", "total_items_purchased",
    "event_count_30d", "session_count_30d", "engagement_score_30d",
    "page_views_30d", "product_views_30d", "add_to_cart_30d",
    "event_count_60d", "session_count_60d", "engagement_score_60d",
    "event_count_90d", "session_count_90d", "engagement_score_90d",
    "total_campaigns_exposed", "treatment_campaigns_count",
    "total_responses", "total_conversions",
    "response_rate", "conversion_rate"
]

df_to_score = df_features.select(["customer_id"] + feature_cols).fillna(0)
print(f"✓ Features preparadas: {df_to_score.count():,} clientes")

# COMMAND ----------

# DBTITLE 1,3. Gerar Scores de Churn
# Converter para pandas e fazer scoring
df_pandas = df_to_score.toPandas()
X = df_pandas[feature_cols]

# Prever
churn_probabilities = model.predict_proba(X)[:, 1]
churn_predictions = model.predict(X)

# Adicionar ao dataframe
df_pandas["churn_probability"] = churn_probabilities
df_pandas["churn_prediction"] = churn_predictions
df_pandas["churn_risk_category"] = pd.cut(
    df_pandas["churn_probability"],
    bins=[0, 0.3, 0.6, 1.0],
    labels=["Low", "Medium", "High"]
)
df_pandas["score_date"] = pd.Timestamp.now()
df_pandas["model_version"] = model_version

print("✓ Scores de churn gerados")
print("\nDistribuição de risco:")
print(df_pandas["churn_risk_category"].value_counts())

# COMMAND ----------

# DBTITLE 1,4. Calcular Scores Adicionais
# Customer Value Score (baseado em RFM)
df_pandas["customer_value_score"] = (
    (1 / (df_pandas["recency_days"] + 1)) * 100 +  # Menor recency = melhor
    df_pandas["frequency"] * 10 +
    df_pandas["monetary_total"] / 100
)

# Engagement Score (baseado em atividade)
df_pandas["engagement_score"] = (
    df_pandas["event_count_30d"] * 1.5 +
    df_pandas["session_count_30d"] * 2.0 +
    df_pandas["product_views_30d"] * 3.0 +
    df_pandas["add_to_cart_30d"] * 5.0
)

# Normalizar scores (0-100)
for score_col in ["customer_value_score", "engagement_score"]:
    max_val = df_pandas[score_col].max()
    if max_val > 0:
        df_pandas[score_col] = (df_pandas[score_col] / max_val * 100).round(2)

print("✓ Scores adicionais calculados")

# COMMAND ----------

# DBTITLE 1,5. Salvar Scores
# Selecionar colunas para salvar
df_scores = df_pandas[[
    "customer_id",
    "churn_probability",
    "churn_prediction",
    "churn_risk_category",
    "customer_value_score",
    "engagement_score",
    "score_date",
    "model_version"
]]

# Converter para Spark e salvar
df_scores_spark = spark.createDataFrame(df_scores)
create_or_replace_table(df_scores_spark, SCHEMA_GOLD, "customer_scores")

print(f"✓ Scores salvos: {get_full_table_name(SCHEMA_GOLD, 'customer_scores')}")
print("\nTop 10 clientes de maior risco:")
print(df_scores.nlargest(10, "churn_probability")[["customer_id", "churn_probability", "churn_risk_category", "customer_value_score"]].to_string(index=False))

# COMMAND ----------

# DBTITLE 1,Resumo de Scoring
print("\n" + "="*60)
print("BATCH SCORING - RESUMO")
print("="*60)
print(f"\n✅ Modelo: {model_version}")
print(f"✅ Clientes scored: {len(df_scores):,}")
print("\n✅ Distribuição de risco de churn:")
for category in ["Low", "Medium", "High"]:
    count = (df_scores["churn_risk_category"] == category).sum()
    pct = count / len(df_scores) * 100
    print(f"   - {category}: {count:,} ({pct:.1f}%)")
print(f"\n✅ Scores salvos em: {get_full_table_name(SCHEMA_GOLD, 'customer_scores')}")
print("\n" + "="*60)
print("✓ SCORING COMPLETO")
print("="*60)

# COMMAND ----------


