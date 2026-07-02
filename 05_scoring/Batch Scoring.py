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

# DBTITLE 1,Configuração
# MAGIC %run "../00_setup/Config e Setup Inicial"
# MAGIC
# MAGIC import mlflow
# MAGIC import mlflow.sklearn
# MAGIC from pyspark.sql import functions as F
# MAGIC import pandas as pd
# MAGIC import warnings
# MAGIC warnings.filterwarnings('ignore')
# MAGIC
# MAGIC print("✓ Configuração carregada")

# COMMAND ----------

# DBTITLE 1,1. Carregar Modelo de Churn do MLflow
# Carregar modelo registrado
model_name = f"{MODEL_REGISTRY_NAME_PREFIX}_churn"
model_version = get_latest_model_version(model_name)

if model_version:
    model_uri = f"models:/{model_name}/{model_version}"
    model = mlflow.sklearn.load_model(model_uri)
    print(f"✓ Modelo carregado: {model_name} v{model_version}")
else:
    print("✗ Modelo não encontrado. Execute o notebook de treino primeiro.")
    raise Exception("Modelo não encontrado")

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
print(f"\nDistribuição de risco:")
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
print(f"\nTop 10 clientes de maior risco:")
print(df_scores.nlargest(10, "churn_probability")[["customer_id", "churn_probability", "churn_risk_category", "customer_value_score"]].to_string(index=False))

# COMMAND ----------

# DBTITLE 1,Resumo de Scoring
print("\n" + "="*60)
print("BATCH SCORING - RESUMO")
print("="*60)
print(f"\n✅ Modelo: {model_name} v{model_version}")
print(f"✅ Clientes scored: {len(df_scores):,}")
print(f"\n✅ Distribuição de risco de churn:")
for category in ["Low", "Medium", "High"]:
    count = (df_scores["churn_risk_category"] == category).sum()
    pct = count / len(df_scores) * 100
    print(f"   - {category}: {count:,} ({pct:.1f}%)")
print(f"\n✅ Scores salvos em: {get_full_table_name(SCHEMA_GOLD, 'customer_scores')}")
print("\n" + "="*60)
print("✓ SCORING COMPLETO")
print("="*60)

# COMMAND ----------

