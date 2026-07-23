# Databricks notebook source
# DBTITLE 1,Modelo de Churn Prediction
# MAGIC %md
# MAGIC # Churn Prediction Model
# MAGIC
# MAGIC ## Objetivo
# MAGIC Prever quais clientes têm maior probabilidade de churn (cancelamento/inatividade)
# MAGIC
# MAGIC ## Abordagem
# MAGIC - **Algoritmo**: XGBoost Classifier
# MAGIC - **Features**: RFM + Behavioral + Campaign History
# MAGIC - **Target**: churn_label (0 = ativo, 1 = churn)
# MAGIC - **Métricas**: AUC-ROC, Precision, Recall, F1
# MAGIC - **MLflow**: Rastreamento de experimentos e registro de modelo

# COMMAND ----------

# DBTITLE 1,Instalar Dependências
# MAGIC %pip install xgboost lightgbm scikit-learn --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuração
# Configurações globais do projeto (inline - sem usar %run)
import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
import random

# Configurações de catálogo e schema
CATALOG = "customer_intelligence"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# Configurações MLflow (usuário detectado dinamicamente, funciona em qualquer conta)
CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]
MLFLOW_EXPERIMENT_PATH = f"/Users/{CURRENT_USER}/customer_intelligence_experiments"
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

def log_metrics_to_mlflow(metrics_dict, step=None):
    import mlflow
    for key, value in metrics_dict.items():
        mlflow.log_metric(key, value, step=step)

# ML imports
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from sklearn.metrics import (
    roc_auc_score, accuracy_score, precision_score, 
    recall_score, f1_score, classification_report, confusion_matrix
)

import warnings
warnings.filterwarnings('ignore')

print("✓ Configuração carregada")
print(f"  Catalog: {CATALOG}")
print(f"  Experiment: {MLFLOW_EXPERIMENT_PATH}")

# COMMAND ----------

# DBTITLE 1,1. Carregar e Preparar Dados
# Carregar features e target
df_features = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_features"))
df_labels = spark.table(get_full_table_name(SCHEMA_GOLD, "churn_labels"))

# Juntar features com labels
df_model = df_features.join(df_labels.select("customer_id", "churn_label"), "customer_id", "inner")

# Selecionar features relevantes (excluir IDs, timestamps, etc)
feature_cols = [
    # Demographics
    "age", "customer_age_days",
    
    # RFM
    "recency_days", "frequency", "monetary_total", "monetary_avg",
    "customer_lifetime_days", "purchase_frequency_per_day",
    "unique_products_purchased", "total_items_purchased",
    
    # Behavioral 30d
    "event_count_30d", "session_count_30d", "engagement_score_30d",
    "page_views_30d", "product_views_30d", "add_to_cart_30d",
    
    # Behavioral 60d
    "event_count_60d", "session_count_60d", "engagement_score_60d",
    
    # Behavioral 90d
    "event_count_90d", "session_count_90d", "engagement_score_90d",
    
    # Campaign
    "total_campaigns_exposed", "treatment_campaigns_count",
    "total_responses", "total_conversions",
    "response_rate", "conversion_rate"
]

target_col = "churn_label"

print(f"✓ Features selecionadas: {len(feature_cols)}")
print(f"   Features: {feature_cols[:5]}... (+{len(feature_cols)-5} mais)")

# COMMAND ----------

# DBTITLE 1,2. Preparar Dataset para Treino
# Converter para Pandas — traz os dados pro driver, teto real de escala (hoje
# trivial com N=10k clientes). Quando isso deixar de caber num único node, o
# caminho de migração é o treino distribuído já demonstrado em
# production/models/sparkml_distributed.py (SparkML nativo, sem trazer nada
# pro driver), não paralelizar manualmente scikit-learn/XGBoost aqui.
df_pandas = df_model.select(["customer_id"] + feature_cols + [target_col]).toPandas()

print(f"\nDataset shape: {df_pandas.shape}")
print(f"\nDistribuição do target:")
print(df_pandas[target_col].value_counts())
print(f"\nTaxa de churn: {df_pandas[target_col].mean():.2%}")

# Separar features e target
X = df_pandas[feature_cols]
y = df_pandas[target_col]
customer_ids = df_pandas["customer_id"]

# Split train/test (80/20)
X_train, X_test, y_train, y_test, ids_train, ids_test = train_test_split(
    X, y, customer_ids, 
    test_size=0.2, 
    random_state=42, 
    stratify=y
)

print(f"\n✓ Train set: {X_train.shape}")
print(f"✓ Test set: {X_test.shape}")

# COMMAND ----------

# DBTITLE 1,3. Treinar Modelo com MLflow
# Treinar modelo e registrar no MLflow (compatível com serverless)
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
import tempfile
import os

print("Iniciando treino do modelo...")

# Parâmetros do modelo
params = {
    "n_estimators": 100,
    "max_depth": 6,
    "learning_rate": 0.1,
    "min_child_weight": 1,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "random_state": 42
}

# Treinar modelo
print("\nTreinando modelo XGBoost...")
model = XGBClassifier(**params)
model.fit(X_train, y_train)
print("✓ Modelo treinado")

# Predições
y_pred = model.predict(X_test)
y_pred_proba = model.predict_proba(X_test)[:, 1]

# Calcular métricas
metrics = {
    "auc_roc": roc_auc_score(y_test, y_pred_proba),
    "accuracy": accuracy_score(y_test, y_pred),
    "precision": precision_score(y_test, y_pred),
    "recall": recall_score(y_test, y_pred),
    "f1_score": f1_score(y_test, y_pred)
}

print("\n" + "="*60)
print("MÉTRICAS DO MODELO")
print("="*60)
for metric, value in metrics.items():
    print(f"{metric}: {value:.4f}")

# Feature importance
feature_importance = pd.DataFrame({
    "feature": feature_cols,
    "importance": model.feature_importances_
}).sort_values("importance", ascending=False)

print("\n" + "="*60)
print("TOP 10 FEATURES MAIS IMPORTANTES")
print("="*60)
print(feature_importance.head(10).to_string(index=False))

# COMMAND ----------

# DBTITLE 1,3.1 Comparação com LightGBM
# Comparação exploratória: mesmo split, mesmos hiperparâmetros equivalentes,
# outro algoritmo de gradient boosting. O modelo registrado no Model Registry
# continua sendo o XGBoost acima — isso aqui é só benchmark/curiosidade, não
# promove o LightGBM a lugar nenhum. Como o dataset é sintético, "quem ganhou"
# não prova nada sobre o mundo real — o valor está em saber comparar com
# rigor, não na conclusão em si.
print("\nTreinando modelo LightGBM (comparação)...")
lgbm_params = {
    "n_estimators": 100,
    "max_depth": 6,
    "learning_rate": 0.1,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "random_state": 42,
    "verbose": -1
}
model_lgbm = LGBMClassifier(**lgbm_params)
model_lgbm.fit(X_train, y_train)

y_pred_lgbm = model_lgbm.predict(X_test)
y_pred_proba_lgbm = model_lgbm.predict_proba(X_test)[:, 1]

metrics_lgbm = {
    "auc_roc": roc_auc_score(y_test, y_pred_proba_lgbm),
    "accuracy": accuracy_score(y_test, y_pred_lgbm),
    "precision": precision_score(y_test, y_pred_lgbm),
    "recall": recall_score(y_test, y_pred_lgbm),
    "f1_score": f1_score(y_test, y_pred_lgbm)
}

comparison_df = pd.DataFrame({
    "Métrica": list(metrics.keys()),
    "XGBoost (champion)": list(metrics.values()),
    "LightGBM (comparação)": list(metrics_lgbm.values())
})
comparison_df["Diferença"] = comparison_df["LightGBM (comparação)"] - comparison_df["XGBoost (champion)"]

print("\n" + "="*60)
print("XGBOOST vs LIGHTGBM")
print("="*60)
print(comparison_df.to_string(index=False))
print("\n⚠️ Dataset sintético: diferença de performance aqui não generaliza para")
print("   dados reais — o objetivo é demonstrar comparação rigorosa entre algoritmos.")

# COMMAND ----------

# Criar signature do modelo
signature = infer_signature(X_train, model.predict_proba(X_train))
input_example = X_train.iloc[:5]

# Registrar modelo no Unity Catalog Model Registry (não o registry legado por
# workspace, que falha no serverless por depender de spark.mlflow.modelRegistryUri
# — ver docs/05_MIGRATION.md para o histórico desse bug). Usa alias Champion/
# Challenger em vez de número de versão fixo: quem consome o modelo (SHAP,
# Batch Scoring, Model Serving, Retraining) sempre aponta pro mesmo nome+alias
# (models:/{model_name}@Champion), sem precisar saber a versão exata.
print("\nRegistrando modelo no Unity Catalog Model Registry...")
mlflow.set_registry_uri("databricks-uc")
model_name = f"{CATALOG}.{SCHEMA_GOLD}.churn_model"

with mlflow.start_run(run_name="churn_xgboost_v1") as run:
    mlflow.log_params(params)
    mlflow.log_metrics(metrics)
    model_info = mlflow.sklearn.log_model(
        model,
        "model",
        signature=signature,
        input_example=input_example,
        registered_model_name=model_name
    )
    run_id = run.info.run_id

from mlflow.tracking import MlflowClient
client = MlflowClient()
try:
    current_champion = client.get_model_version_by_alias(model_name, "champion")
    client.set_registered_model_alias(model_name, "challenger", current_champion.version)
    print(f"✓ Champion anterior (v{current_champion.version}) rebaixado para challenger")
except Exception:
    print("ℹ️ Primeira execução — ainda não existia um champion registrado")

client.set_registered_model_alias(model_name, "champion", model_info.registered_model_version)
print(f"✓ Modelo registrado: {model_name}@champion (v{model_info.registered_model_version})")

print("\n" + "="*60)
print("RESULTADOS DO MODELO")
print("="*60)
for metric, value in metrics.items():
    print(f"{metric}: {value:.4f}")

print("\n" + "="*60)
print("TOP 10 FEATURES MAIS IMPORTANTES")
print("="*60)
print(feature_importance.head(10).to_string(index=False))

print(f"\n✓ Run ID: {run_id}")
print(f"✓ Modelo disponível em: models:/{model_name}@champion")

# COMMAND ----------

# DBTITLE 1,4. Matriz de Confusão e Relatório
# Matriz de confusão
print("\n" + "="*60)
print("MATRIZ DE CONFUSÃO")
print("="*60)
cm = confusion_matrix(y_test, y_pred)
print("\n               Previsto")
print("               0      1")
print(f"Real    0    {cm[0][0]:5d}  {cm[0][1]:5d}")
print(f"        1    {cm[1][0]:5d}  {cm[1][1]:5d}")

# Relatório de classificação
print("\n" + "="*60)
print("RELATÓRIO DE CLASSIFICAÇÃO")
print("="*60)
print(classification_report(y_test, y_pred, target_names=["Não Churn", "Churn"]))

# COMMAND ----------

# DBTITLE 1,5. Salvar Predições
# Criar DataFrame com predições do conjunto de teste
df_predictions = pd.DataFrame({
    "customer_id": ids_test.values,
    "actual_churn": y_test.values,
    "predicted_churn": y_pred,
    "churn_probability": y_pred_proba,
    "model_version": "v1",
    "prediction_date": pd.Timestamp.now()
})

# Classificar por probabilidade (maiores riscos primeiro)
df_predictions = df_predictions.sort_values("churn_probability", ascending=False)

# Adicionar categoria de risco
df_predictions["risk_category"] = pd.cut(
    df_predictions["churn_probability"],
    bins=[0, 0.3, 0.6, 1.0],
    labels=["Low", "Medium", "High"]
)

print("\n✓ Predições criadas")
print(f"   Total: {len(df_predictions):,}")
print("\nTop 10 clientes com maior risco de churn:")
print(df_predictions.head(10).to_string(index=False))

# Converter para Spark e salvar
df_predictions_spark = spark.createDataFrame(df_predictions)
create_or_replace_table(df_predictions_spark, SCHEMA_GOLD, "churn_predictions")

print(f"\n✓ Predições salvas em: {get_full_table_name(SCHEMA_GOLD, 'churn_predictions')}")

# COMMAND ----------

# DBTITLE 1,Resumo do Modelo
print("\n" + "="*60)
print("MODELO CHURN - RESUMO COMPLETO")
print("="*60)
print(f"\n✅ Modelo treinado: XGBoost Classifier")
print(f"   - Features: {len(feature_cols)}")
print(f"   - Train samples: {len(X_train):,}")
print(f"   - Test samples: {len(X_test):,}")
print(f"\n✅ Métricas de performance:")
for metric, value in metrics.items():
    print(f"   - {metric}: {value:.4f}")
print(f"\n✅ Modelo registrado no Unity Catalog Model Registry:")
print(f"   - Registry name: {model_name}@champion")
print(f"   - Run ID: {run_id}")
print(f"\n✅ Predições salvas:")
print(f"   - Tabela: {get_full_table_name(SCHEMA_GOLD, 'churn_predictions')}")
print(f"   - Registros: {len(df_predictions):,}")
print("\n" + "="*60)
print("✓ MODELO PRONTO PARA PRODUÇÃO")
print("="*60)

# COMMAND ----------


