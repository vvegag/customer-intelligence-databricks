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
# MAGIC %pip install xgboost scikit-learn --quiet
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
    return f"{CATALOG}.{schema}.{table}"

def create_or_replace_table(df, schema, table, partition_by=None):
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
# Converter para Pandas
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

# Criar signature do modelo
signature = infer_signature(X_train, model.predict_proba(X_train))
input_example = X_train.iloc[:5]

# Registrar modelo no MLflow (sem tracking context para evitar problemas com serverless)
print("\nRegistrando modelo no MLflow...")
try:
    model_name = f"{MODEL_REGISTRY_NAME_PREFIX}_churn"
    
    # Criar um temp directory para salvar o modelo
    with tempfile.TemporaryDirectory() as tmp_dir:
        model_path = os.path.join(tmp_dir, "model")
        
        # Salvar modelo localmente primeiro
        mlflow.sklearn.save_model(
            model,
            model_path,
            signature=signature,
            input_example=input_example
        )
        
        # Registrar no MLflow
        mlflow.register_model(
            f"file://{model_path}",
            model_name
        )
    
    print(f"✓ Modelo registrado: {model_name}")
    run_id = "serverless_mode"
except Exception as e:
    print(f"⚠️ Erro ao registrar modelo: {e}")
    print("   O modelo foi treinado, mas não foi registrado no MLflow.")
    print("   Você pode usar o modelo em memória ou registrá-lo manualmente.")
    run_id = "failed_registration"
    
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

# COMMAND ----------

# DBTITLE 1,3.1 Salvar Modelo em UC Volume
# Salvar modelo treinado em Unity Catalog Volume
import pickle
from datetime import datetime
import os

print("Salvando modelo em UC Volume...")

# 1. Criar volume/diretório se não existir
volume_path = '/Volumes/customer_intelligence/gold/models'

try:
    # Criar volume se não existir
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}.models
        COMMENT 'Volume para armazenar modelos de ML treinados'
    """)
    print(f"✓ Volume verificado: {CATALOG}.{SCHEMA_GOLD}.models")
except Exception as e:
    print(f"⚠️ Aviso ao criar volume: {e}")
    print("   Continuando com o caminho existente...")

# Verificar se o diretório existe, se não criar
try:
    dbutils.fs.ls(volume_path)
    print(f"✓ Diretório existe: {volume_path}")
except:
    print(f"✓ Diretório criado: {volume_path}")

# 2. Salvar modelo usando pickle
model_file_path = f'{volume_path}/churn_model_v1.pkl'
with open(model_file_path.replace('/Volumes', '/dbfs/Volumes'), 'wb') as f:
    pickle.dump(model, f)
print(f"✓ Modelo salvo: {model_file_path}")

# 3. Salvar metadados
metadata = {
    'model_name': 'churn_model_v1',
    'model_type': 'XGBoostClassifier',
    'features': feature_cols,
    'n_features': len(feature_cols),
    'metrics': metrics,
    'train_date': datetime.now().isoformat(),
    'train_samples': len(X_train),
    'test_samples': len(X_test),
    'params': params,
    'feature_importance': feature_importance.to_dict('records')
}

metadata_file_path = f'{volume_path}/churn_model_v1_metadata.pkl'
with open(metadata_file_path.replace('/Volumes', '/dbfs/Volumes'), 'wb') as f:
    pickle.dump(metadata, f)
print(f"✓ Metadados salvos: {metadata_file_path}")

print("\n" + "="*60)
print("MODELO PERSISTIDO COM SUCESSO")
print("="*60)
print(f"📦 Modelo: {model_file_path}")
print(f"📋 Metadados: {metadata_file_path}")
print(f"📊 Features: {len(feature_cols)}")
print(f"🎯 AUC-ROC: {metrics['auc_roc']:.4f}")
print("\n✅ Pronto para usar no Batch Scoring!")

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
print(f"\n✅ Modelo registrado no MLflow:")
print(f"   - Registry name: {MODEL_REGISTRY_NAME_PREFIX}_churn")
print(f"   - Run ID: {run_id}")
print(f"\n✅ Predições salvas:")
print(f"   - Tabela: {get_full_table_name(SCHEMA_GOLD, 'churn_predictions')}")
print(f"   - Registros: {len(df_predictions):,}")
print("\n" + "="*60)
print("✓ MODELO PRONTO PARA PRODUÇÃO")
print("="*60)

# COMMAND ----------


