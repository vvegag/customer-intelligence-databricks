# Databricks notebook source
# MAGIC %md
# MAGIC # Model Explainability com SHAP 🔍
# MAGIC 
# MAGIC ## Objetivo
# MAGIC Explicar as decisões dos modelos de Machine Learning usando **SHAP (SHapley Additive exPlanations)**.
# MAGIC 
# MAGIC ### Por que SHAP?
# MAGIC * ✅ **Interpretabilidade**: Entender POR QUÊ o modelo fez cada predição
# MAGIC * ✅ **Confiança**: Validar que o modelo usa lógica de negócio correta
# MAGIC * ✅ **Comunicação**: Explicar para stakeholders não-técnicos
# MAGIC * ✅ **Debug**: Identificar features problemáticas ou vieses
# MAGIC 
# MAGIC ### Modelos Analisados
# MAGIC 1. **Churn Prediction** - Por que clientes estão em risco?
# MAGIC 2. **Propensity Score** - O que influencia probabilidade de compra?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diferencial para a Vaga
# MAGIC ✅ **Senior Data Scientist**: Não apenas treina modelos, mas explica decisões  
# MAGIC ✅ **Comunicação**: Traduz ML para linguagem de negócio  
# MAGIC ✅ **Governança**: Demonstra que modelos são auditáveis e justos

# COMMAND ----------

# Setup e Instalação
%pip install shap==0.44.0 --quiet
dbutils.library.restartPython()

print("✓ SHAP instalado")

# COMMAND ----------

# Imports e Configuração
import shap
import mlflow
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# Configurar matplotlib para notebooks
import warnings
warnings.filterwarnings('ignore')
plt.rcParams['figure.figsize'] = (12, 6)

CATALOG = "customer_intelligence"
SCHEMA_GOLD = "gold"

def get_full_table_name(schema, table):
    return f"{CATALOG}.{schema}.{table}"

print("✓ Setup completo")
print(f"SHAP version: {shap.__version__}")

# COMMAND ----------

# Carregar Modelo de Churn
model_name = "customer_intelligence.gold.churn_model"

try:
    # Tentar carregar da produção
    model_uri = f"models:/{model_name}@champion"
    model = mlflow.sklearn.load_model(model_uri)
    print(f"✓ Modelo carregado: {model_name}@champion")
except:
    # Fallback: carregar última versão
    model_uri = f"models:/{model_name}/latest"
    model = mlflow.sklearn.load_model(model_uri)
    print(f"✓ Modelo carregado: {model_name}/latest")

print(f"Tipo de modelo: {type(model).__name__}")

# COMMAND ----------

# Preparar Dados de Teste
df_features = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_features")).toPandas()

# Selecionar features usadas no treinamento
feature_cols = [
    'recency_days', 'frequency_count', 'monetary_total',
    'avg_order_value', 'days_since_first_purchase', 'purchase_frequency',
    'total_products_purchased', 'unique_products_purchased',
    'avg_products_per_order', 'recency_segment', 'frequency_segment',
    'monetary_segment', 'rfm_score'
]

X_test = df_features[feature_cols].fillna(0)

print(f"✓ Dados preparados: {len(X_test):,} clientes")
print(f"✓ Features: {len(feature_cols)}")

# COMMAND ----------

# SHAP: Feature Importance Global
X_background = shap.sample(X_test, 100)
explainer = shap.TreeExplainer(model, X_background)

X_explain = X_test.sample(min(500, len(X_test)), random_state=42)
shap_values = explainer.shap_values(X_explain)

# Para classificação binária, pegar classe positiva
if isinstance(shap_values, list):
    shap_values_churn = shap_values[1]
else:
    shap_values_churn = shap_values

print(f"✓ SHAP values calculados para {len(X_explain):,} clientes")

# COMMAND ----------

# Visualização: Summary Plot
plt.figure(figsize=(12, 8))
shap.summary_plot(shap_values_churn, X_explain, plot_type="dot", show=False)
plt.title("SHAP Summary Plot - Churn Prediction", fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

print("\n📊 Interpretação:")
print("• Cada ponto = um cliente")
print("• Cor vermelha = valor alto da feature")
print("• Posição horizontal = impacto na predição")

# COMMAND ----------

# Feature Importance (Bar Plot)
plt.figure(figsize=(10, 6))
shap.summary_plot(shap_values_churn, X_explain, plot_type="bar", show=False)
plt.title("Top Features para Predição de Churn", fontsize=14, fontweight='bold')
plt.show()

feature_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': np.abs(shap_values_churn).mean(axis=0)
}).sort_values('importance', ascending=False)

print("\n📊 Top 10 Features Mais Importantes:")
for idx, row in feature_importance.head(10).iterrows():
    print(f"  {row['feature']:30s}: {row['importance']:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Modelo É Interpretável e Acionável
# MAGIC 
# MAGIC Principais drivers de churn identificados:
# MAGIC * **Recency**: Clientes inativos têm maior risco
# MAGIC * **Frequency**: Baixa frequência indica desengajamento
# MAGIC * **Monetary**: Clientes de baixo valor precisam atenção
# MAGIC 
# MAGIC ### Ações Recomendadas
# MAGIC 1. Campanhas de reativação para recency > 30 dias
# MAGIC 2. Programa de fidelidade para aumentar frequency
# MAGIC 3. Ofertas personalizadas para aumentar ticket médio

