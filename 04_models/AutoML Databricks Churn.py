# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Introdução - AutoML Databricks
# MAGIC %md
# MAGIC # AutoML Databricks - Churn Prediction
# MAGIC
# MAGIC ## Objetivo
# MAGIC Utilizar o **Databricks AutoML** para treinar automaticamente modelos de classificação para prever churn de clientes, comparando resultados com o modelo XGBoost manual desenvolvido anteriormente.
# MAGIC
# MAGIC ## AutoML do Databricks
# MAGIC O AutoML automatiza:
# MAGIC - **Feature Engineering**: Transformações automáticas
# MAGIC - **Seleção de Algoritmos**: Testa múltiplos algoritmos (LightGBM, XGBoost, Random Forest, etc)
# MAGIC - **Hyperparameter Tuning**: Otimização automática de hiperparâmetros
# MAGIC - **Model Selection**: Escolhe o melhor modelo baseado em métricas
# MAGIC
# MAGIC ## Comparação com Modelo Manual
# MAGIC | Aspecto | Modelo Manual (XGBoost) | AutoML |
# MAGIC |---------|------------------------|--------|
# MAGIC | **AUC** | 0.9411 | ? |
# MAGIC | **Precision** | 0.89 | ? |
# MAGIC | **Recall** | 0.87 | ? |
# MAGIC | **Tempo de dev** | ~2h | ~15 min |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Vantagens do AutoML:**
# MAGIC - ✅ Rápido desenvolvimento
# MAGIC - ✅ Testa múltiplos algoritmos
# MAGIC - ✅ Otimização automática
# MAGIC - ✅ Notebooks gerados automaticamente
# MAGIC
# MAGIC **Quando usar modelo manual:**
# MAGIC - ✅ Controle fino sobre features
# MAGIC - ✅ Explicabilidade customizada
# MAGIC - ✅ Requisitos de negócio específicos

# COMMAND ----------

# DBTITLE 1,Setup e Configuração
# Configs
from pyspark.sql import functions as F
import pandas as pd

CATALOG = "customer_intelligence"
SCHEMA_GOLD = "gold"

print("✓ Setup OK")
print(f"  Catalog: {CATALOG}")

# COMMAND ----------

# DBTITLE 1,Preparar Dados para AutoML
# Carregar dados de features e labels
df_features = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.customer_features")
df_labels = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.churn_labels")

# Renomear colunas duplicadas em df_labels para evitar ambiguidade
df_labels = df_labels.withColumnRenamed("recency_days", "recency_days_label") \
                     .withColumnRenamed("frequency", "frequency_label")

# Join features com labels
df_automl = df_features.join(df_labels, "customer_id", "inner")

# Selecionar features relevantes (mesmo dataset do modelo manual)
feature_cols = [
    "recency_days",  # recency_days da tabela de features
    "frequency",     # frequency da tabela de features
    "monetary_total",
    "monetary_avg",
    "customer_lifetime_days",
    "event_count_30d",
    "session_count_30d",
    "engagement_score_30d",
    "page_views_30d",
    "product_views_30d",
    "add_to_cart_30d",
    "total_campaigns_exposed",
    "response_rate",
    "conversion_rate"
]

# Usar 'churn_label' como target (substituindo 'will_churn')
df_automl_final = df_automl.select(
    ["customer_id"] + [f"customer_features.{col}" for col in feature_cols] + ["churn_label"]
).withColumnRenamed("churn_label", "will_churn")

print(f"✓ Dataset preparado para AutoML")
print(f"  Linhas: {df_automl_final.count():,}")
print(f"  Features: {len(feature_cols)}")
print(f"  Target: will_churn")

# Visualizar sample
display(df_automl_final.limit(5))

# COMMAND ----------

# DBTITLE 1,Distribuição do Target
# Verificar balanço das classes
target_distribution = df_automl_final.groupBy("will_churn").count().toPandas()

print("Distribuição do Target:")
for _, row in target_distribution.iterrows():
    pct = (row['count'] / df_automl_final.count()) * 100
    label = "Churn" if row['will_churn'] == 1 else "Não-Churn"
    print(f"  {label}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Executar AutoML
# Executar AutoML via UI: 
# No Databricks, use o menu "Machine Learning" > "AutoML" para iniciar experimentos.
# Alternativamente, use o notebook gerado automaticamente pelo AutoML.

print("❌ AutoML não disponível via API nesta versão do Databricks Runtime.")
print("➡️ Execute AutoML pelo menu: Machine Learning > AutoML > Create Experiment")
print("➡️ Ou utilize o notebook gerado automaticamente pelo AutoML para análise dos resultados.")





### Forma alternativa não rodar este notebook 


import databricks.automl

# Executar AutoML para classificação
summary = databricks.automl.classify(
    dataset=df_automl_final,
    target_col="will_churn",
    primary_metric="roc_auc",
    timeout_minutes=10,
    max_trials=10
)

print("\n✓ AutoML concluído!")
print(f"  Melhor modelo: {summary.best_trial.model_description}")
print(f"  Melhor AUC: {summary.best_trial.metrics['val_roc_auc_score']:.4f}")

# COMMAND ----------

# DBTITLE 1,Comparar com Modelo Manual
# Métricas do modelo manual (XGBoost)
manual_metrics = {
    "AUC": 0.9411,
    "Precision": 0.89,
    "Recall": 0.87,
    "F1-Score": 0.88
}

# Métricas do AutoML
automl_metrics = {
    "AUC": summary.best_trial.metrics.get('val_roc_auc_score', 0),
    "Precision": summary.best_trial.metrics.get('val_precision_score', 0),
    "Recall": summary.best_trial.metrics.get('val_recall_score', 0),
    "F1-Score": summary.best_trial.metrics.get('val_f1_score', 0)
}

# Tabela comparativa
import pandas as pd
comparison_df = pd.DataFrame({
    "Métrica": list(manual_metrics.keys()),
    "Modelo Manual (XGBoost)": list(manual_metrics.values()),
    "AutoML": list(automl_metrics.values())
})

comparison_df['Diferença'] = comparison_df['AutoML'] - comparison_df['Modelo Manual (XGBoost)']

print("\n=" * 70)
print("COMPARAÇÃO: MODELO MANUAL vs AUTOML")
print("=" * 70)
print(comparison_df.to_string(index=False))
print("\n")

# Vencedor
if automl_metrics['AUC'] > manual_metrics['AUC']:
    print("✅ AutoML superou o modelo manual!")
else:
    print("✅ Modelo manual ainda é superior (controle fino compensa)")

# COMMAND ----------

# DBTITLE 1,Conclusões e Próximos Passos
# MAGIC %md
# MAGIC ## 🎯 Conclusões
# MAGIC
# MAGIC ### Quando usar AutoML:
# MAGIC - ✅ **Prototipagem rápida**: Validar hipóteses rapidamente
# MAGIC - ✅ **Baseline forte**: Estabelecer linha de base para modelos customizados
# MAGIC - ✅ **Exploração de algoritmos**: Descobrir qual algoritmo funciona melhor
# MAGIC - ✅ **Projetos com deadline curto**: Entregar valor rapidamente
# MAGIC
# MAGIC ### Quando usar modelo manual:
# MAGIC - ✅ **Requisitos específicos**: Controle fino sobre features e hiperparâmetros
# MAGIC - ✅ **Explicabilidade**: Necessidade de interpretar decisões do modelo
# MAGIC - ✅ **Otimização avançada**: Ajustes finos para máxima performance
# MAGIC - ✅ **Compliance**: Requisitos regulatórios específicos
# MAGIC
# MAGIC ### Abordagem Híbrida (Recomendada):
# MAGIC 1. **Fase 1**: AutoML para baseline e exploração (1-2 dias)
# MAGIC 2. **Fase 2**: Modelo manual para otimização (1-2 semanas)
# MAGIC 3. **Resultado**: Melhor dos dois mundos
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Resultados Obtidos
# MAGIC
# MAGIC | Critério | Modelo Manual | AutoML | Vencedor |
# MAGIC |-----------|---------------|--------|----------|
# MAGIC | **Performance** | AUC 0.9411 | Varia | - |
# MAGIC | **Tempo de dev** | 2h | 15 min | AutoML |
# MAGIC | **Explicação** | Alta | Média | Manual |
# MAGIC | **Manutenção** | Manual | Automatizada | AutoML |
# MAGIC | **Controle** | Total | Limitado | Manual |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Próximos Passos
# MAGIC
# MAGIC 1. **Registrar melhor modelo** no MLflow Model Registry
# MAGIC 2. **Deploy em produção** (batch ou real-time)
# MAGIC 3. **Monitorar performance** (drift detection)
# MAGIC 4. **Re-treinar periodicamente** (monthly)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Notebook gerado automaticamente pelo AutoML disponível no MLflow Experiments.**

# COMMAND ----------


