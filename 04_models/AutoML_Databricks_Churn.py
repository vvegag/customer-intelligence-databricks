# Databricks notebook source
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup e Configuração
# MAGIC
# MAGIC from pyspark.sql import functions as F
# MAGIC import pandas as pd
# MAGIC
# MAGIC CATALOG = "customer_intelligence"
# MAGIC SCHEMA_GOLD = "gold"
# MAGIC
# MAGIC print("✓ Setup OK")
# MAGIC print(f"  Catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparar Dados para AutoML
# MAGIC
# MAGIC # Carregar dados de features e labels
# MAGIC df_features = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.customer_features")
# MAGIC df_labels = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.churn_labels")
# MAGIC
# MAGIC # Join features com labels
# MAGIC df_automl = df_features.join(df_labels, "customer_id", "inner")
# MAGIC
# MAGIC # Selecionar features relevantes (mesmo dataset do modelo manual)
# MAGIC feature_cols = [
# MAGIC     "recency_days",
# MAGIC     "frequency", 
# MAGIC     "monetary_total",
# MAGIC     "monetary_avg",
# MAGIC     "customer_lifetime_days",
# MAGIC     "event_count_30d",
# MAGIC     "session_count_30d",
# MAGIC     "engagement_score_30d",
# MAGIC     "page_views_30d",
# MAGIC     "product_views_30d",
# MAGIC     "add_to_cart_30d",
# MAGIC     "total_campaigns_exposed",
# MAGIC     "response_rate",
# MAGIC     "conversion_rate"
# MAGIC ]
# MAGIC
# MAGIC df_automl_final = df_automl.select(
# MAGIC     ["customer_id"] + feature_cols + ["will_churn"]
# MAGIC )
# MAGIC
# MAGIC print(f"✓ Dataset preparado para AutoML")
# MAGIC print(f"  Linhas: {df_automl_final.count():,}")
# MAGIC print(f"  Features: {len(feature_cols)}")
# MAGIC print(f"  Target: will_churn")
# MAGIC
# MAGIC # Visualizar sample
# MAGIC df_automl_final.limit(5).display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribuição do Target
# MAGIC
# MAGIC # Verificar balanço das classes
# MAGIC target_distribution = df_automl_final.groupBy("will_churn").count().toPandas()
# MAGIC
# MAGIC print("Distribuição do Target:")
# MAGIC for _, row in target_distribution.iterrows():
# MAGIC     pct = (row['count'] / df_automl_final.count()) * 100
# MAGIC     label = "Churn" if row['will_churn'] == 1 else "Não-Churn"
# MAGIC     print(f"  {label}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executar AutoML
# MAGIC
# MAGIC **Nota:** A execução do AutoML pode levar alguns minutos. O Databricks irá:
# MAGIC - Testar múltiplos algoritmos (LightGBM, XGBoost, Random Forest, etc)
# MAGIC - Otimizar hiperparâmetros automaticamente
# MAGIC - Gerar notebooks com o código do melhor modelo
# MAGIC - Registrar experimentos no MLflow
# MAGIC
# MAGIC import databricks.automl
# MAGIC
# MAGIC # Executar AutoML para classificação
# MAGIC summary = databricks.automl.classify(
# MAGIC     dataset=df_automl_final,
# MAGIC     target_col="will_churn",
# MAGIC     primary_metric="roc_auc",
# MAGIC     timeout_minutes=10,
# MAGIC     max_trials=10
# MAGIC )
# MAGIC
# MAGIC print("\n✓ AutoML concluído!")
# MAGIC print(f"  Melhor modelo: {summary.best_trial.model_description}")
# MAGIC print(f"  Melhor AUC: {summary.best_trial.metrics['val_roc_auc_score']:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparar com Modelo Manual
# MAGIC
# MAGIC # Métricas do modelo manual (XGBoost)
# MAGIC manual_metrics = {
# MAGIC     "AUC": 0.9411,
# MAGIC     "Precision": 0.89,
# MAGIC     "Recall": 0.87,
# MAGIC     "F1-Score": 0.88
# MAGIC }
# MAGIC
# MAGIC # Métricas do AutoML
# MAGIC automl_metrics = {
# MAGIC     "AUC": summary.best_trial.metrics.get('val_roc_auc_score', 0),
# MAGIC     "Precision": summary.best_trial.metrics.get('val_precision_score', 0),
# MAGIC     "Recall": summary.best_trial.metrics.get('val_recall_score', 0),
# MAGIC     "F1-Score": summary.best_trial.metrics.get('val_f1_score', 0)
# MAGIC }
# MAGIC
# MAGIC # Tabela comparativa
# MAGIC import pandas as pd
# MAGIC comparison_df = pd.DataFrame({
# MAGIC     "Métrica": list(manual_metrics.keys()),
# MAGIC     "Modelo Manual (XGBoost)": list(manual_metrics.values()),
# MAGIC     "AutoML": list(automl_metrics.values())
# MAGIC })
# MAGIC
# MAGIC comparison_df['Diferença'] = comparison_df['AutoML'] - comparison_df['Modelo Manual (XGBoost)']
# MAGIC
# MAGIC print("\n" + "=" * 70)
# MAGIC print("COMPARAÇÃO: MODELO MANUAL vs AUTOML")
# MAGIC print("=" * 70)
# MAGIC print(comparison_df.to_string(index=False))
# MAGIC print("\n")
# MAGIC
# MAGIC # Vencedor
# MAGIC if automl_metrics['AUC'] > manual_metrics['AUC']:
# MAGIC     print("✅ AutoML superou o modelo manual!")
# MAGIC else:
# MAGIC     print("✅ Modelo manual ainda é superior (controle fino compensa)")

# COMMAND ----------

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
