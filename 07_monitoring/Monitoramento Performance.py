# Databricks notebook source
# DBTITLE 1,Monitoramento de Performance e Drift
# MAGIC %md
# MAGIC # Monitoramento de Performance e Drift
# MAGIC
# MAGIC ## Objetivo
# MAGIC Monitorar a saúde dos modelos e dados ao longo do tempo:
# MAGIC - **Data Drift**: Mudanças nas distribuições de features
# MAGIC - **Concept Drift**: Mudanças na relação features-target
# MAGIC - **Model Performance**: Métricas de performance ao longo do tempo
# MAGIC - **Business Metrics**: KPIs de negócio
# MAGIC
# MAGIC ## Métricas Monitoradas:
# MAGIC 1. Distribuição de features chave
# MAGIC 2. Taxa de churn real vs prevista
# MAGIC 3. Taxa de conversão de campanhas
# MAGIC 4. Receita por cliente
# MAGIC 5. Engajamento geral

# COMMAND ----------

# DBTITLE 1,Configuração
# MAGIC %run "../00_setup/Config e Setup Inicial"
# MAGIC
# MAGIC from pyspark.sql import functions as F, Window
# MAGIC import pandas as pd
# MAGIC import warnings
# MAGIC warnings.filterwarnings('ignore')
# MAGIC
# MAGIC print("✓ Configuração carregada")

# COMMAND ----------

# DBTITLE 1,1. Monitorar Distribuição de Features (Data Drift)
# Carregar features
df_features = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_features"))

# Features chave para monitorar
key_features = ["recency_days", "frequency", "monetary_total", "engagement_score_30d"]

# Calcular estatísticas descritivas
feature_stats = []
for feature in key_features:
    stats = df_features.select(
        F.lit(feature).alias("feature_name"),
        F.mean(feature).alias("mean"),
        F.stddev(feature).alias("stddev"),
        F.min(feature).alias("min"),
        F.expr(f"percentile({feature}, 0.25)").alias("p25"),
        F.expr(f"percentile({feature}, 0.50)").alias("median"),
        F.expr(f"percentile({feature}, 0.75)").alias("p75"),
        F.max(feature).alias("max"),
        F.current_timestamp().alias("snapshot_date")
    ).collect()[0]
    feature_stats.append(stats)

df_feature_stats = spark.createDataFrame(feature_stats)

print("\n" + "="*80)
print("ESTATÍSTICAS DE FEATURES (DATA DRIFT MONITORING)")
print("="*80)
df_feature_stats.show(truncate=False)

# Salvar para histórico
df_feature_stats.write.format("delta").mode("append").saveAsTable(
    get_full_table_name(SCHEMA_GOLD, "feature_drift_monitoring")
)
print("✓ Estatísticas salvas para monitoramento")

# COMMAND ----------

# DBTITLE 1,2. Monitorar Taxa de Churn Real vs Prevista
# Comparar churn real vs previsto
df_labels = spark.table(get_full_table_name(SCHEMA_GOLD, "churn_labels"))
df_scores = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_scores"))

df_comparison = df_labels.join(df_scores, "customer_id", "inner")

# Calcular taxas
actual_churn_rate = df_comparison.agg(F.avg("churn_label")).collect()[0][0]
predicted_churn_rate = df_comparison.agg(F.avg("churn_prediction")).collect()[0][0]
avg_churn_probability = df_comparison.agg(F.avg("churn_probability")).collect()[0][0]

print("\n" + "="*80)
print("MONITORAMENTO DE CHURN")
print("="*80)
print(f"\nTaxa de churn real:      {actual_churn_rate:.2%}")
print(f"Taxa de churn prevista:  {predicted_churn_rate:.2%}")
print(f"Probabilidade média:     {avg_churn_probability:.2%}")
print(f"Diferença:                {abs(actual_churn_rate - predicted_churn_rate):.2%}")

if abs(actual_churn_rate - predicted_churn_rate) > 0.05:
    print("\n⚠️ ALERTA: Diferença significativa detectada - considere retreinar o modelo")
else:
    print("\n✓ Modelo alinhado com realidade")

# COMMAND ----------

# DBTITLE 1,3. Monitorar Performance de Campanhas ao Longo do Tempo
# Análise temporal de campanhas
df_responses = spark.table(get_full_table_name(SCHEMA_SILVER, "campaign_responses"))
df_exposures = spark.table(get_full_table_name(SCHEMA_SILVER, "campaign_exposures"))

# Performance por mês
df_campaign_trend = df_exposures.join(
    df_responses.select("exposure_id", "is_conversion"),
    "exposure_id",
    "left"
).withColumn("year_month", F.date_format("exposure_date", "yyyy-MM"))

df_monthly_performance = df_campaign_trend.groupBy("year_month").agg(
    F.count("exposure_id").alias("total_exposures"),
    F.sum(F.coalesce("is_conversion", F.lit(0))).alias("total_conversions"),
    (F.sum(F.coalesce("is_conversion", F.lit(0))) / F.count("exposure_id") * 100).alias("conversion_rate")
).orderBy("year_month")

print("\n" + "="*80)
print("PERFORMANCE DE CAMPANHAS POR MÊS")
print("="*80)
df_monthly_performance.show(12, truncate=False)

# Salvar trend
create_or_replace_table(df_monthly_performance, SCHEMA_GOLD, "campaign_performance_trend")
print("✓ Tendência de performance salva")

# COMMAND ----------

# DBTITLE 1,4. KPIs de Negócio
# Calcular KPIs gerais
df_customers = spark.table(get_full_table_name(SCHEMA_SILVER, "customers"))
df_transactions = spark.table(get_full_table_name(SCHEMA_SILVER, "transactions"))
df_events = spark.table(get_full_table_name(SCHEMA_SILVER, "behavioral_events"))

# KPIs
total_customers = df_customers.count()
active_customers = df_customers.filter(F.col("is_active") == 1).count()
churned_customers = df_customers.filter(F.col("is_churned") == 1).count()

total_revenue = df_transactions.agg(F.sum("total_amount")).collect()[0][0] or 0
avg_order_value = df_transactions.agg(F.avg("total_amount")).collect()[0][0] or 0

total_events = df_events.count()

kpis = {
    "snapshot_date": pd.Timestamp.now(),
    "total_customers": total_customers,
    "active_customers": active_customers,
    "churned_customers": churned_customers,
    "churn_rate_pct": round(churned_customers / total_customers * 100, 2) if total_customers > 0 else 0,
    "total_revenue": round(total_revenue, 2),
    "avg_revenue_per_customer": round(total_revenue / total_customers, 2) if total_customers > 0 else 0,
    "avg_order_value": round(avg_order_value, 2),
    "total_events": total_events
}

print("\n" + "="*80)
print("KPIs DE NEGÓCIO")
print("="*80)
for key, value in kpis.items():
    if key != "snapshot_date":
        print(f"{key}: {value:,}")

# Salvar KPIs para histórico
df_kpis = spark.createDataFrame([kpis])
df_kpis.write.format("delta").mode("append").saveAsTable(
    get_full_table_name(SCHEMA_GOLD, "business_kpis_history")
)
print("\n✓ KPIs salvos para monitoramento")

# COMMAND ----------

# DBTITLE 1,Resumo de Monitoramento
print("\n" + "="*80)
print("RESUMO DE MONITORAMENTO")
print("="*80)
print("\n✅ Monitoramento de drift executado")
print("   - Feature statistics salvos")
print("\n✅ Performance de modelo verificada")
print("   - Churn real vs previsto comparado")
print("\n✅ Tendências de campanha atualizadas")
print("   - Performance mensal calculada")
print("\n✅ KPIs de negócio atualizados")
print("   - Snapshot salvo para histórico")
print("\n" + "="*80)
print("✓ MONITORAMENTO COMPLETO")
print("="*80)
print("\n❗ Recomendações:")
print("   1. Execute este notebook semanalmente")
print("   2. Crie alertas para drift > threshold")
print("   3. Retreine modelos se drift significativo")
print("   4. Acompanhe KPIs em dashboard executivo")

# COMMAND ----------

