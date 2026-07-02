# Databricks notebook source
# DBTITLE 1,Segmentação de Clientes com Clustering
# MAGIC %md
# MAGIC # Customer Segmentation - K-Means Clustering
# MAGIC
# MAGIC ## Objetivo
# MAGIC Segmentar clientes em grupos homogêneos baseados em comportamento.
# MAGIC
# MAGIC ## Features para Clustering
# MAGIC - RFM (Recency, Frequency, Monetary)
# MAGIC - Engajamento
# MAGIC - Valor do cliente
# MAGIC
# MAGIC ## Saída
# MAGIC Segmentos interpretáveis para marketing direcionado.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run "../00_setup/Config e Setup Inicial"
# MAGIC
# MAGIC from pyspark.sql import functions as F
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC from sklearn.preprocessing import StandardScaler
# MAGIC from sklearn.cluster import KMeans
# MAGIC import warnings
# MAGIC warnings.filterwarnings('ignore')
# MAGIC
# MAGIC print("✓ Setup OK")

# COMMAND ----------

# DBTITLE 1,Preparar Features RFM
df_features = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_features"))

# Selecionar features para clustering
cluster_features = [
    "recency_days",
    "frequency",
    "monetary_total",
    "engagement_score_30d",
    "customer_lifetime_days"
]

df_pandas = df_features.select(["customer_id"] + cluster_features).fillna(0).toPandas()
X = df_pandas[cluster_features]

# Normalizar features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

print(f"✓ Features preparadas: {X.shape}")

# COMMAND ----------

# DBTITLE 1,Determinar Número Ótimo de Clusters (Elbow Method)
# Método do cotovelo
inertias = []
K_range = range(2, 11)

for k in K_range:
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    kmeans.fit(X_scaled)
    inertias.append(kmeans.inertia_)

print("\nInércia por número de clusters:")
for k, inertia in zip(K_range, inertias):
    print(f"  k={k}: {inertia:,.0f}")

# Escolher k=5 como balanço entre granularidade e interpretabilidade
optimal_k = 5
print(f"\n✓ K escolhido: {optimal_k}")

# COMMAND ----------

# DBTITLE 1,Treinar K-Means
# Treinar modelo final
kmeans = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
df_pandas["cluster"] = kmeans.fit_predict(X_scaled)

print(f"\n✓ Clustering completo")
print(f"\nDistribuição de clusters:")
print(df_pandas["cluster"].value_counts().sort_index())

# COMMAND ----------

# DBTITLE 1,Analisar Perfil dos Clusters
# Calcular estatísticas por cluster
cluster_profiles = df_pandas.groupby("cluster")[cluster_features].mean().round(2)

print("\n" + "="*80)
print("PERFIL DOS CLUSTERS")
print("="*80)
print(cluster_profiles.to_string())

# Nomear clusters baseado em perfil
cluster_names = {
    0: "Champions",        # Baixa recency, alta frequency/monetary
    1: "At Risk",          # Alta recency, baixa activity
    2: "Potential Loyalists",  # Média em tudo
    3: "New Customers",    # Baixo lifetime
    4: "Need Attention"    # Baixo engagement
}

# Mapear nomes (ajuste conforme perfil real)
df_pandas["segment_name"] = df_pandas["cluster"].map(
    lambda x: f"Segment {x}" if x not in cluster_names else cluster_names.get(x, f"Segment {x}")
)

print("\n" + "="*80)
print("SEGMENTOS NOMEADOS")
print("="*80)
print(df_pandas.groupby("segment_name").size())

# COMMAND ----------

# DBTITLE 1,Salvar Segmentação
# Salvar segmentos
df_segments = df_pandas[["customer_id", "cluster", "segment_name"]]
df_segments_spark = spark.createDataFrame(df_segments)
create_or_replace_table(df_segments_spark, SCHEMA_GOLD, "customer_segments")

print(f"\n✓ Segmentos salvos: {get_full_table_name(SCHEMA_GOLD, 'customer_segments')}")
print(f"\nResumo:")
print(f"  - Total clientes: {len(df_segments):,}")
print(f"  - Número de segmentos: {optimal_k}")
print(f"  - Features usadas: {', '.join(cluster_features)}")

# COMMAND ----------

# DBTITLE 1,Insights por Segmento
# Juntar com scores de churn e propensão para insights
df_churn = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_scores")).select("customer_id", "churn_probability")
df_segments_insights = df_segments_spark.join(df_churn, "customer_id", "left")

# Calcular métricas por segmento
df_segment_insights = df_segments_insights.groupBy("segment_name").agg(
    F.count("customer_id").alias("count"),
    F.avg("churn_probability").alias("avg_churn_risk")
).orderBy(F.desc("count"))

print("\n" + "="*80)
print("INSIGHTS POR SEGMENTO")
print("="*80)
df_segment_insights.show(truncate=False)

print("\n✅ Segmentação completa! Use para marketing direcionado.")

# COMMAND ----------

