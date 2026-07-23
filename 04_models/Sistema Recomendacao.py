# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Sistema de Recomendação
# MAGIC %md
# MAGIC # Sistema de Recomendação
# MAGIC
# MAGIC ## Objetivo
# MAGIC Implementar três tipos de sistemas de recomendação para clientes:
# MAGIC
# MAGIC ### 1. **Next Best Product** 🛍️
# MAGIC * Produto com maior probabilidade de compra
# MAGIC * Baseado em histórico de transações
# MAGIC * Score: probabilidade de compra (0.0 a 1.0)
# MAGIC
# MAGIC ### 2. **Next Best Action** 🎯
# MAGIC * Ação recomendada por segmento de cliente
# MAGIC * Baseado em perfil RFM e comportamento
# MAGIC * Ações: Retention, Upsell, Cross-sell, Reactivation
# MAGIC
# MAGIC ### 3. **Collaborative Filtering** 👥
# MAGIC * Produtos similares aos já comprados
# MAGIC * Baseado em co-ocorrência (quem comprou X também comprou Y)
# MAGIC * Top-N produtos recomendados
# COMMAND ----------

# DBTITLE 1,Setup e Configuração
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from scipy.sparse import csr_matrix

CATALOG = "customer_intelligence"
SCHEMA_GOLD = "gold"
SCHEMA_SILVER = "silver"
SCHEMA_BRONZE = "bronze"

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

print("✓ Setup OK")
print(f"  Catalog: {CATALOG}")

# COMMAND ----------

# DBTITLE 1,1️⃣ Next Best Product - Preparar Dados
# Carregar transações e produtos
df_transactions = spark.table(get_full_table_name(SCHEMA_SILVER, "transactions"))
df_products = spark.table(get_full_table_name(SCHEMA_BRONZE, "products_raw"))

# Calcular popularidade de cada produto
df_product_popularity = df_transactions.groupBy("product_id").agg(
    F.count("*").alias("purchase_count"),
    F.countDistinct("customer_id").alias("unique_customers"),
    F.sum("total_amount").alias("total_revenue")
)

# Calcular taxa de conversão por produto
total_customers = spark.table(get_full_table_name(SCHEMA_BRONZE, "customers_raw")).count()
df_product_popularity = df_product_popularity.withColumn(
    "conversion_rate",
    F.col("unique_customers") / F.lit(total_customers)
)

print(f"✓ Dados preparados: {df_product_popularity.count()} produtos")
df_product_popularity.orderBy(F.desc("purchase_count")).limit(10).display()

# COMMAND ----------

# DBTITLE 1,1️⃣ Next Best Product - Calcular Scores
# Para cada cliente, calcular score de propensão de compra para cada produto
# Score = combinação de:
# - Popularidade do produto (conversão geral)
# - Categoria já comprada pelo cliente
# - Preço compatível com histórico do cliente

# Join transactions com products para pegar categoria
df_trans_enriched = df_transactions.join(df_products.select("product_id", "category"), "product_id")

df_customer_history = df_trans_enriched.groupBy("customer_id").agg(
    F.collect_set("product_id").alias("purchased_products"),
    F.avg("total_amount").alias("avg_purchase_amount"),
    F.collect_set("category").alias("purchased_categories")
)

# Enriquecer produtos com score
df_products_scored = df_products.join(
    df_product_popularity,
    "product_id",
    "left"
).fillna({"conversion_rate": 0.01, "purchase_count": 1})

print(f"✓ Scores calculados para {df_products_scored.count()} produtos")
df_products_scored.orderBy(F.desc("conversion_rate")).limit(10).display()

# COMMAND ----------

# DBTITLE 1,1️⃣ Next Best Product - Recomendações Personalizadas (híbrido com cold-start)
# Para cada cliente, recomendar TOP 5 produtos que ele NUNCA comprou.
#
# Score híbrido de verdade (o comentário da célula anterior já prometia isso,
# mas antes só rankeava por popularidade pura):
#   score = conversion_rate (popularidade global — funciona pra todo mundo)
#         + 0.5 se a categoria do produto já foi comprada pelo cliente (content-based)
#         + até 0.3 por proximidade de preço com o ticket médio do cliente
#
# Cliente sem nenhuma transação (cold-start) não casa em nenhuma categoria/preço
# médio, então o score colapsa exatamente para popularidade pura — fallback
# automático, sem precisar de lógica condicional separada. Isso é o "modelo
# híbrido content-based + collaborative com fallback de cold-start" que
# marketplaces com alta rotatividade de participantes novos (tipo o caso da
# Vertem) precisam.
df_customers = spark.table(get_full_table_name(SCHEMA_BRONZE, "customers_raw"))

# Cross join clientes x produtos
df_recommendations = df_customers.select("customer_id").crossJoin(
    df_products_scored.select(
        "product_id", "product_name", "category",
        "price", "conversion_rate", "purchase_count"
    )
)

# Trazer o perfil do cliente (categorias já compradas + ticket médio) — null
# para quem nunca comprou nada, que é justamente o caso cold-start.
df_recommendations = df_recommendations.join(
    df_customer_history.select("customer_id", "purchased_categories", "avg_purchase_amount"),
    "customer_id",
    "left"
)

df_recommendations = df_recommendations.withColumn(
    "is_cold_start", F.col("purchased_categories").isNull()
).withColumn(
    "category_affinity",
    F.when(
        F.col("purchased_categories").isNotNull() & F.array_contains(F.col("purchased_categories"), F.col("category")),
        0.5
    ).otherwise(0.0)
).withColumn(
    "price_affinity",
    F.when(
        F.col("avg_purchase_amount").isNotNull(),
        F.greatest(F.lit(0.0), F.lit(0.3) - (F.abs(F.col("price") - F.col("avg_purchase_amount")) / F.col("avg_purchase_amount")) * F.lit(0.3))
    ).otherwise(F.lit(0.0))
).withColumn(
    "recommendation_score",
    F.col("conversion_rate") + F.col("category_affinity") + F.col("price_affinity")
).withColumn(
    "score_basis",
    F.when(F.col("is_cold_start"), "cold_start_popularity").otherwise("hybrid_personalized")
)

# Remover produtos já comprados
# Criar um set de produtos comprados por cliente com aliases claros
df_purchased = df_customer_history.select(
    F.col("customer_id").alias("cust_id_purchased"),
    F.explode("purchased_products").alias("purchased_product_id")
)

# Anti-join com aliases para evitar ambiguidade
df_recommendations = df_recommendations.join(
    df_purchased,
    (df_recommendations.customer_id == df_purchased.cust_id_purchased) &
    (df_recommendations.product_id == df_purchased.purchased_product_id),
    "left_anti"
)

# Ranking: Top 5 por cliente, agora pelo score híbrido
window_spec = Window.partitionBy("customer_id").orderBy(F.desc("recommendation_score"))
df_top_recommendations = df_recommendations.withColumn(
    "rank",
    F.row_number().over(window_spec)
).filter(F.col("rank") <= 5)

n_cold_start = df_customers.select("customer_id").exceptAll(
    df_customer_history.select("customer_id")
).count()
n_total = df_customers.count()
print(f"✓ Recomendações geradas: {df_top_recommendations.count():,} recomendações")
print("  Top 5 produtos por cliente (score híbrido: popularidade + categoria + preço)")
print(f"  Clientes cold-start (sem histórico de compra): {n_cold_start:,} de {n_total:,} ({n_cold_start/n_total:.1%})")
df_top_recommendations.filter(F.col("customer_id") == "C001").select(
    "customer_id", "product_name", "category", "recommendation_score", "score_basis", "rank"
).display()

# COMMAND ----------

# DBTITLE 1,2️⃣ Next Best Action - Definir Ações por Segmento
# Carregar segmentos de clientes
df_segments = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_segments"))
df_features = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_features"))

# Definir ação recomendada por perfil
df_actions = df_segments.join(df_features, "customer_id")

# Lógica de recomendação:
# - Cluster 0 (High Value): Retention (manter engajado)
# - Cluster 1 (At Risk): Reactivation (recuperar)
# - Cluster 2 (New): Onboarding (engajar)
# - Cluster 3 (Low Value): Cross-sell (aumentar ticket)
# - Cluster 4 (Medium): Upsell (upgrade)

action_mapping = {
    0: "Retention - Programa VIP",
    1: "Reactivation - Oferta de retorno",
    2: "Onboarding - Tutorial + desconto",
    3: "Cross-sell - Bundle de produtos",
    4: "Upsell - Upgrade de categoria"
}

from pyspark.sql.types import StringType
import pyspark.sql.functions as F

action_udf = F.udf(lambda x: action_mapping.get(x, "Unknown"), StringType())
df_actions = df_actions.withColumn(
    "recommended_action",
    action_udf(F.col("cluster"))
)

print("✓ Ações recomendadas por segmento:")
df_actions.groupBy("cluster", "recommended_action").count().orderBy("cluster").display()

# COMMAND ----------

# DBTITLE 1,2️⃣ Next Best Action - Priorização
# Adicionar score de prioridade baseado em:
# - Valor do cliente (monetary_total)
# - Risco de churn (se tiver)
# - Engajamento recente

df_actions = df_actions.withColumn(
    "action_priority",
    F.when(F.col("cluster") == 0, 5)  # High value = prioridade máxima
     .when(F.col("cluster") == 1, 4)  # At risk = alta prioridade
     .when(F.col("cluster") == 4, 3)  # Medium = média prioridade
     .when(F.col("cluster") == 2, 2)  # New = baixa prioridade
     .otherwise(1)                     # Low value = mínima prioridade
)

df_actions = df_actions.withColumn(
    "expected_impact",
    F.when(F.col("cluster") == 0, F.col("monetary_total") * 0.1)  # 10% uplift
     .when(F.col("cluster") == 1, F.col("monetary_total") * 0.3)  # 30% recovery
     .when(F.col("cluster") == 4, F.col("monetary_total") * 0.2)  # 20% upsell
     .otherwise(F.col("monetary_total") * 0.15)                     # 15% default
)

print("✓ Priorização calculada")
df_actions.select(
    "customer_id", "cluster", "recommended_action", 
    "action_priority", "expected_impact"
).orderBy(F.desc("action_priority"), F.desc("expected_impact")).limit(20).display()

# COMMAND ----------

# DBTITLE 1,3️⃣ Collaborative Filtering - Matriz de Co-ocorrência (Cosine Similarity)
# Criar matriz cliente-produto (quem comprou o quê)
df_customer_product = df_transactions.select(
    "customer_id", "product_id"
).distinct()

# Similaridade item-a-item de cosseno de verdade — a versão anterior calculava
# só co_purchase_count / count_a, uma razão assimétrica (mais próxima de
# "confidence" de regras de associação) que não é cosine, apesar do nome.
# Catálogo é pequeno o bastante (poucas centenas de produtos) pra caber em
# memória: monta a matriz binária esparsa cliente×produto e usa
# cosine_similarity do scikit-learn nas colunas (produtos).
df_customer_product_pd = df_customer_product.toPandas()

customers_cf = df_customer_product_pd["customer_id"].astype("category")
products_cf = df_customer_product_pd["product_id"].astype("category")

matriz_interacao = csr_matrix(
    (
        np.ones(len(df_customer_product_pd)),
        (customers_cf.cat.codes, products_cf.cat.codes)
    ),
    shape=(len(customers_cf.cat.categories), len(products_cf.cat.categories))
)

similaridade_produtos = cosine_similarity(matriz_interacao.T)
produtos_ordenados = products_cf.cat.categories.tolist()

pares_similaridade = [
    (produtos_ordenados[i], produtos_ordenados[j], float(similaridade_produtos[i, j]))
    for i in range(len(produtos_ordenados))
    for j in range(len(produtos_ordenados))
    if i != j and similaridade_produtos[i, j] > 0
]

df_similarity = spark.createDataFrame(
    pd.DataFrame(pares_similaridade, columns=["product_a", "product_b", "similarity_score"])
)

print(f"✓ Matriz de similaridade (cosine) criada: {df_similarity.count():,} pares")
df_similarity.orderBy(F.desc("similarity_score")).limit(20).display()

# COMMAND ----------

# DBTITLE 1,3️⃣ Collaborative Filtering - Recomendações
# Para cada cliente, recomendar produtos baseado no que compraram
# Usar alias para evitar ambiguidade
df_customer_purchased = df_customer_product.alias("cp")
df_collab_recommendations = df_customer_purchased.join(
    df_similarity,
    F.col("cp.product_id") == df_similarity.product_a
).select(
    F.col("cp.customer_id").alias("customer_id"),
    F.col("product_b").alias("recommended_product_id"),
    "similarity_score"
)

# Remover produtos já comprados
df_already_purchased = df_customer_product.select(
    F.col("customer_id").alias("cust_id"),
    F.col("product_id").alias("already_purchased")
)

df_collab_recommendations = df_collab_recommendations.join(
    df_already_purchased,
    (df_collab_recommendations.customer_id == df_already_purchased.cust_id) &
    (df_collab_recommendations.recommended_product_id == df_already_purchased.already_purchased),
    "left_anti"
)

# Top 5 recomendações por cliente
window_spec = Window.partitionBy("customer_id").orderBy(F.desc("similarity_score"))
df_collab_top5 = df_collab_recommendations.withColumn(
    "rank",
    F.row_number().over(window_spec)
).filter(F.col("rank") <= 5)

print(f"✓ Recomendações collaborative filtering: {df_collab_top5.count():,}")
df_collab_top5.join(
    df_products.select("product_id", "product_name", "category"),
    df_collab_top5.recommended_product_id == df_products.product_id
).filter(F.col("customer_id") == "C001").display()

# COMMAND ----------

# DBTITLE 1,💾 Salvar Todas as Recomendações
# 1. Next Best Product
df_nbp = df_top_recommendations.select(
    "customer_id",
    F.col("product_id").alias("recommended_product_id"),
    F.lit("next_best_product").alias("recommendation_type"),
    F.col("recommendation_score").alias("score"),
    F.col("rank"),
    F.col("score_basis")
)

# 2. Next Best Action
df_nba = df_actions.select(
    "customer_id",
    F.lit(None).cast("string").alias("recommended_product_id"),
    F.lit("next_best_action").alias("recommendation_type"),
    F.col("action_priority").cast("double").alias("score"),
    F.lit(1).alias("rank"),
    F.lit(None).cast("string").alias("score_basis")
)

# 3. Collaborative Filtering
df_cf = df_collab_top5.select(
    "customer_id",
    F.col("recommended_product_id"),
    F.lit("collaborative_filtering").alias("recommendation_type"),
    F.col("similarity_score").alias("score"),
    "rank",
    F.lit(None).cast("string").alias("score_basis")
)

# Union de todas as recomendações
df_all_recommendations = df_nbp.union(df_nba).union(df_cf)

create_or_replace_table(df_all_recommendations, SCHEMA_GOLD, "recommendations")

print(f"\n✓ Total de recomendações salvas: {df_all_recommendations.count():,}")
print("\nDistribuição por tipo:")
df_all_recommendations.groupBy("recommendation_type").count().display()

# COMMAND ----------

# DBTITLE 1,📊 Métricas e Avaliação
# Estatísticas gerais
total_customers = spark.table(get_full_table_name(SCHEMA_BRONZE, "customers_raw")).count()
total_products = spark.table(get_full_table_name(SCHEMA_BRONZE, "products_raw")).count()

print("=" * 70)
print("SISTEMA DE RECOMENDAÇÃO - RESUMO")
print("=" * 70)
print("\n1️⃣ NEXT BEST PRODUCT (híbrido: popularidade + categoria + preço):")
print(f"   Clientes com recomendações: {df_top_recommendations.select('customer_id').distinct().count():,}")
print(f"   Total de recomendações: {df_top_recommendations.count():,}")
print(f"   Média de score: {df_top_recommendations.agg(F.avg('recommendation_score')).collect()[0][0]:.4f}")
print(f"   Cold-start (fallback popularidade pura): {n_cold_start:,} clientes ({n_cold_start/n_total:.1%})")

print("\n2️⃣ NEXT BEST ACTION:")
print(f"   Clientes com ações: {df_actions.count():,}")
actions_dist = df_actions.groupBy("recommended_action").count().collect()
for row in actions_dist:
    print(f"   - {row['recommended_action']}: {row['count']:,}")

print("\n3️⃣ COLLABORATIVE FILTERING:")
print(f"   Clientes com recomendações: {df_collab_top5.select('customer_id').distinct().count():,}")
print(f"   Total de recomendações: {df_collab_top5.count():,}")
print(f"   Média de similaridade: {df_collab_top5.agg(F.avg('similarity_score')).collect()[0][0]:.4f}")

print("\n📊 COBERTURA:")
print(f"   Total clientes: {total_customers:,}")
print(f"   Clientes com alguma recomendação: {df_all_recommendations.select('customer_id').distinct().count():,}")
print(f"   Taxa de cobertura: {(df_all_recommendations.select('customer_id').distinct().count() / total_customers * 100):.1f}%")

print("\n" + "=" * 70)
print("✓ SISTEMA DE RECOMENDAÇÃO COMPLETO")
print("=" * 70)

# COMMAND ----------


