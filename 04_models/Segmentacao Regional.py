# Databricks notebook source
# DBTITLE 1,Segmentação Regional
# MAGIC %md
# MAGIC # Segmentação Regional 🗺️
# MAGIC
# MAGIC ## Objetivo
# MAGIC **Cruzamento de segmento RFM × PIB per capita regional**: cruzar a segmentação
# MAGIC comportamental (RFM/K-Means, `Segmentacao Clientes Clustering.py`) com uma
# MAGIC dimensão **geográfica**, no mesmo espírito de um case real que já fiz: comparar
# MAGIC poder aquisitivo por cidade (PIB per capita, dado do IBGE) pra mostrar que "cidade
# MAGIC pequena" não é sinônimo de "cliente de baixo valor" — uma cidade pequena e rica
# MAGIC pode ter potencial de compra maior que um bairro populoso de uma capital.
# MAGIC
# MAGIC ## Nota de transparência sobre os dados desta seção
# MAGIC Diferente do resto do projeto, aqui eu **não tenho** a planilha original do IBGE
# MAGIC que usei no case real (baixei na época, não guardei o arquivo). Então:
# MAGIC - **Cidade/estado por cliente**: `customers_raw` só tem `country` (não tem
# MAGIC   estado/cidade) — atribuído aqui de forma sintética, só para clientes `country='BR'`,
# MAGIC   sem tocar em `01_bronze` (que já está validado e usado por todo o resto do
# MAGIC   pipeline).
# MAGIC - **PIB per capita por cidade**: valores **aproximados/ilustrativos**, na ordem de
# MAGIC   grandeza pública conhecida do IBGE, não uma consulta atualizada em tempo real.
# MAGIC   Servem para demonstrar a técnica de cruzamento, não como fonte oficial.

# COMMAND ----------

# DBTITLE 1,Setup
from pyspark.sql import functions as F
import pandas as pd
import numpy as np

CATALOG = "customer_intelligence"
SCHEMA_BRONZE = "bronze"
SCHEMA_GOLD = "gold"

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

print("✓ Setup OK")

# COMMAND ----------

# DBTITLE 1,1. Atribuir cidade/estado sintético (só clientes country='BR')
# Lista de cidades reais cobrindo perfis bem diferentes de porte x renda —
# incluindo o padrão do case real: cidade pequena só que rica (agro/industrial)
# ao lado de capitais grandes com renda per capita mediana.
cidades_referencia = pd.DataFrame([
    {"cidade": "São Paulo",        "estado": "SP", "peso_populacional": 22, "pib_per_capita_ilustrativo": 61000},
    {"cidade": "Rio de Janeiro",   "estado": "RJ", "peso_populacional": 13, "pib_per_capita_ilustrativo": 45000},
    {"cidade": "Brasília",         "estado": "DF", "peso_populacional": 6,  "pib_per_capita_ilustrativo": 88000},
    {"cidade": "Belo Horizonte",   "estado": "MG", "peso_populacional": 6,  "pib_per_capita_ilustrativo": 42000},
    {"cidade": "Porto Alegre",     "estado": "RS", "peso_populacional": 4,  "pib_per_capita_ilustrativo": 48000},
    {"cidade": "Curitiba",         "estado": "PR", "peso_populacional": 4,  "pib_per_capita_ilustrativo": 51000},
    {"cidade": "Salvador",         "estado": "BA", "peso_populacional": 6,  "pib_per_capita_ilustrativo": 27000},
    {"cidade": "Fortaleza",        "estado": "CE", "peso_populacional": 6,  "pib_per_capita_ilustrativo": 24000},
    {"cidade": "Recife",           "estado": "PE", "peso_populacional": 4,  "pib_per_capita_ilustrativo": 30000},
    {"cidade": "Manaus",           "estado": "AM", "peso_populacional": 4,  "pib_per_capita_ilustrativo": 34000},
    # Cidades pequenas e ricas (agro/industrial) — o padrão do case real
    {"cidade": "Uberlândia",       "estado": "MG", "peso_populacional": 2,  "pib_per_capita_ilustrativo": 55000},
    {"cidade": "Sorriso",          "estado": "MT", "peso_populacional": 1,  "pib_per_capita_ilustrativo": 72000},
    {"cidade": "São José dos Campos", "estado": "SP", "peso_populacional": 2, "pib_per_capita_ilustrativo": 58000},
    {"cidade": "Ilhéus",           "estado": "BA", "peso_populacional": 1,  "pib_per_capita_ilustrativo": 22000},
    {"cidade": "Chapecó",          "estado": "SC", "peso_populacional": 1,  "pib_per_capita_ilustrativo": 49000},
])
cidades_referencia["peso_normalizado"] = cidades_referencia["peso_populacional"] / cidades_referencia["peso_populacional"].sum()

df_customers = spark.table(get_full_table_name(SCHEMA_BRONZE, "customers_raw"))
df_customers_br = df_customers.filter(F.col("country") == "BR").select("customer_id").toPandas()

rng = np.random.default_rng(42)
cidades_sorteadas = rng.choice(
    cidades_referencia["cidade"],
    size=len(df_customers_br),
    p=cidades_referencia["peso_normalizado"]
)
df_customers_br["cidade"] = cidades_sorteadas
df_customer_regiao = df_customers_br.merge(
    cidades_referencia[["cidade", "estado", "pib_per_capita_ilustrativo"]], on="cidade", how="left"
)

print(f"✓ Cidade/estado sintético atribuído a {len(df_customer_regiao):,} clientes BR")
print(f"  (de {df_customers.count():,} clientes totais no catálogo — os demais não são")
print(f"   country='BR', então essa análise regional não se aplica a eles)")
df_customer_regiao["cidade"].value_counts().head(10)

# COMMAND ----------

# DBTITLE 1,2. Cruzar com Segmentação RFM
df_segments = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_segments")).toPandas()
df_features = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_features")).select(
    "customer_id", "monetary_total", "frequency", "recency_days"
).toPandas()

df_regional = (
    df_customer_regiao
    .merge(df_segments, on="customer_id", how="inner")
    .merge(df_features, on="customer_id", how="inner")
)

# Tier de PIB per capita (tercis) — Alto/Médio/Baixo
df_regional["tier_pib"] = pd.qcut(
    df_regional["pib_per_capita_ilustrativo"], q=3, labels=["Baixo", "Médio", "Alto"]
)

print(f"✓ {len(df_regional):,} clientes com segmento + região cruzados")

# COMMAND ----------

# DBTITLE 1,3. Cruzamento: segmento × tier de PIB regional
cruzamento = df_regional.groupby(["segment_name", "tier_pib"], observed=True).agg(
    n_clientes=("customer_id", "count"),
    monetary_medio=("monetary_total", "mean"),
    frequency_media=("frequency", "mean")
).round(2).reset_index()

print("="*80)
print("SEGMENTO × TIER DE PIB REGIONAL")
print("="*80)
print(cruzamento.to_string(index=False))

# COMMAND ----------

# DBTITLE 1,4. Insight acionável: oportunidades por região
# O ponto do case real: destacar clusters "de baixo valor aparente" que, quando
# cruzados com uma região de alto PIB, revelam oportunidade de reativação/upsell
# maior do que o valor médio observado sugere.
oportunidades = cruzamento[
    cruzamento["segment_name"].isin(["At Risk", "Need Attention"])
].sort_values("monetary_medio", ascending=False)

print("\n" + "="*80)
print("OPORTUNIDADES: segmentos de risco/baixo engajamento em regiões de alto PIB")
print("="*80)
print("Esses grupos merecem régua de reativação diferenciada — não pela recência")
print("de compra, mas pelo potencial de gasto da região onde estão.")
print(oportunidades.to_string(index=False))

create_or_replace_table(
    spark.createDataFrame(df_regional[["customer_id", "cidade", "estado", "tier_pib", "segment_name"]]),
    SCHEMA_GOLD, "customer_regional_segmentation"
)
print(f"\n✓ Segmentação regional salva: {get_full_table_name(SCHEMA_GOLD, 'customer_regional_segmentation')}")

# COMMAND ----------

# DBTITLE 1,Resumo
print("="*60)
print("SEGMENTAÇÃO REGIONAL - RESUMO")
print("="*60)
print(f"✅ Clientes BR com região atribuída: {len(df_customer_regiao):,}")
print(f"✅ Cidades de referência: {len(cidades_referencia)}")
print(f"✅ Cruzamento segmento × PIB regional salvo em customer_regional_segmentation")
print("⚠️  Cidade/estado e PIB per capita são sintéticos/ilustrativos nesta demo —")
print("   ver nota de transparência no topo do notebook.")
print("="*60)

# COMMAND ----------
