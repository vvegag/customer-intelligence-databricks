# Databricks notebook source
# DBTITLE 1,A/B Testing e Causal Inference
# MAGIC %md
# MAGIC # A/B Testing e Análise Causal
# MAGIC
# MAGIC ## Objetivo
# MAGIC Medir o impacto causal de campanhas de marketing usando:
# MAGIC - **Grupo Controle vs Tratamento**
# MAGIC - **Testes estatísticos** (t-test, chi-square)
# MAGIC - **Lift e Uplift modeling**
# MAGIC - **Significância estatística**
# MAGIC - **Causal inference** simples
# MAGIC
# MAGIC ## Métricas Chave
# MAGIC 1. **Conversion Rate**: Taxa de conversão
# MAGIC 2. **Lift**: Incremento relativo vs controle
# MAGIC 3. **Uplift**: Diferença absoluta
# MAGIC 4. **Statistical Significance**: p-value < 0.05
# MAGIC 5. **ROAS**: Return on Ad Spend

# COMMAND ----------

# DBTITLE 1,Configuração
# Configs inline
from pyspark.sql import functions as F
import pandas as pd
from scipy import stats

CATALOG = "customer_intelligence"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
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

print("✓ Configuração carregada")
print(f"  Catalog: {CATALOG}")

# COMMAND ----------

# DBTITLE 1,1. Preparar Dados de Experimento
# Carregar dados de campanhas
df_exposures = spark.table(get_full_table_name(SCHEMA_SILVER, "campaign_exposures"))
df_responses = spark.table(get_full_table_name(SCHEMA_SILVER, "campaign_responses"))
df_campaigns = spark.table(get_full_table_name(SCHEMA_SILVER, "campaigns"))

# Join para criar dataset de experimento
df_experiment = df_exposures.join(
    df_responses.select("exposure_id", "response_id", "response_value", "is_conversion"),
    "exposure_id",
    "left"
).join(
    df_campaigns.select("campaign_id", "campaign_name", "campaign_type", "budget", "discount_pct"),
    "campaign_id",
    "inner"
)

# Criar flags de resposta (0/1)
df_experiment = df_experiment.withColumn(
    "has_response",
    F.when(F.col("response_id").isNotNull(), 1).otherwise(0)
).withColumn(
    "has_conversion",
    F.coalesce(F.col("is_conversion"), F.lit(0))
).withColumn(
    "revenue",
    F.coalesce(F.col("response_value"), F.lit(0))
)

print(f"✓ Dataset de experimento criado: {df_experiment.count():,} registros")
df_experiment.show(5)

# COMMAND ----------

# DBTITLE 1,2. Análise por Campanha - Controle vs Tratamento
# Agregar métricas por campanha e grupo
df_campaign_results = df_experiment.groupBy("campaign_id", "campaign_name", "treatment_group").agg(
    F.count("customer_id").alias("total_customers"),
    F.sum("has_response").alias("total_responses"),
    F.sum("has_conversion").alias("total_conversions"),
    F.sum("revenue").alias("total_revenue"),
    F.avg("revenue").alias("avg_revenue_per_customer")
)

# Calcular taxas
df_campaign_results = df_campaign_results.withColumn(
    "response_rate",
    F.round((F.col("total_responses") / F.col("total_customers")) * 100, 2)
).withColumn(
    "conversion_rate",
    F.round((F.col("total_conversions") / F.col("total_customers")) * 100, 2)
)

print("✓ Resultados agregados por campanha e grupo:")
df_campaign_results.orderBy("campaign_id", "treatment_group").show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,3. Calcular Lift e Uplift
# Pivotar para comparar controle vs tratamento
df_pivot = df_campaign_results.groupBy("campaign_id", "campaign_name").pivot("treatment_group").agg(
    F.first("total_customers").alias("customers"),
    F.first("total_conversions").alias("conversions"),
    F.first("conversion_rate").alias("conversion_rate"),
    F.first("total_revenue").alias("revenue")
)

# Calcular lift e uplift
df_lift = df_pivot.withColumn(
    "lift_pct",
    F.round(
        ((F.col("Treatment_conversion_rate") - F.col("Control_conversion_rate")) / F.col("Control_conversion_rate")) * 100,
        2
    )
).withColumn(
    "uplift_absolute",
    F.round(F.col("Treatment_conversion_rate") - F.col("Control_conversion_rate"), 2)
).withColumn(
    "incremental_conversions",
    F.col("Treatment_conversions") - 
    (F.col("Treatment_customers") * (F.col("Control_conversion_rate") / 100))
).withColumn(
    "incremental_revenue",
    F.col("Treatment_revenue") - 
    (F.col("Treatment_customers") * (F.col("Control_revenue") / F.col("Control_customers")))
)

print("\n" + "="*80)
print("RESULTADOS DE LIFT E UPLIFT POR CAMPANHA")
print("="*80)
df_lift.select(
    "campaign_id",
    "campaign_name",
    "Control_conversion_rate",
    "Treatment_conversion_rate",
    "lift_pct",
    "uplift_absolute",
    "incremental_revenue"
).show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,3.1 Checagem de SRM (Sample Ratio Mismatch)
# Antes de confiar em qualquer resultado de lift/significância, valida se a
# proporção OBSERVADA de Controle/Tratamento bate com o desenho do
# experimento. O gerador de dados usa F.rand(seed=45) < 0.3
# (01_bronze/Ingestao Dados Bronze.py:277) — ou seja, o desenho real é 30%
# controle / 70% tratamento, não 50/50. SRM detectado indica problema na
# randomização/coleta (ex: bug de atribuição, bot filtrando um grupo mais que
# outro) — os resultados de lift ficam não confiáveis até isso ser investigado.
EXPECTED_CONTROL_RATIO = 0.30  # ver 01_bronze/Ingestao Dados Bronze.py:277
SRM_P_VALUE_THRESHOLD = 0.01  # mais rígido que 0.05: SRM deveria ser raro sob randomização correta

df_srm_pd = df_pivot.select("campaign_id", "campaign_name", "Control_customers", "Treatment_customers").toPandas()

srm_results = []
for _, row in df_srm_pd.iterrows():
    n_control = row["Control_customers"] or 0
    n_treatment = row["Treatment_customers"] or 0
    total = n_control + n_treatment

    if total == 0:
        continue

    expected = [total * EXPECTED_CONTROL_RATIO, total * (1 - EXPECTED_CONTROL_RATIO)]
    chi2_srm, p_value_srm = stats.chisquare([n_control, n_treatment], f_exp=expected)

    srm_results.append({
        "campaign_id": row["campaign_id"],
        "campaign_name": row["campaign_name"],
        "n_control": int(n_control),
        "n_treatment": int(n_treatment),
        "observed_control_ratio": round(n_control / total, 4),
        "expected_control_ratio": EXPECTED_CONTROL_RATIO,
        "p_value_srm": round(p_value_srm, 4),
        "srm_detected": p_value_srm < SRM_P_VALUE_THRESHOLD
    })

df_srm = pd.DataFrame(srm_results)
print("\n" + "="*80)
print("CHECAGEM DE SRM (Sample Ratio Mismatch)")
print("="*80)
print(df_srm.to_string(index=False))

campanhas_com_srm = df_srm[df_srm["srm_detected"]]["campaign_name"].tolist()
if campanhas_com_srm:
    print(f"\n⚠️ SRM detectado em {len(campanhas_com_srm)} campanha(s): {campanhas_com_srm}")
    print("   Resultados de lift/significância dessas campanhas não são confiáveis")
    print("   até a causa do desbalanceamento ser investigada (randomização/coleta).")
else:
    print("\n✓ Nenhum SRM detectado — proporção observada bate com o desenho do experimento.")

# COMMAND ----------

# DBTITLE 1,4. Teste de Significância Estatística
# Converter para Pandas para testes estatísticos
df_experiment_pd = df_experiment.select(
    "campaign_id",
    "treatment_group",
    "has_conversion",
    "revenue"
).toPandas()

# Calcular p-value para cada campanha
significance_results = []

for campaign_id in df_experiment_pd["campaign_id"].unique():
    df_camp = df_experiment_pd[df_experiment_pd["campaign_id"] == campaign_id]
    
    control = df_camp[df_camp["treatment_group"] == "Control"]["has_conversion"]
    treatment = df_camp[df_camp["treatment_group"] == "Treatment"]["has_conversion"]
    
    if len(control) > 0 and len(treatment) > 0:
        # T-test para comparação de médias
        t_stat, p_value = stats.ttest_ind(treatment, control)
        
        # Chi-square para tabela de contingência
        contingency_table = pd.crosstab(
            df_camp["treatment_group"],
            df_camp["has_conversion"]
        )
        chi2, p_value_chi2, dof, expected = stats.chi2_contingency(contingency_table)
        
        significance_results.append({
            "campaign_id": campaign_id,
            "p_value_ttest": round(p_value, 4),
            "p_value_chi2": round(p_value_chi2, 4),
            "is_significant_ttest": p_value < 0.05,
            "is_significant_chi2": p_value_chi2 < 0.05,
            "t_statistic": round(t_stat, 3),
            "chi2_statistic": round(chi2, 3)
        })

# Correção de múltiplas comparações: N campanhas x 2 testes cada, todos contra
# p<0.05 sem correção, infla a taxa de falso-positivo conforme N cresce (family-
# wise error rate). Benjamini-Hochberg (FDR) é o padrão pra esse cenário
# exploratório — menos conservador que Bonferroni, que seria adequado demais
# pra um cenário onde perder um resultado real (falso-negativo) também tem custo.
# Mantém o resultado bruto (is_significant_chi2) ao lado do corrigido — mesmo
# espírito "comparação, não substituição" já usado em outras partes do projeto.
from statsmodels.stats.multitest import multipletests

df_significance_pd = pd.DataFrame(significance_results)
if len(df_significance_pd) > 0:
    _, p_values_fdr, _, _ = multipletests(df_significance_pd["p_value_chi2"], method="fdr_bh")
    df_significance_pd["p_value_chi2_fdr"] = p_values_fdr.round(4)
    df_significance_pd["is_significant_chi2_fdr"] = p_values_fdr < 0.05

df_significance = spark.createDataFrame(df_significance_pd)

print("\n" + "="*80)
print("TESTE DE SIGNIFICÂNCIA ESTATÍSTICA")
print("="*80)
print("\np-value < 0.05 = Estatisticamente significante (bruto, sem correção)")
print("p_value_chi2_fdr = corrigido por Benjamini-Hochberg (FDR) para múltiplas comparações")
df_significance.show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,5. Tabela Final de Resultados de Experimento
# Juntar lift com significância
df_experiment_results = df_lift.join(df_significance, "campaign_id", "left")

# Adicionar classificação de resultado — usa is_significant_chi2_fdr (corrigido
# por múltiplas comparações), não o p-value bruto, pra evitar classificar como
# "Positive"/"Negative" uma campanha que só parece significante por acaso
# quando testamos várias campanhas ao mesmo tempo.
df_experiment_results = df_experiment_results.withColumn(
    "result_category",
    F.when(
        (F.col("is_significant_chi2_fdr") == True) & (F.col("lift_pct") > 10),
        "Strong Positive"
    ).when(
        (F.col("is_significant_chi2_fdr") == True) & (F.col("lift_pct") > 0),
        "Positive"
    ).when(
        (F.col("is_significant_chi2_fdr") == True) & (F.col("lift_pct") < 0),
        "Negative"
    ).otherwise("Not Significant")
)

# Salvar resultados
create_or_replace_table(df_experiment_results, SCHEMA_GOLD, "campaign_ab_test_results")

print("\n" + "="*80)
print("RESUMO FINAL - EXPERIMENTOS POR CAMPANHA")
print("="*80)
df_experiment_results.select(
    "campaign_id",
    "campaign_name",
    "Control_conversion_rate",
    "Treatment_conversion_rate",
    "lift_pct",
    "incremental_revenue",
    "is_significant_chi2",
    "result_category"
).orderBy(F.desc("lift_pct")).show(20, truncate=False)

print(f"\n✓ Resultados salvos em: {get_full_table_name(SCHEMA_GOLD, 'campaign_ab_test_results')}")

# COMMAND ----------

# DBTITLE 1,6. Análise de ROAS (Return on Ad Spend)
# Calcular ROAS por campanha
df_campaigns_full = spark.table(get_full_table_name(SCHEMA_SILVER, "campaigns"))

df_roas = df_experiment_results.join(
    df_campaigns_full.select("campaign_id", "budget"),
    "campaign_id",
    "left"
).withColumn(
    "roas",
    F.round(F.col("incremental_revenue") / F.col("budget"), 2)
).withColumn(
    "roi_pct",
    F.round(((F.col("incremental_revenue") - F.col("budget")) / F.col("budget")) * 100, 2)
)

print("\n" + "="*80)
print("ANÁLISE DE ROAS (Return on Ad Spend)")
print("="*80)
df_roas.select(
    "campaign_id",
    "campaign_name",
    "budget",
    "incremental_revenue",
    "roas",
    "roi_pct",
    "result_category"
).orderBy(F.desc("roas")).show(20, truncate=False)

# Salvar ROAS
create_or_replace_table(df_roas, SCHEMA_GOLD, "campaign_roas")
print(f"\n✓ ROAS salvo em: {get_full_table_name(SCHEMA_GOLD, 'campaign_roas')}")

# COMMAND ----------

# DBTITLE 1,Resumo de Experimentação
# Estatísticas gerais
total_campaigns = df_roas.count()
significant_campaigns = df_roas.filter(F.col("is_significant_chi2") == True).count()
positive_campaigns = df_roas.filter(F.col("result_category").isin(["Positive", "Strong Positive"])).count()

total_budget = df_roas.agg(F.sum("budget")).collect()[0][0]
total_incremental_revenue = df_roas.agg(F.sum("incremental_revenue")).collect()[0][0]
overall_roas = total_incremental_revenue / total_budget if total_budget > 0 else 0

print("\n" + "="*80)
print("RESUMO GERAL DE EXPERIMENTAÇÃO")
print("="*80)
print(f"\n✅ Total de campanhas analisadas: {total_campaigns}")
print(f"   - Estatisticamente significantes: {significant_campaigns} ({significant_campaigns/total_campaigns*100:.1f}%)")
print(f"   - Com resultado positivo: {positive_campaigns} ({positive_campaigns/total_campaigns*100:.1f}%)")
print("\n✅ Performance financeira:")
print(f"   - Budget total: ${total_budget:,.2f}")
print(f"   - Receita incremental: ${total_incremental_revenue:,.2f}")
print(f"   - ROAS geral: {overall_roas:.2f}x")
print(f"   - ROI geral: {(overall_roas - 1) * 100:.1f}%")
print("\n" + "="*80)
print("✓ ANÁLISE CAUSAL COMPLETA")
print("="*80)

# COMMAND ----------


