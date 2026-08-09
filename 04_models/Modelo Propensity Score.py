# Databricks notebook source
# DBTITLE 1,Modelo de Propensão de Compra
# MAGIC %md
# MAGIC # Propensity to Buy Model
# MAGIC
# MAGIC ## Objetivo
# MAGIC Prever a probabilidade de um cliente realizar uma compra nos próximos 30 dias.
# MAGIC
# MAGIC ## Features
# MAGIC - RFM (recency, frequency, monetary)
# MAGIC - Engajamento recente
# MAGIC - Histórico de campanhas
# MAGIC - Comportamento de navegação
# MAGIC
# MAGIC ## Target
# MAGIC Cliente comprou nos últimos 30 dias? (0/1)

# COMMAND ----------

# DBTITLE 1,Instalar XGBoost
# MAGIC %pip install xgboost --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Setup
# Configurações globais do projeto (inline)
from pyspark.sql import functions as F
import pandas as pd

# Configs
CATALOG = "customer_intelligence"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]
MLFLOW_EXPERIMENT_PATH = f"/Users/{CURRENT_USER}/customer_intelligence_experiments"

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

# ML imports
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score
import warnings
warnings.filterwarnings('ignore')

print("✓ Setup OK")
print(f"  Catalog: {CATALOG}")

# COMMAND ----------

# DBTITLE 1,Criar Target - Compra nos Últimos 30 dias
# Criar target: comprou nos últimos 30 dias?
df_transactions = spark.table(get_full_table_name(SCHEMA_SILVER, "transactions"))
max_date = df_transactions.agg(F.max("transaction_date")).collect()[0][0]
cutoff_date = max_date - pd.Timedelta(days=30)

df_recent_buyers = df_transactions.filter(F.col("transaction_date") >= cutoff_date).select("customer_id").distinct()
# Adiciona uma coluna 'purchased_last_30d' com valor 1 para clientes que compraram nos últimos 30 dias
df_recent_buyers = df_recent_buyers.withColumn("purchased_last_30d", F.lit(1))

print(f"✓ Target criado: {df_recent_buyers.count():,} compraram")

# COMMAND ----------

# DBTITLE 1,Features de Treino (ponto-no-tempo, sem vazamento)
# IMPORTANTE — vazamento temporal (corrigido nesta versão): antes, as features de
# treino vinham direto de gold.customer_features, que é sempre "estado atual
# calculado até max_date" (03_gold/Feature Engineering Gold.py — reference_date
# global, sem corte por cliente). Como o alvo é "comprou entre
# cutoff_date=max_date-30d e max_date", quem comprou nessa janela já tinha
# recency_days baixo quase por definição — o modelo aprendia a sobreposição
# temporal entre feature e alvo, não comportamento preditivo real.
#
# A correção: para TREINO, recalcular RFM/comportamental/campanhas usando
# só dado anterior a cutoff_date (ponto-no-tempo, "como estava antes da janela
# que queremos prever") — duplica a lógica de agregação de
# Feature Engineering Gold.py (mesma convenção de helper duplicado já usada no
# projeto), mas parametrizada por data de corte em vez de reference_date global.
# Para SCORING (célula "Salvar Scores", mais abaixo), continua correto usar
# gold.customer_features (estado atual) — ali estamos prevendo o futuro de
# verdade a partir do presente, não há look-ahead.
df_events = spark.table(get_full_table_name(SCHEMA_SILVER, "behavioral_events"))
df_exposures = spark.table(get_full_table_name(SCHEMA_SILVER, "campaign_exposures"))
df_responses = spark.table(get_full_table_name(SCHEMA_SILVER, "campaign_responses"))
df_customers = spark.table(get_full_table_name(SCHEMA_SILVER, "customers"))


def calcular_features_point_in_time(df_transactions, df_events, df_exposures, df_responses, df_customers, data_corte):
    """RFM + comportamental 30d + campanhas, calculados só com dado anterior a
    data_corte (exclusive) — mesma agregação de Feature Engineering Gold.py,
    parametrizada por corte em vez do reference_date global do notebook Gold."""
    df_transacoes_ate_corte = df_transactions.filter(F.col("transaction_date") < data_corte)
    df_rfm = df_transacoes_ate_corte.groupBy("customer_id").agg(
        F.datediff(F.lit(data_corte), F.max("transaction_date")).alias("recency_days"),
        F.count("transaction_id").alias("frequency"),
        F.sum("total_amount").alias("monetary_total"),
        F.avg("total_amount").alias("monetary_avg"),
    )

    inicio_janela_30d = data_corte - pd.Timedelta(days=30)
    df_eventos_janela = df_events.filter(
        (F.col("event_date_only") >= inicio_janela_30d) & (F.col("event_date_only") < data_corte)
    )
    df_behavioral = df_eventos_janela.groupBy("customer_id").agg(
        F.count("event_id").alias("event_count_30d"),
        F.countDistinct("session_id").alias("session_count_30d"),
        F.sum("event_value").alias("engagement_score_30d"),
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_views_30d"),
        F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("product_views_30d"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_30d"),
    )

    df_exposicoes_ate_corte = df_exposures.filter(F.col("exposure_date") < data_corte)
    df_respostas_ate_corte = df_responses.filter(F.col("response_date") < data_corte)
    df_campanhas = df_exposicoes_ate_corte.groupBy("customer_id").agg(
        F.count("exposure_id").alias("total_campaigns_exposed")
    )
    df_respostas_agg = df_respostas_ate_corte.groupBy("customer_id").agg(
        F.count("response_id").alias("total_responses"),
        F.sum("is_conversion").alias("total_conversions")
    )
    df_campanhas = df_campanhas.join(df_respostas_agg, "customer_id", "left")
    df_campanhas = df_campanhas.withColumn(
        "response_rate",
        F.when(F.col("total_campaigns_exposed") > 0,
               F.col("total_responses") / F.col("total_campaigns_exposed")).otherwise(0)
    ).withColumn(
        "conversion_rate",
        F.when(F.col("total_campaigns_exposed") > 0,
               F.col("total_conversions") / F.col("total_campaigns_exposed")).otherwise(0)
    )

    df_resultado = df_customers.select("customer_id") \
        .join(df_rfm, "customer_id", "left") \
        .join(df_behavioral, "customer_id", "left") \
        .join(df_campanhas, "customer_id", "left")

    for c in df_resultado.columns:
        if c != "customer_id":
            df_resultado = df_resultado.fillna({c: 0})
    return df_resultado


df_features_treino = calcular_features_point_in_time(
    df_transactions, df_events, df_exposures, df_responses, df_customers, data_corte=cutoff_date
)
df_propensity_treino = df_features_treino.join(df_recent_buyers, "customer_id", "left") \
    .fillna({"purchased_last_30d": 0})

# COMMAND ----------

# DBTITLE 1,Preparar Dados
feature_cols = [
    "recency_days", "frequency", "monetary_total", "monetary_avg",
    "event_count_30d", "session_count_30d", "engagement_score_30d",
    "page_views_30d", "product_views_30d", "add_to_cart_30d",
    "total_campaigns_exposed", "response_rate", "conversion_rate"
]

# .toPandas() traz pro driver — trivial em N=10k, mas é o teto de escala deste
# notebook. Ver production/models/sparkml_distributed.py pro caminho distribuído.
df_pandas = df_propensity_treino.select(["customer_id"] + feature_cols + ["purchased_last_30d"]).fillna(0).toPandas()
X = df_pandas[feature_cols]
y = df_pandas["purchased_last_30d"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
print(f"✓ Train: {X_train.shape}, Test: {X_test.shape}")

# COMMAND ----------

# DBTITLE 1,Treinar Modelo
# =============================================================================
# TIPO DE MODELO: CLASSIFICAÇÃO BINÁRIA
# =============================================================================
# Target: purchased_last_30d (0 ou 1)
# Output: Probabilidade de compra (0.0 a 1.0) - "propensity score"
# 
# NÃO é regressão! Embora o output seja contínuo (probabilidade), o problema
# é de classificação porque prevemos uma classe binária (comprou: sim/não).
# 
# XGBClassifier.predict_proba() retorna P(y=1|X), que interpretamos como
# "propensão de compra" - quanto maior, mais provável o cliente comprar.
# =============================================================================

mlflow.set_experiment(MLFLOW_EXPERIMENT_PATH)

# Registrar no Unity Catalog Model Registry (nome de 3 níveis catalog.schema.model),
# não no registry legado "achatado" — mesmo fix aplicado em Modelo Churn Prediction.py.
# Usa alias Champion/Challenger em vez de número de versão fixo.
mlflow.set_registry_uri("databricks-uc")
model_name = f"{CATALOG}.{SCHEMA_GOLD}.propensity_model"

with mlflow.start_run(run_name="propensity_xgboost_v1") as run:
    # XGBClassifier = Classificação Binária (não XGBRegressor)
    model = XGBClassifier(
        n_estimators=100,      # Número de árvores (boosting rounds)
        max_depth=6,           # Profundidade máxima de cada árvore
        learning_rate=0.1,     # Taxa de aprendizado (step size)
        random_state=42        # Semente para reprodução dos resultados
    )
    model.fit(X_train, y_train)

    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)

    metrics = {
        "auc_roc": roc_auc_score(y_test, y_pred_proba),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1": f1_score(y_test, y_pred)
    }

    mlflow.log_params({"n_estimators": 100, "max_depth": 6})
    mlflow.log_metrics(metrics)

    # Criar signature e input_example para Unity Catalog
    from mlflow.models.signature import infer_signature
    signature = infer_signature(X_train, y_pred_proba)
    input_example = X_train.head(5)

    model_info = mlflow.sklearn.log_model(
        model,
        "model",
        signature=signature,
        input_example=input_example,
        registered_model_name=model_name
    )

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

print("\n✓ Modelo treinado")
for k, v in metrics.items():
    print(f"  {k}: {v:.4f}")

# COMMAND ----------

# DBTITLE 1,Salvar Scores
# Score todos os clientes usando o ESTADO ATUAL (gold.customer_features), não o
# dataframe de treino ponto-no-tempo — aqui estamos prevendo os próximos 30 dias
# de verdade a partir de hoje, então usar a feature mais recente é correto (não é
# o mesmo vazamento da célula de treino: não há look-ahead, o alvo é futuro
# desconhecido, não um resultado que já aconteceu no passado).
# predict_proba()[:, 1] = Probabilidade da classe positiva (comprou = 1)
# Isso é o "propensity score" - quanto maior, mais propensão de compra
df_features_atual = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_features"))
df_pandas_atual = df_features_atual.select(["customer_id"] + feature_cols).fillna(0).toPandas()
X_all = df_pandas_atual[feature_cols]
propensity_scores = model.predict_proba(X_all)[:, 1]  # Valores entre 0.0 e 1.0

df_pandas_atual["propensity_score"] = propensity_scores
df_pandas_atual["propensity_category"] = pd.cut(propensity_scores, bins=[0, 0.3, 0.7, 1.0], labels=["Low", "Medium", "High"])

df_scores = df_pandas_atual[["customer_id", "propensity_score", "propensity_category"]]
df_scores_spark = spark.createDataFrame(df_scores)
create_or_replace_table(df_scores_spark, SCHEMA_GOLD, "propensity_scores")

print(f"\n✓ Scores salvos: {get_full_table_name(SCHEMA_GOLD, 'propensity_scores')}")
print("\nDistribuição:")
print(df_scores["propensity_category"].value_counts())
