# Databricks notebook source
# DBTITLE 1,Ativação de Saldo Dormente
# MAGIC %md
# MAGIC # Ativação de Saldo Dormente 💰
# MAGIC
# MAGIC ## Objetivo
# MAGIC Responder ao cenário "cliente com saldo alto parado, sem modelo que oriente a
# MAGIC ativação": priorizar quais clientes têm mais **valor em risco de nunca ser
# MAGIC resgatado** — cruzando saldo acumulado não utilizado com a propensão de resgate.
# MAGIC
# MAGIC ## Abordagem
# MAGIC 1. Derivar um **saldo de pontos/cashback dormente** por cliente (não existe uma
# MAGIC    coluna de saldo pronta no projeto — é calculado a partir de transações e
# MAGIC    resgates já existentes, ver seção 1 abaixo para a fórmula exata).
# MAGIC 2. Treinar um classificador binário: probabilidade do cliente **resgatar nos
# MAGIC    próximos 30 dias** (mesmo padrão metodológico do `Modelo Propensity Score.py`).
# MAGIC 3. **Valor em risco** = saldo dormente × (1 − probabilidade de resgate). Alto
# MAGIC    saldo + baixa propensão de resgate = prioridade máxima de ativação.
# MAGIC
# MAGIC ## Nota de transparência
# MAGIC O dataset sintético deste projeto não tem uma coluna de "saldo de pontos"
# MAGIC nativa nem "canal de aquisição" do cliente. Este notebook deriva um saldo
# MAGIC honesto a partir de dados que já existem (transações geram pontos, resgates
# MAGIC consomem pontos) e usa o canal de campanha mais frequente como proxy de canal de
# MAGIC contato — sem inventar nenhuma tabela ou coluna bruta nova.

# COMMAND ----------

# DBTITLE 1,Instalar Dependências
# MAGIC %pip install xgboost scikit-learn --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuração
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd

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

# COMMAND ----------

# DBTITLE 1,1. Derivar Saldo Dormente
# Regra simples e documentada (não existe coluna de saldo pronta no dataset):
#   pontos_ganhos    = soma de total_amount em silver.transactions (1 ponto por R$1)
#   pontos_resgatados = soma de response_value em silver.campaign_responses onde
#                       is_conversion == 1 (resgate efetivo, não só clique/cadastro)
#   saldo_dormente    = max(pontos_ganhos - pontos_resgatados, 0)
df_transactions = spark.table(get_full_table_name(SCHEMA_SILVER, "transactions"))
df_responses = spark.table(get_full_table_name(SCHEMA_SILVER, "campaign_responses"))
df_exposures = spark.table(get_full_table_name(SCHEMA_SILVER, "campaign_exposures"))

df_pontos_ganhos = df_transactions.groupBy("customer_id").agg(
    F.sum("total_amount").alias("pontos_ganhos")
)

df_resgates = df_responses.filter(F.col("is_conversion") == 1)
df_pontos_resgatados = df_resgates.groupBy("customer_id").agg(
    F.sum("response_value").alias("pontos_resgatados"),
    F.max("response_date").alias("ultimo_resgate_date")
)

# Canal de contato predominante: proxy honesto (o dataset não tem "canal de
# aquisição" do cliente) — canal de campanha ao qual o cliente mais foi exposto.
window_canal = Window.partitionBy("customer_id").orderBy(F.desc("n_exposicoes"))
df_canal_predominante = (
    df_exposures.groupBy("customer_id", "channel")
    .agg(F.count("*").alias("n_exposicoes"))
    .withColumn("rank", F.row_number().over(window_canal))
    .filter(F.col("rank") == 1)
    .select("customer_id", F.col("channel").alias("canal_predominante"))
)

df_saldo = (
    df_pontos_ganhos
    .join(df_pontos_resgatados, "customer_id", "left")
    .join(df_canal_predominante, "customer_id", "left")
    .fillna({"pontos_resgatados": 0.0})
    .withColumn("saldo_dormente", F.greatest(F.col("pontos_ganhos") - F.col("pontos_resgatados"), F.lit(0.0)))
)

max_response_date = df_responses.agg(F.max("response_date")).collect()[0][0]
df_saldo = df_saldo.withColumn(
    "dias_desde_ultimo_resgate",
    F.when(F.col("ultimo_resgate_date").isNotNull(), F.datediff(F.lit(max_response_date), F.col("ultimo_resgate_date")))
     .otherwise(F.lit(9999))  # nunca resgatou
)

print(f"✓ Saldo derivado para {df_saldo.count():,} clientes")
print(f"  Saldo dormente total: R$ {df_saldo.agg(F.sum('saldo_dormente')).collect()[0][0]:,.2f}")
df_saldo.orderBy(F.desc("saldo_dormente")).limit(10).select(
    "customer_id", "pontos_ganhos", "pontos_resgatados", "saldo_dormente", "dias_desde_ultimo_resgate", "canal_predominante"
).display()

# COMMAND ----------

# DBTITLE 1,2. Criar Target - Resgatou nos Últimos 30 Dias
# Mesmo padrão metodológico do Modelo Propensity Score.py: usa o max(date) da
# própria tabela como referência de "agora" (dado sintético não é recente em
# relação a current_date()).
cutoff_date = pd.Timestamp(max_response_date) - pd.Timedelta(days=30)

df_resgatou_recente = (
    df_resgates
    .filter(F.col("response_date") >= cutoff_date)
    .select("customer_id").distinct()
    .withColumn("resgatou_ultimos_30d", F.lit(1))
)

df_features = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_features"))
df_model_data = (
    df_features
    .join(df_saldo.select("customer_id", "saldo_dormente", "dias_desde_ultimo_resgate", "pontos_ganhos"), "customer_id", "inner")
    .join(df_resgatou_recente, "customer_id", "left")
    .fillna({"resgatou_ultimos_30d": 0})
)

print(f"✓ Target criado: {df_model_data.filter(F.col('resgatou_ultimos_30d') == 1).count():,} resgataram nos últimos 30 dias")

# COMMAND ----------

# DBTITLE 1,3. Preparar Dados e Treinar Modelo
# Features RFM/comportamentais já existentes + as duas novas derivadas de saldo
feature_cols = [
    "recency_days", "frequency", "monetary_total", "monetary_avg",
    "customer_lifetime_days", "purchase_frequency_per_day",
    "event_count_30d", "session_count_30d", "engagement_score_30d",
    "total_campaigns_exposed", "treatment_campaigns_count",
    "total_responses", "total_conversions", "response_rate", "conversion_rate",
    "saldo_dormente", "dias_desde_ultimo_resgate"
]

# .toPandas() traz pro driver — trivial em N=10k, mas é o teto de escala deste
# notebook. Ver production/models/sparkml_distributed.py pro caminho distribuído.
df_pandas = df_model_data.select(["customer_id"] + feature_cols + ["resgatou_ultimos_30d"]).fillna(0).toPandas()
X = df_pandas[feature_cols]
y = df_pandas["resgatou_ultimos_30d"]

from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, accuracy_score, precision_score, recall_score

X_train, X_test, y_train, y_test, ids_train, ids_test = train_test_split(
    X, y, df_pandas["customer_id"], test_size=0.2, random_state=42, stratify=y
)
print(f"✓ Train: {X_train.shape}, Test: {X_test.shape}")

# COMMAND ----------

import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature

CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]
MLFLOW_EXPERIMENT_PATH = f"/Users/{CURRENT_USER}/customer_intelligence_experiments"
mlflow.set_experiment(MLFLOW_EXPERIMENT_PATH)

# Registro no Unity Catalog Model Registry (mesmo padrão de Modelo Churn
# Prediction.py / Modelo Propensity Score.py: nome de 3 níveis + alias champion)
mlflow.set_registry_uri("databricks-uc")
model_name = f"{CATALOG}.{SCHEMA_GOLD}.saldo_dormente_model"

with mlflow.start_run(run_name="saldo_dormente_xgboost_v1") as run:
    model = XGBClassifier(n_estimators=100, max_depth=6, learning_rate=0.1, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]

    metrics = {
        "auc_roc": roc_auc_score(y_test, y_pred_proba),
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, zero_division=0),
        "recall": recall_score(y_test, y_pred, zero_division=0)
    }
    mlflow.log_params({"n_estimators": 100, "max_depth": 6})
    mlflow.log_metrics(metrics)

    signature = infer_signature(X_train, model.predict_proba(X_train))
    input_example = X_train.iloc[:5]
    model_info = mlflow.sklearn.log_model(
        model, "model", signature=signature, input_example=input_example,
        registered_model_name=model_name
    )

from mlflow.tracking import MlflowClient
client = MlflowClient()
try:
    current_champion = client.get_model_version_by_alias(model_name, "champion")
    client.set_registered_model_alias(model_name, "challenger", current_champion.version)
except Exception:
    print("ℹ️ Primeira execução — ainda não existia um champion registrado")
client.set_registered_model_alias(model_name, "champion", model_info.registered_model_version)

print("\n" + "="*60)
print("MÉTRICAS DO MODELO")
print("="*60)
for metric, value in metrics.items():
    print(f"{metric}: {value:.4f}")
print(f"\n✓ Modelo registrado: {model_name}@champion (v{model_info.registered_model_version})")

# COMMAND ----------

# DBTITLE 1,4. Priorizar Ativação por Valor em Risco
# valor_em_risco = saldo dormente × probabilidade de NÃO resgatar. Alto saldo +
# baixa propensão de resgate = maior prioridade de ativação (é a resposta direta
# ao caso "cliente com R$1MM parado, sem modelo que oriente a ativação").
X_all = df_pandas[feature_cols]
df_pandas["probabilidade_resgate_30d"] = model.predict_proba(X_all)[:, 1]
df_pandas["valor_em_risco"] = df_pandas["saldo_dormente"] * (1 - df_pandas["probabilidade_resgate_30d"])

df_priorizacao = df_pandas[[
    "customer_id", "saldo_dormente", "dias_desde_ultimo_resgate",
    "probabilidade_resgate_30d", "valor_em_risco"
]].sort_values("valor_em_risco", ascending=False)

# duplicates='drop': se a distribuição tiver muitos zeros (cliente sem saldo
# dormente), os quantis 70/90 podem coincidir — pd.cut quebraria sem isso.
df_priorizacao["prioridade_ativacao"] = pd.cut(
    df_priorizacao["valor_em_risco"],
    bins=[-0.01, df_priorizacao["valor_em_risco"].quantile(0.7), df_priorizacao["valor_em_risco"].quantile(0.9), float("inf")],
    labels=None,
    duplicates="drop"
)
# Com duplicates='drop' o número de bins pode variar; mapeia os intervalos
# resultantes (do menor pro maior) para os rótulos de prioridade.
categorias_ordenadas = sorted(df_priorizacao["prioridade_ativacao"].cat.categories, key=lambda c: c.left)
labels_disponiveis = ["Baixa", "Média", "Alta"][-len(categorias_ordenadas):]
mapa_labels = dict(zip(categorias_ordenadas, labels_disponiveis))
df_priorizacao["prioridade_ativacao"] = df_priorizacao["prioridade_ativacao"].map(mapa_labels)

print("✓ Priorização calculada")
print("\nTop 10 clientes por valor em risco:")
print(df_priorizacao.head(10).to_string(index=False))

df_priorizacao_spark = spark.createDataFrame(df_priorizacao)
create_or_replace_table(df_priorizacao_spark, SCHEMA_GOLD, "saldo_dormente_priorizacao")
print(f"\n✓ Priorização salva: {get_full_table_name(SCHEMA_GOLD, 'saldo_dormente_priorizacao')}")

# COMMAND ----------

# DBTITLE 1,Resumo
print("="*60)
print("ATIVAÇÃO DE SALDO DORMENTE - RESUMO")
print("="*60)
print(f"✅ Modelo: {model_name}@champion")
print(f"✅ AUC-ROC: {metrics['auc_roc']:.4f}")
print(f"✅ Saldo dormente total mapeado: R$ {df_pandas['saldo_dormente'].sum():,.2f}")
print(f"✅ Valor total em risco (alta prioridade): R$ {df_priorizacao[df_priorizacao['prioridade_ativacao']=='Alta']['valor_em_risco'].sum():,.2f}")
print(f"✅ Clientes priorizados para ativação (Alta): {(df_priorizacao['prioridade_ativacao']=='Alta').sum():,}")
print("="*60)

# COMMAND ----------
