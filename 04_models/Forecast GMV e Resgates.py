# Databricks notebook source
# DBTITLE 1,Forecast de GMV / Resgates
# MAGIC %md
# MAGIC # Forecast de GMV / Resgates 📈
# MAGIC
# MAGIC ## Objetivo
# MAGIC Prever o resgate (soma de `response_value` em conversões de campanha) nas
# MAGIC duas granularidades que um time de negócio realmente usa: **semanal**
# MAGIC (operacional — ajustar campanha da próxima semana) e **mensal**
# MAGIC (estratégico — bater meta do mês, alinhar metodologia entre times).
# MAGIC
# MAGIC ## Abordagem
# MAGIC Prophet (Meta/Facebook) no dado real — lida nativamente com tendência +
# MAGIC intervalo de confiança sem precisar diferenciar a série manualmente. A seção
# MAGIC ilustrativa compara Prophet com `holidays` contra SARIMA sazonal (grid search
# MAGIC por AIC), nas duas granularidades também.
# MAGIC
# MAGIC ## Nota de transparência sobre sazonalidade
# MAGIC As datas de exposição/resposta de campanha neste dataset são geradas
# MAGIC **uniformemente aleatórias** (ver `01_bronze/Ingestao Dados Bronze.py` —
# MAGIC `exposure_date`/`response_date` são deslocamentos aleatórios dentro da janela
# MAGIC de cada campanha) — não têm nenhum efeito de calendário real embutido. Isso
# MAGIC **não muda com mais anos de histórico**: mais dados dão mais ciclos pra
# MAGIC estimar estatisticamente, mas não criam sazonalidade que não existe no
# MAGIC processo gerador. Por isso o forecast real (seções 1-3) continua sem
# MAGIC `yearly_seasonality`/SARIMA sazonal — ligar isso aqui seria o Prophet/SARIMA
# MAGIC "encontrando" um padrão de calendário em ruído uniforme, que é exatamente o
# MAGIC tipo de erro que este notebook está evitando de propósito. A seção
# MAGIC ilustrativa (5-6) mostra a mesma metodologia numa série onde a sazonalidade
# MAGIC existe de verdade, porque foi injetada deliberadamente.
# MAGIC
# MAGIC ## O que tem em cada seção
# MAGIC 1. Forecast real **semanal** (Prophet, holdout de 8 semanas, registro no MLflow/UC)
# MAGIC 2. Forecast real **mensal** (Prophet, holdout de 3 meses — informativo/dashboard, não registrado)
# MAGIC 3. Real vs. Meta — forecast mensal contra uma meta ilustrativa
# MAGIC 4. Calendário de datas comerciais brasileiras (gerado programaticamente)
# MAGIC 5. Ilustrativo **semanal**: Prophet com `holidays` vs. SARIMA sazonal (m=52)
# MAGIC 6. Ilustrativo **mensal**: Prophet com `holidays` vs. SARIMA sazonal (m=12)
# MAGIC 7. Persistência em tabelas Gold (pra alimentar dashboards no futuro)
# MAGIC
# MAGIC Nenhum modelo das seções 4-6 (ilustrativas) vai pro MLflow Registry — só o
# MAGIC Prophet semanal real (seção 1).

# COMMAND ----------

# DBTITLE 1,Instalar Dependências
# MAGIC %pip install prophet --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuração
from pyspark.sql import functions as F
import pandas as pd
import numpy as np

CATALOG = "customer_intelligence"
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

import mlflow
import mlflow.prophet
from mlflow.tracking import MlflowClient

mlflow.set_registry_uri("databricks-uc")

print("✓ Configuração carregada")

# COMMAND ----------

# DBTITLE 1,1. Forecast Real Semanal — Agregar Resgate
df_responses = spark.table(get_full_table_name(SCHEMA_SILVER, "campaign_responses"))
df_resgates = df_responses.filter(F.col("is_conversion") == 1)

df_semanal = (
    df_resgates
    .withColumn("semana", F.date_trunc("week", F.col("response_date")))
    .groupBy("semana")
    .agg(
        F.sum("response_value").alias("total_resgate"),
        F.count("*").alias("n_resgates")
    )
    .orderBy("semana")
).toPandas()

print(f"✓ {len(df_semanal)} semanas agregadas")
print(f"  Período: {df_semanal['semana'].min().date()} a {df_semanal['semana'].max().date()}")
print(f"  Total de resgate no período: R$ {df_semanal['total_resgate'].sum():,.2f}")
df_semanal.tail(10)

# COMMAND ----------

# DBTITLE 1,Forecast Semanal com Prophet
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error

# Prophet exige colunas 'ds' (data) e 'y' (valor)
df_prophet = df_semanal.rename(columns={"semana": "ds", "total_resgate": "y"})[["ds", "y"]]

# Holdout honesto: últimas 8 semanas pra validar antes de confiar no forecast futuro
N_HOLDOUT = 8
df_treino = df_prophet.iloc[:-N_HOLDOUT]
df_teste = df_prophet.iloc[-N_HOLDOUT:]

modelo_prophet = Prophet(
    yearly_seasonality=False,   # dado gerado uniformemente aleatório, sem sazonalidade real (ver nota no topo)
    weekly_seasonality=False,   # já agregado por semana, não faz sentido aqui
    daily_seasonality=False,
    interval_width=0.90
)
modelo_prophet.fit(df_treino)

# Prevê o período de holdout + mais 8 semanas à frente (futuro real).
# freq="W-MON": o date_trunc('week', ...) do Spark ancora em segunda-feira,
# não em domingo (o padrão freq="W" do pandas) — usar "W" aqui faria as datas
# do forecast nunca baterem com as datas reais do holdout (df_teste).
futuro = modelo_prophet.make_future_dataframe(periods=N_HOLDOUT + 8, freq="W-MON")
previsao = modelo_prophet.predict(futuro)

# Validação no holdout
previsao_holdout = previsao.set_index("ds").loc[df_teste["ds"], "yhat"]
mae = mean_absolute_error(df_teste["y"], previsao_holdout)
mape = mean_absolute_percentage_error(df_teste["y"], previsao_holdout)

# Registro no Unity Catalog Model Registry, mesmo padrão de Modelo Propensity
# Score.py. IMPORTANTE: só este modelo (semanal, treinado no dado real) é
# registrado — nenhum modelo mensal ou ilustrativo (seções 2 e 4-6) vai pro
# registry, seria desonesto apresentar como forecast de verdade.
model_name = f"{CATALOG}.{SCHEMA_GOLD}.forecast_gmv_model"

with mlflow.start_run(run_name="forecast_prophet_v1") as run:
    mlflow.log_params({
        "n_holdout_weeks": N_HOLDOUT,
        "interval_width": 0.90,
        "yearly_seasonality": False,
        "weekly_seasonality": False
    })
    mlflow.log_metrics({"mae": mae, "mape": mape})

    model_info = mlflow.prophet.log_model(
        modelo_prophet, "model",
        registered_model_name=model_name
    )

client = MlflowClient()
try:
    current_champion = client.get_model_version_by_alias(model_name, "champion")
    client.set_registered_model_alias(model_name, "challenger", current_champion.version)
    print(f"✓ Champion anterior (v{current_champion.version}) rebaixado para challenger")
except Exception:
    print("ℹ️ Primeira execução — ainda não existia um champion registrado")

client.set_registered_model_alias(model_name, "champion", model_info.registered_model_version)
print(f"✓ Modelo registrado: {model_name}@champion (v{model_info.registered_model_version})")

print(f"✓ Modelo treinado com {len(df_treino)} semanas, validado em {N_HOLDOUT} semanas de holdout")
print(f"  MAE no holdout: R$ {mae:,.2f}")
print(f"  MAPE no holdout: {mape:.1%}")

# COMMAND ----------

# DBTITLE 1,Visualizar forecast semanal
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(df_prophet["ds"], df_prophet["y"], "k.", label="Observado")
ax.plot(previsao["ds"], previsao["yhat"], label="Forecast")
ax.fill_between(previsao["ds"], previsao["yhat_lower"], previsao["yhat_upper"], alpha=0.2, label="Intervalo 90%")
ax.axvline(df_teste["ds"].iloc[0], color="gray", linestyle="--", label="Início holdout")
ax.set_title("Forecast de Resgate Semanal (R$)")
ax.set_xlabel("Semana")
ax.set_ylabel("Total resgatado (R$)")
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,2. Forecast Real Mensal
# MAGIC %md
# MAGIC ## Forecast Mensal (dado real)
# MAGIC Mesma série real, agregada por mês — visão estratégica pra alinhar meta
# MAGIC mensal com o time, complementar à visão semanal operacional acima. Mesma
# MAGIC lógica (Prophet, sem sazonalidade — dado ainda uniformemente aleatório),
# MAGIC só que **não registrado no MLflow**: é a mesma série de negócio vista em
# MAGIC outra granularidade, não um segundo modelo concorrente — só o Prophet
# MAGIC semanal continua sendo `@champion`.

# COMMAND ----------

# DBTITLE 1,Agregar e prever resgate mensal
df_mensal_real = (
    df_resgates
    .withColumn("mes", F.date_trunc("month", F.col("response_date")))
    .groupBy("mes")
    .agg(F.sum("response_value").alias("total_resgate"))
    .orderBy("mes")
).toPandas()

df_prophet_mensal = df_mensal_real.rename(columns={"mes": "ds", "total_resgate": "y"})[["ds", "y"]]

N_HOLDOUT_MENSAL = 3
df_treino_mensal = df_prophet_mensal.iloc[:-N_HOLDOUT_MENSAL]
df_teste_mensal = df_prophet_mensal.iloc[-N_HOLDOUT_MENSAL:]

modelo_prophet_mensal = Prophet(
    yearly_seasonality=False,  # mesmo motivo do semanal — ver nota de transparência
    weekly_seasonality=False,
    daily_seasonality=False,
    interval_width=0.90
)
modelo_prophet_mensal.fit(df_treino_mensal)
futuro_mensal = modelo_prophet_mensal.make_future_dataframe(periods=N_HOLDOUT_MENSAL + 3, freq="MS")
previsao_mensal = modelo_prophet_mensal.predict(futuro_mensal)

previsao_holdout_mensal = previsao_mensal.set_index("ds").loc[df_teste_mensal["ds"], "yhat"]
mae_mensal = mean_absolute_error(df_teste_mensal["y"], previsao_holdout_mensal)
mape_mensal = mean_absolute_percentage_error(df_teste_mensal["y"], previsao_holdout_mensal)

print(f"✓ Modelo mensal treinado com {len(df_treino_mensal)} meses, validado em {N_HOLDOUT_MENSAL} meses de holdout")
print(f"  MAE no holdout: R$ {mae_mensal:,.2f}")
print(f"  MAPE no holdout: {mape_mensal:.1%}")
print("  ℹ️ Informativo — não registrado no MLflow (só o modelo semanal é @champion)")

# COMMAND ----------

# DBTITLE 1,3. Real vs. Meta
# MAGIC %md
# MAGIC ## Real vs. Meta
# MAGIC Compara o forecast mensal real (seção 2) contra uma meta de negócio — aqui
# MAGIC uma meta **ilustrativa/sintética**, não um valor real de nenhuma empresa.

# COMMAND ----------

# DBTITLE 1,Comparar forecast mensal contra meta ilustrativa
df_mensal_resumo = previsao_mensal[["ds", "yhat"]].copy()

# Meta ilustrativa: +10% sobre a média histórica mensal — valor sintético só
# para demonstrar o padrão de comparação, não uma meta real.
media_historica_mensal = df_prophet_mensal.set_index("ds")["y"].mean()
df_mensal_resumo["meta_ilustrativa"] = media_historica_mensal * 1.10
df_mensal_resumo["gap_pct"] = (df_mensal_resumo["yhat"] / df_mensal_resumo["meta_ilustrativa"] - 1) * 100

print("=" * 60)
print("FORECAST MENSAL vs. META (ilustrativa)")
print("=" * 60)
print(df_mensal_resumo.to_string(index=False, formatters={
    "yhat": "R$ {:,.2f}".format,
    "meta_ilustrativa": "R$ {:,.2f}".format,
    "gap_pct": "{:+.1f}%".format,
}))

# COMMAND ----------

# DBTITLE 1,4. Calendário de datas comerciais brasileiras (gerado programaticamente)
# MAGIC %md
# MAGIC ⚠️ **A partir daqui, seções 5 e 6 usam uma série 100% sintética**, criada só
# MAGIC pra esta demonstração, não vem de nenhuma tabela do catálogo. Mostram a
# MAGIC metodologia — calendário de datas comerciais como regressor, Prophet com
# MAGIC `holidays`, SARIMA sazonal com seleção de hiperparâmetros por AIC, nas duas
# MAGIC granularidades — numa série onde esses efeitos existem de verdade (ver nota
# MAGIC de transparência no topo do notebook sobre por que o dado real não usa isso).

# COMMAND ----------

# DBTITLE 1,Gerar calendário
# Datas móveis (Carnaval, Páscoa, Dia das Mães/Pais) calculadas por regra, não
# por lista de datas hardcoded por ano — evita manutenção manual ano a ano.
from datetime import datetime, timedelta
import calendar as calendar_module

from dateutil.easter import easter


def nth_weekday_of_month(year, month, weekday, n):
    """N-ésima ocorrência de um dia da semana num mês (weekday: Monday=0..Sunday=6)."""
    first_day = datetime(year, month, 1)
    days_until = (weekday - first_day.weekday()) % 7
    day = 1 + days_until + (n - 1) * 7
    return datetime(year, month, day).date()


def last_weekday_of_month(year, month, weekday):
    """Última ocorrência de um dia da semana num mês (ex: última sexta = Black Friday)."""
    last_day_num = calendar_module.monthrange(year, month)[1]
    last_day = datetime(year, month, last_day_num)
    days_back = (last_day.weekday() - weekday) % 7
    return (last_day - timedelta(days=days_back)).date()


def gerar_calendario_datas_comerciais(anos):
    """Calendário de datas comerciais do varejo brasileiro, formato Prophet `holidays`
    (colunas: holiday, ds, lower_window, upper_window)."""
    eventos = []
    for ano in anos:
        pascoa = easter(ano)
        carnaval = pascoa - timedelta(days=47)
        eventos.append(("Carnaval", carnaval, 0, 4))
        eventos.append(("Pascoa", pascoa, -7, 0))
        eventos.append(("Dia do Consumidor", datetime(ano, 3, 15).date(), -7, 0))
        eventos.append(("Dia das Maes", nth_weekday_of_month(ano, 5, 6, 2), -7, 0))
        eventos.append(("Dia dos Namorados", datetime(ano, 6, 12).date(), -15, 7))
        eventos.append(("Dia dos Pais", nth_weekday_of_month(ano, 8, 6, 2), -7, 0))
        eventos.append(("Dia das Criancas", datetime(ano, 10, 12).date(), -5, 0))
        eventos.append(("Black Friday", last_weekday_of_month(ano, 11, 4), -3, 1))
        eventos.append(("Natal", datetime(ano, 12, 25).date(), -10, 0))
    df = pd.DataFrame(eventos, columns=["holiday", "ds", "lower_window", "upper_window"])
    df["ds"] = pd.to_datetime(df["ds"])
    return df


# 4 anos, mesma janela do dado real (01_bronze/Ingestao Dados Bronze.py: DATA_BASE=2022-01-01, JANELA_DIAS=4 anos)
calendario_comercial = gerar_calendario_datas_comerciais([2022, 2023, 2024, 2025])
print(f"✓ Calendário gerado: {len(calendario_comercial)} eventos em {calendario_comercial['ds'].dt.year.nunique()} anos")
calendario_comercial

# COMMAND ----------

# DBTITLE 1,Simular série ilustrativa (4 anos, com sazonalidade + datas comerciais)
rng = np.random.default_rng(11)
N_SEMANAS_ILUSTRATIVAS = 208  # 4 anos — mesma janela do dado real, dá ciclos suficientes pra SARIMA sazonal
datas_ilustrativas = pd.date_range("2022-01-01", periods=N_SEMANAS_ILUSTRATIVAS, freq="W")
tendencia = np.linspace(1000, 2600, N_SEMANAS_ILUSTRATIVAS)
sazonalidade_anual = 400 * np.sin(2 * np.pi * np.arange(N_SEMANAS_ILUSTRATIVAS) / 52)  # ciclo anual

# Injeta um bump nas semanas dentro da janela de cada data comercial — ao
# contrário da sazonalidade senoidal genérica acima, isso simula picos
# pontuais de campanha/resgate ao redor de datas de varejo específicas.
efeito_datas_comerciais = np.zeros(N_SEMANAS_ILUSTRATIVAS)
for _, evento in calendario_comercial.iterrows():
    janela_inicio = evento["ds"] + pd.Timedelta(days=evento["lower_window"])
    janela_fim = evento["ds"] + pd.Timedelta(days=evento["upper_window"])
    mask = (datas_ilustrativas >= janela_inicio) & (datas_ilustrativas <= janela_fim)
    efeito_datas_comerciais[mask] += 350

ruido = rng.normal(0, 60, N_SEMANAS_ILUSTRATIVAS)
serie_ilustrativa = pd.DataFrame({
    "ds": datas_ilustrativas,
    "y": tendencia + sazonalidade_anual + efeito_datas_comerciais + ruido
})
# log1p (não log puro): mais robusto a valores próximos de zero.
serie_ilustrativa["y_log1p"] = np.log1p(serie_ilustrativa["y"])

print(f"✓ Série ilustrativa gerada: {len(serie_ilustrativa)} semanas ({N_SEMANAS_ILUSTRATIVAS / 52:.0f} anos), "
      f"{(efeito_datas_comerciais > 0).sum()} com efeito de data comercial")

# COMMAND ----------

# DBTITLE 1,Helper reaproveitável — grid search SARIMA por AIC
# Usado tanto pra visão semanal (m=52) quanto mensal (m=12) — evita duplicar
# a mesma lógica de busca duas vezes.
import itertools
import warnings

import statsmodels.api as sm

warnings.filterwarnings("ignore")


def buscar_melhor_sarima(valores, ordem_grid, ordem_sazonal_grid, m):
    """Grid search de SARIMA(p,d,q)(P,D,Q,m) por AIC.
    ordem_grid: iterável de tuplas (p,d,q). ordem_sazonal_grid: iterável de (P,D,Q).
    Retorna (melhor_modelo_ajustado, melhor_ordem_completa, melhor_aic)."""
    melhor_aic = np.inf
    melhor_modelo = None
    melhor_ordem = None
    for p, d, q in ordem_grid:
        for P, D, Q in ordem_sazonal_grid:
            try:
                resultado = sm.tsa.statespace.SARIMAX(
                    valores, order=(p, d, q), seasonal_order=(P, D, Q, m),
                    enforce_stationarity=False, enforce_invertibility=False
                ).fit(disp=False)
                if resultado.aic < melhor_aic:
                    melhor_aic = resultado.aic
                    melhor_modelo = resultado
                    melhor_ordem = (p, d, q, P, D, Q, m)
            except Exception:
                continue  # combinação não convergiu, pula pra próxima
    return melhor_modelo, melhor_ordem, melhor_aic

# COMMAND ----------

# DBTITLE 1,5. Ilustrativo Semanal — Prophet vs. SARIMA (m=52)
# MAGIC %md
# MAGIC ### Visão semanal (m=52)
# MAGIC Comparação exploratória, mesmo espírito do "XGBoost vs LightGBM" em
# MAGIC `Modelo Churn Prediction.py`: outro algoritmo, mesmo dado, só pra comparar
# MAGIC com rigor — nenhum dos dois vai pro MLflow Registry.
# MAGIC
# MAGIC Grid sazonal deliberadamente pequeno (`P,Q ∈ {0,1}`, `D` fixo): `m=52` é caro
# MAGIC pra grid search — cada ajuste SARIMAX com sazonalidade semanal é bem mais
# MAGIC lento que sem termo sazonal. Com 4 anos (208 semanas) já dá pra tentar
# MAGIC sazonalidade anual com alguma confiança, mas não pra bancar um grid grande.

# COMMAND ----------

# DBTITLE 1,Prophet semanal com calendário de datas comerciais
df_prophet_ilustrativo = serie_ilustrativa[["ds", "y_log1p"]].rename(columns={"y_log1p": "y"})

modelo_ilustrativo_semanal = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=False,
    daily_seasonality=False,
    holidays=calendario_comercial
)
modelo_ilustrativo_semanal.fit(df_prophet_ilustrativo)
futuro_ilustrativo_semanal = modelo_ilustrativo_semanal.make_future_dataframe(periods=26, freq="W")
previsao_ilustrativa_semanal = modelo_ilustrativo_semanal.predict(futuro_ilustrativo_semanal)

fig2 = modelo_ilustrativo_semanal.plot_components(previsao_ilustrativa_semanal)
plt.suptitle("Decomposição semanal: tendência + sazonalidade + datas comerciais (série ilustrativa)", y=1.02)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,SARIMA semanal sazonal (m=52)
melhor_modelo_semanal, melhor_ordem_semanal, aic_semanal = buscar_melhor_sarima(
    serie_ilustrativa["y_log1p"].values,
    ordem_grid=itertools.product(range(2), [0], range(2)),          # p,q ∈ {0,1}, d=0
    ordem_sazonal_grid=itertools.product(range(2), [0], range(2)),  # P,Q ∈ {0,1}, D=0
    m=52
)
print(f"✓ Melhor SARIMA semanal sazonal: ordem={melhor_ordem_semanal}, AIC={aic_semanal:.2f}")

forecast_sarima_semanal = melhor_modelo_semanal.get_forecast(steps=26)
previsao_sarima_semanal_original = np.expm1(forecast_sarima_semanal.predicted_mean)

# COMMAND ----------

# DBTITLE 1,Comparação semanal Prophet vs. SARIMA
previsao_prophet_semanal_original = np.expm1(previsao_ilustrativa_semanal["yhat"].tail(26).values)

comparacao_semanal_df = pd.DataFrame({
    "semana_futura": range(1, 27),
    "prophet_com_holidays": np.round(previsao_prophet_semanal_original, 2),
    "sarima_melhor_aic": np.round(previsao_sarima_semanal_original, 2),
})
comparacao_semanal_df["diferenca_pct"] = (
    (comparacao_semanal_df["sarima_melhor_aic"] / comparacao_semanal_df["prophet_com_holidays"] - 1) * 100
).round(1)

print("=" * 60)
print("PROPHET vs. SARIMA — SEMANAL, PRÓXIMAS 26 SEMANAS (série ilustrativa)")
print("=" * 60)
print(comparacao_semanal_df.head(10).to_string(index=False))
print("\n⚠️ Série ilustrativa: diferença entre os dois aqui não generaliza para dado real.")

# COMMAND ----------

# DBTITLE 1,6. Ilustrativo Mensal — Prophet vs. SARIMA (m=12)
# MAGIC %md
# MAGIC ### Visão mensal (m=12)
# MAGIC Mesma série ilustrativa, reagregada por mês. `m=12` é bem mais barato que
# MAGIC `m=52` pra grid search — dá pra usar um grid maior aqui, mais fiel ao SARIMA
# MAGIC de exemplo real que motivou esta seção (SARIMA(2,0,3)(0,0,3,12) sobre dado
# MAGIC mensal, mesma ideia de calendário comercial como regressor).

# COMMAND ----------

# DBTITLE 1,Reagregar série ilustrativa por mês
# Soma o valor (escala original, não log) por mês, depois reaplica log1p —
# somar em escala log daria um resultado incorreto (log da soma ≠ soma dos logs).
serie_ilustrativa_mensal = (
    serie_ilustrativa.set_index("ds")["y"]
    .resample("MS").sum()
    .reset_index()
)
serie_ilustrativa_mensal["y_log1p"] = np.log1p(serie_ilustrativa_mensal["y"])

print(f"✓ Série ilustrativa mensal: {len(serie_ilustrativa_mensal)} meses")

# COMMAND ----------

# DBTITLE 1,Prophet mensal com calendário de datas comerciais
df_prophet_ilustrativo_mensal = serie_ilustrativa_mensal[["ds", "y_log1p"]].rename(columns={"y_log1p": "y"})

modelo_ilustrativo_mensal = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=False,
    daily_seasonality=False,
    holidays=calendario_comercial
)
modelo_ilustrativo_mensal.fit(df_prophet_ilustrativo_mensal)
futuro_ilustrativo_mensal = modelo_ilustrativo_mensal.make_future_dataframe(periods=6, freq="MS")
previsao_ilustrativa_mensal = modelo_ilustrativo_mensal.predict(futuro_ilustrativo_mensal)

fig3 = modelo_ilustrativo_mensal.plot_components(previsao_ilustrativa_mensal)
plt.suptitle("Decomposição mensal: tendência + sazonalidade + datas comerciais (série ilustrativa)", y=1.02)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,SARIMA mensal sazonal (m=12)
melhor_modelo_mensal, melhor_ordem_mensal, aic_mensal = buscar_melhor_sarima(
    serie_ilustrativa_mensal["y_log1p"].values,
    ordem_grid=itertools.product(range(3), range(2), range(3)),         # p ∈ {0,1,2}, d ∈ {0,1}, q ∈ {0,1,2}
    ordem_sazonal_grid=itertools.product(range(2), range(2), range(2)),  # P,D,Q ∈ {0,1}
    m=12
)
print(f"✓ Melhor SARIMA mensal sazonal: ordem={melhor_ordem_mensal}, AIC={aic_mensal:.2f}")

forecast_sarima_mensal = melhor_modelo_mensal.get_forecast(steps=6)
previsao_sarima_mensal_original = np.expm1(forecast_sarima_mensal.predicted_mean)

# COMMAND ----------

# DBTITLE 1,Comparação mensal Prophet vs. SARIMA
previsao_prophet_mensal_original = np.expm1(previsao_ilustrativa_mensal["yhat"].tail(6).values)

comparacao_mensal_df = pd.DataFrame({
    "mes_futuro": range(1, 7),
    "prophet_com_holidays": np.round(previsao_prophet_mensal_original, 2),
    "sarima_melhor_aic": np.round(previsao_sarima_mensal_original, 2),
})
comparacao_mensal_df["diferenca_pct"] = (
    (comparacao_mensal_df["sarima_melhor_aic"] / comparacao_mensal_df["prophet_com_holidays"] - 1) * 100
).round(1)

print("=" * 60)
print("PROPHET vs. SARIMA — MENSAL, PRÓXIMOS 6 MESES (série ilustrativa)")
print("=" * 60)
print(comparacao_mensal_df.to_string(index=False))
print("\n⚠️ Série ilustrativa: diferença entre os dois aqui não generaliza para dado real.")

# COMMAND ----------

# DBTITLE 1,7. Persistir Tabelas Gold (dashboards futuros)
# MAGIC %md
# MAGIC Salva os resultados como tabelas Delta pra um notebook de dashboard futuro
# MAGIC consumir via SQL (mesmo padrão do resto do projeto — `08_dashboards/SQL
# MAGIC Queries para Dashboards.ipynb`), em vez de só imprimir/plotar. Não conecto
# MAGIC essas tabelas ao notebook de dashboards agora — fica pronto pra quando for
# MAGIC decidido usar.

# COMMAND ----------

# DBTITLE 1,Forecast real (semanal + mensal)
df_forecast_semanal_gold = (
    previsao[["ds", "yhat", "yhat_lower", "yhat_upper"]]
    .merge(df_prophet, on="ds", how="left")
    .rename(columns={"y": "observado", "yhat": "forecast", "yhat_lower": "forecast_min", "yhat_upper": "forecast_max"})
)
create_or_replace_table(spark.createDataFrame(df_forecast_semanal_gold), SCHEMA_GOLD, "forecast_gmv_semanal")

df_forecast_mensal_gold = df_mensal_resumo.rename(columns={"yhat": "forecast"})
create_or_replace_table(spark.createDataFrame(df_forecast_mensal_gold), SCHEMA_GOLD, "forecast_gmv_mensal")

# COMMAND ----------

# DBTITLE 1,Comparação ilustrativa (semanal + mensal)
create_or_replace_table(
    spark.createDataFrame(comparacao_semanal_df), SCHEMA_GOLD, "forecast_ilustrativo_semanal"
)
create_or_replace_table(
    spark.createDataFrame(comparacao_mensal_df), SCHEMA_GOLD, "forecast_ilustrativo_mensal"
)

# COMMAND ----------

# DBTITLE 1,Resumo
print("="*60)
print("FORECAST DE RESGATE - RESUMO")
print("="*60)
print(f"✅ Série real: {len(df_semanal)} semanas / {len(df_mensal_real)} meses de resgate agregado")
print(f"✅ MAE holdout semanal: R$ {mae:,.2f} | MAPE: {mape:.1%}")
print(f"✅ MAE holdout mensal: R$ {mae_mensal:,.2f} | MAPE: {mape_mensal:.1%}")
print("✅ Real vs. Meta: comparação mensal contra meta ilustrativa")
print(f"✅ Ilustrativo semanal (m=52): SARIMA {melhor_ordem_semanal}, AIC={aic_semanal:.1f}")
print(f"✅ Ilustrativo mensal (m=12): SARIMA {melhor_ordem_mensal}, AIC={aic_mensal:.1f}")
print("✅ 4 tabelas Gold salvas: forecast_gmv_semanal, forecast_gmv_mensal,")
print("   forecast_ilustrativo_semanal, forecast_ilustrativo_mensal")
print("="*60)

# COMMAND ----------
