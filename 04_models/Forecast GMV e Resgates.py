# Databricks notebook source
# DBTITLE 1,Forecast de GMV / Resgates
# MAGIC %md
# MAGIC # Forecast de GMV / Resgates 📈
# MAGIC
# MAGIC ## Objetivo
# MAGIC Prever o total semanal de resgate (soma de `response_value` em conversões de
# MAGIC campanha) — o mesmo tipo de métrica que motiva um forecast de GMV: "quanto a
# MAGIC gente vai resgatar/faturar nas próximas semanas, pra decidir se é agressivo ou
# MAGIC conservador nas campanhas do próximo mês".
# MAGIC
# MAGIC ## Abordagem
# MAGIC Prophet (Meta/Facebook), que lida nativamente com tendência + sazonalidade +
# MAGIC intervalos de confiança, sem precisar diferenciar a série manualmente como no ARIMA
# MAGIC puro.
# MAGIC
# MAGIC ## Nota de transparência sobre sazonalidade
# MAGIC As datas de exposição/resposta de campanha neste dataset são geradas
# MAGIC **uniformemente aleatórias** (ver `01_bronze/Ingestao Dados Bronze.py`, linhas
# MAGIC 199/252/276 — composição de 3 uniformes aninhadas) — não têm nenhum efeito de
# MAGIC calendário real embutido, e cobrem só ~22 meses (menos de 2 ciclos anuais
# MAGIC completos). Por isso, o forecast principal (seção 2) vai mostrar tendência +
# MAGIC ruído, não sazonalidade — isso é esperado e é honesto sobre a limitação do dado
# MAGIC sintético, não um bug. A seção 3 mostra, à parte e claramente rotulada como
# MAGIC ilustrativa, como o Prophet e o SARIMA lidam com sazonalidade e datas comerciais
# MAGIC quando elas existem de verdade nos dados.
# MAGIC
# MAGIC ## O que tem em cada seção
# MAGIC 1-2. Forecast real (Prophet, holdout de 8 semanas, registro no MLflow/UC)
# MAGIC 3. Real vs. Meta — comparação do forecast/observado contra uma meta mensal
# MAGIC    ilustrativa
# MAGIC 4. Ilustrativo: calendário de datas comerciais brasileiras (gerado
# MAGIC    programaticamente), Prophet com `holidays`, SARIMA com grid search de
# MAGIC    hiperparâmetros por AIC, e comparação entre os dois — tudo em série 100%
# MAGIC    sintética, nunca registrado no MLflow (mesma regra de sempre: só o
# MAGIC    `modelo_prophet` da seção 1-2 vai pro registry)

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

import warnings
warnings.filterwarnings('ignore')

import mlflow
import mlflow.prophet
from mlflow.tracking import MlflowClient

mlflow.set_registry_uri("databricks-uc")

print("✓ Configuração carregada")

# COMMAND ----------

# DBTITLE 1,1. Agregar Resgate Semanal (dado real do projeto)
df_responses = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.campaign_responses")
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

# DBTITLE 1,2. Forecast com Prophet
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error

# Prophet exige colunas 'ds' (data) e 'y' (valor)
df_prophet = df_semanal.rename(columns={"semana": "ds", "total_resgate": "y"})[["ds", "y"]]

# Holdout honesto: últimas 8 semanas pra validar antes de confiar no forecast futuro
N_HOLDOUT = 8
df_treino = df_prophet.iloc[:-N_HOLDOUT]
df_teste = df_prophet.iloc[-N_HOLDOUT:]

modelo_prophet = Prophet(
    yearly_seasonality=False,   # dado não cobre múltiplos anos com sinal real
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
# Score.py. IMPORTANTE: só modelo_prophet (treinado no dado real acima) é
# registrado — o modelo_ilustrativo da seção 3 (sazonalidade sintética) nunca
# deve ir pro registry, seria desonesto apresentar como forecast de verdade.
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
print("\n  ⚠️ Como a série não tem sazonalidade real (dado sintético uniforme), o Prophet")
print("     essencialmente projeta a tendência recente + intervalo de confiança —")
print("     não há padrão de calendário real pra ele aprender aqui.")

# COMMAND ----------

# DBTITLE 1,Visualizar forecast
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

# DBTITLE 1,3. Real vs. Meta
# MAGIC %md
# MAGIC ## Real vs. Meta
# MAGIC Compara o observado/forecast mensal contra uma meta de negócio — aqui uma
# MAGIC meta **ilustrativa/sintética**, não um valor real de nenhuma empresa. O
# MAGIC objetivo é demonstrar o padrão de análise (mensal, gap percentual), que é
# MAGIC independente de a série ter sazonalidade real ou não — por isso entra aqui,
# MAGIC na seção do dado real, sem contradizer a nota de transparência acima.

# COMMAND ----------

# DBTITLE 1,Comparar observado/forecast mensal contra meta ilustrativa
# Agrega observado (histórico) + forecast (futuro) por mês
df_mensal = previsao[["ds", "yhat"]].copy()
df_mensal["mes"] = df_mensal["ds"].dt.to_period("M").dt.to_timestamp()
df_mensal_resumo = df_mensal.groupby("mes")["yhat"].sum().reset_index()

# Meta ilustrativa: crescimento mensal simples sobre a média histórica —
# valor sintético só para demonstrar o padrão de comparação, não uma meta real.
media_historica_mensal = df_prophet.set_index("ds")["y"].resample("MS").sum().mean()
df_mensal_resumo["meta_ilustrativa"] = media_historica_mensal * 1.10  # +10% de meta sobre a média histórica
df_mensal_resumo["gap_pct"] = (df_mensal_resumo["yhat"] / df_mensal_resumo["meta_ilustrativa"] - 1) * 100

print("=" * 60)
print("REAL/FORECAST vs. META (ilustrativa)")
print("=" * 60)
print(df_mensal_resumo.to_string(index=False, formatters={
    "yhat": "R$ {:,.2f}".format,
    "meta_ilustrativa": "R$ {:,.2f}".format,
    "gap_pct": "{:+.1f}%".format,
}))

# COMMAND ----------

# DBTITLE 1,4. Ilustrativo: calendário comercial, Prophet vs. SARIMA (dado sintético à parte)
# MAGIC %md
# MAGIC ⚠️ **Esta seção usa uma série 100% sintética, criada só para esta demonstração**,
# MAGIC não vem de nenhuma tabela do catálogo. A série real de resgates (seção 1) não
# MAGIC tem sazonalidade real nem alcance temporal suficiente (~22 meses, < 2 ciclos
# MAGIC anuais — ver nota de transparência no topo do notebook) pra justificar
# MAGIC calendário de datas comerciais ou SARIMA sazonal. Esta seção mostra a
# MAGIC metodologia — calendário de datas comerciais brasileiras como regressor,
# MAGIC Prophet com `holidays`, SARIMA com seleção de hiperparâmetros por AIC — numa
# MAGIC série onde esses efeitos existem de verdade.

# COMMAND ----------

# DBTITLE 1,Calendário de datas comerciais brasileiras (gerado programaticamente)
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


calendario_comercial = gerar_calendario_datas_comerciais([2022, 2023])
print(f"✓ Calendário gerado: {len(calendario_comercial)} eventos em {calendario_comercial['ds'].dt.year.nunique()} anos")
calendario_comercial

# COMMAND ----------

# DBTITLE 1,Simular série com sazonalidade + efeito de datas comerciais
rng = np.random.default_rng(11)
datas_ilustrativas = pd.date_range("2022-01-01", periods=104, freq="W")
tendencia = np.linspace(1000, 1800, 104)
sazonalidade_anual = 400 * np.sin(2 * np.pi * np.arange(104) / 52)  # ciclo anual

# Injeta um bump nas semanas dentro da janela de cada data comercial — ao
# contrário da sazonalidade senoidal genérica acima, isso simula picos
# pontuais de campanha/resgate ao redor de datas de varejo específicas.
efeito_datas_comerciais = np.zeros(104)
for _, evento in calendario_comercial.iterrows():
    janela_inicio = evento["ds"] + pd.Timedelta(days=evento["lower_window"])
    janela_fim = evento["ds"] + pd.Timedelta(days=evento["upper_window"])
    mask = (datas_ilustrativas >= janela_inicio) & (datas_ilustrativas <= janela_fim)
    efeito_datas_comerciais[mask] += 350

ruido = rng.normal(0, 60, 104)
serie_ilustrativa = pd.DataFrame({
    "ds": datas_ilustrativas,
    "y": tendencia + sazonalidade_anual + efeito_datas_comerciais + ruido
})
# log1p (não log puro): mais robusto a valores próximos de zero — convenção
# adotada mesmo aqui, onde a série é sempre positiva por construção, para
# manter o padrão consistente caso este bloco seja reaproveitado numa série
# que tenha zeros de verdade.
serie_ilustrativa["y_log1p"] = np.log1p(serie_ilustrativa["y"])

print(f"✓ Série ilustrativa gerada: {len(serie_ilustrativa)} semanas, "
      f"{(efeito_datas_comerciais > 0).sum()} com efeito de data comercial")

# COMMAND ----------

# DBTITLE 1,Prophet com calendário de datas comerciais
df_prophet_ilustrativo = serie_ilustrativa[["ds", "y_log1p"]].rename(columns={"y_log1p": "y"})

modelo_ilustrativo = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=False,
    daily_seasonality=False,
    holidays=calendario_comercial
)
modelo_ilustrativo.fit(df_prophet_ilustrativo)
futuro_ilustrativo = modelo_ilustrativo.make_future_dataframe(periods=26, freq="W")
previsao_ilustrativa = modelo_ilustrativo.predict(futuro_ilustrativo)

fig2 = modelo_ilustrativo.plot_components(previsao_ilustrativa)
plt.suptitle("Decomposição tendência + sazonalidade + datas comerciais (série ilustrativa)", y=1.02)
plt.tight_layout()
plt.show()

print("✓ Com sazonalidade e datas comerciais reais nos dados, o Prophet separa")
print("  tendência, sazonalidade anual e efeito de cada data comercial automaticamente.")

# COMMAND ----------

# DBTITLE 1,SARIMA com grid search de hiperparâmetros (comparação)
# Comparação exploratória, mesmo espírito do "XGBoost vs LightGBM" em
# Modelo Churn Prediction.py: outro algoritmo, mesmo dado, só pra comparar
# com rigor — nenhum dos dois modelos desta seção ilustrativa vai pro
# MLflow Registry (só o modelo_prophet da seção 1, com dado real).
#
# Grid search só em (p,d,q), SEM termo sazonal (P,D,Q,m): tanto a série real
# (~22 meses) quanto esta ilustrativa (104 semanas ≈ 2 anos) não têm ciclos
# suficientes pra estimar sazonalidade de período 52 com confiança — rodar
# grid search sazonal aqui seria caro e o resultado não seria confiável.
import itertools
import warnings

import statsmodels.api as sm

warnings.filterwarnings("ignore")

y_sarima = serie_ilustrativa["y_log1p"].values
melhor_aic = np.inf
melhor_ordem = None
modelo_sarima = None

for p, d, q in itertools.product(range(3), range(3), range(3)):
    try:
        resultado = sm.tsa.statespace.SARIMAX(
            y_sarima, order=(p, d, q),
            enforce_stationarity=False, enforce_invertibility=False
        ).fit(disp=False)
        if resultado.aic < melhor_aic:
            melhor_aic = resultado.aic
            melhor_ordem = (p, d, q)
            modelo_sarima = resultado
    except Exception:
        continue  # combinação não convergiu, pula pra próxima

print(f"✓ Melhor ordem SARIMA (menor AIC entre 27 combinações testadas): {melhor_ordem}, AIC={melhor_aic:.2f}")

forecast_sarima = modelo_sarima.get_forecast(steps=26)
previsao_sarima_original = np.expm1(forecast_sarima.predicted_mean)

# COMMAND ----------

# DBTITLE 1,Comparação Prophet vs. SARIMA (série ilustrativa)
previsao_prophet_original = np.expm1(previsao_ilustrativa["yhat"].tail(26).values)

comparacao_df = pd.DataFrame({
    "semana_futura": range(1, 27),
    "Prophet (com holidays)": np.round(previsao_prophet_original, 2),
    "SARIMA (melhor AIC)": np.round(previsao_sarima_original, 2),
})
comparacao_df["Diferença %"] = (
    (comparacao_df["SARIMA (melhor AIC)"] / comparacao_df["Prophet (com holidays)"] - 1) * 100
).round(1)

print("=" * 60)
print("PROPHET vs. SARIMA — PRÓXIMAS 26 SEMANAS (série ilustrativa)")
print("=" * 60)
print(comparacao_df.head(10).to_string(index=False))
print("\n⚠️ Série ilustrativa: diferença entre os dois aqui não generaliza para dado")
print("   real — o objetivo é demonstrar comparação metodológica rigorosa, como já")
print("   feito para Churn (XGBoost vs LightGBM).")

# COMMAND ----------

# DBTITLE 1,Resumo
print("="*60)
print("FORECAST DE RESGATE - RESUMO")
print("="*60)
print(f"✅ Série real: {len(df_semanal)} semanas de resgate agregado")
print(f"✅ MAE holdout: R$ {mae:,.2f} | MAPE holdout: {mape:.1%}")
print("✅ Forecast gerado para as próximas 8 semanas")
print("✅ Real vs. Meta: comparação mensal contra meta ilustrativa")
print("✅ Seção ilustrativa: calendário comercial + Prophet(holidays) vs. SARIMA(AIC)")
print("="*60)

# COMMAND ----------
