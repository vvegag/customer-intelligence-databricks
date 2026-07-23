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
# MAGIC **uniformemente aleatórias** (ver `01_bronze/Ingestao Dados Bronze.py`) — não têm
# MAGIC nenhum efeito de calendário real embutido (sem sazonalidade semanal, mensal ou de
# MAGIC datas comerciais). Por isso, o forecast principal (seção 2) vai mostrar tendência
# MAGIC + ruído, não sazonalidade — isso é esperado e é honesto sobre a limitação do dado
# MAGIC sintético, não um bug. A seção 3 mostra, à parte e claramente rotulada como
# MAGIC ilustrativa, como o Prophet decompõe sazonalidade quando ela existe de verdade.

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

import warnings
warnings.filterwarnings('ignore')

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

print(f"✓ Modelo treinado com {len(df_treino)} semanas, validado em {N_HOLDOUT} semanas de holdout")
print(f"  MAE no holdout: R$ {mae:,.2f}")
print(f"  MAPE no holdout: {mape:.1%}")
print(f"\n  ⚠️ Como a série não tem sazonalidade real (dado sintético uniforme), o Prophet")
print(f"     essencialmente projeta a tendência recente + intervalo de confiança —")
print(f"     não há padrão de calendário real pra ele aprender aqui.")

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

# DBTITLE 1,3. Ilustrativo: decomposição de sazonalidade (dado sintético à parte)
# MAGIC %md
# MAGIC ⚠️ **Esta seção usa uma série 100% sintética, criada só para esta demonstração**,
# MAGIC não vem de nenhuma tabela do catálogo — mostra como o Prophet decompõe
# MAGIC tendência + sazonalidade quando ela existe de verdade nos dados (o que não é o
# MAGIC caso da série de resgates real acima).

# COMMAND ----------

# DBTITLE 1,Simular série com sazonalidade real e decompor
rng = np.random.default_rng(11)
datas_ilustrativas = pd.date_range("2022-01-01", periods=104, freq="W")
tendencia = np.linspace(1000, 1800, 104)
sazonalidade_anual = 400 * np.sin(2 * np.pi * np.arange(104) / 52)  # ciclo anual
ruido = rng.normal(0, 60, 104)
serie_ilustrativa = pd.DataFrame({
    "ds": datas_ilustrativas,
    "y": tendencia + sazonalidade_anual + ruido
})

modelo_ilustrativo = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False)
modelo_ilustrativo.fit(serie_ilustrativa)
futuro_ilustrativo = modelo_ilustrativo.make_future_dataframe(periods=26, freq="W")
previsao_ilustrativa = modelo_ilustrativo.predict(futuro_ilustrativo)

fig2 = modelo_ilustrativo.plot_components(previsao_ilustrativa)
plt.suptitle("Decomposição tendência + sazonalidade (série sintética ilustrativa)", y=1.02)
plt.tight_layout()
plt.show()

print("✓ Com sazonalidade real nos dados, o Prophet separa tendência de sazonalidade")
print("  automaticamente — é essa decomposição que faltaria fazer manualmente com um")
print("  ARIMA puro, e que motivou a mistura ARIMA+Prophet mencionada na entrevista.")

# COMMAND ----------

# DBTITLE 1,Resumo
print("="*60)
print("FORECAST DE RESGATE - RESUMO")
print("="*60)
print(f"✅ Série real: {len(df_semanal)} semanas de resgate agregado")
print(f"✅ MAE holdout: R$ {mae:,.2f} | MAPE holdout: {mape:.1%}")
print(f"✅ Forecast gerado para as próximas 8 semanas")
print(f"✅ Seção ilustrativa demonstra decomposição de sazonalidade (dado sintético à parte)")
print("="*60)

# COMMAND ----------
