# Databricks notebook source
# DBTITLE 1,Multi-Armed Bandit — Thompson Sampling
# MAGIC %md
# MAGIC # Multi-Armed Bandit — Thompson Sampling 🎰
# MAGIC
# MAGIC ## Objetivo
# MAGIC O `AB Testing e Causal Inference.py` mede o efeito de uma campanha já encerrada
# MAGIC (controle vs. tratamento, alocação de tráfego fixa do início ao fim). Este notebook
# MAGIC resolve um problema diferente: **alocar tráfego dinamicamente entre variantes
# MAGIC enquanto o experimento ainda está rodando**, reduzindo o custo de continuar mandando
# MAGIC tráfego pra uma variante fraca só porque o teste "ainda não acabou".
# MAGIC
# MAGIC ## Abordagem
# MAGIC **Thompson Sampling** com prior Beta-Bernoulli: cada variante tem uma distribuição
# MAGIC Beta(sucessos+1, falhas+1); a cada rodada, sorteamos uma amostra de cada distribuição
# MAGIC e mandamos mais tráfego pra quem sorteou o maior valor. Variantes fracas recebem
# MAGIC tráfego cada vez menor sem nunca chegar a zero (exploração contínua).
# MAGIC
# MAGIC A lógica central (`calcular_alocacao_thompson`) é a mesma que já implementei e testei
# MAGIC (pytest) num case separado — `mab_api_sql_py`, uma API real (FastAPI + PostgreSQL)
# MAGIC de recomendação de alocação de tráfego. Aqui ela é portada standalone, sem trazer
# MAGIC nenhum dado daquele projeto — só a matemática, alimentada 100% com dado deste
# MAGIC catálogo (`customer_intelligence`).
# MAGIC
# MAGIC ## Duas partes
# MAGIC 1. **Grounded nos dados do projeto**: os dois braços reais que já existem em
# MAGIC    `campaign_exposures_raw`/`campaign_responses_raw` (controle vs. tratamento).
# MAGIC 2. **Simulação didática multi-braço**: os dados sintéticos deste projeto não variam
# MAGIC    taxa de resposta por campanha/tipo/desconto (só por controle/tratamento — ver
# MAGIC    `01_bronze/Ingestao Dados Bronze.py`), então não dá pra extrair 3+ braços com
# MAGIC    taxas verdadeiramente diferentes dos dados reais sem inventar sinal que não existe.
# MAGIC    Por isso, a demonstração com múltiplas variantes usa uma simulação **claramente
# MAGIC    rotulada como sintética**, só para mostrar o algoritmo convergindo e o ganho de
# MAGIC    regret vs. alocação fixa (A/B estático).

# COMMAND ----------

# DBTITLE 1,Configuração
from pyspark.sql import functions as F
import numpy as np
import pandas as pd

CATALOG = "customer_intelligence"
SCHEMA_BRONZE = "bronze"

print("✓ Configuração carregada")

# COMMAND ----------

# DBTITLE 1,Núcleo do algoritmo (portado de mab_api_sql_py/bandit/thompson_sampling.py)
# Mesma lógica testada (pytest) no projeto mab_api_sql_py: prior Beta(1,1),
# alpha = sucessos + 1, beta = falhas + 1, estimativa de probabilidade de
# vitória via amostragem Monte Carlo, com piso mínimo de alocação por variante
# (nenhuma variante cai a 0% de tráfego, mesmo perdendo feio).
def calcular_alocacao_thompson(estatisticas: list[dict], numero_amostras: int = 20000,
                                alocacao_minima_variante: float = 0.05, seed: int | None = None) -> pd.DataFrame:
    """
    estatisticas: lista de dicts com nome_variante, impressos (tentativas), cliques (sucessos)
    Retorna DataFrame com percentual de tráfego recomendado por variante.
    """
    if not estatisticas:
        return pd.DataFrame()

    if len(estatisticas) == 1:
        item = estatisticas[0]
        return pd.DataFrame([{
            "nome_variante": item["nome_variante"],
            "percentual_trafego": 1.0,
            "probabilidade_vitoria": 1.0,
            "taxa_estimada": item["cliques"] / item["impressos"] if item["impressos"] else 0.0
        }])

    rng = np.random.default_rng(seed)
    alphas = [item["cliques"] + 1 for item in estatisticas]
    betas = [max(item["impressos"] - item["cliques"], 0) + 1 for item in estatisticas]

    amostras = np.vstack([rng.beta(a, b, size=numero_amostras) for a, b in zip(alphas, betas)])
    vencedores = np.argmax(amostras, axis=0)
    probabilidades_brutas = np.bincount(vencedores, minlength=len(estatisticas)) / float(numero_amostras)

    piso = min(max(alocacao_minima_variante, 0.0), 1.0 / len(estatisticas))
    probabilidades = np.maximum(probabilidades_brutas, piso)
    probabilidades = probabilidades / probabilidades.sum()

    resultados = []
    for item, prob_bruta, prob_final in zip(estatisticas, probabilidades_brutas, probabilidades):
        resultados.append({
            "nome_variante": item["nome_variante"],
            "percentual_trafego": float(prob_final),
            "probabilidade_vitoria": float(prob_bruta),
            "taxa_estimada": item["cliques"] / item["impressos"] if item["impressos"] else 0.0
        })

    return pd.DataFrame(resultados).sort_values("percentual_trafego", ascending=False)

print("✓ Função de alocação carregada")

# COMMAND ----------

# DBTITLE 1,1️⃣ Grounded: braços reais do projeto (Controle vs. Tratamento)
# Impressões = exposições à campanha; cliques = respondeu (campaign_responses_raw
# só tem as linhas onde responded==True, ver Ingestao Dados Bronze.py).
df_exposures = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.campaign_exposures_raw")
df_responses = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.campaign_responses_raw")

df_impressoes = (
    df_exposures.groupBy("is_control_group")
    .agg(F.count("*").alias("impressos"))
)

df_cliques = (
    df_responses
    .join(df_exposures.select("exposure_id", "is_control_group"), "exposure_id")
    .groupBy("is_control_group")
    .agg(F.count("*").alias("cliques"))
)

df_stats_real = df_impressoes.join(df_cliques, "is_control_group").toPandas()

estatisticas_reais = [
    {
        "nome_variante": "Controle" if row["is_control_group"] else "Tratamento",
        "impressos": int(row["impressos"]),
        "cliques": int(row["cliques"])
    }
    for _, row in df_stats_real.iterrows()
]

print("Estatísticas reais (campaign_exposures_raw / campaign_responses_raw):")
for item in estatisticas_reais:
    taxa = item["cliques"] / item["impressos"]
    print(f"  {item['nome_variante']}: {item['impressos']:,} exposições, {item['cliques']:,} respostas ({taxa:.2%})")

alocacao_real = calcular_alocacao_thompson(estatisticas_reais, seed=42)
print("\nAlocação de tráfego recomendada pelo Thompson Sampling:")
print(alocacao_real.to_string(index=False))
print("\n📊 Com só 2 braços e uma diferença de taxa grande (5% vs 12%), o Thompson")
print("   Sampling converge quase todo o tráfego pro Tratamento — o mesmo resultado")
print("   que o AB Testing estático já mostra, só que aqui seria decidido em tempo")
print("   real, sem esperar o experimento acabar pra agir.")

# COMMAND ----------

# DBTITLE 1,2️⃣ Simulação didática multi-braço (dados sintéticos, não do projeto)
# MAGIC %md
# MAGIC ⚠️ **A partir daqui os dados são simulados especificamente para esta demonstração**,
# MAGIC não vêm de nenhuma tabela do catálogo — servem só para mostrar o algoritmo
# MAGIC convergindo com mais de 2 variantes e o ganho de regret vs. alocação fixa.

# COMMAND ----------

# DBTITLE 1,Simular rodadas sequenciais
# 5 variantes de campanha com taxas de conversão "verdadeiras" diferentes
# (desconhecidas pelo algoritmo — ele só vê sucesso/falha a cada rodada).
taxas_verdadeiras = {
    "Variante A (controle)": 0.05,
    "Variante B (10% off)": 0.07,
    "Variante C (15% off)": 0.09,
    "Variante D (frete grátis)": 0.12,
    "Variante E (cashback 2x)": 0.06,
}
nomes_variantes = list(taxas_verdadeiras.keys())
melhor_taxa = max(taxas_verdadeiras.values())

rng = np.random.default_rng(7)
N_RODADAS = 2000

# Thompson Sampling: aloca 1 "cliente" por rodada pra variante amostrada como melhor
contadores_ts = {nome: {"impressos": 0, "cliques": 0} for nome in nomes_variantes}
regret_ts = []
regret_acumulado = 0.0

# A/B estático: aloca uniformemente entre as 5 variantes o tempo todo (baseline)
regret_ab = []
regret_ab_acumulado = 0.0

for rodada in range(1, N_RODADAS + 1):
    # --- Thompson Sampling: amostra da Beta de cada variante, escolhe a maior ---
    amostras = {
        nome: rng.beta(contadores_ts[nome]["cliques"] + 1,
                       contadores_ts[nome]["impressos"] - contadores_ts[nome]["cliques"] + 1)
        for nome in nomes_variantes
    }
    escolhida_ts = max(amostras, key=amostras.get)
    sucesso_ts = rng.random() < taxas_verdadeiras[escolhida_ts]
    contadores_ts[escolhida_ts]["impressos"] += 1
    contadores_ts[escolhida_ts]["cliques"] += int(sucesso_ts)
    regret_acumulado += melhor_taxa - taxas_verdadeiras[escolhida_ts]
    regret_ts.append(regret_acumulado)

    # --- A/B estático: escolhe uniformemente, sem aprender ---
    escolhida_ab = nomes_variantes[rodada % len(nomes_variantes)]
    regret_ab_acumulado += melhor_taxa - taxas_verdadeiras[escolhida_ab]
    regret_ab.append(regret_ab_acumulado)

print(f"✓ Simulação completa: {N_RODADAS:,} rodadas")
print(f"\nAlocação final de tráfego (Thompson Sampling, últimas {N_RODADAS} rodadas):")
for nome in nomes_variantes:
    c = contadores_ts[nome]
    pct = c["impressos"] / N_RODADAS
    print(f"  {nome}: {c['impressos']:,} rodadas ({pct:.1%}) — taxa observada {c['cliques']/c['impressos'] if c['impressos'] else 0:.2%} (real: {taxas_verdadeiras[nome]:.2%})")

print(f"\n📉 Regret acumulado — Thompson Sampling: {regret_acumulado:.1f}")
print(f"📉 Regret acumulado — A/B estático (uniforme): {regret_ab_acumulado:.1f}")
print(f"✓ Redução de regret: {(1 - regret_acumulado/regret_ab_acumulado):.1%}")
print("\n💡 Regret = quanto de conversão se perdeu por não estar sempre na melhor")
print("   variante. Thompson Sampling aprende e converge pra 'Variante D' (a real")
print("   melhor), enquanto o A/B estático continua desperdiçando tráfego nas piores")
print("   variantes até o fim do experimento.")

# COMMAND ----------

# DBTITLE 1,Visualização: regret acumulado
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 5))
plt.plot(regret_ts, label="Thompson Sampling")
plt.plot(regret_ab, label="A/B estático (uniforme)")
plt.xlabel("Rodada")
plt.ylabel("Regret acumulado")
plt.title("Regret acumulado: Thompson Sampling vs. A/B estático\n(simulação didática, não são dados do projeto)")
plt.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Conclusões
# MAGIC %md
# MAGIC ## Quando usar cada abordagem
# MAGIC
# MAGIC | | A/B Testing estático (`AB Testing e Causal Inference.py`) | Thompson Sampling (este notebook) |
# MAGIC |---|---|---|
# MAGIC | Objetivo | Medir o efeito causal de uma campanha já definida | Decidir dinamicamente pra onde mandar tráfego |
# MAGIC | Alocação | Fixa do início ao fim (ex: 70/30) | Muda a cada rodada, conforme aprende |
# MAGIC | Quando parar | Teste de significância estatística (p-valor) | Nunca "para" — converge continuamente |
# MAGIC | Caso de uso | "Essa campanha funcionou?" (relatório pós-fato) | "Qual variante devo favorecer agora?" (decisão em produção) |
# MAGIC | Risco | Continua mandando tráfego pra variante ruim até o teste acabar | Praticamente elimina esse desperdício |
# MAGIC
# MAGIC Os dois são complementares, não concorrentes: Thompson Sampling otimiza alocação
# MAGIC em tempo real; A/B testing com inferência causal ainda é necessário pra provar
# MAGIC *por que* uma variante ganhou (ex: causal, não só correlação com sazonalidade).

# COMMAND ----------
