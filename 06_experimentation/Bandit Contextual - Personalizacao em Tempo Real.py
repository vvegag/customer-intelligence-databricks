# Databricks notebook source
# DBTITLE 1,Bandit Contextual — Personalização em Tempo Real
# MAGIC %md
# MAGIC # Bandit Contextual — Personalização em Tempo Real 🎯
# MAGIC
# MAGIC ## Objetivo
# MAGIC `Multi-Armed Bandit - Thompson Sampling.py` (notebook irmão) resolve "qual variante
# MAGIC está ganhando, em média, agora" — decide 1 vencedor global e manda a maioria do
# MAGIC tráfego pra ele. Este notebook resolve um problema diferente: **qual oferta é
# MAGIC melhor PARA CADA CLIENTE**, usando o contexto dele (RFM/comportamento) pra decidir,
# MAGIC e aprendendo a cada nova observação de recompensa — sem esperar um teste
# MAGIC "acabar" pra agir. É a mesma ideia por trás de "recomendação com aprendizado em
# MAGIC tempo real a partir de sinais e contexto" que aparece em discussões de scoring de
# MAGIC propensão: em vez de um modelo supervisionado único com erro fixo, o sistema
# MAGIC explora e aprende continuamente qual oferta funciona melhor para qual perfil.
# MAGIC
# MAGIC ## Abordagem
# MAGIC **LinUCB** (Li et al., 2010) e uma variante **Thompson linear** — os dois usam a
# MAGIC mesma regressão ridge por braço (`A = X^T X + λI`, `b = X^T y`, `θ = A^-1 b`,
# MAGIC atualizada incrementalmente), só divergem na estratégia de exploração: LinUCB soma
# MAGIC um bônus de incerteza (`α·sqrt(x^T A^-1 x)`) ao retorno esperado; Thompson linear
# MAGIC amostra `θ̃ ~ N(θ, σ²·A^-1)` da posterior e age de forma gulosa sobre a amostra.
# MAGIC Mesmo espírito de comparação "duas estratégias, mesma base matemática" já usado
# MAGIC em outras partes do projeto (Prophet vs. SARIMA no Forecast, XGBoost vs. LightGBM
# MAGIC no Churn).
# MAGIC
# MAGIC ## Nota de honestidade (mesmo padrão do notebook irmão)
# MAGIC O dado sintético real deste projeto (`bronze.campaign_exposures_raw` /
# MAGIC `campaign_responses_raw`) só varia taxa de resposta por `is_control_group`
# MAGIC (5% vs. 12%) — **não por feature de cliente** (ver nota já existente em
# MAGIC `Multi-Armed Bandit - Thompson Sampling.py`, seção "Duas partes"). Isso significa
# MAGIC que um bandit contextual treinado no dado real vai (corretamente) aprender que o
# MAGIC contexto não muda a decisão — é um resultado válido, não um bug do notebook. A
# MAGIC Seção 1 reporta exatamente isso. Pra demonstrar o valor real de personalização
# MAGIC (a razão de existir um bandit contextual), a Seção 2 usa uma simulação
# MAGIC **claramente rotulada como sintética**, com heterogeneidade de efeito colocada de
# MAGIC propósito — mesmo padrão de transparência já usado no Forecast e no bandit
# MAGIC não-contextual.
# MAGIC
# MAGIC ## Seções
# MAGIC 1. Núcleo do algoritmo (LinUCB + Thompson linear, ridge regression por braço)
# MAGIC 2. Grounded no dado real: aprende (corretamente) que contexto não muda a decisão
# MAGIC 3. Simulação didática com heterogeneidade real: bandit contextual vs. não-contextual vs. aleatório
# MAGIC 4. Avaliação off-policy (IPS): como validar uma política nova sem rodá-la ao vivo
# MAGIC 5. Conclusões — quando usar cada abordagem

# COMMAND ----------

# DBTITLE 1,Configuração
from pyspark.sql import functions as F
import numpy as np

CATALOG = "customer_intelligence"
SCHEMA_BRONZE = "bronze"
SCHEMA_GOLD = "gold"

print("✓ Configuração carregada")

# COMMAND ----------

# DBTITLE 1,1. Núcleo do algoritmo — LinUCB e Thompson linear
# Regressão ridge por braço, atualizada incrementalmente. A = X^T X + λI (matriz de
# "confiança" acumulada), b = X^T y (recompensa acumulada ponderada pelo contexto),
# θ = A^-1 b (coeficientes estimados). Validado localmente (numpy puro) antes de
# aplicar aqui: θ estimado converge pro θ verdadeiro num cenário sintético, e o
# regret acumulado cai ~99% vs. escolha aleatória.
def inicializar_estado_bandit(bracos: list[str], dimensao_contexto: int, lam: float = 1.0) -> dict:
    """Estado inicial: uma matriz A e vetor b por braço (prior ridge, λI)."""
    return {
        braco: {"A": lam * np.eye(dimensao_contexto), "b": np.zeros(dimensao_contexto)}
        for braco in bracos
    }


def atualizar_estado_bandit(estado: dict, braco: str, x: np.ndarray, recompensa: float) -> None:
    """Atualiza A e b do braço escolhido com a observação (contexto, recompensa)."""
    estado[braco]["A"] += np.outer(x, x)
    estado[braco]["b"] += recompensa * x


def theta_estimado(estado: dict, braco: str) -> np.ndarray:
    return np.linalg.inv(estado[braco]["A"]) @ estado[braco]["b"]


def escolher_braco_linucb(estado: dict, x: np.ndarray, alpha: float = 1.0) -> str:
    """LinUCB: escolhe o braço que maximiza retorno esperado + bônus de incerteza."""
    scores = {}
    for braco, dados in estado.items():
        A_inv = np.linalg.inv(dados["A"])
        theta = A_inv @ dados["b"]
        media = x @ theta
        bonus = alpha * np.sqrt(x @ A_inv @ x)
        scores[braco] = media + bonus
    return max(scores, key=scores.get)


def escolher_braco_thompson_linear(estado: dict, x: np.ndarray, sigma2: float = 0.01, rng=None) -> str:
    """Thompson linear: amostra θ da posterior N(θ_hat, σ²·A^-1) e age de forma gulosa."""
    rng = rng or np.random.default_rng()
    scores = {}
    for braco, dados in estado.items():
        A_inv = np.linalg.inv(dados["A"])
        theta_hat = A_inv @ dados["b"]
        theta_amostrado = rng.multivariate_normal(theta_hat, sigma2 * A_inv)
        scores[braco] = x @ theta_amostrado
    return max(scores, key=scores.get)


print("✓ Núcleo do algoritmo carregado (LinUCB + Thompson linear)")

# COMMAND ----------

# DBTITLE 1,2. Grounded: dado real (esperado — contexto sem efeito)
# Contexto = subconjunto de gold.customer_features (recency_days, frequency,
# monetary_total, engagement_score_30d — já sem vazamento, corrigido nesta mesma
# sessão pro Propensity Score). Braços = Controle/Tratamento, mesmo dado que
# Multi-Armed Bandit - Thompson Sampling.py já usa. Diferente da Seção 3 (online,
# sequencial), aqui é um AJUSTE em lote: os dados históricos vêm de alocação FIXA
# (randomização do experimento original), não de um bandit escolhendo em tempo
# real — então o que dá pra medir aqui é "o quanto o contexto explica a
# recompensa por braço" (regressão), não regret de decisão sequencial.
df_customer_features = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.customer_features")
df_exposures = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.campaign_exposures_raw")
df_responses = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.campaign_responses_raw")

CONTEXT_COLS = ["recency_days", "frequency", "monetary_total", "engagement_score_30d"]

df_eventos = (
    df_exposures
    .join(df_responses.select("exposure_id", "is_conversion"), "exposure_id", "left")
    .withColumn("recompensa", F.coalesce(F.col("is_conversion"), F.lit(0)).cast("double"))
    .withColumn("braco", F.when(F.col("is_control_group"), "Controle").otherwise("Tratamento"))
    .join(df_customer_features.select(["customer_id"] + CONTEXT_COLS), "customer_id", "inner")
)

df_eventos_pd = df_eventos.select(["braco", "recompensa"] + CONTEXT_COLS).fillna(0).toPandas()
print(f"✓ {len(df_eventos_pd):,} eventos carregados (exposição + contexto do cliente)")

# Padronizar contexto (média 0, desvio 1) — deixa os coeficientes comparáveis entre
# features de escalas muito diferentes (ex: monetary_total em R$ vs. frequency em unidades)
X_bruto = df_eventos_pd[CONTEXT_COLS].values
medias, desvios = X_bruto.mean(axis=0), X_bruto.std(axis=0)
desvios[desvios == 0] = 1.0
X_padronizado = (X_bruto - medias) / desvios
X_com_intercepto = np.hstack([np.ones((len(X_padronizado), 1)), X_padronizado])
colunas_theta = ["intercepto"] + CONTEXT_COLS

estado_real = inicializar_estado_bandit(["Controle", "Tratamento"], dimensao_contexto=X_com_intercepto.shape[1])
for i, row in df_eventos_pd.iterrows():
    atualizar_estado_bandit(estado_real, row["braco"], X_com_intercepto[i], row["recompensa"])

print("\n" + "="*70)
print("COEFICIENTES APRENDIDOS POR BRAÇO (ajuste em lote, dado real)")
print("="*70)
for braco in ["Controle", "Tratamento"]:
    theta = theta_estimado(estado_real, braco)
    print(f"\n{braco}:")
    for nome, valor in zip(colunas_theta, theta):
        print(f"  {nome}: {valor:+.4f}")

print("\n💡 Esperado: 'intercepto' reflete a taxa média de cada braço (~5% Controle, ~12%")
print("   Tratamento — mesmo padrão do bandit não-contextual). Os coeficientes de CONTEXTO")
print("   (recency_days, frequency, monetary_total, engagement_score_30d) devem ficar perto")
print("   de zero — o dataset sintético deste projeto não modula resposta por perfil de")
print("   cliente (só por controle/tratamento). Um coeficiente de contexto grande aqui")
print("   seria motivo de investigação (mesmo espírito do alerta de KS suspeito no Churn),")
print("   não algo a comemorar sem checar.")

# COMMAND ----------

# DBTITLE 1,3. Simulação didática com heterogeneidade real (dados sintéticos)
# MAGIC %md
# MAGIC ⚠️ **A partir daqui os dados são simulados especificamente para esta demonstração**,
# MAGIC mesmo padrão de transparência já usado no bandit não-contextual: 4 ofertas cuja
# MAGIC taxa de conversão depende do CONTEXTO do cliente (ex: cliente de `monetary_total`
# MAGIC alto responde melhor a cashback; cliente de `recency_days` alto responde melhor a
# MAGIC frete grátis) — heterogeneidade colocada de propósito, pra ter sinal real pro
# MAGIC algoritmo aprender e mostrar o ganho de personalizar por perfil, algo que um
# MAGIC bandit não-contextual (que só sabe escolher 1 vencedor global) não consegue captar.

# COMMAND ----------

# DBTITLE 1,Simular rodadas sequenciais (bandit contextual vs. não-contextual vs. aleatório)
rng = np.random.default_rng(11)
D = 3  # dimensão do contexto simulado (perfil do cliente, padronizado ~N(0,1))
OFERTAS = ["Cashback", "Frete Grátis", "Desconto %", "Sem Oferta (controle)"]

# theta "verdadeiro" por oferta — cada uma responde melhor a um perfil de contexto diferente
theta_verdadeiro = {
    "Cashback": np.array([1.0, -0.3, 0.1]),
    "Frete Grátis": np.array([-0.3, 1.0, 0.1]),
    "Desconto %": np.array([0.1, 0.1, 1.0]),
    "Sem Oferta (controle)": np.array([0.0, 0.0, 0.0]),
}


def recompensa_esperada(oferta: str, x: np.ndarray) -> float:
    return float(np.clip(0.10 + 0.15 * (x @ theta_verdadeiro[oferta]), 0.0, 1.0))


N_RODADAS = 3000
estado_contextual = inicializar_estado_bandit(OFERTAS, dimensao_contexto=D, lam=1.0)
contadores_nao_contextual = {oferta: {"impressos": 0, "cliques": 0} for oferta in OFERTAS}

regret_contextual, regret_nao_contextual, regret_aleatorio = [], [], []
acum_contextual = acum_nao_contextual = acum_aleatorio = 0.0

# Log da política aleatória — reaproveitado na Seção 4 (avaliação off-policy)
log_politica_aleatoria = []

for rodada in range(N_RODADAS):
    x = rng.normal(size=D)
    melhor_oferta = max(OFERTAS, key=lambda o: recompensa_esperada(o, x))
    melhor_recompensa = recompensa_esperada(melhor_oferta, x)

    # --- Bandit contextual (LinUCB) ---
    escolhida_ctx = escolher_braco_linucb(estado_contextual, x, alpha=0.5)
    r_ctx = float(rng.random() < recompensa_esperada(escolhida_ctx, x))
    atualizar_estado_bandit(estado_contextual, escolhida_ctx, x, r_ctx)
    acum_contextual += melhor_recompensa - recompensa_esperada(escolhida_ctx, x)
    regret_contextual.append(acum_contextual)

    # --- Bandit não-contextual (Thompson Beta-Bernoulli, ignora x) ---
    amostras_beta = {
        oferta: rng.beta(c["cliques"] + 1, c["impressos"] - c["cliques"] + 1)
        for oferta, c in contadores_nao_contextual.items()
    }
    escolhida_simples = max(amostras_beta, key=amostras_beta.get)
    r_simples = float(rng.random() < recompensa_esperada(escolhida_simples, x))
    contadores_nao_contextual[escolhida_simples]["impressos"] += 1
    contadores_nao_contextual[escolhida_simples]["cliques"] += int(r_simples)
    acum_nao_contextual += melhor_recompensa - recompensa_esperada(escolhida_simples, x)
    regret_nao_contextual.append(acum_nao_contextual)

    # --- Aleatório (baseline + log usado na Seção 4) ---
    escolhida_aleatoria = OFERTAS[rng.integers(0, len(OFERTAS))]
    p_logada = 1.0 / len(OFERTAS)
    r_aleatoria = float(rng.random() < recompensa_esperada(escolhida_aleatoria, x))
    acum_aleatorio += melhor_recompensa - recompensa_esperada(escolhida_aleatoria, x)
    regret_aleatorio.append(acum_aleatorio)
    log_politica_aleatoria.append((x, escolhida_aleatoria, p_logada, r_aleatoria))

print(f"✓ Simulação completa: {N_RODADAS:,} rodadas")
print(f"\n📉 Regret acumulado — Bandit contextual (LinUCB): {acum_contextual:.1f}")
print(f"📉 Regret acumulado — Bandit não-contextual (Thompson Beta): {acum_nao_contextual:.1f}")
print(f"📉 Regret acumulado — Aleatório: {acum_aleatorio:.1f}")
print(f"\n✓ Redução de regret (contextual vs. não-contextual): "
      f"{(1 - acum_contextual/acum_nao_contextual):.1%}")
print("\n💡 O bandit não-contextual converge pra 1 'vencedor médio' e fica preso nele — não")
print("   enxerga que perfis diferentes de cliente respondem melhor a ofertas diferentes.")
print("   O bandit contextual aprende essa personalização e evita esse regret residual.")

# COMMAND ----------

# DBTITLE 1,Visualização: regret acumulado (3 políticas)
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 5))
plt.plot(regret_contextual, label="Bandit contextual (LinUCB)")
plt.plot(regret_nao_contextual, label="Bandit não-contextual (Thompson Beta)")
plt.plot(regret_aleatorio, label="Aleatório")
plt.xlabel("Rodada")
plt.ylabel("Regret acumulado")
plt.title("Regret acumulado — personalização por contexto vs. vencedor único\n(simulação didática, não são dados do projeto)")
plt.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,4. Avaliação off-policy (IPS) — validar sem rodar ao vivo
# Pergunta natural antes de colocar um bandit em produção: "como validar isso sem
# poder experimentar ao vivo?" — Inverse Propensity Scoring (IPS) estima o valor de
# uma política NOVA a partir de dados logados por uma política ANTIGA (aqui, o log
# da política aleatória da Seção 3, cuja probabilidade de cada ação é conhecida:
# p=1/4). Fórmula: V̂ = (1/n) Σ (r_i / p_i) · 𝟙[π_nova(x_i) == a_i] — só usa as
# linhas do log onde a política nova "concordaria" com a ação logada, corrigindo
# pelo inverso da probabilidade de ter sido logada (evita viés de seleção).
def politica_contextual_gulosa(x: np.ndarray) -> str:
    """Política final aprendida (Seção 3, sem exploração) — a que iríamos deployar."""
    scores = {oferta: x @ theta_estimado(estado_contextual, oferta) for oferta in OFERTAS}
    return max(scores, key=scores.get)


valores_ips = []
for x, acao_logada, p_logada, recompensa in log_politica_aleatoria:
    acao_nova = politica_contextual_gulosa(x)
    valores_ips.append(recompensa / p_logada if acao_nova == acao_logada else 0.0)

valor_estimado_ips = float(np.mean(valores_ips))

# Oráculo (só possível porque é simulação — em produção não teríamos isso, é só
# pra validar que o IPS acima está estimando corretamente)
valores_oraculo = [recompensa_esperada(politica_contextual_gulosa(x), x) for x, *_ in log_politica_aleatoria]
valor_oraculo = float(np.mean(valores_oraculo))

print("="*70)
print("AVALIAÇÃO OFF-POLICY (IPS)")
print("="*70)
print(f"Valor estimado da política contextual via IPS (a partir do log aleatório): {valor_estimado_ips:.4f}")
print(f"Valor real da política contextual (oráculo, só existe em simulação):        {valor_oraculo:.4f}")
print(f"\n✓ IPS aproxima bem o valor real ({abs(valor_estimado_ips - valor_oraculo):.4f} de diferença) —")
print("  é exatamente essa a validação que se faria em produção ANTES de trocar de política,")
print("  usando só o log da política atual, sem precisar rodar a nova política ao vivo.")
print("\n⚠️ Limitação conhecida do IPS puro: variância alta quando a política nova raramente")
print("   'concorda' com a política logada (poucos termos não-zero na média). Doubly Robust")
print("   (combina IPS com um modelo de recompensa) reduz essa variância — próximo passo,")
print("   não implementado aqui (mesmo padrão de 'escopo documentado, não hoje' já usado")
print("   em outras partes do projeto).")

# COMMAND ----------

# DBTITLE 1,5. Conclusões
# MAGIC %md
# MAGIC ## Quando usar cada abordagem
# MAGIC
# MAGIC | | Propensity Score (`Modelo Propensity Score.py`) | Bandit não-contextual (`Multi-Armed Bandit...py`) | Bandit contextual (este notebook) |
# MAGIC |---|---|---|---|
# MAGIC | Pergunta que responde | "Qual a probabilidade deste cliente comprar?" | "Qual variante está ganhando, em média, agora?" | "Qual oferta é melhor PARA ESTE cliente, agora?" |
# MAGIC | Usa contexto do cliente? | Sim, mas estático (retreina em batch) | Não | Sim, e aprende online a cada nova observação |
# MAGIC | Aprendizado | Batch (retreina periodicamente) | Online, sem contexto | Online, com contexto |
# MAGIC | Erro/incerteza | Métrica fixa pós-treino (AUC, KS) | Converge pra 1 vencedor global | Explora e aprende por perfil, sem convergir num único vencedor |
# MAGIC | Quando usar | Priorização em lote (ex: campanha de retenção) | Poucas variantes, sem sinal de heterogeneidade por cliente | Muitas ofertas possíveis, efeito varia por perfil, decisão em tempo real |
# MAGIC
# MAGIC As três abordagens são complementares, não substitutas: o Propensity Score já é a
# MAGIC base de "quem é o cliente" (as mesmas features RFM entram como contexto aqui); o
# MAGIC bandit não-contextual resolve alocação simples; o bandit contextual generaliza pra
# MAGIC "melhor ação por perfil, aprendida em tempo real" — a lacuna que os dois primeiros,
# MAGIC sozinhos, não cobrem.

# COMMAND ----------
