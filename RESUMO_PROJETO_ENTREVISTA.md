# Customer Intelligence Platform — Resumo Técnico para Entrevista

> Documento preparado para uso pessoal em preparação de entrevista (colar em ChatGPT
> junto com a vaga e o LinkedIn do entrevistador para gerar talking points).

## 1. O que é o projeto, em uma frase

Uma plataforma end-to-end de Customer Intelligence em Databricks — arquitetura
medallion (Bronze/Silver/Gold), modelos de churn/propensão/segmentação/recomendação,
experimentação com A/B testing e inferência causal, e MLOps completo (MLflow,
CI/CD via Databricks Asset Bundles, monitoramento de drift).

**Repositório:** `customer-intelligence-databricks` (GitHub, público)
**Plataforma:** Databricks (Unity Catalog, Delta Lake, Serverless Compute)

> ⚠️ **Use este documento junto com o do segundo projeto**: existe um projeto irmão,
> `ai-credit-risk-platform-databricks` (pasta local em
> `C:\Users\valdo\OneDrive\dev\ai-credit-risk-platform-databricks`, também no GitHub),
> que cobre **modelos de crédito (scoring/behavior)** e **GenAI/RAG** — exatamente o
> que ESTE projeto não cobre. O resumo dele já tem uma seção de cobertura combinada:
> `09_docs/RESUMO_PROJETO_ENTREVISTA.md` desse outro repositório. Para uma vaga que
> pede propensão/retenção **e** crédito/GenAI ao mesmo tempo, cole os dois documentos
> no ChatGPT junto com a vaga — não só este.

---

## 2. Contexto de negócio (história real, não caso de estudo genérico)

Reconstrução, com dados sintéticos, do tipo de trabalho que meu time fazia na
CRMBonus (empresa de cashback/fidelidade B2C): o time era dividido por
especialidade — um colega focado em **propensão**, outro em **churn**, eu
concentrado em **segmentação de clientes e testes A/B/inferência causal**.
Colaborávamos de perto entre as frentes e conhecíamos bem as bases reais
(volume de linhas, características dos usuários, comportamento de uso do
programa). Não há dado real de empresa nenhuma aqui — a reconstrução é
100% sintética, mas as decisões de arquitetura e a divisão de responsabilidade
refletem a experiência real.

**Minha frente própria era segmentação e A/B testing** — é onde eu tenho mais
profundidade de decisão técnica pra defender em detalhe. Os módulos de churn e
propensão eu entendo bem (colaboração próxima com quem construía), e reconstruí
aqui pra fechar o ciclo completo de ponta a ponta — é honesto dizer isso se
perguntarem "você mesmo que fez o modelo de churn?".

## 3. Problema de negócio que resolve

Uma base de clientes precisa de:
- Prever **quem vai dar churn** (cancelar/ficar inativo)
- Prever **quem tem maior propensão de compra** nos próximos 30 dias
- **Segmentar clientes** por comportamento (RFM + engajamento)
- **Recomendar a próxima melhor ação** por cliente
- Medir **campanhas de marketing com rigor estatístico** (A/B testing, não só correlação)
- **Monitorar** se os modelos e os dados continuam confiáveis ao longo do tempo

---

## 4. Arquitetura

```
Bronze (raw) → Silver (limpo/validado) → Gold (features + scores + modelos)
```

- **Catálogo Unity Catalog:** `customer_intelligence`, com schemas `bronze`, `silver`, `gold`
- **Formato de tabela:** Delta Lake (ACID, time travel)
- **~29 tabelas**: 7 bronze, 7 silver, 15 gold (features, labels, scores, resultados de
  experimentos, monitoramento)
- **Dados sintéticos gerados com seed fixo** (10k clientes, 500 produtos, 50k
  transações, 20 campanhas, 100k eventos comportamentais) — 100% reproduzível, sem
  depender de fonte externa

---

## 5. Stack técnico

| Camada | Tecnologia |
|---|---|
| Processamento | **PySpark** (DataFrame API + Spark SQL) — pandas só usado no limite exato onde é necessário: fronteira com scikit-learn/XGBoost/SHAP |
| Storage | Delta Lake, Unity Catalog (catálogo/schema/volumes) |
| ML | XGBoost, scikit-learn, K-Means, SHAP (explicabilidade) |
| MLOps | MLflow (tracking, model registry), Databricks Model Serving (endpoint REST) |
| Orquestração/IaC | **Databricks Asset Bundles** (`databricks.yml`) — jobs, tasks, dependências, ambientes dev/prod |
| Experimentação | scipy (t-test, chi-square), causal inference (lift, uplift, ROAS) |
| Versionamento | Git + GitHub (branch `dev` → `master`), sincronizado com Databricks Repos |

---

## 6. Estrutura do pipeline (o que cada etapa faz)

| Pasta | Conteúdo |
|---|---|
| `00_setup` | Cria catálogo, schemas, helpers, configura MLflow (usuário detectado dinamicamente) |
| `01_bronze` | Ingestão/simulação de dados raw |
| `02_silver` | Limpeza, deduplicação, validação, enriquecimento |
| `03_gold` | Feature engineering: RFM, features comportamentais (30/60/90d), histórico de campanha, feature store |
| `04_models` | **8 notebooks**: Churn (XGBoost), Propensity Score, Segmentação (K-Means, 5 clusters), Sistema de Recomendação, comparação com AutoML, SHAP (explicabilidade), Model Serving (endpoint REST), retreino automatizado |
| `05_scoring` | Batch scoring de toda a base de clientes |
| `06_experimentation` | A/B testing: controle vs. tratamento, lift, significância estatística, ROAS |
| `07_monitoring` | Data drift, model drift, KPIs de negócio ao longo do tempo |
| `08_dashboards` | Queries SQL prontas para BI |
| `09_integrations` | Integração com CRM |
| `production/` | 5 notebooks "avançados" separados do fluxo principal: Delta Live Tables (Auto Loader, CDC, SCD2), MLflow avançado (nested runs, aliases Champion/Challenger), treino distribuído com SparkML, CI/CD completo (Databricks Asset Bundles + testes + GitHub Actions), feature store online/offline |

---

## 7. Resultados documentados (da execução real do pipeline)

- **Modelo de Churn (XGBoost):** AUC-ROC ≈ 0.94, Precision ≈ 0.99, Recall ≈ 0.88
- **A/B Testing:** 20 campanhas analisadas, 65% estatisticamente significantes
  (p < 0.05); melhor campanha com lift de +535% e ROAS de 3.12x
- **Segmentação:** K-Means com 5 clusters comportamentais (RFM + engajamento),
  validado por Silhouette Score
- **Monitoramento:** sistema de detecção de drift entre churn previsto vs. real,
  com threshold de alerta configurável

---

## 8. Skills que dá pra falar na entrevista (com histórias reais, não só teoria)

### Engenharia de dados / PySpark
- Pipeline medallion completo, com foco explícito em substituir loops
  Python+pandas por operações vetorizadas PySpark (`F.rand()`, `F.when()`,
  window functions) para escalabilidade
- Delta Lake / Unity Catalog: catálogo, schemas, volumes, controle de acesso

### Machine Learning
- Classificação (churn, propensão), clustering (segmentação), sistema de
  recomendação, explicabilidade (SHAP: waterfall, dependence, force plot)
- MLflow: logging de experimentos, model registry, signature de modelo,
  comparação com AutoML da própria plataforma

### Experimentação / Causal Inference
- Desenho de A/B test (grupo controle vs. tratamento), testes de hipótese
  (t-test, chi-square), cálculo de lift/uplift incremental, ROAS — sabe
  diferenciar correlação de causalidade

### MLOps / Produção
- CI/CD real via Databricks Asset Bundles (`databricks.yml`): jobs
  parametrizados, múltiplos ambientes (dev/prod), dependências entre tasks
- Model Serving: deploy de modelo como endpoint REST
- Monitoramento de drift de dados e de modelo em produção

### Engenharia de software / disciplina de projeto (história real e forte)
- **Recuperação de um repositório com histórico Git divergente em 3 pontos**
  (GitHub direto, branch `dev`, Databricks Repos desatualizado) — reconciliado
  sem perda de trabalho, resolvendo conflitos modify/delete manualmente
- **Debugging de bugs sutis de ambiente**: uma célula de instalação de pacote
  (`%pip install` + `restartPython()`) posicionada depois da célula de config
  apagava variáveis da memória e quebrava o notebook num "Run All" limpo —
  identificado por leitura de código, não por tentativa e erro
- **Portabilidade entre contas/workspaces**: e-mails e paths hardcoded
  trocados por descoberta dinâmica do usuário logado (`current_user()`),
  e o `databricks.yml` ajustado para não travar num único workspace
  (`workspace.host` omitido de propósito, resolvido pelo profile do CLI
  no momento do deploy) — pensando em portfólio avaliado por terceiros
- Auditoria sistemática de documentação vs. código real, para garantir que
  o que está escrito bate com o que o código realmente faz

---

## 9. Perguntas que provavelmente vêm e pontos de resposta

- **"Por que PySpark e não só pandas?"** → Escalabilidade: pandas roda em um
  único nó, PySpark distribui. Pandas só é usado no ponto exato onde uma lib
  sem equivalente distribuído (XGBoost, SHAP) exige um DataFrame local, e
  sempre sobre uma tabela já pequena/agregada.
- **"Como você decide se uma campanha funcionou?"** → Significância
  estatística (p-value < 0.05) antes de olhar pra métricas de negócio (lift,
  ROAS) — lift alto sem significância pode ser ruído.
- **"Como isso rodaria em produção de verdade?"** → `databricks.yml` já
  define os jobs, dependências e agendamento (cron); falta só plugar
  alertas automáticos (Slack/e-mail) no monitoramento de drift.
- **"Maior desafio técnico?"** → A reconciliação de Git/Databricks Repos
  divergentes e o bug de ordem de célula do `%pip install` — ambos exigiram
  ler o código/histórico com cuidado em vez de só reagir ao erro na tela.
- **"Você mesmo construiu o modelo de churn/propensão?"** → Resposta honesta:
  na CRMBonus, essas duas frentes eram de colegas — a minha era segmentação e
  A/B testing. Aqui reconstruí o pipeline inteiro, incluindo churn/propensão,
  pra ter domínio de ponta a ponta — mas se for pra detalhar decisão técnica
  fina, é em segmentação e experimentação que eu falo com mais profundidade
  de primeira mão.

---

## 10. Links úteis pra ter em mãos

- Repositório: `github.com/vvegag/customer-intelligence-databricks`
- README.md e `docs/03_NOTEBOOKS_GUIDE.md` — visão geral e guia de execução atualizados
