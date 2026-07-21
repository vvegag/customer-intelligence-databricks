# Customer Intelligence & Growth Project

## 🎯 Contexto e Objetivo

Este projeto reconstrói, com dados sintéticos, o tipo de trabalho de Customer
Intelligence que meu time fazia na CRMBonus: um time dividido por especialidade
(um colega focado em propensão, outro em churn, eu concentrado em **segmentação
e testes A/B/inferência causal**), colaborando de perto e com conhecimento real
das bases — volume de linhas, características dos usuários, comportamento de
uso do programa de cashback/fidelidade. Não há acesso a dados reais de nenhuma
empresa aqui; a arquitetura e as decisões técnicas é que refletem esse contexto
real, reconstruídas do zero para fechar o ciclo completo (algo que o dia a dia
corrido raramente permite documentar).

Cobre:
- **Churn Prediction**: Identificar clientes com risco de cancelamento
- **Propensity Modeling**: Prever probabilidade de compra/renovação
- **Recommendation**: Sugerir próxima melhor ação
- **Segmentation**: Agrupar clientes por comportamento (RFM) — minha frente principal na CRMBonus
- **A/B Testing**: Medir efetividade de campanhas com grupos controle/tratamento — idem
- **Causal Inference**: Entender impacto causal de ações de marketing

---

## 📊 Arquitetura Medallion

```
Bronze (Raw Data)     →     Silver (Clean)     →     Gold (Features & Scores)
      ↓                           ↓                          ↓
   Ingestão              Transformação           Feature Engineering
   Simulação              Limpeza                Agregações
                            Validação              ML Ready
                                                         ↓
                                                    Models & Scoring
                                                         ↓
                                                    Predictions
```

---

## 📁 Estrutura do Projeto

```
customer-intelligence-databricks/
├── 00_setup/
│   └── Config e Setup Inicial.ipynb          # Cria catálogo, schemas, helpers, MLflow
├── 01_bronze/
│   └── Ingestao Dados Bronze.py               # Simulação de dados raw
├── 02_silver/
│   └── Transformacao Silver.py                # Limpeza e transformação
├── 03_gold/
│   └── Feature Engineering Gold.py            # RFM, behavioral, campaign features
├── 04_models/
│   ├── Modelo Churn Prediction.py             # XGBoost + MLflow (fluxo principal)
│   ├── Modelo Propensity Score.py             # Probabilidade de compra
│   ├── Segmentacao Clientes Clustering.py     # K-Means (5 segmentos)
│   ├── Sistema Recomendacao.py                # Next best action
│   ├── AutoML Databricks Churn.py             # Comparação com Databricks AutoML
│   ├── Automated Model Retraining.py          # Pipeline de retreino automático
│   ├── Model_Explainability_SHAP.py           # Explicabilidade (waterfall/dependence/force plot)
│   └── Model_Serving_Deployment.py            # Deploy do modelo em endpoint REST
├── 05_scoring/
│   └── Batch Scoring.py                       # Scoring em lote
├── 06_experimentation/
│   └── AB Testing e Causal Inference.py       # Controle vs Tratamento, Lift, ROAS
├── 07_monitoring/
│   └── Monitoramento Performance.py           # Drift detection, KPIs
├── 08_dashboards/
│   └── SQL Queries para Dashboards.ipynb      # Queries prontas para BI
├── 09_integrations/
│   └── CRM Integration.py                     # Integração com CRM (Salesforce/HubSpot)
├── production/                                # Notebooks "avançados", fora do fluxo principal
│   ├── cicd/DABs_CI_CD_Complete.py            # Infra as Code + CI/CD (Databricks Asset Bundles)
│   ├── feature_store/feature_store_complete.py
│   ├── models/mlflow_advanced.py              # MLflow: nested runs, aliases Champion/Challenger
│   ├── models/sparkml_distributed.py          # Treino distribuído com SparkML
│   └── pipelines/Delta_Live_Tables_Advanced.py # Delta Live Tables (Auto Loader, CDC, SCD2)
├── docs/                                      # Documentação detalhada (ver seção abaixo)
├── databricks.yml                             # Databricks Asset Bundle
├── requirements.txt
└── README.md                                  # Este arquivo
```

> `00_setup` e `08_dashboards` estão em formato Jupyter (`.ipynb`); todo o resto é
> formato "Databricks source" (`.py` com `# Databricks notebook source`) — ambos
> abrem normalmente como notebook dentro do Databricks, a diferença só aparece no
> diff do Git.

---

## 🛠️ Tecnologias Utilizadas

- **Databricks**: Plataforma de dados unificada
- **Delta Lake**: Tabelas ACID, versionamento
- **PySpark**: Processamento distribuído
- **MLflow**: Rastreamento de experimentos, registro de modelos
- **XGBoost**: Algoritmo de ML para churn
- **Pandas/NumPy**: Manipulação de dados
- **SciPy**: Testes estatísticos (t-test, chi-square)

---

## ✅ Testes e CI

Testes locais (não precisam de cluster Databricks — rodam em segundos):

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

- **`test_notebook_syntax.py`**: garante que todo notebook `.py` é Python
  sintaticamente válido (pega erro de `%pip install` sem o prefixo `# MAGIC`,
  por exemplo — já pegou 2 bugs reais nesse projeto).
- **`test_config_consistency.py`**: garante que as funções auxiliares
  duplicadas entre notebooks (`get_full_table_name`, `create_or_replace_table`)
  não divergiram silenciosamente.
- **`test_databricks_yml.py`**: valida a estrutura do Asset Bundle sem precisar
  de credenciais (indentação correta, `notebook_path` apontando pra arquivo que
  existe, variáveis declaradas) — já pegou 2 bugs reais de configuração.

GitHub Actions roda esses testes a cada push em `dev`/`master`
(`.github/workflows/ci.yml`). Se os secrets `DATABRICKS_HOST`/`DATABRICKS_TOKEN`
estiverem configurados no repositório, também valida o bundle contra um
workspace real (`databricks bundle validate`).

---

## 🚀 Como Executar

### 1. Setup Inicial
```bash
# Executar primeiro
00_setup/Config e Setup Inicial
```
Cria o catálogo `customer_intelligence` e os schemas `bronze`, `silver`, `gold`,
e configura o experimento MLflow (detecta o usuário logado automaticamente).

### 2. Ingestão (Bronze)
```bash
01_bronze/Ingestao Dados Bronze
```
Simula dados de:
- 10.000 clientes
- 500 produtos
- 50.000 transações
- 20 campanhas
- 30.000 exposições a campanhas
- 100.000 eventos comportamentais

### 3. Transformação (Silver)
```bash
02_silver/Transformacao Silver
```
Limpa dados, remove duplicatas, padroniza formatos, cria flags.

### 4. Feature Engineering (Gold)
```bash
03_gold/Feature Engineering Gold
```
Cria features:
- **RFM**: Recency, Frequency, Monetary
- **Behavioral**: Engajamento 30d/60d/90d
- **Campaign History**: Histórico de respostas
- **Churn Labels**: Target para modelo

### 5. Modelagem
```bash
04_models/Modelo Churn Prediction      # obrigatório — os demais dependem deste
```
Treina modelo XGBoost:
- AUC-ROC, Precision, Recall, F1
- Feature importance
- Registra no MLflow / salva em Volume do Unity Catalog

Opcionais, no mesmo diretório (cada um roda de forma independente):
- `Modelo Propensity Score` — probabilidade de compra nos próximos 30 dias
- `Segmentacao Clientes Clustering` — K-Means, 5 segmentos comportamentais
- `Sistema Recomendacao` — próxima melhor ação por cliente
- `AutoML Databricks Churn` — comparação com Databricks AutoML
- `Model_Explainability_SHAP` — waterfall/dependence/force plot para o modelo de churn
- `Model_Serving_Deployment` — publica o modelo de churn como endpoint REST
- `Automated Model Retraining` — pipeline de retreino agendado

### 6. Scoring
```bash
05_scoring/Batch Scoring
```
Gera scores para todos os clientes:
- Churn probability
- Risk category (Low/Medium/High)
- Customer value score
- Engagement score

### 7. Experimentação
```bash
06_experimentation/AB Testing e Causal Inference
```
Analisa campanhas:
- Controle vs Tratamento
- Lift e Uplift
- Significância estatística
- ROAS (Return on Ad Spend)

### 8. Monitoramento
```bash
07_monitoring/Monitoramento Performance
```
Monitora:
- Data drift (mudanças em features)
- Model performance (churn real vs previsto)
- Campaign trends
- Business KPIs

---

## 📊 Tabelas Criadas

### Bronze Layer
- `customers_raw`
- `products_raw`
- `transactions_raw`
- `campaigns_raw`
- `campaign_exposures_raw`
- `campaign_responses_raw`
- `behavioral_events_raw`

### Silver Layer
- `customers`
- `products`
- `transactions`
- `campaigns`
- `campaign_exposures`
- `campaign_responses`
- `behavioral_events`

### Gold Layer
- `rfm_features`
- `behavioral_features`
- `campaign_history_features`
- `customer_features` (feature store master)
- `churn_labels`
- `churn_predictions`
- `customer_scores`
- `campaign_ab_test_results`
- `campaign_roas`
- `feature_drift_monitoring`
- `campaign_performance_trend`
- `business_kpis_history`

---

## 🧪 Casos de Uso por Persona

### 💼 Marketing Team
- **Questão**: Quais clientes estão em risco de churn?
- **Solução**: Query `customer_scores` com `churn_risk_category = 'High'`
- **Ação**: Campanha de retenção direcionada

### 📊 Growth Team
- **Questão**: Qual campanha teve melhor ROI?
- **Solução**: Dashboard `campaign_roas` ordenado por ROAS
- **Ação**: Replicar estratégias de campanhas bem-sucedidas

### 🔬 Data Scientist
- **Questão**: Modelo precisa ser retreinado?
- **Solução**: Notebook `Monitoramento Performance` - verificar drift
- **Ação**: Retreinar se drift > threshold

### 💼 Executivo
- **Questão**: Qual a saúde geral do negócio?
- **Solução**: Dashboard `business_kpis_history` - KPIs consolidados
- **Ação**: Decisões estratégicas baseadas em dados

---

## 📈 Métricas Chave

### Modelo de Churn
- **AUC-ROC**: ~0.85+ (excelente)
- **Precision**: Poucos falsos positivos
- **Recall**: Captura maioria dos churns
- **F1-Score**: Balanço entre precision e recall

### Experimentação (A/B Testing)
- **Lift**: % de melhoria vs controle
- **p-value**: < 0.05 = estatisticamente significante
- **ROAS**: Retorno por dólar investido
- **Incremental Revenue**: Receita adicional da campanha

### Business KPIs
- **Churn Rate**: % de clientes churned
- **Average Revenue Per Customer (ARPC)**
- **Customer Lifetime Value (CLV)**
- **Engagement Score**: Atividade recente

---

## 🔄 Workflow de Produção Recomendado

### Diário
- Ingestão de novos dados (01_bronze)
- Transformação (02_silver)

### Semanal
- Feature engineering (03_gold)
- Batch scoring (05_scoring)
- Monitoramento (07_monitoring)

### Mensal
- Análise de experimentação (06_experimentation)
- Revisão de performance de campanhas
- Retreinar modelos se necessário (04_models)

### Trimestral
- Revisão completa de arquitetura
- Otimização de features
- Atualização de dashboards

---

## 📚 Próximos Passos (Evolução)

### Curto Prazo
1. ✅ Adicionar modelo de propensity (probabilidade de compra)
2. ✅ Implementar segmentação com clustering (K-Means)
3. ✅ Criar sistema de recomendação básico (collaborative filtering)

### Médio Prazo
4. ☐ Criar jobs automatizados (Databricks Workflows)
5. ☐ Implementar real-time scoring (Model Serving)
6. ☐ Adicionar alertas automáticos (drift, performance)

### Longo Prazo
7. ☐ Integrar com ferramentas de Marketing (Salesforce, HubSpot)
8. ☐ Implementar MLOps completo (CI/CD para modelos)
9. ☐ Adicionar mais modelos (LTV prediction, Next Best Action)

---

## 👥 Contato

Projeto desenvolvido para demonstração de expertise em:
- Data Engineering (Databricks, Delta Lake)
- Machine Learning (MLflow, XGBoost)
- Experimentação (A/B Testing, Causal Inference)
- MLOps (Batch Scoring, Monitoring)

---

## 📝 Licença

Este é um projeto de demonstração acadêmica/técnica.



---

## 📚 Documentação

Toda a documentação do projeto está organizada na pasta `/docs/`:

| Documento | Descrição |
|-----------|-----------|
| [01_PROJETO_OVERVIEW.md](docs/01_PROJETO_OVERVIEW.md) | Overview do projeto, checklist e roadmap |
| [02_PLANO_SEMANA.md](docs/02_PLANO_SEMANA.md) | Plano da semana e gaps para entrevistas |
| [03_NOTEBOOKS_GUIDE.md](docs/03_NOTEBOOKS_GUIDE.md) | Guia passo a passo de execução, do setup aos notebooks de `production/` |
| [04_APRESENTACAO.md](docs/04_APRESENTACAO.md) | Apresentação executiva e scripts de entrevista |
| [05_MIGRATION.md](docs/05_MIGRATION.md) | Guia de migração e correções |

### 🎯 Acesso Rápido:

- **Para entrevistas:** [04_APRESENTACAO.md](docs/04_APRESENTACAO.md)
- **Para executar notebooks:** [03_NOTEBOOKS_GUIDE.md](docs/03_NOTEBOOKS_GUIDE.md)
- **Para entender o projeto:** [01_PROJETO_OVERVIEW.md](docs/01_PROJETO_OVERVIEW.md)

---
---

**✅ Projeto Completo e Pronto para Apresentação Técnica em Entrevistas**