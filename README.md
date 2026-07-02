# Customer Intelligence & Growth Project

## 🎯 Objetivo

Projeto completo de Customer Intelligence em Databricks focado em:
- **Churn Prediction**: Identificar clientes com risco de cancelamento
- **Propensity Modeling**: Prever probabilidade de compra/renovação
- **Recommendation**: Sugerir próxima melhor ação
- **Segmentation**: Agrupar clientes por comportamento (RFM)
- **A/B Testing**: Medir efetividade de campanhas com grupos controle/tratamento
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
customer_intelligence_project/
├── 00_setup/
│   └── Config e Setup Inicial.ipynb          # Configurações, schemas, helpers
├── 01_bronze/
│   └── Ingestao Dados Bronze.ipynb           # Simulação de dados raw
├── 02_silver/
│   └── Transformacao Silver.ipynb            # Limpeza e transformação
├── 03_gold/
│   └── Feature Engineering Gold.ipynb       # RFM, behavioral, campaign features
├── 04_models/
│   └── Modelo Churn Prediction.ipynb        # XGBoost + MLflow
├── 05_scoring/
│   └── Batch Scoring.ipynb                  # Scoring em lote
├── 06_experimentation/
│   └── AB Testing e Causal Inference.ipynb  # Controle vs Tratamento, Lift, ROAS
├── 07_monitoring/
│   └── Monitoramento Performance.ipynb      # Drift detection, KPIs
├── 08_dashboards/
│   └── SQL Queries para Dashboards.ipynb    # Queries prontas para BI
└── README.md                                # Este arquivo
```

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

## 🚀 Como Executar

### 1. Setup Inicial
```bash
# Executar primeiro
00_setup/Config e Setup Inicial
```
Cria schemas `customer_intelligence_bronze`, `silver`, `gold` e configura MLflow.

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
04_models/Modelo Churn Prediction
```
Treina modelo XGBoost:
- AUC-ROC, Precision, Recall, F1
- Feature importance
- Registra no MLflow Model Registry

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

**✅ Projeto Completo e Pronto para Apresentação Técnica em Entrevistas**