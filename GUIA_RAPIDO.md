# ⚡ Guia Rápido de Execução
## Customer Intelligence Project

---

## 🚀 Quick Start (10 minutos)

### Passo 1: Setup Inicial
```bash
Abra: 00_setup/Config e Setup Inicial
Clique: Run All
Tempo: ~2 minutos
```
✅ **O que faz**: Cria schemas, configura MLflow, valida ambiente

### Passo 2: Gerar Dados
```bash
Abra: 01_bronze/Ingestao Dados Bronze
Clique: Run All
Tempo: ~3 minutos
```
✅ **O que faz**: Simula 10k clientes, 50k transações, 100k eventos

### Passo 3: Limpar Dados
```bash
Abra: 02_silver/Transformacao Silver
Clique: Run All
Tempo: ~2 minutos
```
✅ **O que faz**: Limpa, valida e transforma dados

### Passo 4: Features
```bash
Abra: 03_gold/Feature Engineering Gold
Clique: Run All
Tempo: ~3 minutos
```
✅ **O que faz**: Cria RFM, behavioral features, targets

---

## 🤖 Modelos de ML (Escolha o Que Quiser)

### Opção A: Churn Prediction
```bash
Abra: 04_models/Modelo Churn Prediction
Clique: Run All
Tempo: ~4 minutos
Output: Modelo com AUC 0.85+, registrado no MLflow
```

### Opção B: Propensity Score
```bash
Abra: 04_models/Modelo Propensity Score
Clique: Run All
Tempo: ~3 minutos
Output: Scores de propensão para toda base
```

### Opção C: Segmentação
```bash
Abra: 04_models/Segmentacao Clientes Clustering
Clique: Run All
Tempo: ~3 minutos
Output: 5 segmentos comportamentais
```

---

## 📊 Experimentação e Análise

### A/B Testing
```bash
Abra: 06_experimentation/AB Testing e Causal Inference
Clique: Run All
Tempo: ~4 minutos
Output: Lift, ROAS, significância por campanha
```

### Scoring em Lote
```bash
Abra: 05_scoring/Batch Scoring
Clique: Run All
Tempo: ~3 minutos
Pré-requisito: Executar Modelo Churn antes
Output: Scores para todos os clientes
```

### Monitoramento
```bash
Abra: 07_monitoring/Monitoramento Performance
Clique: Run All
Tempo: ~2 minutos
Output: Drift detection, KPIs, alertas
```

---

## 📈 Dashboards

### Queries Prontas
```bash
Abra: 08_dashboards/SQL Queries para Dashboards
Copie as queries SQL
Cole no Databricks SQL ou Lakeview
Crie visualizações
```

### Dashboards Recomendados:
1. **Executive KPIs**: Métricas consolidadas
2. **Churn Risk**: Top clientes em risco
3. **Campaign Performance**: ROAS e lift
4. **Segmentation**: Perfil dos clusters

---

## 🔄 Fluxo Completo End-to-End

Para executar TODO o pipeline de uma vez:

```
1. Setup                    (2 min)   ✅
   ↓
2. Bronze Ingestion         (3 min)   ✅
   ↓
3. Silver Transformation    (2 min)   ✅
   ↓
4. Gold Features            (3 min)   ✅
   ↓
5. Churn Model              (4 min)   ✅
   ↓
6. Batch Scoring            (3 min)   ✅
   ↓
7. A/B Testing              (4 min)   ✅
   ↓
8. Monitoring               (2 min)   ✅

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TEMPO TOTAL: ~23 minutos
```

---

## 📊 Verificar Resultados

### Tabelas Criadas
```sql
-- Listar todas as tabelas
SHOW TABLES IN main.customer_intelligence_gold;

-- Verificar scores de churn
SELECT * FROM main.customer_intelligence_gold.customer_scores
WHERE churn_risk_category = 'High'
ORDER BY churn_probability DESC
LIMIT 10;

-- Verificar resultados de experimentos
SELECT * FROM main.customer_intelligence_gold.campaign_ab_test_results
WHERE is_significant_chi2 = true
ORDER BY lift_pct DESC;
```

### Verificar MLflow
```python
import mlflow

# Ver experimentos
mlflow.set_experiment("/Users/valdomirovega@hotmail.com/customer_intelligence_experiments")
experiments = mlflow.search_experiments()

# Ver modelos registrados
from mlflow.tracking import MlflowClient
client = MlflowClient()
models = client.search_registered_models()

for model in models:
    print(f"Modelo: {model.name}")
    print(f"Versões: {len(model.latest_versions)}")
```

---

## 🐛 Troubleshooting

### Erro: "Schema não existe"
**Solução**: Execute `00_setup/Config e Setup Inicial` primeiro

### Erro: "Modelo não encontrado"
**Solução**: Execute `04_models/Modelo Churn Prediction` antes do scoring

### Erro: "Tabela não existe"
**Solução**: Siga a ordem:
1. Bronze (01_bronze)
2. Silver (02_silver)
3. Gold (03_gold)
4. Models (04_models)

### Performance lenta?
**Solução**: 
- Use cluster maior
- Habilite Photon
- Particione tabelas grandes

---

## 📚 Recursos Adicionais

### Documentação
- [README.md](README.md): Documentação completa
- [APRESENTACAO_EXECUTIVA.md](APRESENTACAO_EXECUTIVA.md): Business case
- Este arquivo: Guia prático

### Estrutura de Pastas
```
customer_intelligence_project/
├── 00_setup/          # Começar aqui
├── 01_bronze/         # Dados raw
├── 02_silver/         # Dados limpos
├── 03_gold/           # Features
├── 04_models/         # ML models
├── 05_scoring/        # Batch scoring
├── 06_experimentation/ # A/B test
├── 07_monitoring/     # Drift & KPIs
└── 08_dashboards/     # SQL queries
```

---

## ✨ Dicas Pro

### 1. Use Widgets para Parâmetros
```python
dbutils.widgets.text("date_from", "2024-01-01")
date_from = dbutils.widgets.get("date_from")
```

### 2. Schedule Jobs no Databricks
```
Workflows → Create Job
Tasks: 
  - Bronze Ingestion (daily)
  - Silver Transform (daily)
  - Gold Features (daily)
  - Batch Scoring (weekly)
```

### 3. Crie Alertas
```python
if churn_rate > 0.15:
    send_slack_alert("Churn rate acima de 15%!")
```

### 4. Versionamento de Modelos
```python
# Promover modelo para Production
client.transition_model_version_stage(
    name="customer_intelligence_churn",
    version=2,
    stage="Production"
)
```

---

## 🎯 Quick Wins (Primeiros Resultados)

### Em 1 Hora
- ✅ Pipeline completo executado
- ✅ 10k+ clientes scored
- ✅ Modelo de churn treinado
- ✅ Dashboards básicos criados

### Em 1 Dia
- ✅ Integração com dados reais
- ✅ Customização de features
- ✅ Tuning de hiperparâmetros
- ✅ Dashboards executivos

### Em 1 Semana
- ✅ Produção (jobs agendados)
- ✅ Alertas automáticos
- ✅ Integração com CRM
- ✅ Apresentação para stakeholders

---

## 📧 Suporte

Dúvidas ou sugestões? 
- Revise a documentação: [README.md](README.md)
- Veja exemplos nos notebooks
- Consulte código comentado

---

**⚡ Pronto para Começar!**

_Comece pelo notebook `00_setup/Config e Setup Inicial` e siga a numeração._