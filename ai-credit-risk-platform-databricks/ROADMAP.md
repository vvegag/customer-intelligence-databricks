# 🗺️ Roadmap: Produtização do AI Credit Risk Platform

> Evolução de POC para Produção - Refatoração Incremental

---

## 📍 Status Atual

### ✅ Fase 1: POC Funcional (COMPLETO)
- Estrutura baseada em notebooks (01-10)
- Pipeline end-to-end funcionando
- RAG Agent modular com testes
- Dashboards operacionais

### 🏗️ Fase 2: Estrutura de Produção (EM ANDAMENTO)
- ✅ Criada estrutura `src/`, `research/`, `tests/`, `deploy/`
- ✅ Separação Experimentação vs Produção
- ⏳ Refatoração incremental dos módulos
- ⏳ Testes automatizados
- ⏳ CI/CD com Databricks Asset Bundles

---

## 🎯 Roadmap de Refatoração

### **FASE 2.1: Fundação** (✅ COMPLETO - 2h)

**Objetivo**: Criar estrutura sem quebrar nada existente

- [x] Criar `src/` - Código de produção
- [x] Criar `research/` - Experimentação
- [x] Criar `tests/` - Testes automatizados
- [x] Criar `deploy/` - Deployment configs
- [x] Mover RAG Agent para `src/models/rag_agent/`
- [x] Documentar roadmap (este arquivo)

**Resultado**: Estrutura profissional mantendo código atual funcionando

---

### **FASE 2.2: RAG Agent** (⏳ PRÓXIMO - 3-4h)

**Objetivo**: Consolidar primeiro módulo produzido

**Tarefas**:
- [ ] Mover `10_rag_agent/` → `src/models/rag_agent/`
- [ ] Atualizar imports e paths
- [ ] Adicionar testes de integração
- [ ] Deploy script para Model Serving
- [ ] Documentação de API

**Critério de Sucesso**:
- ✅ RAG Agent 100% em `src/`
- ✅ Testes passando
- ✅ Deploy automatizado funcionando

---

### **FASE 2.3: Pipelines de Ingestão** (⏳ PRÓXIMO - 4-6h)

**Objetivo**: Modularizar ingestão de dados (Bronze)

**Tarefas**:
- [ ] Refatorar `02_ingestion/` → `src/pipelines/ingestion/`
- [ ] Criar classes Python:
  - `IngestionPipeline` (base)
  - `ClientDataIngestion`
  - `InvoiceIngestion`
- [ ] Adicionar testes unitários
- [ ] Config files (YAML/JSON)
- [ ] Manter notebooks originais em `research/notebooks/`

**Critério de Sucesso**:
- ✅ Pipelines em `src/pipelines/ingestion/`
- ✅ 80%+ cobertura de testes
- ✅ Configuração externalizada

---

### **FASE 2.4: Feature Engineering** (⏳ PRÓXIMO - 4-6h)

**Objetivo**: Modularizar transformações (Silver → Gold)

**Tarefas**:
- [ ] Refatorar `03_feature_engineering/` → `src/pipelines/transformation/`
- [ ] Criar classes:
  - `FeatureTransformer` (base)
  - `AggregationFeatures`
  - `RiskFeatures`
- [ ] Testes de qualidade de dados
- [ ] Validação de schema
- [ ] Notebooks exploratórios → `research/`

**Critério de Sucesso**:
- ✅ Features em `src/pipelines/transformation/`
- ✅ Validação de dados automatizada
- ✅ Testes de qualidade

---

### **FASE 2.5: Modelos ML** (⏳ PRÓXIMO - 6-8h)

**Objetivo**: Modularizar modelos de ML

**Tarefas**:
- [ ] Refatorar `04_modeling/` → `src/models/credit_risk/`
- [ ] Criar classes:
  - `BaseModel` (abstrata)
  - `DelinquencyClassifier`
  - `ValueRegressor`
  - `CashflowForecaster`
- [ ] Testes de modelo:
  - Performance metrics
  - Data drift detection
  - Model bias
- [ ] MLflow integration
- [ ] Notebooks de treinamento → `research/experiments/`

**Critério de Sucesso**:
- ✅ Modelos em `src/models/credit_risk/`
- ✅ Testes de ML
- ✅ Tracking MLflow automatizado

---

### **FASE 2.6: Monitoring** (⏳ PRÓXIMO - 3-4h)

**Objetivo**: Modularizar monitoramento

**Tarefas**:
- [ ] Refatorar `07_monitoring/` → `src/monitoring/`
- [ ] Criar módulos:
  - `drift_detector.py`
  - `data_quality.py`
  - `model_performance.py`
  - `alerting.py`
- [ ] Testes de alertas
- [ ] Dashboard de monitoring

**Critério de Sucesso**:
- ✅ Monitoring em `src/monitoring/`
- ✅ Alertas configuráveis
- ✅ Dashboard operacional

---

### **FASE 3: CI/CD & Automação** (⏳ FUTURO - 8-12h)

**Objetivo**: Deployment automatizado completo

**Tarefas**:
- [ ] Databricks Asset Bundles (DABs)
  - `databricks.yml` (config principal)
  - `resources/` (jobs, pipelines, endpoints)
  - Environments: `dev`, `staging`, `prod`
- [ ] GitHub Actions:
  - Testes automatizados em PR
  - Deploy em merge to main
  - Rollback automatizado
- [ ] Versionamento:
  - Semantic versioning
  - Changelog automatizado
  - Release notes
- [ ] Observabilidade:
  - Logs centralizados
  - Métricas de SLO
  - Tracing distribuído

**Critério de Sucesso**:
- ✅ Deploy com 1 comando
- ✅ Rollback automatizado
- ✅ Zero-downtime deployments

---

## 📊 Métricas de Progresso

### Cobertura de Testes
- **Atual**: 15% (apenas RAG Agent)
- **Meta Fase 2**: 70%
- **Meta Fase 3**: 85%+

### Código Modular
- **Atual**: 10% (apenas RAG Agent)
- **Meta Fase 2**: 80%
- **Meta Fase 3**: 95%

### Automação
- **Atual**: Manual
- **Meta Fase 2**: Semi-automatizado
- **Meta Fase 3**: Totalmente automatizado

---

## 🔄 Princípios da Refatoração

### 1. **Incremental, Não Big Bang**
- Um módulo por vez
- Mantém código antigo funcionando
- Rollback fácil

### 2. **Test-Driven**
- Escrever testes ANTES de refatorar
- Cobertura mínima 70% para cada módulo
- Testes de integração obrigatórios

### 3. **Backward Compatibility**
- Notebooks antigos continuam funcionando
- Deprecation warnings antes de remover
- Período de migração de 2 sprints

### 4. **Documentation First**
- Documentar API antes de implementar
- Exemplos de uso obrigatórios
- README em cada módulo

---

## 📝 Como Contribuir

### Para Adicionar Novo Módulo em `src/`

1. Criar estrutura:
   ```
   src/novo_modulo/
   ├── __init__.py
   ├── core.py
   ├── config.py
   └── README.md
   ```

2. Adicionar testes:
   ```
   tests/unit/test_novo_modulo.py
   tests/integration/test_novo_modulo_integration.py
   ```

3. Documentar:
   - Docstrings (Google style)
   - README com exemplos
   - Atualizar este ROADMAP

4. Testar:
   ```bash
   pytest tests/unit/test_novo_modulo.py -v
   ```

---

## 🎯 Quando Considerar "Pronto para Produção"

Checklist por módulo:

- [ ] Código em `src/` (não em notebooks)
- [ ] Testes unitários (70%+ cobertura)
- [ ] Testes de integração
- [ ] Documentação (README + docstrings)
- [ ] Type hints
- [ ] Logging configurável
- [ ] Error handling robusto
- [ ] Config externalizada
- [ ] Deploy script
- [ ] Monitoring básico

---

## 👥 Responsáveis

- **Tech Lead**: Valdomiro Vega García
- **MLOps**: A definir
- **QA**: A definir

## 📅 Timeline Estimado

- **Fase 2.1**: ✅ Completo (2h)
- **Fase 2.2-2.6**: 3-4 semanas (20-30h)
- **Fase 3**: 2-3 semanas (10-15h)

**Total**: 6-8 semanas para produtização completa

---

_Última atualização: 2026-01-20_
