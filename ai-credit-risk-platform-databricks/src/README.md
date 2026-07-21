# 🏗️ src/ - Production Code

> Código modular de produção - Refatorado de notebooks para Python packages

---

## 📁 Estrutura

```
src/
├── pipelines/          # Pipelines de dados (ETL)
│   ├── ingestion/      # Bronze layer - Ingestão
│   └── transformation/ # Silver → Gold - Features
│
├── models/             # Modelos ML e RAG
│   ├── credit_risk/    # Modelos de risco
│   └── rag_agent/      # ✅ RAG Agent (já modularizado)
│
├── monitoring/         # Observabilidade
│   ├── drift_detector.py
│   ├── data_quality.py
│   └── alerting.py
│
└── utils/              # Utilitários compartilhados
    ├── config.py
    ├── logging.py
    └── spark_utils.py
```

---

## ✅ Status de Migração

| Módulo | Status | Testes | Deploy |
|--------|--------|--------|--------|
| RAG Agent | ✅ Completo | ✅ 85% | ✅ Script pronto |
| Ingestão | ⏳ Planejado | ❌ 0% | ❌ Pendente |
| Features | ⏳ Planejado | ❌ 0% | ❌ Pendente |
| Modelos ML | ⏳ Planejado | ❌ 0% | ❌ Pendente |
| Monitoring | ⏳ Planejado | ❌ 0% | ❌ Pendente |

---

## 🎯 Princípios de Código

### 1. Modular & Testável
```python
# ✅ BOM
class DataIngestion:
    def __init__(self, config: Config):
        self.config = config
    
    def ingest(self, source: str) -> DataFrame:
        # Lógica testável
        pass

# ❌ RUIM (notebook)
df = spark.read.table("...")  # Hard-coded
```

### 2. Type Hints
```python
# ✅ BOM
def transform_data(df: DataFrame, columns: List[str]) -> DataFrame:
    pass

# ❌ RUIM
def transform_data(df, columns):
    pass
```

### 3. Config Externalizada
```python
# ✅ BOM
config = load_config("config/prod.yml")
table_name = config.bronze.table

# ❌ RUIM
table_name = "credit_risk.bronze.clientes"  # Hard-coded
```

### 4. Error Handling
```python
# ✅ BOM
try:
    df = read_data(source)
except DataSourceError as e:
    logger.error(f"Failed to read {source}: {e}")
    raise

# ❌ RUIM
df = read_data(source)  # Sem tratamento
```

---

## 📦 Como Usar

### Instalar Dependências
```bash
pip install -r requirements.txt
```

### Importar Módulos
```python
# No notebook ou script
import sys
sys.path.append('/Repos/<user>/ai-credit-risk-platform-databricks')

from src.models.rag_agent import RAGAgent
from src.pipelines.ingestion import DataIngestion
```

### Executar Pipeline
```python
from src.pipelines.ingestion import IngestionPipeline

pipeline = IngestionPipeline(config_path="config/dev.yml")
pipeline.run()
```

---

## 🧪 Testes

### Rodar Todos os Testes
```bash
pytest tests/ -v
```

### Rodar Testes de um Módulo
```bash
pytest tests/unit/test_rag_agent.py -v
```

### Cobertura
```bash
pytest --cov=src tests/
```

---

## 🚀 Deploy

Cada módulo em `src/` deve ter um script de deploy em `deploy/`:

- `deploy/model_serving/rag_agent.py` - Deploy RAG no Model Serving
- `deploy/jobs/ingestion_job.yml` - Job de ingestão
- `deploy/pipelines/etl_pipeline.json` - Pipeline Delta Live Tables

---

## 📝 Contribuindo

### Adicionar Novo Módulo

1. **Criar estrutura**:
   ```bash
   mkdir -p src/novo_modulo
   touch src/novo_modulo/__init__.py
   touch src/novo_modulo/core.py
   touch src/novo_modulo/README.md
   ```

2. **Adicionar testes**:
   ```bash
   touch tests/unit/test_novo_modulo.py
   ```

3. **Documentar**:
   - Docstrings (Google style)
   - README com exemplos
   - Type hints obrigatórios

4. **Submeter PR**:
   - Testes passando
   - Cobertura > 70%
   - Lint clean (ruff)

---

## 🔗 Links Úteis

- [ROADMAP.md](../ROADMAP.md) - Plano de refatoração
- [tests/README.md](../tests/README.md) - Guia de testes
- [deploy/README.md](../deploy/README.md) - Guia de deployment

---

_Última atualização: 2026-01-20_
