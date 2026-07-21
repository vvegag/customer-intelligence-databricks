# 🔄 Guia de Migração & Correções

Este documento consolida o guia de migração e correções de catálogo.

---

## 📖 MIGRATION GUIDE

# 📦 GUIA COMPLETO DE MIGRAÇÃO
## Customer Intelligence Platform - Databricks Unity Catalog

---

## ✅ STATUS DA EXPORTAÇÃO

**Data**: Janeiro 2026  
**Origem**: Conta Databricks Free Edition (créditos esgotados)  
**Destino**: Nova conta Databricks com créditos

### 📊 O QUE FOI EXPORTADO

✅ **27 tabelas** exportadas em formato Parquet  
✅ **434,000+ linhas** de dados  
✅ **Todos os schemas preservados**

**Breakdown**:
- 🥉 Bronze: 7 tabelas (193k linhas)
- 🥈 Silver: 7 tabelas (173k linhas)
- 🥇 Gold: 13 tabelas (68k linhas)

**Localização dos arquivos**:
```
/Volumes/customer_intelligence/gold/models/migration_backup/
├── bronze/
│   ├── behavioral_events_raw/
│   ├── campaign_exposures_raw/
│   ├── campaign_responses_raw/
│   ├── campaigns_raw/
│   ├── customers_raw/
│   ├── products_raw/
│   └── transactions_raw/
├── silver/
│   ├── behavioral_events/
│   ├── campaign_exposures/
│   ├── campaign_responses/
│   ├── campaigns/
│   ├── customers/
│   ├── products/
│   └── transactions/
└── gold/
    ├── behavioral_features/
    ├── business_kpis_history/
    ├── campaign_ab_test_results/
    ├── campaign_history_features/
    ├── campaign_performance_trend/
    ├── campaign_roas/
    ├── churn_labels/
    ├── churn_predictions/
    ├── customer_features/
    ├── customer_scores/
    ├── customer_segments/
    ├── feature_drift_monitoring/
    └── rfm_features/
```

---

## 🚀 PASSO A PASSO: MIGRAÇÃO COMPLETA

### **PASSO 1: BAIXAR ARQUIVOS DO VOLUME** (Conta Atual)

#### Opção A: Via Databricks UI (Mais Fácil)
1. Acesse: **Catalog Explorer**
2. Navegue: `customer_intelligence` → `gold` → `volumes` → `models`
3. Entre na pasta `migration_backup/`
4. Botão direito na pasta → **Download**
5. Aguarde download do ZIP (~50-200 MB)

#### Opção B: Via Databricks CLI (Recomendado para volumes maiores)
```bash
# Instalar Databricks CLI (se ainda não tem)
pip install databricks-cli

# Configurar autenticação (conta atual)
databricks configure --token
# Cole o host (ex: https://dbc-xxxxx.cloud.databricks.com)
# Cole seu Personal Access Token

# Baixar todos os arquivos
databricks fs cp -r \
  dbfs:/Volumes/customer_intelligence/gold/models/migration_backup \
  ./local_backup/

# Verificar download
ls -lh local_backup/
```

---

### **PASSO 2: SUBIR PARA NOVA CONTA** (Conta com Créditos)

#### 2.1 Criar Estrutura Unity Catalog

```sql
-- Na nova conta Databricks, abra um SQL Editor e execute:

-- Criar catálogo
CREATE CATALOG IF NOT EXISTS customer_intelligence
COMMENT 'Plataforma de Customer Intelligence e Growth';

-- Criar schemas
CREATE SCHEMA IF NOT EXISTS customer_intelligence.bronze
COMMENT 'Raw data layer - dados brutos sem transformação';

CREATE SCHEMA IF NOT EXISTS customer_intelligence.silver
COMMENT 'Clean data layer - dados limpos e validados';

CREATE SCHEMA IF NOT EXISTS customer_intelligence.gold
COMMENT 'Business layer - features e aggregações para ML';

-- Criar volume para modelos
CREATE VOLUME IF NOT EXISTS customer_intelligence.gold.models
COMMENT 'Armazenamento de modelos ML e backups';
```

#### 2.2 Upload dos Arquivos

**Opção A: Via Databricks UI**
1. Catalog Explorer → `customer_intelligence` → `gold` → `models`
2. Clique **Upload**
3. Selecione a pasta `migration_backup/` completa baixada
4. Aguarde upload concluir

**Opção B: Via Databricks CLI**
```bash
# Configurar nova conta (use novo token da conta com créditos)
databricks configure --token

# Upload dos arquivos
databricks fs cp -r \
  ./local_backup/ \
  dbfs:/Volumes/customer_intelligence/gold/models/migration_backup/

# Verificar upload
databricks fs ls dbfs:/Volumes/customer_intelligence/gold/models/migration_backup/
```

---

### **PASSO 3: CRIAR TODAS AS TABELAS** (Nova Conta)

Execute os comandos SQL abaixo na nova conta para recriar as 27 tabelas:

#### 🥉 **3.1 BRONZE LAYER (7 tabelas)**

```sql
-- 1. Behavioral Events Raw
CREATE TABLE customer_intelligence.bronze.behavioral_events_raw
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/bronze/behavioral_events_raw'
COMMENT 'Eventos comportamentais brutos (page_view, product_view, add_to_cart, etc)';

-- 2. Campaign Exposures Raw
CREATE TABLE customer_intelligence.bronze.campaign_exposures_raw
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/bronze/campaign_exposures_raw'
COMMENT 'Exposições de clientes a campanhas de marketing';

-- 3. Campaign Responses Raw
CREATE TABLE customer_intelligence.bronze.campaign_responses_raw
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/bronze/campaign_responses_raw'
COMMENT 'Respostas de clientes a campanhas (click, conversion)';

-- 4. Campaigns Raw
CREATE TABLE customer_intelligence.bronze.campaigns_raw
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/bronze/campaigns_raw'
COMMENT 'Metadados de campanhas de marketing';

-- 5. Customers Raw
CREATE TABLE customer_intelligence.bronze.customers_raw
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/bronze/customers_raw'
COMMENT 'Dados demográficos de clientes (10k clientes)';

-- 6. Products Raw
CREATE TABLE customer_intelligence.bronze.products_raw
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/bronze/products_raw'
COMMENT 'Catálogo de produtos (500 SKUs)';

-- 7. Transactions Raw
CREATE TABLE customer_intelligence.bronze.transactions_raw
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/bronze/transactions_raw'
COMMENT 'Transações brutas (50k transações)';
```

#### 🥈 **3.2 SILVER LAYER (7 tabelas)**

```sql
-- 1. Behavioral Events Clean
CREATE TABLE customer_intelligence.silver.behavioral_events
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/silver/behavioral_events'
COMMENT 'Eventos comportamentais limpos com enriquecimento temporal';

-- 2. Campaign Exposures Clean
CREATE TABLE customer_intelligence.silver.campaign_exposures
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/silver/campaign_exposures'
COMMENT 'Exposições limpas com validação de datas';

-- 3. Campaign Responses Clean
CREATE TABLE customer_intelligence.silver.campaign_responses
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/silver/campaign_responses'
COMMENT 'Respostas validadas e enriquecidas';

-- 4. Campaigns Clean
CREATE TABLE customer_intelligence.silver.campaigns
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/silver/campaigns'
COMMENT 'Campanhas com cálculos de métricas base';

-- 5. Customers Clean
CREATE TABLE customer_intelligence.silver.customers
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/silver/customers'
COMMENT 'Clientes validados com enriquecimento (age_group, is_active, etc)';

-- 6. Products Clean
CREATE TABLE customer_intelligence.silver.products
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/silver/products'
COMMENT 'Produtos validados com categoria e atributos';

-- 7. Transactions Clean
CREATE TABLE customer_intelligence.silver.transactions
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/silver/transactions'
COMMENT 'Transações validadas com enriquecimento temporal (year, month, quarter)';
```

#### 🥇 **3.3 GOLD LAYER (13 tabelas)**

```sql
-- 1. Behavioral Features
CREATE TABLE customer_intelligence.gold.behavioral_features
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/gold/behavioral_features'
COMMENT 'Features comportamentais agregadas por cliente (últimos 30d/90d/180d)';

-- 2. Business KPIs History
CREATE TABLE customer_intelligence.gold.business_kpis_history
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/gold/business_kpis_history'
COMMENT 'Histórico de KPIs de negócio (revenue, churn rate, ARPU)';

-- 3. Campaign A/B Test Results
CREATE TABLE customer_intelligence.gold.campaign_ab_test_results
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/gold/campaign_ab_test_results'
COMMENT 'Resultados de testes A/B por campanha (uplift, statistical significance)';

-- 4. Campaign History Features
CREATE TABLE customer_intelligence.gold.campaign_history_features
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/gold/campaign_history_features'
COMMENT 'Histórico de interações com campanhas por cliente';

-- 5. Campaign Performance Trend
CREATE TABLE customer_intelligence.gold.campaign_performance_trend
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/gold/campaign_performance_trend'
COMMENT 'Tendências de performance de campanhas ao longo do tempo';

-- 6. Campaign ROAS
CREATE TABLE customer_intelligence.gold.campaign_roas
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/gold/campaign_roas'
COMMENT 'Return on Ad Spend por campanha (20 campanhas analisadas)';

-- 7. Churn Labels
CREATE TABLE customer_intelligence.gold.churn_labels
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/gold/churn_labels'
COMMENT 'Labels de churn para treinamento (10k clientes)';

-- 8. Churn Predictions
CREATE TABLE customer_intelligence.gold.churn_predictions
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/gold/churn_predictions'
COMMENT 'Predições de churn (AUC 0.9411, 64% high-risk identificado)';

-- 9. Customer Features
CREATE TABLE customer_intelligence.gold.customer_features
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/gold/customer_features'
COMMENT 'Features consolidadas por cliente (57 features RFM + comportamentais)';

-- 10. Customer Scores
CREATE TABLE customer_intelligence.gold.customer_scores
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/gold/customer_scores'
COMMENT 'Scores ML por cliente (churn_score, propensity_score, value_score)';

-- 11. Customer Segments
CREATE TABLE customer_intelligence.gold.customer_segments
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/gold/customer_segments'
COMMENT 'Segmentação K-Means (5 clusters: Champions, At Risk, etc)';

-- 12. Feature Drift Monitoring
CREATE TABLE customer_intelligence.gold.feature_drift_monitoring
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/gold/feature_drift_monitoring'
COMMENT 'Monitoramento de drift em features (PSI, KS Test)';

-- 13. RFM Features
CREATE TABLE customer_intelligence.gold.rfm_features
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/gold/rfm_features'
COMMENT 'Features RFM (Recency, Frequency, Monetary) por cliente';
```

---

### **PASSO 4: VALIDAR MIGRAÇÃO** (Nova Conta)

Execute este SQL para validar contagens de linhas:

```sql
-- Validação Bronze
SELECT 'BRONZE' as layer, 'behavioral_events_raw' as table, COUNT(*) as rows 
FROM customer_intelligence.bronze.behavioral_events_raw
UNION ALL
SELECT 'BRONZE', 'campaign_exposures_raw', COUNT(*) 
FROM customer_intelligence.bronze.campaign_exposures_raw
UNION ALL
SELECT 'BRONZE', 'campaign_responses_raw', COUNT(*) 
FROM customer_intelligence.bronze.campaign_responses_raw
UNION ALL
SELECT 'BRONZE', 'campaigns_raw', COUNT(*) 
FROM customer_intelligence.bronze.campaigns_raw
UNION ALL
SELECT 'BRONZE', 'customers_raw', COUNT(*) 
FROM customer_intelligence.bronze.customers_raw
UNION ALL
SELECT 'BRONZE', 'products_raw', COUNT(*) 
FROM customer_intelligence.bronze.products_raw
UNION ALL
SELECT 'BRONZE', 'transactions_raw', COUNT(*) 
FROM customer_intelligence.bronze.transactions_raw

-- Validação Silver
UNION ALL
SELECT 'SILVER', 'behavioral_events', COUNT(*) 
FROM customer_intelligence.silver.behavioral_events
UNION ALL
SELECT 'SILVER', 'campaign_exposures', COUNT(*) 
FROM customer_intelligence.silver.campaign_exposures
UNION ALL
SELECT 'SILVER', 'campaign_responses', COUNT(*) 
FROM customer_intelligence.silver.campaign_responses
UNION ALL
SELECT 'SILVER', 'campaigns', COUNT(*) 
FROM customer_intelligence.silver.campaigns
UNION ALL
SELECT 'SILVER', 'customers', COUNT(*) 
FROM customer_intelligence.silver.customers
UNION ALL
SELECT 'SILVER', 'products', COUNT(*) 
FROM customer_intelligence.silver.products
UNION ALL
SELECT 'SILVER', 'transactions', COUNT(*) 
FROM customer_intelligence.silver.transactions

-- Validação Gold (principais)
UNION ALL
SELECT 'GOLD', 'customer_features', COUNT(*) 
FROM customer_intelligence.gold.customer_features
UNION ALL
SELECT 'GOLD', 'customer_scores', COUNT(*) 
FROM customer_intelligence.gold.customer_scores
UNION ALL
SELECT 'GOLD', 'customer_segments', COUNT(*) 
FROM customer_intelligence.gold.customer_segments
UNION ALL
SELECT 'GOLD', 'churn_predictions', COUNT(*) 
FROM customer_intelligence.gold.churn_predictions
UNION ALL
SELECT 'GOLD', 'campaign_roas', COUNT(*) 
FROM customer_intelligence.gold.campaign_roas

ORDER BY layer, table;
```

**Contagens Esperadas:**
```
BRONZE:
- behavioral_events_raw: 100,000
- campaign_exposures_raw: 30,000
- campaign_responses_raw: 2,999
- campaigns_raw: 20
- customers_raw: 10,000
- products_raw: 500
- transactions_raw: 50,000

SILVER:
- behavioral_events: 100,000
- campaign_exposures: 30,000
- campaign_responses: 2,999
- campaigns: 20
- customers: 10,000
- products: 500
- transactions: 29,963

GOLD:
- customer_features: 10,000
- customer_scores: 10,000
- customer_segments: 10,000
- churn_predictions: 2,000
- campaign_roas: 20
```

---

### **PASSO 5: VERIFICAR SCHEMAS**

```sql
-- Verificar schema de uma tabela importante
DESCRIBE TABLE EXTENDED customer_intelligence.gold.customer_features;

-- Ver todas as tabelas criadas
SHOW TABLES IN customer_intelligence.bronze;
SHOW TABLES IN customer_intelligence.silver;
SHOW TABLES IN customer_intelligence.gold;

-- Consulta de teste
SELECT 
  segment,
  COUNT(*) as customers,
  AVG(churn_score) as avg_churn_score,
  SUM(revenue_total) as total_revenue
FROM customer_intelligence.gold.customer_segments
GROUP BY segment
ORDER BY total_revenue DESC;
```

---

## 📋 CHECKLIST COMPLETO DE MIGRAÇÃO

### Conta Atual ✅
- [x] Exportar 27 tabelas para Parquet
- [x] Verificar arquivos no Volume
- [ ] Baixar arquivos localmente (UI ou CLI)

### Nova Conta 🔜
- [ ] Criar catálogo `customer_intelligence`
- [ ] Criar schemas (bronze, silver, gold)
- [ ] Criar volume `customer_intelligence.gold.models`
- [ ] Upload dos arquivos Parquet
- [ ] Executar CREATE TABLE (7 Bronze)
- [ ] Executar CREATE TABLE (7 Silver)
- [ ] Executar CREATE TABLE (13 Gold)
- [ ] Validar contagens de linhas (SQL acima)
- [ ] Testar queries nas tabelas Gold
- [ ] (Opcional) Copiar modelo serializado
- [ ] (Opcional) Re-executar notebooks

---

## 🎯 COMANDOS RÁPIDOS

### Download Completo (Databricks CLI)
```bash
databricks fs cp -r \
  dbfs:/Volumes/customer_intelligence/gold/models/migration_backup \
  ./backup/
```

### Upload Completo (Databricks CLI na nova conta)
```bash
databricks fs cp -r \
  ./backup/ \
  dbfs:/Volumes/customer_intelligence/gold/models/migration_backup/
```

### Criar Todas as Tabelas (executar na ordem)
1. Executar todos os CREATE TABLE do Bronze (7)
2. Executar todos os CREATE TABLE do Silver (7)
3. Executar todos os CREATE TABLE do Gold (13)
4. Executar query de validação

---

## ⚠️ OBSERVAÇÕES IMPORTANTES

### Delta vs Parquet
- **Exportado como**: Parquet (portabilidade máxima)
- **Na nova conta**: Tabelas são criadas como **External Tables** apontando para Parquet
- **Opcional**: Converter para Delta depois:
  ```sql
  CREATE OR REPLACE TABLE customer_intelligence.bronze.customers_raw
  USING DELTA
  AS SELECT * FROM parquet.`/Volumes/.../bronze/customers_raw`;
  ```

### Permissões Unity Catalog
Certifique-se de ter:
- `CREATE CATALOG` no metastore
- `CREATE SCHEMA` no catálogo
- `CREATE TABLE` nos schemas
- `WRITE FILES` no volume

```sql
-- Se necessário, pedir para admin conceder:
GRANT CREATE CATALOG ON METASTORE TO `seu_usuario@empresa.com`;
GRANT CREATE ON CATALOG customer_intelligence TO `seu_usuario@empresa.com`;
```

### Tamanho Estimado
- **Parquet comprimido**: ~50-200 MB
- **Delta Lake (se converter)**: ~100-300 MB
- **Tempo de upload**: 5-15 minutos

---

## 🚨 TROUBLESHOOTING

### ❌ Problema: "Volume not found"
**Solução**: Criar volume primeiro
```sql
CREATE VOLUME IF NOT EXISTS customer_intelligence.gold.models;
```

### ❌ Problema: "Permission denied"
**Solução**: Verificar permissões UC
```sql
SHOW GRANTS ON CATALOG customer_intelligence;
```

### ❌ Problema: "Path does not exist"
**Solução**: Verificar upload dos arquivos
```bash
databricks fs ls dbfs:/Volumes/customer_intelligence/gold/models/migration_backup/
```

### ❌ Problema: "Schema mismatch"
**Solução**: Schemas são inferidos automaticamente. Se houver problema:
```sql
-- Ver schema inferido
DESCRIBE TABLE customer_intelligence.bronze.customers_raw;

-- Se necessário, recriar com schema explícito
CREATE TABLE customer_intelligence.bronze.customers_raw (
  customer_id STRING,
  age INT,
  ...
) USING PARQUET
LOCATION '...';
```

---

## ✅ PÓS-MIGRAÇÃO

### 1. Re-executar Notebooks (Opcional)
Os notebooks do repositório Git podem ser executados na nova conta para:
- Validar que tudo funciona
- Atualizar dados com novos cálculos
- Retreinar modelos

### 2. Dashboards Lakeview
As queries SQL do `08_dashboards/` podem ser copiadas para criar novos dashboards:
- Churn Risk Dashboard
- Customer Segmentation View
- Campaign Performance
- Executive KPIs

### 3. MLflow Models
Modelo de Churn está serializado no Volume:
```
/Volumes/customer_intelligence/gold/models/churn_model_v1.pkl.parquet
```
Pode ser registrado no MLflow Registry da nova conta.

### 4. Agendamento (Jobs)
Criar Databricks Jobs para:
- Refresh diário das features Gold
- Re-scoring semanal de clientes
- Monitoramento de drift

---

## 📊 RESUMO DA MIGRAÇÃO

| Item | Valor |
|------|-------|
| **Tabelas exportadas** | 27 |
| **Linhas totais** | 434,000+ |
| **Tamanho estimado** | 50-200 MB |
| **Tempo de download** | 5-10 min |
| **Tempo de upload** | 10-20 min |
| **Tempo de CREATE TABLE** | 10-15 min |
| **Tempo de validação** | 5-10 min |
| **TOTAL ESTIMADO** | **30-60 minutos** |

---

**🎯 GUIA ATUALIZADO COM SCRIPTS COMPLETOS!**

_Customer Intelligence Platform • Janeiro 2026 • Valdomiro Vega_


---

## 🔧 CORREÇÕES DE CATÁLOGO

> 📜 **Nota histórica**: esta seção documenta um bug de uma fase muito anterior
> do projeto, quando o catálogo se chamava `workspace` (por não existir `main`
> no workspace da época). O nome final do catálogo é `customer_intelligence`
> (não `workspace`) — mantido aqui só como exemplo real de troubleshooting.

# ⚙️ Correção do Catálogo - Resolvido

## 🐛 Problema Encontrado

**Erro original:**
```
[NO_SUCH_CATALOG_EXCEPTION] Catalog 'main' was not found. 
Please verify the catalog name and then retry the query or command again. 
SQLSTATE: 42704
```

## 🔍 Causa Raiz

O projeto foi criado assumindo o catálogo padrão `main` (comum em Unity Catalog), mas o workspace atual possui:
- ✅ `workspace` - Catálogo de trabalho padrão
- ✅ `samples` - Dados de exemplo
- ✅ `system` - Catálogo do sistema
- ❌ `main` - **NÃO EXISTE**

## ✅ Solução Aplicada

### Alterações Realizadas

#### 1. Notebook de Setup (`00_setup/Config e Setup Inicial`)

**Antes:**
```python
CATALOG = "main"  # Ajuste conforme seu workspace
```

**Depois:**
```python
CATALOG = "workspace"  # Catálogo padrão do workspace
```

**SQL Corrigido:**
```sql
-- Antes
CREATE SCHEMA IF NOT EXISTS main.customer_intelligence_bronze...

-- Depois
CREATE SCHEMA IF NOT EXISTS workspace.customer_intelligence_bronze...
```

#### 2. Notebook de Dashboards (`08_dashboards/SQL Queries para Dashboards`)

**7 queries SQL atualizadas:**
```sql
-- Antes
FROM main.customer_intelligence_gold.customer_scores
INNER JOIN main.customer_intelligence_silver.customers

-- Depois
FROM workspace.customer_intelligence_gold.customer_scores
INNER JOIN workspace.customer_intelligence_silver.customers
```

#### 3. Outros Notebooks (01-07)

✅ **NÃO PRECISAM ALTERAÇÃO**

Por quê?
- Usam a variável `CATALOG` definida no setup
- Usam a função `get_full_table_name(schema, table)`
- A função já retorna o nome correto: `workspace.schema.table`

---

## 📊 Schemas Criados

Depois da correção, os seguintes schemas foram criados:

```sql
-- Schemas no catálogo workspace
workspace.customer_intelligence_bronze   -- Raw data
workspace.customer_intelligence_silver   -- Clean data  
workspace.customer_intelligence_gold     -- Features & Scores
```

Verificar:
```sql
SHOW SCHEMAS IN workspace LIKE 'customer_intelligence%';
```

---

## 🧪 Testado e Funcionando

✅ Setup executado com sucesso
✅ Schemas criados
✅ MLflow configurado
✅ Helper functions carregadas
✅ Projeto pronto para uso

---

## 🚀 Como Executar Agora

### Ordem de Execução

```bash
1. ✅ 00_setup/Config e Setup Inicial          [JÁ EXECUTADO]
2. ⏭️  01_bronze/Ingestao Dados Bronze         [PRÓXIMO]
3. ⏭️  02_silver/Transformacao Silver
4. ⏭️  03_gold/Feature Engineering Gold
5. ⏭️  04_models/Modelo Churn Prediction
6. ⏭️  05_scoring/Batch Scoring
7. ⏭️  06_experimentation/AB Testing
8. ⏭️  07_monitoring/Monitoramento Performance
```

---

## 🔧 Para Workspaces Diferentes

Se você clonar este projeto em outro workspace que tenha catálogo `main`, ajuste:

**Opção 1: Usar outro catálogo**
```python
# Em: 00_setup/Config e Setup Inicial
CATALOG = "seu_catalogo_aqui"  # Exemplo: "main", "dev", "prod"
```

**Opção 2: Verificar catálogos disponíveis**
```sql
SHOW CATALOGS;
```

Saída típica:
```
+----------+
| catalog  |
+----------+
| main     |  ← Se tiver, pode usar
| workspace|  ← Default
| samples  |
| system   |
+----------+
```

---

## 📝 Para GitHub

Ao subir para GitHub, documente no README:

```markdown
## ⚠️ Configuração Inicial

Antes de executar, ajuste o catálogo no setup:

1. Verifique catálogos disponíveis:
   ```sql
   SHOW CATALOGS;
   ```

2. Edite `00_setup/Config e Setup Inicial`:
   ```python
   CATALOG = "workspace"  # ou "main", conforme disponível
   ```

3. Execute o setup:
   ```bash
   Run All em: 00_setup/Config e Setup Inicial
   ```
```

---

## 🎯 Resumo

| Item | Status |
|------|--------|
| Erro identificado | ✅ |
| Causa raiz encontrada | ✅ |
| Setup corrigido | ✅ |
| Dashboards atualizados | ✅ |
| Schemas criados | ✅ |
| MLflow configurado | ✅ |
| Projeto funcional | ✅ |

---

## 💡 Lições Aprendidas

1. **Sempre verifique catálogos disponíveis** antes de hardcodear nomes
2. **Use variáveis de configuração** (como fizemos com `CATALOG`)
3. **Documente catálogos default** no README para novos usuários
4. **Teste em diferentes workspaces** para garantir portabilidade

---

**✅ Problema Resolvido - Projeto Pronto para Usar!**

_Última atualização: 02 Jul 2026_
