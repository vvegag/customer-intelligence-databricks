# 📦 GUIA COMPLETO DE MIGRAÇÃO
## Customer Intelligence Platform - Databricks Unity Catalog

---

## ✅ STATUS DA EXPORTAÇÃO

**Data**: Janeiro 2026  
**Origem**: Conta atual (valdomirovega@hotmail.com)  
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
├── bronze/ (7 tabelas)
├── silver/ (7 tabelas)
└── gold/ (13 tabelas)
```

---

## 🚀 PASSO A PASSO: MIGRAÇÃO PARA NOVA CONTA

### **PASSO 1: BAIXAR ARQUIVOS DO VOLUME**

#### Opção A: Via Databricks UI (Mais Fácil)
1. Catalog Explorer → customer_intelligence → gold → models → migration_backup
2. Botão direito → Download
3. Aguardar download (~50-200 MB)

#### Opção B: Via Databricks CLI
```bash
databricks fs cp -r dbfs:/Volumes/customer_intelligence/gold/models/migration_backup ./backup/
```

---

### **PASSO 2: SUBIR PARA NOVA CONTA**

```sql
-- 1. Criar estrutura
CREATE CATALOG customer_intelligence;
CREATE SCHEMA customer_intelligence.bronze;
CREATE SCHEMA customer_intelligence.silver;
CREATE SCHEMA customer_intelligence.gold;
CREATE VOLUME customer_intelligence.gold.models;
```

```bash
# 2. Upload via CLI
databricks fs cp -r ./backup/ dbfs:/Volumes/customer_intelligence/gold/models/migration_backup/
```

---

### **PASSO 3: CRIAR TABELAS**

```sql
-- Bronze (7 tabelas)
CREATE TABLE customer_intelligence.bronze.customers_raw
USING PARQUET
LOCATION '/Volumes/customer_intelligence/gold/models/migration_backup/bronze/customers_raw';

-- Repetir para todas as 7 tabelas Bronze
-- Repetir para todas as 7 tabelas Silver
-- Repetir para todas as 13 tabelas Gold
```

Ver scripts SQL completos de importação no notebook de migração.

---

## 📋 CHECKLIST

- [ ] Baixar arquivos do Volume
- [ ] Criar catálogo e schemas na nova conta
- [ ] Upload dos arquivos
- [ ] Executar CREATE TABLE (27 statements)
- [ ] Validar contagens de linhas
- [ ] Copiar modelo serializado
- [ ] Re-executar notebooks (opcional)

---

**🎯 TEMPO ESTIMADO: 30-60 minutos**
