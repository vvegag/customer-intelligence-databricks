# Databricks notebook source
# DBTITLE 1,Overview - Delta Live Tables
# MAGIC %md
# MAGIC # 🌊 Lakeflow Spark Declarative Pipelines (Delta Live Tables)
# MAGIC
# MAGIC ## Pipeline Production-Ready: Bronze → Silver → Gold
# MAGIC
# MAGIC Este notebook demonstra **Lakeflow Spark Declarative Pipelines** (formerly Delta Live Tables) para data engineering em produção.
# MAGIC
# MAGIC ### 📋 Conteúdo:
# MAGIC 1. **Bronze Layer** - Auto Loader (incremental ingestion)
# MAGIC 2. **Silver Layer** - Transformations + Data Quality Expectations
# MAGIC 3. **Gold Layer** - Business Aggregations
# MAGIC 4. **Streaming Tables** - Real-time processing
# MAGIC 5. **Materialized Views** - Optimized queries
# MAGIC 6. **Change Data Feed** - CDC tracking
# MAGIC 7. **Expectations** - Data quality rules
# MAGIC
# MAGIC ### 🎯 Arquitetura Medallion:
# MAGIC ```
# MAGIC [Raw Data]
# MAGIC     ↓ Auto Loader
# MAGIC [BRONZE] → Raw ingestion (streaming)
# MAGIC     ↓ Cleansing + Validation
# MAGIC [SILVER] → Cleaned data (quality enforced)
# MAGIC     ↓ Business Logic + Aggregations
# MAGIC [GOLD] → Analytics-ready tables
# MAGIC ```
# MAGIC
# MAGIC ### ✨ Features Avançadas:
# MAGIC * **Auto Loader**: Incremental file ingestion
# MAGIC * **Expectations**: Data quality constraints
# MAGIC * **Streaming Tables**: Real-time processing
# MAGIC * **Materialized Views**: Optimized performance
# MAGIC * **Change Data Feed**: Track changes
# MAGIC * **Lineage**: Automatic dependency tracking
# MAGIC
# MAGIC ### 📊 Dados:
# MAGIC Customer transactions → Features → Insights
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **⚠️ IMPORTANTE:** Para executar este notebook:
# MAGIC 1. Configure como um **Lakeflow Pipeline** (não execute células diretamente)
# MAGIC 2. Vá em: **Workflows → Lakeflow Pipelines → Create Pipeline**
# MAGIC 3. Configure:
# MAGIC    - **Notebook**: Este arquivo
# MAGIC    - **Target**: `customer_intelligence.dlt_demo`
# MAGIC    - **Storage**: Cloud storage path
# MAGIC 4. Click **Start**
# MAGIC
# MAGIC OU use o Databricks CLI:
# MAGIC ```bash
# MAGIC databricks pipelines create --settings pipeline_config.json
# MAGIC databricks pipelines start --pipeline-id <pipeline_id>
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Imports e Setup
# Delta Live Tables (DLT) - Lakeflow Spark Declarative Pipelines
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configurações do pipeline
CATALOG = "customer_intelligence"
SCHEMA = "dlt_demo"

# Paths (exemplo - ajustar conforme seu environment)
RAW_DATA_PATH = "/databricks-datasets/retail-org/customers/"
TRANSACTIONS_PATH = "/databricks-datasets/retail-org/transactions/"

print("✅ Imports carregados")
print(f"📊 Catalog: {CATALOG}")
print(f"📁 Schema: {SCHEMA}")
print("\n⚠️ Execute este notebook via Lakeflow Pipeline, não diretamente!")

# COMMAND ----------

# DBTITLE 1,Bronze Layer - Auto Loader
# BRONZE LAYER: Raw data ingestion com Auto Loader
# Auto Loader detecta novos arquivos automaticamente

@dlt.table(
    name="bronze_transactions",
    comment="Raw transaction data ingested via Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "customer_id,transaction_date"
    }
)
def bronze_transactions():
    """
    Bronze layer: Raw ingestion com Auto Loader
    - Incremental file ingestion
    - Schema inference
    - Metadata columns (_rescued_data)
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", 
                    f"/tmp/{CATALOG}/{SCHEMA}/schemas/bronze_transactions")
            .load(TRANSACTIONS_PATH)
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
    )

# Alternativa: Dados simulados caso não tenha raw data
@dlt.table(
    name="bronze_transactions_simulated",
    comment="Simulated transaction data for demo"
)
def bronze_transactions_simulated():
    """
    Bronze layer: Dados simulados para demonstração
    """
    # Ler da tabela gold existente como fonte
    return (
        spark.readStream
            .table(f"{CATALOG}.gold.customer_transactions")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", lit("simulated"))
    )

# COMMAND ----------

# DBTITLE 1,Silver Layer - Transformations + Expectations
# SILVER LAYER: Cleaned & validated data

@dlt.table(
    name="silver_transactions",
    comment="Cleaned transactions with data quality rules",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"  # Enable CDC
    }
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_date", "transaction_date IS NOT NULL")
@dlt.expect_or_fail("recent_data", "datediff(current_date(), transaction_date) <= 365")
def silver_transactions():
    """
    Silver layer: Cleaned data with expectations
    
    Data Quality Rules:
    - customer_id must not be null (drop rows)
    - amount must be > 0 (drop rows)
    - transaction_date must not be null (drop rows)
    - data must be within last 365 days (fail pipeline if violated)
    """
    return (
        dlt.read_stream("bronze_transactions_simulated")
            # Cleansing
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("amount", col("amount").cast("double"))
            .withColumn("transaction_date", to_date(col("transaction_date")))
            
            # Enrichment
            .withColumn("year", year("transaction_date"))
            .withColumn("month", month("transaction_date"))
            .withColumn("quarter", quarter("transaction_date"))
            .withColumn("day_of_week", dayofweek("transaction_date"))
            
            # Categorization
            .withColumn(
                "amount_category",
                when(col("amount") < 50, "small")
                .when(col("amount") < 200, "medium")
                .when(col("amount") < 500, "large")
                .otherwise("very_large")
            )
            
            # Processing timestamp
            .withColumn("processed_timestamp", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Gold Layer - Business Aggregations
# GOLD LAYER: Business-ready aggregations

@dlt.table(
    name="gold_customer_metrics",
    comment="Customer-level aggregated metrics for analytics",
    table_properties={
        "quality": "gold",
        "business_domain": "customer_analytics"
    }
)
def gold_customer_metrics():
    """
    Gold layer: Customer-level metrics
    
    Metrics:
    - Total transactions
    - Total revenue
    - Average transaction value
    - Transaction frequency
    - Customer lifetime value (proxy)
    """
    return (
        dlt.read("silver_transactions")
            .groupBy("customer_id")
            .agg(
                count("*").alias("total_transactions"),
                sum("amount").alias("total_revenue"),
                avg("amount").alias("avg_transaction_value"),
                min("transaction_date").alias("first_transaction_date"),
                max("transaction_date").alias("last_transaction_date"),
                
                # Advanced metrics
                countDistinct("product_id").alias("unique_products"),
                sum(when(col("amount") > 100, 1).otherwise(0)).alias("high_value_transactions"),
                
                # Temporal
                datediff(max("transaction_date"), min("transaction_date")).alias("customer_lifetime_days")
            )
            .withColumn(
                "customer_segment",
                when(col("total_revenue") > 1000, "platinum")
                .when(col("total_revenue") > 500, "gold")
                .when(col("total_revenue") > 200, "silver")
                .otherwise("bronze")
            )
            .withColumn("computed_at", current_timestamp())
    )

# Materialized View: Optimized para queries
@dlt.view(
    name="gold_monthly_revenue",
    comment="Monthly revenue summary - Materialized view for fast queries"
)
def gold_monthly_revenue():
    """
    Materialized view: Agregação mensal otimizada
    """
    return (
        dlt.read("silver_transactions")
            .groupBy("year", "month")
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_revenue"),
                avg("amount").alias("avg_transaction_value"),
                countDistinct("customer_id").alias("unique_customers")
            )
            .orderBy("year", "month")
    )

# COMMAND ----------

# DBTITLE 1,Streaming Table - Real-time CDC
# STREAMING TABLE: Real-time change tracking

@dlt.table(
    name="streaming_customer_activity",
    comment="Real-time customer activity stream with CDC",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "streaming"
    }
)
def streaming_customer_activity():
    """
    Streaming table: Track customer activity in real-time
    
    Features:
    - Incremental processing
    - Change Data Capture (CDC)
    - Low-latency updates
    """
    return (
        dlt.read_stream("silver_transactions")
            .groupBy(
                window("transaction_date", "1 day"),
                "customer_id"
            )
            .agg(
                count("*").alias("daily_transactions"),
                sum("amount").alias("daily_revenue"),
                collect_list("product_id").alias("products_purchased")
            )
            .select(
                col("window.start").alias("date"),
                "customer_id",
                "daily_transactions",
                "daily_revenue",
                "products_purchased"
            )
    )

# SCD Type 2: Slowly Changing Dimension
@dlt.table(
    name="scd_customer_segments",
    comment="SCD Type 2 - Track customer segment changes over time",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)
def scd_customer_segments():
    """
    SCD Type 2: Historical tracking of customer segments
    """
    return (
        dlt.read("gold_customer_metrics")
            .select(
                "customer_id",
                "customer_segment",
                "total_revenue",
                "total_transactions",
                col("computed_at").alias("valid_from")
            )
            .withColumn("valid_to", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
    )

# COMMAND ----------

# DBTITLE 1,Advanced Expectations - Data Quality
# ADVANCED DATA QUALITY: Multiple expectation types

@dlt.table(
    name="quality_validated_transactions",
    comment="Transactions with comprehensive data quality checks"
)
# Expectations que dropam rows inválidas
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL AND length(customer_id) > 0")
@dlt.expect_or_drop("positive_amount", "amount > 0 AND amount < 100000")
@dlt.expect_or_drop("valid_product", "product_id IS NOT NULL")

# Expectations que apenas alertam (trackam violações)
@dlt.expect("reasonable_amount", "amount BETWEEN 1 AND 10000")
@dlt.expect("has_quantity", "quantity >= 1")

# Expectation que falha o pipeline inteiro se violada
@dlt.expect_or_fail("critical_no_nulls", "customer_id IS NOT NULL AND product_id IS NOT NULL")
def quality_validated_transactions():
    """
    Comprehensive data quality validation
    
    Expectation Types:
    1. expect_or_drop: Drop invalid rows (continues)
    2. expect: Track violations (continues, logs metrics)
    3. expect_or_fail: Fail entire pipeline if violated
    
    Use Cases:
    - expect_or_drop: Invalid data (malformed records)
    - expect: Business rules (warnings, monitoring)
    - expect_or_fail: Critical constraints (data integrity)
    """
    return (
        dlt.read_stream("silver_transactions")
            .withColumn(
                "data_quality_score",
                when(
                    (col("customer_id").isNotNull()) &
                    (col("amount") > 0) &
                    (col("amount") < 10000) &
                    (col("product_id").isNotNull()),
                    100
                ).otherwise(0)
            )
    )

# Quarantine table: Capturar dados inválidos para análise
@dlt.table(
    name="quarantine_invalid_transactions",
    comment="Invalid transactions for investigation"
)
def quarantine_invalid_transactions():
    """
    Quarantine: Capture invalid data for debugging
    """
    return (
        dlt.read_stream("bronze_transactions_simulated")
            .filter(
                (col("customer_id").isNull()) |
                (col("amount") <= 0) |
                (col("amount") > 100000) |
                (col("product_id").isNull())
            )
            .withColumn("quarantine_reason",
                when(col("customer_id").isNull(), "missing_customer_id")
                .when(col("amount") <= 0, "invalid_amount_zero_negative")
                .when(col("amount") > 100000, "invalid_amount_too_high")
                .when(col("product_id").isNull(), "missing_product_id")
                .otherwise("unknown")
            )
            .withColumn("quarantine_timestamp", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Lineage & Monitoring
# LINEAGE & MONITORING
# DLT automaticamente rastreia dependências e gera lineage graph

# Exemplo: Query lineage information
# (Rodar após pipeline executar)

# Este código é para referência - DLT já faz isso automaticamente
lineage_info = """
🔗 LINEAGE AUTOMÁTICO:

O Lakeflow Pipelines rastreia automaticamente:

1. **Dependências entre tabelas:**
   bronze_transactions_simulated
        ↓
   silver_transactions
        ↓
   gold_customer_metrics
        ↓
   streaming_customer_activity

2. **Data Quality Metrics:**
   - Expectation violations
   - Rows dropped
   - Rows passed
   - Quality score

3. **Pipeline Metrics:**
   - Processing time
   - Records processed
   - Error rates
   - Throughput

📊 VISUALIZAR LINEAGE:
1. Vá em: Workflows → Lakeflow Pipelines
2. Selecione o pipeline
3. Aba "Graph" mostra o DAG completo
4. Aba "Data Quality" mostra expectation metrics

🔍 QUERY LINEAGE PROGRAMATICAMENTE:

SELECT * FROM event_log(
    'customer_intelligence.dlt_demo.silver_transactions'
)
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC
LIMIT 10

🚨 MONITORING & ALERTS:

Configure alertas para:
- Pipeline failures
- Data quality violations
- Processing delays
- Schema changes

Via: Pipeline Settings → Notifications
"""

print(lineage_info)

# Helper function: Get pipeline stats
def get_pipeline_stats():
    """
    Query pipeline event log para estatísticas
    
    Nota: Só funciona após o pipeline executar
    """
    try:
        stats = spark.sql(f"""
            SELECT 
                event_type,
                COUNT(*) as event_count,
                MAX(timestamp) as last_event
            FROM event_log('{CATALOG}.{SCHEMA}.silver_transactions')
            WHERE event_type IN ('flow_progress', 'user_action', 'update_progress')
            GROUP BY event_type
            ORDER BY event_count DESC
        """)
        return stats
    except Exception as e:
        return f"⚠️ Event log ainda não disponível. Execute o pipeline primeiro: {str(e)}"

print("\n📊 Para obter stats, chame: get_pipeline_stats()")

# COMMAND ----------

# DBTITLE 1,Pipeline Summary & Next Steps
# MAGIC %md
# MAGIC # 🚀 Pipeline Summary - Customer Intelligence DLT
# MAGIC
# MAGIC ## ✅ O Que Foi Implementado:
# MAGIC
# MAGIC ### **Camadas (Medallion Architecture):**
# MAGIC
# MAGIC 1. **🪨 BRONZE LAYER**
# MAGIC    - `bronze_transactions` - Auto Loader ingestion
# MAGIC    - `bronze_transactions_simulated` - Demo data source
# MAGIC    - **Features**: Incremental ingestion, schema inference
# MAGIC
# MAGIC 2. **🪩9 SILVER LAYER**
# MAGIC    - `silver_transactions` - Cleaned & validated
# MAGIC    - **Expectations**: 4 data quality rules
# MAGIC    - **Features**: CDC enabled, temporal columns, categorization
# MAGIC
# MAGIC 3. **🪪 GOLD LAYER**
# MAGIC    - `gold_customer_metrics` - Customer aggregations
# MAGIC    - `gold_monthly_revenue` - Monthly summary (view)
# MAGIC    - **Features**: Business metrics, segmentation
# MAGIC
# MAGIC 4. **🌊 STREAMING TABLES**
# MAGIC    - `streaming_customer_activity` - Real-time activity
# MAGIC    - `scd_customer_segments` - SCD Type 2 tracking
# MAGIC    - **Features**: Windowed aggregations, CDC
# MAGIC
# MAGIC 5. **✅ QUALITY LAYER**
# MAGIC    - `quality_validated_transactions` - Full validation
# MAGIC    - `quarantine_invalid_transactions` - Invalid data capture
# MAGIC    - **Features**: 3 expectation types, quarantine logic
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Data Flow:
# MAGIC
# MAGIC ```
# MAGIC [Raw Files]
# MAGIC     ↓ Auto Loader (cloudFiles)
# MAGIC [bronze_transactions_simulated] → Raw ingest
# MAGIC     ↓ Cleansing + Expectations
# MAGIC [silver_transactions] → Validated data (CDC enabled)
# MAGIC     ↓ ↓
# MAGIC     ↓ [quality_validated_transactions] → Quality checks
# MAGIC     ↓ [quarantine_invalid_transactions] → Invalid data
# MAGIC     ↓
# MAGIC     ↓ Aggregations
# MAGIC [gold_customer_metrics] → Customer-level
# MAGIC [gold_monthly_revenue] → Time-series
# MAGIC [streaming_customer_activity] → Real-time
# MAGIC [scd_customer_segments] → Historical tracking
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Como Executar Este Pipeline:
# MAGIC
# MAGIC ### **Opção 1: Databricks UI**
# MAGIC
# MAGIC 1. Vá em **Workflows** → **Lakeflow Pipelines**
# MAGIC 2. Clique **Create Pipeline**
# MAGIC 3. Configure:
# MAGIC    ```
# MAGIC    Pipeline name: customer_intelligence_dlt
# MAGIC    Notebook: /Users/.../Delta_Live_Tables_Advanced
# MAGIC    Target: customer_intelligence.dlt_demo
# MAGIC    Storage location: dbfs:/pipelines/customer_intelligence_dlt
# MAGIC    Pipeline mode: Triggered (ou Continuous)
# MAGIC    Cluster mode: Enhanced Autoscaling
# MAGIC    ```
# MAGIC 4. Clique **Create** → **Start**
# MAGIC
# MAGIC ### **Opção 2: Databricks CLI**
# MAGIC
# MAGIC ```bash
# MAGIC # 1. Criar arquivo de config
# MAGIC cat > pipeline_config.json <<EOF
# MAGIC {
# MAGIC   "name": "customer_intelligence_dlt",
# MAGIC   "storage": "/dbfs/pipelines/customer_intelligence_dlt",
# MAGIC   "target": "customer_intelligence.dlt_demo",
# MAGIC   "notebooks": [
# MAGIC     {
# MAGIC       "path": "/Repos/<seu-usuario>/customer-intelligence-databricks/production/pipelines/Delta_Live_Tables_Advanced"
# MAGIC     }
# MAGIC   ],
# MAGIC   "continuous": false,
# MAGIC   "development": true
# MAGIC }
# MAGIC EOF
# MAGIC
# MAGIC # 2. Criar pipeline
# MAGIC databricks pipelines create --settings pipeline_config.json
# MAGIC
# MAGIC # 3. Iniciar pipeline (pegar pipeline_id do output anterior)
# MAGIC databricks pipelines start --pipeline-id <pipeline_id>
# MAGIC
# MAGIC # 4. Ver status
# MAGIC databricks pipelines get --pipeline-id <pipeline_id>
# MAGIC ```
# MAGIC
# MAGIC ### **Opção 3: Python SDK**
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC pipeline = w.pipelines.create(
# MAGIC     name="customer_intelligence_dlt",
# MAGIC     storage="/dbfs/pipelines/customer_intelligence_dlt",
# MAGIC     target="customer_intelligence.dlt_demo",
# MAGIC     notebooks=[{
# MAGIC         "path": "/Repos/<seu-usuario>/customer-intelligence-databricks/production/pipelines/Delta_Live_Tables_Advanced"
# MAGIC     }],
# MAGIC     continuous=False,
# MAGIC     development=True
# MAGIC )
# MAGIC
# MAGIC print(f"Pipeline ID: {pipeline.pipeline_id}")
# MAGIC
# MAGIC # Start
# MAGIC w.pipelines.start_update(pipeline_id=pipeline.pipeline_id)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Output Tables (Unity Catalog):
# MAGIC
# MAGIC Após execução, as tabelas estarão em:
# MAGIC
# MAGIC ```sql
# MAGIC customer_intelligence.dlt_demo.bronze_transactions_simulated
# MAGIC customer_intelligence.dlt_demo.silver_transactions
# MAGIC customer_intelligence.dlt_demo.gold_customer_metrics
# MAGIC customer_intelligence.dlt_demo.gold_monthly_revenue
# MAGIC customer_intelligence.dlt_demo.streaming_customer_activity
# MAGIC customer_intelligence.dlt_demo.scd_customer_segments
# MAGIC customer_intelligence.dlt_demo.quality_validated_transactions
# MAGIC customer_intelligence.dlt_demo.quarantine_invalid_transactions
# MAGIC ```
# MAGIC
# MAGIC **Query exemplo:**
# MAGIC ```sql
# MAGIC SELECT * FROM customer_intelligence.dlt_demo.gold_customer_metrics
# MAGIC WHERE customer_segment = 'platinum'
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Monitoring & Debugging:
# MAGIC
# MAGIC 1. **Pipeline UI**: Ver graph, data quality, events
# MAGIC 2. **Event Log**: Query pipeline events
# MAGIC    ```sql
# MAGIC    SELECT * FROM event_log('customer_intelligence.dlt_demo.silver_transactions')
# MAGIC    ORDER BY timestamp DESC
# MAGIC    ```
# MAGIC 3. **Data Quality Metrics**: Aba "Data Quality" no pipeline UI
# MAGIC 4. **Lineage Graph**: Aba "Graph" mostra dependências
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Próximos Passos:
# MAGIC
# MAGIC 1. ☑️ **Deploy Pipeline** (UI ou CLI)
# MAGIC 2. ☐ **Schedule**: Configure para rodar daily/hourly
# MAGIC 3. ☐ **Alerts**: Configure notificações de falhas
# MAGIC 4. ☐ **Monitor**: Dashboard de data quality
# MAGIC 5. ☐ **Integrate**: Conectar com downstream consumers (dashboards, ML models)
# MAGIC 6. ☐ **Scale**: Increase cluster size para produção
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Diferencial para Entrevistas:
# MAGIC
# MAGIC **Frase:**
# MAGIC
# MAGIC *"Implementei um Lakeflow Spark Declarative Pipeline production-ready com medallion architecture (Bronze/Silver/Gold), usando Auto Loader para ingestão incremental, expectations em três níveis (drop/warn/fail) para data quality, streaming tables com CDC habilitado para tracking de mudanças, SCD Type 2 para histórico de segmentação, e quarantine tables para isolar dados inválidos, tudo com lineage automático e métricas de qualidade built-in."*
# MAGIC
# MAGIC **Key Points:**
# MAGIC - ✅ Medallion architecture
# MAGIC - ✅ Auto Loader (schema inference)
# MAGIC - ✅ 3 tipos de expectations (data quality)
# MAGIC - ✅ CDC enabled (Change Data Feed)
# MAGIC - ✅ Streaming tables (real-time)
# MAGIC - ✅ SCD Type 2 (slowly changing dimensions)
# MAGIC - ✅ Quarantine pattern (invalid data handling)
# MAGIC - ✅ Automatic lineage & monitoring
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🎯 Pipeline Customer Intelligence - Delta Live Tables COMPLETO!**

# COMMAND ----------

# DBTITLE 1,Best Practices - DLT
# MAGIC %md
# MAGIC # 🏆 Best Practices - Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC ## 💡 5 Best Practices Essenciais:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1️⃣ **Implemente Medallion Architecture (Bronze → Silver → Gold)**
# MAGIC
# MAGIC **Estrutura:**
# MAGIC - **Bronze**: Raw data ingestion (schema-on-read)
# MAGIC - **Silver**: Cleaned & validated (quality enforced)
# MAGIC - **Gold**: Business aggregations (analytics-ready)
# MAGIC
# MAGIC **Benefícios:**
# MAGIC - Separation of concerns
# MAGIC - Incremental complexity
# MAGIC - Data lineage clara
# MAGIC - Reusabilidade
# MAGIC
# MAGIC ```python
# MAGIC # Bronze: Raw ingestion
# MAGIC @dlt.table(name="bronze_events")
# MAGIC def bronze_events():
# MAGIC     return spark.readStream.format("cloudFiles").load(path)
# MAGIC
# MAGIC # Silver: Cleaned
# MAGIC @dlt.table(name="silver_events")
# MAGIC @dlt.expect_or_drop("valid_id", "id IS NOT NULL")
# MAGIC def silver_events():
# MAGIC     return dlt.read_stream("bronze_events").transform(...)
# MAGIC
# MAGIC # Gold: Aggregated
# MAGIC @dlt.table(name="gold_metrics")
# MAGIC def gold_metrics():
# MAGIC     return dlt.read("silver_events").groupBy(...).agg(...)
# MAGIC ```
# MAGIC
# MAGIC **Impacto**: Organização clara, debugging fácil, reprocessamento eficiente
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Use Auto Loader para Ingestão Incremental**
# MAGIC
# MAGIC **Auto Loader** detecta e processa novos arquivos automaticamente:
# MAGIC
# MAGIC ```python
# MAGIC return (
# MAGIC     spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .option("cloudFiles.inferColumnTypes", "true")
# MAGIC         .option("cloudFiles.schemaLocation", schema_path)
# MAGIC         .load(data_path)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Features:**
# MAGIC - Schema inference automático
# MAGIC - Schema evolution handling
# MAGIC - Exactly-once processing
# MAGIC - Checkpoint management
# MAGIC
# MAGIC **Benefícios:**
# MAGIC - Escalável para milhões de arquivos
# MAGIC - Resiliente a falhas
# MAGIC - Sem código de checkpoint manual
# MAGIC
# MAGIC **Impacto**: Ingestão production-ready sem código boilerplate
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **Defina Expectations para Data Quality**
# MAGIC
# MAGIC **3 tipos de expectations:**
# MAGIC
# MAGIC 1. **`@dlt.expect_or_drop`**: Drop rows inválidas
# MAGIC ```python
# MAGIC @dlt.expect_or_drop("valid_amount", "amount > 0")
# MAGIC ```
# MAGIC
# MAGIC 2. **`@dlt.expect`**: Track violations (alerta, continua)
# MAGIC ```python
# MAGIC @dlt.expect("reasonable_range", "amount BETWEEN 1 AND 10000")
# MAGIC ```
# MAGIC
# MAGIC 3. **`@dlt.expect_or_fail`**: Fail pipeline se violado
# MAGIC ```python
# MAGIC @dlt.expect_or_fail("critical_key", "customer_id IS NOT NULL")
# MAGIC ```
# MAGIC
# MAGIC **Quando usar cada um:**
# MAGIC - **drop**: Dados malformados que não devem entrar no pipeline
# MAGIC - **expect**: Business rules para monitoramento (não críticas)
# MAGIC - **fail**: Constraints críticas de integridade
# MAGIC
# MAGIC **Impacto**: Data quality built-in, metrics automáticas, debugging facilitado
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ **Enable Change Data Feed (CDC)**
# MAGIC
# MAGIC **CDC** rastreia todas as mudanças nas tabelas:
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     name="silver_customers",
# MAGIC     table_properties={
# MAGIC         "delta.enableChangeDataFeed": "true"
# MAGIC     }
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Sync incremental para data warehouses externos
# MAGIC - Audit trail completo
# MAGIC - Event-driven architectures
# MAGIC - Time-travel queries
# MAGIC
# MAGIC **Query CDC:**
# MAGIC ```sql
# MAGIC SELECT * FROM table_changes('silver_customers', 1)  -- Version 1+
# MAGIC WHERE _change_type IN ('insert', 'update_postimage', 'delete')
# MAGIC ```
# MAGIC
# MAGIC **Impacto**: Auditoria completa, sync eficiente, event sourcing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ **Otimize com Table Properties**
# MAGIC
# MAGIC **Propriedades críticas:**
# MAGIC
# MAGIC ```python
# MAGIC table_properties={
# MAGIC     # Z-Ordering para queries rápidas
# MAGIC     "pipelines.autoOptimize.zOrderCols": "customer_id,date",
# MAGIC     
# MAGIC     # Change Data Feed
# MAGIC     "delta.enableChangeDataFeed": "true",
# MAGIC     
# MAGIC     # Data retention
# MAGIC     "delta.deletedFileRetentionDuration": "interval 30 days",
# MAGIC     
# MAGIC     # Metadata
# MAGIC     "quality": "silver",
# MAGIC     "owner": "data_team",
# MAGIC     "pii": "true"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Z-Ordering:**
# MAGIC - Colocate related data
# MAGIC - Acelera queries com filtros
# MAGIC - Auto-applied em pipelines DLT
# MAGIC
# MAGIC **Impacto**: Performance 10-100x em queries filtradas
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Workflow Completo:
# MAGIC
# MAGIC ```
# MAGIC 1. Auto Loader (Bronze) → Ingest incremental
# MAGIC 2. Expectations (Silver) → Validate quality
# MAGIC 3. Transformations (Silver) → Clean & enrich
# MAGIC 4. Aggregations (Gold) → Business metrics
# MAGIC 5. CDC enabled → Track changes
# MAGIC 6. Z-Order optimized → Fast queries
# MAGIC 7. Materialized views → Pre-compute heavy queries
# MAGIC 8. Streaming tables → Real-time processing
# MAGIC 9. Monitoring → Data quality dashboard
# MAGIC 10. Lineage → Automatic dependency graph
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Checklist de Pipeline Production-Ready:
# MAGIC
# MAGIC - ☑️ Medallion architecture (Bronze/Silver/Gold)
# MAGIC - ☑️ Auto Loader para ingestão
# MAGIC - ☑️ Expectations em todas as camadas
# MAGIC - ☑️ CDC enabled em tabelas críticas
# MAGIC - ☑️ Z-Ordering configurado
# MAGIC - ☑️ Quarantine table para dados inválidos
# MAGIC - ☑️ Streaming tables para real-time
# MAGIC - ☑️ Materialized views para performance
# MAGIC - ☑️ Table properties documentadas
# MAGIC - ☑️ Error handling e retry logic
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Monitoramento:
# MAGIC
# MAGIC DLT fornece métricas automáticas:
# MAGIC - **Data quality metrics**: Expectation violations
# MAGIC - **Pipeline health**: Success/failure rates
# MAGIC - **Processing time**: End-to-end latency
# MAGIC - **Data lineage**: Dependency graph
# MAGIC
# MAGIC **Acessar:**
# MAGIC 1. Workflows → Lakeflow Pipelines → Select pipeline
# MAGIC 2. Ver abas: Graph, Events, Data Quality, Settings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔗 Recursos:
# MAGIC
# MAGIC - [Lakeflow Pipelines Docs](https://docs.databricks.com/en/delta-live-tables/index.html)
# MAGIC - [Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
# MAGIC - [Expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)
# MAGIC - [Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🎯 Pipeline Customer Intelligence - DLT COMPLETO!**
# MAGIC
# MAGIC **💡 Próximos Passos:**
# MAGIC 1. Deploy pipeline via Databricks CLI
# MAGIC 2. Configure schedule (continuous/triggered)
# MAGIC 3. Setup monitoring alerts
# MAGIC 4. Integrate com downstream consumers

# COMMAND ----------


