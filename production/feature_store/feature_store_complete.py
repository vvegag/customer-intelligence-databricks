# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Título e Overview
# MAGIC %md
# MAGIC # 🎯 Feature Store - Databricks Feature Engineering
# MAGIC
# MAGIC ## Implementação Completa de Feature Store
# MAGIC
# MAGIC Este notebook demonstra o uso completo do **Databricks Feature Store** no projeto Customer Intelligence:
# MAGIC
# MAGIC ### 📋 Conteúdo:
# MAGIC 1. **Offline Feature Tables** - Para treinamento de modelos
# MAGIC 2. **Online Feature Tables** - Para serving de baixa latência
# MAGIC 3. **Point-in-Time Correctness** - Garantir consistência temporal
# MAGIC 4. **On-Demand Features** - Features calculadas em tempo real
# MAGIC 5. **Feature Lineage** - Rastreamento feature → model
# MAGIC
# MAGIC ### 🎯 Objetivo:
# MAGIC Centralizar features reutilizáveis, garantir consistência entre training e serving, e reduzir latência de inference.
# MAGIC
# MAGIC ### 📊 Tabelas Utilizadas:
# MAGIC - Source: `customer_intelligence.gold.customer_features`
# MAGIC - Feature Store: `customer_intelligence.gold.customer_features_fs`
# MAGIC - Online Store: `customer_intelligence.gold.customer_features_online`

# COMMAND ----------

# DBTITLE 1,Point-in-Time Lookup (Training)
# Simular labels para training (exemplo: churn prediction)
print("🎯 Demonstrando Point-in-Time Correctness no Training")
print("=" * 60)

# Criar dataset de labels com timestamps históricos
labels_data = [
    ("C001", "2026-01-15", 1),
    ("C002", "2026-02-20", 0),
    ("C003", "2026-03-10", 1),
    ("C004", "2026-04-05", 0),
    ("C005", "2026-05-12", 1)
]

labels_df = spark.createDataFrame(
    labels_data,
    ["customer_id", "event_timestamp", "is_churned"]
).withColumn("event_timestamp", to_timestamp(col("event_timestamp")))

print(f"\n📖 Labels dataset (exemplo):")
display(labels_df)

# Criar training set com Feature Lookup
print(f"\n🔍 Criando Training Set com Feature Lookup...")

training_set = fe.create_training_set(
    df=labels_df,
    feature_lookups=[
        FeatureLookup(
            table_name=feature_table_name,
            lookup_key="customer_id",
            timestamp_lookup_key="event_timestamp",  # Point-in-time!
            feature_names=["recency_days", "frequency", "monetary_total", 
                          "avg_transaction_value", "clv_estimate"]
        )
    ],
    label="is_churned",
    exclude_columns=["update_timestamp"]
)

# Carregar training data
training_df = training_set.load_df()

print(f"\n✅ Training Set criado com Point-in-Time Correctness!")
print(f"   📊 Features buscadas no timestamp correto (event_timestamp)")
print(f"   🚫 Evita data leakage (features futuras)")
print(f"   ✅ Garante consistência temporal")

print(f"\n📊 Training Data (features + label):")
display(training_df)

# COMMAND ----------

# DBTITLE 1,Online Feature Store (Serving)
# Criar Online Feature Table para serving de baixa latência
online_table_name = f"{catalog}.{schema}.customer_features_online"

print(f"🚀 Configurando Online Feature Store")
print("=" * 60)
print(f"\n🎯 Objetivo: Reduzir latência de inference (<50ms)")
print(f"📊 Online Table: {online_table_name}")

try:
    # Criar online table
    print(f"\n🔧 Criando Online Feature Table...")
    
    online_table_spec = fe.publish_table(
        name=feature_table_name,
        online_store={
            "enabled": True,
            "catalog_name": catalog,
            "schema_name": schema,
            "table_name": "customer_features_online"
        }
    )
    
    print(f"\n✅ Online Feature Store configurado!")
    print(f"\n💡 Benefícios:")
    print(f"   ⚡ Latência: <50ms (vs 500ms+ offline)")
    print(f"   🔄 Sync automático: offline → online")
    print(f"   🎯 Ideal para: Model Serving real-time")
    print(f"   📊 Escala: Milhões de requests/segundo")
    
except Exception as e:
    print(f"\n⚠️ Nota: Online Feature Store requer configuração adicional")
    print(f"   Erro: {str(e)[:200]}")
    print(f"\n📝 Para produção, configure:")
    print(f"   1. Databricks SQL Warehouse")
    print(f"   2. Online Store endpoint")
    print(f"   3. Sync schedule (ex: a cada 5 minutos)")
    
print(f"\n📚 Documentação: https://docs.databricks.com/machine-learning/feature-store/online-tables.html")

# COMMAND ----------

# DBTITLE 1,On-Demand Features
# On-Demand Features: calculadas em tempo real durante inference
print("⚡ On-Demand Features - Cálculo em Tempo Real")
print("=" * 60)

from pyspark.sql.functions import udf, datediff
from datetime import datetime

# Exemplo: calcular "days_since_last_purchase" dinamicamente
@udf(returnType="int")
def calculate_days_since_last_purchase(last_purchase_date):
    """
    Feature on-demand: calcula dias desde última compra
    Executada em tempo real no inference
    """
    if last_purchase_date is None:
        return 9999  # Cliente nunca comprou
    
    # Calcular diferença de dias
    from datetime import datetime
    today = datetime.now()
    delta = (today - last_purchase_date).days
    return delta

print(f"\n✅ Função On-Demand Feature criada!")
print(f"\n💡 Características:")
print(f"   ⚡ Calculada durante inference (não precisa re-train)")
print(f"   🔄 Sempre atualizada (usa timestamp atual)")
print(f"   🎯 Ideal para: features temporais dinâmicas")
print(f"   📊 Exemplos: days_since_X, time_of_day, day_of_week")

# Demonstração
print(f"\n🔍 Demonstração:")
sample_df = spark.table(f"{catalog}.{schema}.customer_features").limit(5)

# Assumindo que existe uma coluna last_purchase_date
if "last_purchase_date" in sample_df.columns:
    sample_with_ondemand = sample_df.withColumn(
        "days_since_last_purchase",
        datediff(current_date(), col("last_purchase_date"))
    )
    display(sample_with_ondemand.select(
        "customer_id", "last_purchase_date", "days_since_last_purchase"
    ))
else:
    print(f"   📝 Exemplo conceitual - adicionar last_purchase_date na prática")

print(f"\n📚 Integração: Use no Model Serving via custom PyFunc model")

# COMMAND ----------

# DBTITLE 1,Feature Lineage
# Feature Lineage: rastrear feature → model
print("🔍 Feature Lineage - Rastreamento Feature → Model")
print("=" * 60)

print(f"\n🎯 Objetivo: Rastrear quais features cada modelo usa")
print(f"\n💡 Benefícios:")
print(f"   📊 Governança: saber impacto de mudanças em features")
print(f"   🔍 Debug: identificar features problemáticas")
print(f"   📝 Documentação: automática e sempre atualizada")
print(f"   ⚙️ Reprodução: versionar features + modelos juntos")

# Exemplo: treinar modelo simples e logar com feature lineage
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import pandas as pd

print(f"\n🧪 Exemplo: Training modelo com Feature Lineage...")

# Usar o training_set criado anteriormente
training_pd = training_df.toPandas()

if len(training_pd) > 0:
    # Preparar X, y
    feature_cols = ["recency_days", "frequency", "monetary_total", 
                   "avg_transaction_value", "clv_estimate"]
    X = training_pd[feature_cols].fillna(0)
    y = training_pd["is_churned"]
    
    # Train modelo
    model = RandomForestClassifier(n_estimators=10, max_depth=3, random_state=42)
    model.fit(X, y)
    
    print(f"\n✅ Modelo treinado!")
    
    # Log model com Feature Store
    with mlflow.start_run(run_name="churn_with_feature_store") as run:
        
        # Log model usando Feature Engineering Client (automático lineage!)
        fe.log_model(
            model=model,
            artifact_path="model",
            flavor=mlflow.sklearn,
            training_set=training_set,  # Aqui está o lineage!
            registered_model_name=f"{catalog}.{schema}.churn_model_fs"
        )
        
        print(f"\n✅ Modelo logado com Feature Lineage!")
        print(f"   🎯 MLflow Run ID: {run.info.run_id}")
        print(f"   🔗 Feature Lineage: automático via training_set")
        print(f"   📊 Features rastreadas: {', '.join(feature_cols)}")
        print(f"\n📚 Ver no MLflow UI: Experiments → Models → Feature Dependencies")
else:
    print(f"\n📝 Exemplo conceitual - usar dados reais em produção")

print(f"\n✅ Feature Lineage configurado! Agora você pode:")
print(f"   1️⃣ Ver quais modelos usam cada feature")
print(f"   2️⃣ Avaliar impacto de mudanças em features")
print(f"   3️⃣ Reproduzir resultados com versões exatas")

# COMMAND ----------

# DBTITLE 1,Best Practices
# MAGIC %md
# MAGIC # 🏆 Best Practices - Databricks Feature Store
# MAGIC
# MAGIC ## 💡 5 Best Practices Essenciais:
# MAGIC
# MAGIC ### 1️⃣ **Use Primary Keys e Timestamp Keys Corretas**
# MAGIC - **Primary Key**: Identificador único da entidade (ex: `customer_id`)
# MAGIC - **Timestamp Key**: Para point-in-time correctness (ex: `update_timestamp`)
# MAGIC - **Impacto**: Evita data leakage no training
# MAGIC
# MAGIC ```python
# MAGIC fe.create_table(
# MAGIC     name="features_table",
# MAGIC     primary_keys=["customer_id"],
# MAGIC     timestamp_keys=["update_timestamp"],  # CRUCIAL!
# MAGIC     df=features_df
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Separe Offline (Training) e Online (Serving)**
# MAGIC - **Offline**: Dados históricos completos para training
# MAGIC - **Online**: Snapshot atual para inference de baixa latência (<50ms)
# MAGIC - **Sync**: Configure sync automático offline → online
# MAGIC - **Impacto**: Melhor performance e custos otimizados
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **Documente Features Exaustivamente**
# MAGIC - **Description**: O que a feature representa
# MAGIC - **Unit**: Unidade de medida (dias, reais, etc)
# MAGIC - **Null handling**: Como tratar valores ausentes
# MAGIC - **Update frequency**: Com que frequência atualiza
# MAGIC - **Impacto**: Facilita reuso e evita retrabalho
# MAGIC
# MAGIC ```python
# MAGIC fe.create_table(
# MAGIC     name="features",
# MAGIC     ...,
# MAGIC     description="RFM features: recency (dias), frequency (count), monetary (R$)"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ **Versione Features com Timestamps**
# MAGIC - Sempre inclua `update_timestamp` em cada registro
# MAGIC - Permite rastrear evolução de features ao longo do tempo
# MAGIC - Essencial para point-in-time correctness
# MAGIC - **Impacto**: Reprodução de experimentos e debugging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ **Monitore Feature Quality**
# MAGIC - **Null rate**: % de valores ausentes
# MAGIC - **Distribution shifts**: Detecção de drift
# MAGIC - **Outliers**: Valores fora do esperado
# MAGIC - **Freshness**: Última atualização
# MAGIC - **Impacto**: Modelos mais robustos e confiáveis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Próximos Passos:
# MAGIC
# MAGIC 1. **Integrar com Model Serving**: Use features no endpoint de produção
# MAGIC 2. **Configurar Online Store**: Para latência <50ms
# MAGIC 3. **Adicionar On-Demand Features**: Features calculadas em tempo real
# MAGIC 4. **Monitorar Lineage**: Rastrear feature → model dependencies
# MAGIC 5. **Automatizar Updates**: Jobs agendados para atualizar features
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Recursos:
# MAGIC
# MAGIC - [Databricks Feature Store Docs](https://docs.databricks.com/machine-learning/feature-store/)
# MAGIC - [Online Feature Store](https://docs.databricks.com/machine-learning/feature-store/online-tables.html)
# MAGIC - [Feature Engineering Best Practices](https://www.databricks.com/blog/2022/10/20/best-practices-feature-engineering.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Checklist de Implementação:
# MAGIC
# MAGIC - ☑️ Offline Feature Table criada
# MAGIC - ☑️ Point-in-time correctness implementado
# MAGIC - ☑️ Training set com Feature Lookup
# MAGIC - ☑️ Online Feature Store configurado (ou planejado)
# MAGIC - ☑️ On-Demand Features examples
# MAGIC - ☑️ Feature Lineage com MLflow
# MAGIC - ☑️ Documentação completa
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🎯 Projeto Customer Intelligence - Feature Store COMPLETO!**

# COMMAND ----------

# DBTITLE 1,Setup e Instalação
# Install Databricks Feature Engineering client
# MAGIC %pip install databricks-feature-engineering --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

print("✅ Installation complete!")

# COMMAND ----------

# DBTITLE 1,Imports
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import mlflow

print("✅ Imports carregados com sucesso!")
print(f"📅 Timestamp: {datetime.now()}")

# COMMAND ----------

# DBTITLE 1,Initialize Feature Store Client
# Initialize Feature Engineering Client
fe = FeatureEngineeringClient()

# Define catalog and schema
catalog = "customer_intelligence"
schema = "gold"

print("✅ Feature Engineering Client inicializado!")
print(f"📊 Catalog: {catalog}")
print(f"📂 Schema: {schema}")
print(f"🎯 Feature Store pronto para uso!")

# COMMAND ----------

# DBTITLE 1,Criar Customer Features (Offline Table)
# Ler tabela de features existente
source_table = f"{catalog}.{schema}.customer_features"

print(f"📖 Lendo features de: {source_table}")
customer_features_df = spark.table(source_table)

# Adicionar timestamp para point-in-time correctness
customer_features_df = customer_features_df.withColumn(
    "update_timestamp", 
    current_timestamp()
)

# Selecionar features para Feature Store
feature_columns = [
    "customer_id",
    "update_timestamp",
    "recency_days",
    "frequency",
    "monetary_total",
    "avg_transaction_value",
    "clv_estimate",
    "segment"
]

features_to_store = customer_features_df.select(feature_columns)

print(f"\n✅ Features preparadas para Feature Store:")
print(f"   📊 Total de customers: {features_to_store.count():,}")
print(f"   📋 Features selecionadas: {len(feature_columns)-2}")  # -2 para excluir customer_id e timestamp
print(f"\n📊 Schema:")
features_to_store.printSchema()

print(f"\n🔍 Sample de 5 registros:")
display(features_to_store.limit(5))

# COMMAND ----------

# DBTITLE 1,Write Features to Feature Store
# Nome da Feature Table
feature_table_name = f"{catalog}.{schema}.customer_features_fs"

print(f"📝 Criando/Atualizando Feature Table: {feature_table_name}")

try:
    # Criar Feature Table (ou sobrescrever se já existe)
    fe.create_table(
        name=feature_table_name,
        primary_keys=["customer_id"],
        timestamp_keys=["update_timestamp"],
        df=features_to_store,
        description="Customer features para modelos de churn, propensity e segmentation"
    )
    print(f"✅ Feature Table criada com sucesso!")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"⚠️ Feature Table já existe. Atualizando com novos dados...")
        # Write/Overwrite features
        fe.write_table(
            name=feature_table_name,
            df=features_to_store,
            mode="merge"  # ou "overwrite" para substituir tudo
        )
        print(f"✅ Features atualizadas com sucesso!")
    else:
        raise e

print(f"\n🎯 Feature Store Status:")
print(f"   ✅ Offline Table: {feature_table_name}")
print(f"   📊 Features disponíveis para training")
print(f"   🔍 Point-in-time correctness habilitado")

# Verificar conteúdo
print(f"\n📊 Conteúdo da Feature Table:")
feature_table_df = spark.table(feature_table_name)
print(f"   Total de registros: {feature_table_df.count():,}")
display(feature_table_df.limit(10))
