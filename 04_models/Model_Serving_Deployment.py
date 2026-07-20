# Databricks notebook source
# MAGIC %md
# MAGIC # Model Serving - Deploy em Produção 🚀
# MAGIC 
# MAGIC ## Objetivo
# MAGIC Deployar modelos de ML como **endpoints REST** para predições em tempo real.
# MAGIC 
# MAGIC ### Modelos a Deployar
# MAGIC 1. **Churn Prediction** - Prever risco de churn de clientes
# MAGIC 2. **Propensity Score** - Prever probabilidade de compra
# MAGIC 
# MAGIC ### Arquitetura
# MAGIC ```
# MAGIC Cliente/Aplicação
# MAGIC     ↓ (HTTP POST)
# MAGIC Databricks Model Serving Endpoint
# MAGIC     ↓ (invoca)
# MAGIC Modelo MLflow (Unity Catalog)
# MAGIC     ↓ (retorna)
# MAGIC Predições JSON
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diferencial para a Vaga
# MAGIC ✅ **MLOps**: Modelos em produção, não apenas notebooks  
# MAGIC ✅ **Escalabilidade**: Endpoints servem milhares de requisições/min  
# MAGIC ✅ **API-first**: Integração com sistemas externos  
# MAGIC ✅ **Monitoring**: Latency, throughput, drift detection

# COMMAND ----------

# Setup e Configuração
import mlflow
from mlflow.tracking import MlflowClient
import pandas as pd
import json
import requests
from pyspark.sql import functions as F

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

CATALOG = "customer_intelligence"
SCHEMA = "gold"

def get_full_model_name(model_name):
    return f"{CATALOG}.{SCHEMA}.{model_name}"

print("✓ Setup completo")

# COMMAND ----------

# 1️⃣ Verificar Modelo de Churn
model_name = get_full_model_name("churn_model")

try:
    versions = client.search_model_versions(f"name='{model_name}'")
    print(f"✓ Modelo '{model_name}' já registrado")
    print(f"  Versões encontradas: {len(versions)}")
    
    if versions:
        latest = versions[0]
        print(f"  Última versão: {latest.version}")
except Exception as e:
    print(f"⚠️ Modelo não encontrado: {e}")
    print("  Execute o notebook 'Churn Prediction' primeiro")

# COMMAND ----------

# 2️⃣ Promover Modelo para Produção (@champion)
try:
    versions = client.search_model_versions(f"name='{model_name}'")
    
    if versions:
        latest_version = versions[0].version
        
        client.set_registered_model_alias(
            name=model_name,
            alias="champion",
            version=latest_version
        )
        
        print(f"✓ Modelo promovido para @champion")
        print(f"  Versão: {latest_version}")
        
        # Adicionar descrição
        client.update_registered_model(
            name=model_name,
            description="""Modelo de predição de churn de clientes.
            
            Features: RFM, comportamento de compra
            Algoritmo: XGBoost Classifier
            Métricas: AUC-ROC, Precision, Recall
            
            Uso: POST /serving-endpoints/churn-prediction/invocations
            """
        )
        
        print("✓ Descrição do modelo atualizada")
except Exception as e:
    print(f"❌ Erro: {e}")

# COMMAND ----------

# 3️⃣ Criar Endpoint de Serving
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    AutoCaptureConfigInput
)

w = WorkspaceClient()
endpoint_name = "churn-prediction-endpoint"

try:
    existing = w.serving_endpoints.get(endpoint_name)
    print(f"✓ Endpoint '{endpoint_name}' já existe")
    print(f"  State: {existing.state}")
    
except Exception as e:
    print(f"🔧 Criando endpoint '{endpoint_name}'...")
    
    try:
        endpoint = w.serving_endpoints.create(
            name=endpoint_name,
            config=EndpointCoreConfigInput(
                served_entities=[
                    ServedEntityInput(
                        entity_name=model_name,
                        entity_version=latest_version,
                        scale_to_zero_enabled=True,
                        workload_size="Small",
                    )
                ],
                auto_capture_config=AutoCaptureConfigInput(
                    catalog_name=CATALOG,
                    schema_name=SCHEMA,
                    table_name_prefix="churn_endpoint",
                    enabled=True
                )
            )
        )
        
        print(f"✓ Endpoint criado!")
        print(f"  Aguarde ~5-10 min para provisionamento")
        
    except Exception as create_error:
        print(f"❌ Erro ao criar endpoint: {create_error}")

# COMMAND ----------

# 4️⃣ Verificar Status do Endpoint
try:
    endpoint_status = w.serving_endpoints.get(endpoint_name)
    
    print(f"Endpoint: {endpoint_name}")
    print(f"Estado: {endpoint_status.state}")
    
    if endpoint_status.state and hasattr(endpoint_status.state, 'ready'):
        if endpoint_status.state.ready:
            print("✅ READY - Endpoint pronto!")
        else:
            print("⏳ NOT READY - Aguarde provisionamento...")
    
except Exception as e:
    print(f"❌ Erro: {e}")

# COMMAND ----------

# 5️⃣ Testar Endpoint
df_features = spark.table(f"{CATALOG}.{SCHEMA}.customer_features").limit(5).toPandas()

feature_cols = [
    'recency_days', 'frequency_count', 'monetary_total',
    'avg_order_value', 'days_since_first_purchase', 'purchase_frequency',
    'total_products_purchased', 'unique_products_purchased',
    'avg_products_per_order', 'recency_segment', 'frequency_segment',
    'monetary_segment', 'rfm_score'
]

test_data = df_features[feature_cols].fillna(0).to_dict(orient='split')
del test_data['index']

print(f"✓ Preparados {len(test_data['data'])} exemplos")

# COMMAND ----------

# 6️⃣ Invocar Endpoint (Predição Real)
try:
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    
    endpoint_url = f"https://{workspace_url}/serving-endpoints/{endpoint_name}/invocations"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {"dataframe_split": test_data}
    
    print(f"🚀 Enviando requisições...")
    
    response = requests.post(endpoint_url, headers=headers, json=payload, timeout=60)
    
    if response.status_code == 200:
        predictions = response.json()
        
        print("\n✅ PREDIÇÕES RECEBIDAS:")
        print("=" * 70)
        
        for i, pred in enumerate(predictions['predictions']):
            churn_prob = pred[1] if isinstance(pred, list) else pred
            risk_level = "🔴 ALTO" if churn_prob > 0.7 else "🟡 MÉDIO" if churn_prob > 0.4 else "🟢 BAIXO"
            print(f"Cliente {i+1:2d} | Risco: {risk_level} | Probabilidade: {churn_prob:.1%}")
        
        print("=" * 70)
        print(f"✓ Endpoint funcionando!")
        print(f"📊 Latency: ~{response.elapsed.total_seconds():.2f}s")
    else:
        print(f"❌ Erro {response.status_code}: {response.text}")
        
except Exception as e:
    print(f"❌ Erro: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 📚 Documentação da API
# MAGIC 
# MAGIC ## Endpoint de Predição de Churn
# MAGIC 
# MAGIC ### URL
# MAGIC ```
# MAGIC POST https://<workspace>/serving-endpoints/churn-prediction-endpoint/invocations
# MAGIC ```
# MAGIC 
# MAGIC ### Autenticação
# MAGIC ```
# MAGIC Authorization: Bearer <databricks-token>
# MAGIC ```
# MAGIC 
# MAGIC ### Request Body (JSON)
# MAGIC ```json
# MAGIC {
# MAGIC   "dataframe_split": {
# MAGIC     "columns": ["recency_days", "frequency_count", ...],
# MAGIC     "data": [[30, 5, 1500.0, ...]]
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC ### Response
# MAGIC ```json
# MAGIC {
# MAGIC   "predictions": [0.23]
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC ### Interpretação
# MAGIC * **< 0.3**: Baixo risco
# MAGIC * **0.3 - 0.7**: Risco médio
# MAGIC * **> 0.7**: Alto risco

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Próximos Passos
# MAGIC 
# MAGIC 1. **Monitoring** - Dashboard de latency, throughput, errors
# MAGIC 2. **A/B Testing** - Testar challenger vs champion
# MAGIC 3. **Integração** - Conectar com CRM (Salesforce, HubSpot)
# MAGIC 4. **Escalar** - Deploy de outros modelos (propensity, LTV)

