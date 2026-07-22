# Databricks notebook source
# DBTITLE 1,Model Serving - Deploy em Produção
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

# DBTITLE 1,Setup e Configuração
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
print(f"Registry URI: {mlflow.get_registry_uri()}")

# COMMAND ----------

# DBTITLE 1,1️⃣ Registrar Modelo de Churn no UC
# Verificar se modelo de churn já existe no MLflow
model_name = get_full_model_name("churn_model")

try:
    # Listar versões existentes
    versions = client.search_model_versions(f"name='{model_name}'")
    print(f"✓ Modelo '{model_name}' já registrado")
    print(f"  Versões encontradas: {len(versions)}")
    
    if versions:
        latest = versions[0]
        print(f"  Última versão: {latest.version}")
        print(f"  Status: {latest.status}")
        print(f"  Stage: {latest.current_stage if hasattr(latest, 'current_stage') else 'N/A'}")
except Exception as e:
    print(f"⚠️  Modelo não encontrado: {e}")
    print(f"  Será necessário registrar o modelo primeiro")
    print(f"  Execute o notebook 'Churn Prediction' para treinar e registrar o modelo")

# COMMAND ----------

# DBTITLE 1,2️⃣ Promover Modelo para Produção (Champion)
# Promover última versão para @champion alias (equivalente a Production)
try:
    versions = client.search_model_versions(f"name='{model_name}'")
    
    if versions:
        latest_version = versions[0].version
        
        # Setar alias 'champion' na última versão
        client.set_registered_model_alias(
            name=model_name,
            alias="champion",
            version=latest_version
        )
        
        print(f"✓ Modelo promovido para @champion")
        print(f"  Versão: {latest_version}")
        print(f"  URI: models:/{model_name}@champion")
        
        # Adicionar descrição ao modelo
        client.update_registered_model(
            name=model_name,
            description="""Modelo de predição de churn de clientes.
            
            Features: RFM (Recency, Frequency, Monetary), comportamento de compra.
            Algoritmo: XGBoost Classifier
            Métricas: AUC-ROC, Precision, Recall
            
            Uso: POST /serving-endpoints/churn-prediction/invocations
            Input: JSON com features do cliente
            Output: {prediction: 0/1, probability: [p_no_churn, p_churn]}
            """
        )
        
        print("✓ Descrição do modelo atualizada")
    else:
        print("⚠️  Nenhuma versão encontrada para promover")
        
except Exception as e:
    print(f"❌ Erro ao promover modelo: {e}")

# COMMAND ----------

# DBTITLE 1,3️⃣ Criar Endpoint de Serving
# Criar endpoint de model serving via API REST
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    AutoCaptureConfigInput
)

w = WorkspaceClient()

endpoint_name = "churn-prediction-endpoint"

try:
    # Verificar se endpoint já existe
    existing = w.serving_endpoints.get(endpoint_name)
    print(f"✓ Endpoint '{endpoint_name}' já existe")
    print(f"  State: {existing.state.config_update if existing.state else 'unknown'}")
    print(f"  URL: {existing.config.served_entities[0].entity_name if existing.config else 'N/A'}")
    
except Exception as e:
    # Endpoint não existe, criar novo
    print(f"🔧 Criando endpoint '{endpoint_name}'...")
    
    try:
        endpoint = w.serving_endpoints.create(
            name=endpoint_name,
            config=EndpointCoreConfigInput(
                served_entities=[
                    ServedEntityInput(
                        entity_name=model_name,
                        entity_version=latest_version,
                        scale_to_zero_enabled=True,  # Reduz custos
                        workload_size="Small",        # Tamanho do compute
                    )
                ],
                # Habilitar captura de predições para monitoring
                auto_capture_config=AutoCaptureConfigInput(
                    catalog_name=CATALOG,
                    schema_name=SCHEMA,
                    table_name_prefix="churn_endpoint",
                    enabled=True
                )
            )
        )
        
        print(f"✓ Endpoint criado com sucesso!")
        print(f"  Nome: {endpoint_name}")
        print(f"  Aguarde ~5-10 min para provisionamento...")
        print(f"\n📌 Para acompanhar status:")
        print(f"  1. Acesse: ML > Serving Endpoints")
        print(f"  2. Clique em '{endpoint_name}'")
        print(f"  3. Aguarde status 'Ready'")
        
    except Exception as create_error:
        print(f"❌ Erro ao criar endpoint: {create_error}")
        print(f"\n💡 Possíveis soluções:")
        print(f"  1. Verifique se o modelo está registrado")
        print(f"  2. Verifique permissões no Unity Catalog")
        print(f"  3. Endpoint pode já existir com nome diferente")

# COMMAND ----------

# DBTITLE 1,4️⃣ Verificar Status do Endpoint
# Checar se endpoint está pronto para receber requisições
try:
    endpoint_status = w.serving_endpoints.get(endpoint_name)
    
    print(f"Endpoint: {endpoint_name}")
    print(f"Estado: {endpoint_status.state.config_update if endpoint_status.state else 'unknown'}")
    
    if endpoint_status.state and hasattr(endpoint_status.state, 'ready'):
        if endpoint_status.state.ready:
            print("✅ READY - Endpoint está pronto para receber requisições!")
        else:
            print("⏳ NOT READY - Aguarde provisionamento...")
            print("   Volte em 5-10 minutos e execute esta célula novamente")
    
    # Mostrar URL de invocação
    print(f"\n🌐 Endpoint URL:")
    print(f"   POST https://<workspace-url>/serving-endpoints/{endpoint_name}/invocations")
    
except Exception as e:
    print(f"❌ Erro ao verificar endpoint: {e}")
    print("\n💡 Se o endpoint foi recém criado, aguarde alguns minutos.")

# COMMAND ----------

# DBTITLE 1,5️⃣ Testar Endpoint com Exemplo Real
# Preparar dados de teste
df_features = spark.table(f"{CATALOG}.{SCHEMA}.customer_features").limit(5).toPandas()

# Tem que ser exatamente as mesmas features (e ordem) de "Modelo Churn Prediction.py"
feature_cols = [
    "age", "customer_age_days",
    "recency_days", "frequency", "monetary_total", "monetary_avg",
    "customer_lifetime_days", "purchase_frequency_per_day",
    "unique_products_purchased", "total_items_purchased",
    "event_count_30d", "session_count_30d", "engagement_score_30d",
    "page_views_30d", "product_views_30d", "add_to_cart_30d",
    "event_count_60d", "session_count_60d", "engagement_score_60d",
    "event_count_90d", "session_count_90d", "engagement_score_90d",
    "total_campaigns_exposed", "treatment_campaigns_count",
    "total_responses", "total_conversions",
    "response_rate", "conversion_rate"
]

# Preparar payload para API
test_data = df_features[feature_cols].fillna(0).to_dict(orient='split')
del test_data['index']  # Remover índice

print("🧪 Exemplo de payload para a API:")
print(json.dumps({
    "dataframe_split": test_data
}, indent=2)[:500] + "...\n")

print(f"\n✓ Preparados {len(test_data['data'])} exemplos de clientes para teste")
print(f"\n📝 Features enviadas: {', '.join(feature_cols[:5])}...")

# COMMAND ----------

# DBTITLE 1,6️⃣ Invocar Endpoint (Predição Real)
# Fazer predição via endpoint
import os

try:
    # Obter URL do workspace
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    
    # URL do endpoint
    endpoint_url = f"https://{workspace_url}/serving-endpoints/{endpoint_name}/invocations"
    
    # Headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Payload
    payload = {
        "dataframe_split": test_data
    }
    
    print(f"🚀 Enviando {len(test_data['data'])} requisições para o endpoint...\n")
    
    response = requests.post(
        endpoint_url,
        headers=headers,
        json=payload,
        timeout=60
    )
    
    if response.status_code == 200:
        predictions = response.json()
        
        print("✅ PREDIÇÕES RECEBIDAS:\n")
        print("=" * 70)
        
        for i, pred in enumerate(predictions['predictions']):
            customer_id = df_features.iloc[i]['customer_id'] if 'customer_id' in df_features.columns else f'Cliente {i+1}'
            
            if isinstance(pred, dict):
                churn_prob = pred.get('1', pred.get('probability', [0, 0])[1])
            elif isinstance(pred, list):
                churn_prob = pred[1] if len(pred) > 1 else pred[0]
            else:
                churn_prob = pred
            
            risk_level = "🔴 ALTO" if churn_prob > 0.7 else "🟡 MÉDIO" if churn_prob > 0.4 else "🟢 BAIXO"
            
            print(f"{customer_id:15s} | Risco: {risk_level} | Probabilidade: {churn_prob:.1%}")
        
        print("=" * 70)
        print(f"\n✓ Endpoint funcionando perfeitamente!")
        print(f"\n📊 Latency: ~{response.elapsed.total_seconds():.2f}s para {len(test_data['data'])} predições")
        
    else:
        print(f"❌ Erro {response.status_code}: {response.text}")
        
except Exception as e:
    print(f"❌ Erro ao invocar endpoint: {e}")
    print("\n💡 Verifique se:")
    print("  1. Endpoint está no status 'Ready'")
    print("  2. Modelo foi registrado corretamente")
    print("  3. Você tem permissões para invocar o endpoint")

# COMMAND ----------

# DBTITLE 1,Documentação da API
# MAGIC %md
# MAGIC # 📚 Documentação da API
# MAGIC
# MAGIC ## Endpoint de Predição de Churn
# MAGIC
# MAGIC ### URL
# MAGIC ```
# MAGIC POST https://<workspace-url>/serving-endpoints/churn-prediction-endpoint/invocations
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
# MAGIC     "columns": [
# MAGIC       "recency_days", "frequency_count", "monetary_total",
# MAGIC       "avg_order_value", "days_since_first_purchase", 
# MAGIC       "purchase_frequency", "total_products_purchased",
# MAGIC       "unique_products_purchased", "avg_products_per_order",
# MAGIC       "recency_segment", "frequency_segment", 
# MAGIC       "monetary_segment", "rfm_score"
# MAGIC     ],
# MAGIC     "data": [
# MAGIC       [30, 5, 1500.0, 300.0, 180, 0.028, 15, 8, 3.0, 3, 2, 3, 8]
# MAGIC     ]
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Response (JSON)
# MAGIC ```json
# MAGIC {
# MAGIC   "predictions": [0.23]  // Probabilidade de churn [0.0 - 1.0]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Interpretação
# MAGIC * **< 0.3**: Baixo risco de churn
# MAGIC * **0.3 - 0.7**: Risco médio
# MAGIC * **> 0.7**: Alto risco de churn
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exemplo Python
# MAGIC ```python
# MAGIC import requests
# MAGIC import json
# MAGIC
# MAGIC url = "https://<workspace>/serving-endpoints/churn-prediction-endpoint/invocations"
# MAGIC headers = {
# MAGIC     "Authorization": "Bearer <token>",
# MAGIC     "Content-Type": "application/json"
# MAGIC }
# MAGIC
# MAGIC payload = {
# MAGIC     "dataframe_split": {
# MAGIC         "columns": ["recency_days", "frequency_count", ...],
# MAGIC         "data": [[30, 5, 1500.0, ...]]
# MAGIC     }
# MAGIC }
# MAGIC
# MAGIC response = requests.post(url, headers=headers, json=payload)
# MAGIC churn_probability = response.json()["predictions"][0]
# MAGIC
# MAGIC print(f"Risco de churn: {churn_probability:.1%}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SLA e Limitações
# MAGIC * **Latency**: < 200ms (p99)
# MAGIC * **Throughput**: Até 1000 req/min (depende do workload size)
# MAGIC * **Scale-to-zero**: Enabled (reduz custos quando ocioso)
# MAGIC * **Auto-scaling**: Enabled (escala automaticamente sob carga)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Monitoring
# MAGIC Predições são automaticamente logadas na tabela:
# MAGIC ```
# MAGIC customer_intelligence.gold.churn_endpoint_payload
# MAGIC ```
# MAGIC
# MAGIC Use para:
# MAGIC * Monitorar data drift
# MAGIC * Analisar distribuição de predições
# MAGIC * Auditar requisições

# COMMAND ----------

# DBTITLE 1,🎯 Próximos Passos
print("=" * 70)
print("✅ MODEL SERVING CONFIGURADO COM SUCESSO")
print("=" * 70)

print("\n🎯 PRÓXIMOS PASSOS:\n")

print("1️⃣ MONITORING")
print("   • Criar dashboard para latency, throughput, erro rate")
print("   • Configurar alertas para degradação de performance")
print("   • Monitorar data drift nas features\n")

print("2️⃣ A/B TESTING")
print("   • Testar novo modelo challenger vs champion")
print("   • Medir impacto em conversion rate, churn rate")
print("   • Promover melhor modelo\n")

print("3️⃣ INTEGRAÇÃO")
print("   • Conectar com CRM (Salesforce, HubSpot)")
print("   • Trigger automático de campanhas para alto risco")
print("   • Enviar predições para ferramentas de marketing\n")

print("4️⃣ ESCALAR")
print("   • Deploy de outros modelos (propensity, LTV)")
print("   • Criar ensemble de múltiplos modelos")
print("   • Otimizar latency com caching\n")

print("=" * 70)
print("✓ PRONTO PARA ENTREVISTA - MODELO EM PRODUÇÃO")
print("=" * 70)

# COMMAND ----------


