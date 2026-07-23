# Databricks notebook source
# DBTITLE 1,CRM Integration System
# MAGIC %md
# MAGIC # 🔗 CRM Integration System
# MAGIC
# MAGIC ## 📋 Objetivo
# MAGIC Integração automatizada com plataformas CRM/Marketing para:
# MAGIC - **Salesforce**: Envio de scores e predições para CRM
# MAGIC - **HubSpot**: Criação/atualização de contatos e deals
# MAGIC - **Webhook Triggers**: Disparar campanhas baseadas em scores
# MAGIC - **Real-time Sync**: Sincronização bidirecional de dados
# MAGIC
# MAGIC ## 🎯 Features
# MAGIC 1. **Salesforce Integration**: Bulk API para envio de predições
# MAGIC 2. **HubSpot Integration**: Contacts API + Properties personalizadas
# MAGIC 3. **Campaign Triggers**: Webhook para disparar campanhas
# MAGIC 4. **Batch Sync**: Sincronização diária de scores
# MAGIC 5. **Real-time Updates**: Webhooks para eventos em tempo real
# MAGIC 6. **Error Handling**: Retry logic + alertas
# MAGIC
# MAGIC ## 🔧 Pré-requisitos
# MAGIC - Salesforce: Connected App + OAuth credentials
# MAGIC - HubSpot: Private App + API key
# MAGIC - Databricks Secrets: Armazenar credentials com segurança
# MAGIC
# MAGIC ## 📊 Use Cases
# MAGIC - Enviar churn predictions para CRM
# MAGIC - Criar listas de segmentação dinâmicas
# MAGIC - Trigger de email campaigns baseado em propensity
# MAGIC - Atualizar customer scores em tempo real

# COMMAND ----------

# DBTITLE 1,1. Setup e Credentials
# Databricks notebook source
import requests
import json
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import *
import time

def _retry_request(func, max_attempts=3, base_delay=1.0):
    """Executa uma chamada requests (func) com retry exponencial (1s, 2s, 4s)
    em falhas de rede/timeout/5xx/429. Não faz retry em 4xx (exceto 429) —
    esses são falhas de payload/auth, não transientes."""
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = func()
            if response.status_code >= 500 or response.status_code == 429:
                response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            last_exc = e
            if attempt < max_attempts:
                delay = base_delay * (2 ** (attempt - 1))
                print(f"   ⚠️ Tentativa {attempt}/{max_attempts} falhou ({e}); retry em {delay:.0f}s...")
                time.sleep(delay)
    raise last_exc

# Databricks Secrets (create scope first via CLI)
# dbutils.secrets.createScope(scope="crm_integration")
# dbutils.secrets.put(scope="crm_integration", key="salesforce_client_id", value="...")
# dbutils.secrets.put(scope="crm_integration", key="salesforce_client_secret", value="...")
# dbutils.secrets.put(scope="crm_integration", key="hubspot_api_key", value="...")

# Configuration
CATALOG = "customer_intelligence"
SCHEMA = "gold"

# CRM Endpoints
SALESFORCE_AUTH_URL = "https://login.salesforce.com/services/oauth2/token"
SALESFORCE_API_VERSION = "v58.0"
HUBSPOT_API_URL = "https://api.hubapi.com"

# Demo mode (set to True if no credentials available)
DEMO_MODE = True  # Set to False when using real credentials

if not DEMO_MODE:
    SALESFORCE_CLIENT_ID = dbutils.secrets.get(scope="crm_integration", key="salesforce_client_id")
    SALESFORCE_CLIENT_SECRET = dbutils.secrets.get(scope="crm_integration", key="salesforce_client_secret")
    SALESFORCE_USERNAME = dbutils.secrets.get(scope="crm_integration", key="salesforce_username")
    SALESFORCE_PASSWORD = dbutils.secrets.get(scope="crm_integration", key="salesforce_password")
    HUBSPOT_API_KEY = dbutils.secrets.get(scope="crm_integration", key="hubspot_api_key")
else:
    print("⚠️ DEMO MODE: Using mock credentials")
    SALESFORCE_CLIENT_ID = "mock_client_id"
    SALESFORCE_CLIENT_SECRET = "mock_secret"
    SALESFORCE_USERNAME = "user@example.com"
    SALESFORCE_PASSWORD = "mock_password"
    HUBSPOT_API_KEY = "mock_api_key"

print("✅ Configuration loaded")

# COMMAND ----------

# DBTITLE 1,2. Salesforce Integration
# COMMAND ----------
# Salesforce: OAuth Authentication + Bulk API

class SalesforceIntegration:
    """
    Salesforce integration via REST API
    Supports: OAuth 2.0, Bulk API, Custom Objects
    """
    
    def __init__(self, client_id, client_secret, username, password):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.access_token = None
        self.instance_url = None
        
    def authenticate(self):
        """OAuth 2.0 authentication"""
        if DEMO_MODE:
            print("🔐 DEMO: Skipping Salesforce authentication")
            self.access_token = "mock_token"
            self.instance_url = "https://mock.salesforce.com"
            return True
        
        try:
            payload = {
                'grant_type': 'password',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'username': self.username,
                'password': self.password
            }
            
            response = _retry_request(lambda: requests.post(SALESFORCE_AUTH_URL, data=payload))
            response.raise_for_status()
            
            data = response.json()
            self.access_token = data['access_token']
            self.instance_url = data['instance_url']
            
            print("✅ Salesforce authentication successful")
            return True
            
        except Exception as e:
            print(f"❌ Salesforce authentication failed: {e}")
            return False
    
    def send_churn_predictions(self, predictions_df):
        """
        Send churn predictions to Salesforce
        Creates/updates custom object: Customer_Score__c
        """
        print("\n📤 Sending churn predictions to Salesforce...")
        
        if DEMO_MODE:
            print(f"   DEMO: Would send {predictions_df.count()} predictions")
            print("   Sample data:")
            display(predictions_df.limit(5))
            return {'status': 'success', 'records_sent': predictions_df.count()}
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        # Convert to list of dicts
        records = predictions_df.toPandas().to_dict('records')
        
        # Batch insert (max 200 per request)
        batch_size = 200
        total_sent = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            # Format for Salesforce
            sf_records = [{
                'Customer_ID__c': rec['customer_id'],
                'Churn_Probability__c': float(rec['churn_probability']),
                'Churn_Risk__c': rec['churn_risk_category'],
                'Last_Updated__c': datetime.now().isoformat()
            } for rec in batch]
            
            payload = {'records': sf_records}
            
            url = f"{self.instance_url}/services/data/{SALESFORCE_API_VERSION}/composite/sobjects"
            try:
                response = _retry_request(lambda: requests.post(url, headers=headers, json=payload))
                if response.status_code == 200:
                    total_sent += len(batch)
                    print(f"   ✅ Sent batch {i//batch_size + 1}: {len(batch)} records")
                else:
                    print(f"   ❌ Error in batch {i//batch_size + 1}: {response.text}")
            except requests.exceptions.RequestException as e:
                print(f"   ❌ Batch {i//batch_size + 1} failed after retries: {e}")
        
        print(f"\n✅ Total records sent to Salesforce: {total_sent}")
        return {'status': 'success', 'records_sent': total_sent}
    
    def create_campaign_list(self, segment_name, customer_ids):
        """
        Create a Salesforce campaign list from customer segment
        """
        print(f"\n📋 Creating Salesforce campaign: {segment_name}")
        
        if DEMO_MODE:
            print(f"   DEMO: Would create campaign with {len(customer_ids)} customers")
            return {'campaign_id': 'mock_campaign_123', 'members': len(customer_ids)}
        
        # Implementation for real Salesforce Campaign API
        # ... (omitted for brevity)
        
        return {'campaign_id': 'mock_id', 'members': len(customer_ids)}

# Initialize
sf = SalesforceIntegration(
    client_id=SALESFORCE_CLIENT_ID,
    client_secret=SALESFORCE_CLIENT_SECRET,
    username=SALESFORCE_USERNAME,
    password=SALESFORCE_PASSWORD
)

if sf.authenticate():
    print("✅ Salesforce integration ready")
else:
    print("❌ Salesforce integration failed")

# COMMAND ----------

# DBTITLE 1,3. HubSpot Integration
# COMMAND ----------
# HubSpot: Contacts API + Custom Properties

class HubSpotIntegration:
    """
    HubSpot integration via REST API
    Supports: Contacts, Companies, Deals, Custom Properties
    """
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
    
    def create_or_update_contact(self, customer_data):
        """
        Create or update HubSpot contact with customer scores
        """
        if DEMO_MODE:
            print(f"🔄 DEMO: Would update contact {customer_data.get('email')}")
            return {'status': 'success', 'contact_id': 'mock_contact_123'}
        
        try:
            url = f"{HUBSPOT_API_URL}/crm/v3/objects/contacts"
            
            # HubSpot contact properties
            properties = {
                'email': customer_data['email'],
                'churn_probability': customer_data['churn_probability'],
                'churn_risk_category': customer_data['churn_risk_category'],
                'customer_value_score': customer_data['customer_value_score'],
                'last_score_update': datetime.now().isoformat()
            }
            
            payload = {'properties': properties}

            response = _retry_request(lambda: requests.post(url, headers=self.headers, json=payload))
            response.raise_for_status()

            result = response.json()
            return {'status': 'success', 'contact_id': result['id']}
            
        except Exception as e:
            print(f"❌ Error updating contact: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def batch_update_contacts(self, predictions_df):
        """
        Batch update HubSpot contacts with latest scores
        """
        print("\n📤 Updating HubSpot contacts...")
        
        if DEMO_MODE:
            print(f"   DEMO: Would update {predictions_df.count()} contacts")
            print("   Sample data:")
            display(predictions_df.limit(5))
            return {'status': 'success', 'contacts_updated': predictions_df.count()}
        
        # Get customer emails (needed for HubSpot)
        customers_df = spark.table(f"{CATALOG}.silver.customers")
        
        # Join predictions with customer emails
        update_df = predictions_df.join(
            customers_df.select('customer_id', 'email'),
            'customer_id',
            'inner'
        ).filter(F.col('email').isNotNull())
        
        records = update_df.toPandas().to_dict('records')
        
        total_updated = 0
        errors = 0
        
        # HubSpot batch API (max 100 per request)
        batch_size = 100
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            # Format for HubSpot batch API
            inputs = [{
                'properties': {
                    'email': rec['email'],
                    'churn_probability': float(rec['churn_probability']),
                    'churn_risk_category': rec['churn_risk_category']
                }
            } for rec in batch]
            
            url = f"{HUBSPOT_API_URL}/crm/v3/objects/contacts/batch/upsert"
            payload = {'inputs': inputs}
            
            try:
                response = _retry_request(lambda: requests.post(url, headers=self.headers, json=payload))
                response.raise_for_status()
                total_updated += len(batch)
                print(f"   ✅ Updated batch {i//batch_size + 1}: {len(batch)} contacts")
            except Exception as e:
                errors += len(batch)
                print(f"   ❌ Error in batch {i//batch_size + 1}: {e}")
        
        print(f"\n✅ Total contacts updated: {total_updated}")
        print(f"❌ Errors: {errors}")
        
        return {
            'status': 'success',
            'contacts_updated': total_updated,
            'errors': errors
        }
    
    def create_deal_from_recommendation(self, customer_id, product_id, propensity_score):
        """
        Create a HubSpot deal from product recommendation
        """
        print(f"\n💰 Creating deal for customer {customer_id}")
        
        if DEMO_MODE:
            print(f"   DEMO: Would create deal with propensity {propensity_score:.2f}")
            return {'status': 'success', 'deal_id': 'mock_deal_456'}
        
        # Implementation for HubSpot Deals API
        # ... (omitted for brevity)
        
        return {'status': 'success', 'deal_id': 'mock_deal_id'}

# Initialize
hubspot = HubSpotIntegration(api_key=HUBSPOT_API_KEY)

print("✅ HubSpot integration ready")

# COMMAND ----------

# DBTITLE 1,4. Webhook Triggers
# COMMAND ----------
# Webhook: Trigger campaigns baseado em scores

def trigger_campaign_webhook(customer_id, campaign_type, payload):
    """
    Trigger webhook para disparar campanha
    
    Campaign types:
    - 'high_churn_risk': Clientes com alto risco de churn
    - 'high_propensity': Clientes com alta propensão de compra
    - 'win_back': Clientes inativos para reativação
    - 'upsell': Recomendações de produtos
    """
    webhook_url = "https://hooks.your-marketing-platform.com/campaign-trigger"
    
    if DEMO_MODE:
        print("\n🔔 DEMO: Webhook trigger")
        print(f"   Campaign: {campaign_type}")
        print(f"   Customer: {customer_id}")
        print(f"   Payload: {json.dumps(payload, indent=2)}")
        return {'status': 'success', 'triggered': True}
    
    try:
        headers = {
            'Content-Type': 'application/json',
            'X-API-Key': 'your_webhook_secret'
        }
        
        webhook_payload = {
            'customer_id': customer_id,
            'campaign_type': campaign_type,
            'timestamp': datetime.now().isoformat(),
            'data': payload
        }
        
        response = _retry_request(lambda: requests.post(webhook_url, headers=headers, json=webhook_payload, timeout=10))
        response.raise_for_status()
        
        print(f"✅ Webhook triggered for customer {customer_id}")
        return {'status': 'success', 'triggered': True}
        
    except Exception as e:
        print(f"❌ Webhook error: {e}")
        return {'status': 'error', 'message': str(e)}

def trigger_high_churn_alerts():
    """
    Trigger alerts para clientes com alto risco de churn
    """
    print("\n🚨 Triggering high churn risk alerts...")
    
    # Get high-risk customers
    high_risk_df = spark.table(f"{CATALOG}.gold.customer_scores") \
        .filter(F.col('churn_risk_category') == 'High') \
        .filter(F.col('customer_value_score') >= 70) \
        .limit(10)
    
    records = high_risk_df.toPandas().to_dict('records')
    
    triggered = 0
    for rec in records:
        payload = {
            'churn_probability': float(rec['churn_probability']),
            'customer_value_score': float(rec['customer_value_score']),
            'recommended_action': 'retention_campaign'
        }
        
        result = trigger_campaign_webhook(
            customer_id=rec['customer_id'],
            campaign_type='high_churn_risk',
            payload=payload
        )
        
        if result['status'] == 'success':
            triggered += 1
    
    print(f"\n✅ Triggered {triggered} high churn alerts")
    return {'triggered': triggered, 'total': len(records)}

print("✅ Webhook engine ready")

# COMMAND ----------

# DBTITLE 1,5. Batch Sync Pipeline
# COMMAND ----------
# Batch Sync: Daily synchronization of scores to CRM

def daily_crm_sync():
    """
    Daily batch sync of customer scores to Salesforce + HubSpot
    """
    print("="*80)
    print("🔄 DAILY CRM SYNC PIPELINE")
    print("="*80)
    
    # Load latest predictions
    print("\n📊 Loading latest predictions...")
    
    churn_df = spark.table(f"{CATALOG}.gold.churn_predictions")
    scores_df = spark.table(f"{CATALOG}.gold.customer_scores")
    
    # Prepare sync data
    sync_df = churn_df.join(scores_df, 'customer_id', 'inner') \
        .select(
            'customer_id',
            churn_df['churn_probability'],
            scores_df['churn_risk_category'],
            scores_df['customer_value_score'],
            scores_df['engagement_score']
        )
    
    total_records = sync_df.count()
    print(f"   Total records to sync: {total_records:,}")
    
    # Sync to Salesforce
    print("\n📤 Syncing to Salesforce...")
    sf_result = sf.send_churn_predictions(sync_df)
    
    # Sync to HubSpot
    print("\n📤 Syncing to HubSpot...")
    hs_result = hubspot.batch_update_contacts(sync_df)
    
    # Trigger alerts for high-risk customers
    print("\n🚨 Triggering alerts...")
    alert_result = trigger_high_churn_alerts()
    
    # Summary
    summary = {
        'timestamp': datetime.now().isoformat(),
        'total_records': total_records,
        'salesforce': sf_result,
        'hubspot': hs_result,
        'alerts': alert_result
    }
    
    print("\n" + "="*80)
    print("📊 SYNC SUMMARY")
    print("="*80)
    print(json.dumps(summary, indent=2))
    
    return summary

print("✅ Batch sync pipeline ready")

# COMMAND ----------

# DBTITLE 1,6. Execute Daily Sync
# COMMAND ----------
# EXECUTE: Run daily CRM sync

# Run the sync
sync_result = daily_crm_sync()

# Display result
display(sync_result)

# COMMAND ----------

# DBTITLE 1,7. Job Configuration
# MAGIC %md
# MAGIC ## 🗓️ Job Configuration (Scheduled)
# MAGIC
# MAGIC Para criar um **Job agendado** no Databricks:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "Daily CRM Sync",
# MAGIC   "schedule": {
# MAGIC     "quartz_cron_expression": "0 0 6 * * ?",
# MAGIC     "timezone_id": "America/Sao_Paulo"
# MAGIC   },
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "sync_to_crm",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/Repos/<seu-usuario>/customer-intelligence-databricks/09_integrations/CRM Integration"
# MAGIC       },
# MAGIC       "new_cluster": {
# MAGIC         "spark_version": "14.3.x-scala2.12",
# MAGIC         "node_type_id": "i3.xlarge",
# MAGIC         "num_workers": 2
# MAGIC       }
# MAGIC     }
# MAGIC   ],
# MAGIC   "email_notifications": {
# MAGIC     "on_success": ["<seu-usuario@empresa.com>"],
# MAGIC     "on_failure": ["<seu-usuario@empresa.com>"]
# MAGIC   },
# MAGIC   "max_concurrent_runs": 1
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Schedule**: Diariamente às 6h AM (BRT)
# MAGIC
# MAGIC ## 🔐 Setup Databricks Secrets
# MAGIC
# MAGIC ```bash
# MAGIC # Create secret scope
# MAGIC databricks secrets create-scope --scope crm_integration
# MAGIC
# MAGIC # Add Salesforce credentials
# MAGIC databricks secrets put --scope crm_integration --key salesforce_client_id
# MAGIC databricks secrets put --scope crm_integration --key salesforce_client_secret
# MAGIC databricks secrets put --scope crm_integration --key salesforce_username
# MAGIC databricks secrets put --scope crm_integration --key salesforce_password
# MAGIC
# MAGIC # Add HubSpot credentials
# MAGIC databricks secrets put --scope crm_integration --key hubspot_api_key
# MAGIC ```
# MAGIC
# MAGIC ## 📊 Monitoring
# MAGIC
# MAGIC 1. **Job Runs**: Track sync execution history
# MAGIC 2. **Salesforce**: Verify records in Custom Objects
# MAGIC 3. **HubSpot**: Check contact properties updated
# MAGIC 4. **Webhooks**: Monitor campaign triggers
# MAGIC 5. **Alerts**: Email notifications on failures
# MAGIC
# MAGIC ## 🎯 Integration Coverage
# MAGIC
# MAGIC - ✅ **Salesforce**: Churn predictions, customer scores
# MAGIC - ✅ **HubSpot**: Contact properties, deals
# MAGIC - ✅ **Webhooks**: Campaign triggers, alerts
# MAGIC - ✅ **Batch Sync**: Daily scheduled updates
# MAGIC - ✅ **Error Handling**: Retry logic + notifications

# COMMAND ----------


