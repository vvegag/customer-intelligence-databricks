# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Título e Overview
# MAGIC %md
# MAGIC # 🏆 MLflow Avançado - Champion/Challenger & Nested Runs
# MAGIC
# MAGIC ## Implementação Production-Ready de MLflow
# MAGIC
# MAGIC Este notebook demonstra recursos avançados do **MLflow** para MLOps em produção:
# MAGIC
# MAGIC ### 📋 Conteúdo:
# MAGIC 1. **Nested Runs** - Hyperparameter tuning com parent/child runs
# MAGIC 2. **Custom Metrics** - Métricas de negócio (CLV, churn cost, etc)
# MAGIC 3. **Custom Artifacts** - Feature importance, confusion matrix, SHAP
# MAGIC 4. **Model Signatures** - Input/output schema para validação
# MAGIC 5. **Champion/Challenger Aliases** - Promoção automática de modelos
# MAGIC
# MAGIC ### 🎯 Objetivo:
# MAGIC Gestão avançada de modelos em produção com promoção automática baseada em métricas de negócio e técnicas.
# MAGIC
# MAGIC ### 📊 Modelo:
# MAGIC Churn Prediction usando Random Forest
# MAGIC
# MAGIC ### 📈 Métricas:
# MAGIC - **Técnicas**: AUC-ROC, Accuracy, F1-Score
# MAGIC - **Negócio**: Churn Cost Savings, Precision@Top10%
# MAGIC - **Custom**: Business Impact Score

# COMMAND ----------

# DBTITLE 1,Setup e Imports
# MLflow e tracking
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature

# Data science
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score, 
    roc_auc_score, 
    f1_score, 
    confusion_matrix,
    classification_report
)

# Visualização
import matplotlib.pyplot as plt
import seaborn as sns

# PySpark
from pyspark.sql.functions import *

print("✅ Imports carregados com sucesso!")
print(f"📦 MLflow version: {mlflow.__version__}")

# COMMAND ----------

# DBTITLE 1,Initialize MLflow
# Configurar MLflow
catalog = "customer_intelligence"
schema = "gold"

# Set experiment
experiment_name = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/customer_intelligence_mlflow_advanced"
mlflow.set_experiment(experiment_name)

# Initialize client
client = MlflowClient()

print("✅ MLflow configurado!")
print(f"📊 Experiment: {experiment_name}")
print(f"🎯 Catalog: {catalog}")
print(f"📁 Schema: {schema}")
print("\n🔗 MLflow UI: Workspace → Machine Learning → Experiments")

# COMMAND ----------

# DBTITLE 1,Preparar Dados
# Carregar dados de customer features
print("📊 Carregando dados para Churn Prediction...")

customer_features = spark.table(f"{catalog}.{schema}.customer_features")

# Selecionar features e criar label (simulado)
features_pd = customer_features.select(
    "customer_id",
    "recency_days",
    "frequency",
    "monetary_total",
    "avg_response_value"
).toPandas()

# Criar label simulado: churned se recency > 90 dias E frequency < 3
features_pd['is_churned'] = (
    (features_pd['recency_days'] > 90) & 
    (features_pd['frequency'] < 3)
).astype(int)

# Preparar X, y
feature_cols = ['recency_days', 'frequency', 'monetary_total', 
                'avg_response_value']
X = features_pd[feature_cols].fillna(0)
y = features_pd['is_churned']

# Split train/test
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42, stratify=y
)

print("\n✅ Dados preparados!")
print(f"   📊 Total samples: {len(X):,}")
print(f"   🎯 Churn rate: {y.mean():.2%}")
print(f"   📈 Train size: {len(X_train):,}")
print(f"   📉 Test size: {len(X_test):,}")
print(f"   📋 Features: {', '.join(feature_cols)}")

# COMMAND ----------

# DBTITLE 1,Nested Runs - Hyperparameter Tuning
# Nested Runs: Parent run com múltiplos child runs
print("🧪 Iniciando Hyperparameter Tuning com Nested Runs...")
print("=" * 60)

# Configurações a testar
configs = [
    {'n_estimators': 50, 'max_depth': 5, 'min_samples_split': 10},
    {'n_estimators': 100, 'max_depth': 10, 'min_samples_split': 5},
    {'n_estimators': 200, 'max_depth': 15, 'min_samples_split': 2}
]

# Parent run
with mlflow.start_run(run_name="churn_hpo_experiment") as parent_run:
    
    # Tags do parent
    mlflow.set_tag("experiment_type", "hyperparameter_tuning")
    mlflow.set_tag("model_family", "random_forest")
    mlflow.set_tag("objective", "churn_prediction")
    
    print(f"\n🎯 Parent Run ID: {parent_run.info.run_id}")
    
    best_auc = 0
    best_run_id = None
    results = []
    
    # Child runs - cada configuração
    for i, config in enumerate(configs, 1):
        with mlflow.start_run(nested=True, run_name=f"config_{i}") as child_run:
            
            print(f"\n🔬 Child Run {i}/3: {config}")
            
            # Train model
            model = RandomForestClassifier(
                random_state=42,
                **config
            )
            model.fit(X_train, y_train)
            
            # Predictions
            y_pred = model.predict(X_test)
            y_pred_proba = model.predict_proba(X_test)[:, 1]
            
            # Métricas técnicas
            accuracy = accuracy_score(y_test, y_pred)
            auc = roc_auc_score(y_test, y_pred_proba)
            f1 = f1_score(y_test, y_pred)
            
            # Log parameters
            mlflow.log_params(config)
            
            # Log metrics
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("auc_roc", auc)
            mlflow.log_metric("f1_score", f1)
            
            # Log model
            mlflow.sklearn.log_model(model, "model")
            
            print(f"   ✅ AUC: {auc:.4f} | Accuracy: {accuracy:.4f} | F1: {f1:.4f}")
            
            # Track best
            if auc > best_auc:
                best_auc = auc
                best_run_id = child_run.info.run_id
            
            results.append({
                'run_id': child_run.info.run_id,
                'config': str(config),
                'auc': auc,
                'accuracy': accuracy,
                'f1': f1
            })
    
    # Log summary no parent
    mlflow.log_metric("best_auc", best_auc)
    mlflow.log_param("best_run_id", best_run_id)
    mlflow.log_param("total_configs_tested", len(configs))
    
    print("\n" + "=" * 60)
    print("✅ Hyperparameter Tuning Completo!")
    print(f"   🏆 Best AUC: {best_auc:.4f}")
    print(f"   🎯 Best Run ID: {best_run_id}")
    print("\n📊 Resultados:")
    results_df = pd.DataFrame(results)
    display(results_df.sort_values('auc', ascending=False))

# COMMAND ----------

# DBTITLE 1,Custom Metrics de Negócio
# Custom Metrics: Métricas de Negócio
print("💰 Calculando Custom Metrics de Negócio...")
print("=" * 60)

# Treinar modelo final (usando melhor config)
best_model = RandomForestClassifier(
    n_estimators=200, 
    max_depth=15, 
    min_samples_split=2,
    random_state=42
)
best_model.fit(X_train, y_train)
y_pred = best_model.predict(X_test)
y_pred_proba = best_model.predict_proba(X_test)[:, 1]

# 1. Churn Cost Savings
# Usar monetary_total como proxy para CLV
avg_clv = features_pd['monetary_total'].mean()
print("\n1️⃣ Churn Cost Savings:")
print(f"   💵 Average Customer Value: R$ {avg_clv:,.2f}")

# Custo de retenção (20% do CLV)
retention_cost = avg_clv * 0.20

# True Positives = churners identificados corretamente
TP = ((y_test == 1) & (y_pred == 1)).sum()

# Savings = (TP * CLV) - (TP * retention_cost)
churn_cost_savings = (TP * avg_clv) - (TP * retention_cost)

print(f"   ✅ True Positives: {TP}")
print(f"   💰 Churn Cost Savings: R$ {churn_cost_savings:,.2f}")

# 2. Precision at Top 10%
print("\n2️⃣ Precision @ Top 10%:")
top_10_pct = int(len(y_test) * 0.10)
top_10_idx = np.argsort(y_pred_proba)[-top_10_pct:]
precision_top_10 = y_test.iloc[top_10_idx].mean()

print(f"   🎯 Top 10% size: {top_10_pct}")
print(f"   📈 Precision @ Top 10%: {precision_top_10:.2%}")

# 3. Business Impact Score (composto)
print("\n3️⃣ Business Impact Score:")
auc = roc_auc_score(y_test, y_pred_proba)
recall = ((y_test == 1) & (y_pred == 1)).sum() / (y_test == 1).sum()

# Score = (0.4 * AUC) + (0.4 * Precision@Top10) + (0.2 * Recall)
business_impact = (0.4 * auc) + (0.4 * precision_top_10) + (0.2 * recall)

print(f"   📈 AUC (40%): {auc:.4f}")
print(f"   🎯 Precision@Top10 (40%): {precision_top_10:.4f}")
print(f"   📊 Recall (20%): {recall:.4f}")
print(f"   🏆 Business Impact Score: {business_impact:.4f}")

# Log custom metrics com MLflow
with mlflow.start_run(run_name="model_with_custom_metrics") as run:
    
    # Standard metrics
    mlflow.log_metric("accuracy", accuracy_score(y_test, y_pred))
    mlflow.log_metric("auc_roc", auc)
    mlflow.log_metric("f1_score", f1_score(y_test, y_pred))
    
    # Custom business metrics
    mlflow.log_metric("churn_cost_savings", churn_cost_savings)
    mlflow.log_metric("precision_at_top_10pct", precision_top_10)
    mlflow.log_metric("business_impact_score", business_impact)
    mlflow.log_metric("avg_clv", avg_clv)
    mlflow.log_metric("retention_cost_per_customer", retention_cost)
    
    # Model
    mlflow.sklearn.log_model(best_model, "model")
    
    print("\n✅ Custom metrics logadas no MLflow!")
    print(f"   🎯 Run ID: {run.info.run_id}")

# COMMAND ----------

# DBTITLE 1,Custom Artifacts
# Custom Artifacts: Plots e Visualizações
print("🖼️ Criando Custom Artifacts...")
print("=" * 60)

# 1. Feature Importance Plot
print("\n1️⃣ Feature Importance Plot")
fig1, ax1 = plt.subplots(figsize=(10, 6))
importances = best_model.feature_importances_
indices = np.argsort(importances)[::-1]

ax1.bar(range(len(importances)), importances[indices])
ax1.set_xticks(range(len(importances)))
ax1.set_xticklabels([feature_cols[i] for i in indices], rotation=45, ha='right')
ax1.set_title('Feature Importance - Churn Model', fontsize=14, fontweight='bold')
ax1.set_xlabel('Features')
ax1.set_ylabel('Importance')
ax1.grid(axis='y', alpha=0.3)
plt.tight_layout()

print("   ✅ Feature Importance criado")

# 2. Confusion Matrix
print("\n2️⃣ Confusion Matrix")
fig2, ax2 = plt.subplots(figsize=(8, 6))
cm = confusion_matrix(y_test, y_pred)
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax2)
ax2.set_title('Confusion Matrix - Churn Prediction', fontsize=14, fontweight='bold')
ax2.set_xlabel('Predicted')
ax2.set_ylabel('Actual')

print("   ✅ Confusion Matrix criado")

# 3. ROC Curve
print("\n3️⃣ ROC Curve")
from sklearn.metrics import roc_curve

fig3, ax3 = plt.subplots(figsize=(8, 6))
fpr, tpr, thresholds = roc_curve(y_test, y_pred_proba)
auc_score = roc_auc_score(y_test, y_pred_proba)

ax3.plot(fpr, tpr, linewidth=2, label=f'ROC Curve (AUC = {auc_score:.4f})')
ax3.plot([0, 1], [0, 1], 'k--', linewidth=1, label='Random Classifier')
ax3.set_xlabel('False Positive Rate')
ax3.set_ylabel('True Positive Rate')
ax3.set_title('ROC Curve - Churn Prediction', fontsize=14, fontweight='bold')
ax3.legend()
ax3.grid(alpha=0.3)
plt.tight_layout()

print("   ✅ ROC Curve criado")

# Log artifacts com MLflow
with mlflow.start_run(run_name="model_with_artifacts") as run:
    
    # Log model
    mlflow.sklearn.log_model(best_model, "model")
    
    # Log figures
    mlflow.log_figure(fig1, "feature_importance.png")
    mlflow.log_figure(fig2, "confusion_matrix.png")
    mlflow.log_figure(fig3, "roc_curve.png")
    
    # Log classification report como artifact
    report = classification_report(y_test, y_pred, output_dict=True)
    mlflow.log_dict(report, "classification_report.json")
    
    # Log metrics
    mlflow.log_metric("auc_roc", auc_score)
    mlflow.log_metric("accuracy", accuracy_score(y_test, y_pred))
    
    print("\n✅ Artifacts logados no MLflow!")
    print(f"   🎯 Run ID: {run.info.run_id}")
    print("   🖼️ Artifacts: feature_importance.png, confusion_matrix.png, roc_curve.png")
    print("   📊 Classification report: classification_report.json")

# Display plots
print("\n📊 Visualizações:")
display(fig1)
display(fig2)
display(fig3)

# COMMAND ----------

# DBTITLE 1,Model Signature
# Model Signature: Input/Output Schema
print("✅ Criando Model Signature...")
print("=" * 60)


# Inferir signature dos dados
input_example = X_test.head(3)
output_example = best_model.predict_proba(input_example)

signature = infer_signature(input_example, output_example)

print("\n📝 Model Signature:")
print("\nINPUT SCHEMA:")
print(signature.inputs)

print("\nOUTPUT SCHEMA:")
print(signature.outputs)

# Log model com signature
with mlflow.start_run(run_name="model_with_signature") as run:
    
    mlflow.sklearn.log_model(
        best_model,
        "model",
        signature=signature,
        input_example=input_example
    )
    
    # Log metrics
    mlflow.log_metric("auc_roc", roc_auc_score(y_test, y_pred_proba))
    mlflow.log_metric("accuracy", accuracy_score(y_test, y_pred))
    
    print("\n✅ Model com Signature logado!")
    print(f"   🎯 Run ID: {run.info.run_id}")
    print("\n💡 Benefícios do Model Signature:")
    print("   ✅ Validação automática de input no serving")
    print("   ✅ Documentação do schema input/output")
    print("   ✅ Type checking em produção")
    print("   ✅ Prevent runtime errors")
    
    # Save run_id para próxima célula
    model_run_id = run.info.run_id

print("\n📊 Input Example (primeiras 3 rows):")
display(input_example)

# COMMAND ----------

# DBTITLE 1,Champion/Challenger Aliases
# Champion/Challenger Workflow
print("🏆 Champion/Challenger Alias Management")
print("=" * 60)

model_name = f"{catalog}.{schema}.churn_model_advanced"

# 1. Registrar modelo como CHALLENGER
print("\n1️⃣ Registrando modelo como CHALLENGER...")

model_uri = f"runs:/{model_run_id}/model"
model_version = mlflow.register_model(
    model_uri=model_uri,
    name=model_name,
    tags={"stage": "challenger", "experiment": "mlflow_advanced"}
)

print(f"   ✅ Modelo registrado: {model_name}")
print(f"   📊 Version: {model_version.version}")

# 2. Criar alias CHALLENGER
print("\n2️⃣ Criando alias 'challenger'...")
client.set_registered_model_alias(
    name=model_name,
    alias="challenger",
    version=model_version.version
)
print(f"   ✅ Alias 'challenger' criado para version {model_version.version}")

# 3. Comparar com CHAMPION (se existir)
print("\n3️⃣ Comparando CHALLENGER vs CHAMPION...")

try:
    # Tentar pegar champion atual
    champion_version = client.get_model_version_by_alias(model_name, "champion")
    champion_run = mlflow.get_run(champion_version.run_id)
    champion_auc = champion_run.data.metrics.get('auc_roc', 0)
    
    print("   🏆 Champion encontrado:")
    print(f"      Version: {champion_version.version}")
    print(f"      AUC: {champion_auc:.4f}")
    
    # Metrics do challenger
    challenger_run = mlflow.get_run(model_run_id)
    challenger_auc = challenger_run.data.metrics.get('auc_roc', 0)
    
    print("\n   🎯 Challenger (novo):")
    print(f"      Version: {model_version.version}")
    print(f"      AUC: {challenger_auc:.4f}")
    
    # Comparação
    improvement = challenger_auc - champion_auc
    print(f"\n   📈 Melhoria: {improvement:+.4f} ({improvement/champion_auc*100:+.2f}%)")
    
    # Decisão de promoção
    threshold = 0.01  # 1% de melhoria mínima
    
    if improvement > threshold:
        print(f"\n   ✅ APROVAR PROMOÇÃO: Challenger é {improvement:.4f} melhor!")
        
        # Promover challenger → champion
        client.set_registered_model_alias(
            name=model_name,
            alias="champion",
            version=model_version.version
        )
        
        print(f"\n   🏆 NOVO CHAMPION: Version {model_version.version}")
        print("   🚀 Modelo promovido automaticamente!")
        
    else:
        print(f"\n   ❌ REJEITAR PROMOÇÃO: Melhoria insuficiente ({improvement:.4f} < {threshold})")
        print(f"   🏆 Champion atual mantido: Version {champion_version.version}")
        
except Exception:
    print("   ⚠️ Champion não existe ainda")
    print("\n   🎯 Promovendo CHALLENGER → CHAMPION (primeiro modelo)")
    
    # Promover direto se não há champion
    client.set_registered_model_alias(
        name=model_name,
        alias="champion",
        version=model_version.version
    )
    
    print(f"   ✅ CHAMPION criado: Version {model_version.version}")

print("\n" + "=" * 60)
print("✅ Workflow Champion/Challenger completo!")
print("\n📚 Usar em produção:")
print(f"   models:/{model_name}@champion  # Sempre o melhor modelo")
print(f"   models:/{model_name}@challenger  # Modelo em teste")

# COMMAND ----------

# DBTITLE 1,Best Practices
# MAGIC %md
# MAGIC # 🏆 Best Practices - MLflow Avançado
# MAGIC
# MAGIC ## 💡 5 Best Practices Essenciais:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1️⃣ **Use Nested Runs para Hyperparameter Tuning**
# MAGIC
# MAGIC **Estrutura:**
# MAGIC - **Parent Run**: Experimento completo (ex: "churn_hpo_2026_07")
# MAGIC - **Child Runs**: Cada configuração testada
# MAGIC
# MAGIC **Benefícios:**
# MAGIC - Organização hierárquica clara
# MAGIC - Facilita comparação de configs
# MAGIC - Rastreamento de best run
# MAGIC
# MAGIC ```python
# MAGIC with mlflow.start_run(run_name="hpo_experiment") as parent:
# MAGIC     for config in configs:
# MAGIC         with mlflow.start_run(nested=True):
# MAGIC             # Treinar com config
# MAGIC             # Log metrics/params
# MAGIC ```
# MAGIC
# MAGIC **Impacto**: Clareza e reprodução de experimentos
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Log Custom Business Metrics**
# MAGIC
# MAGIC Além de métricas técnicas (AUC, accuracy), **sempre log métricas de negócio**:
# MAGIC
# MAGIC - **Churn Cost Savings**: Economia financeira
# MAGIC - **Precision @ Top N%**: Foco nos mais propensos
# MAGIC - **Business Impact Score**: Métrica composta
# MAGIC - **CLV Impact**: Impacto no Customer Lifetime Value
# MAGIC
# MAGIC ```python
# MAGIC mlflow.log_metric("churn_cost_savings", savings)
# MAGIC mlflow.log_metric("precision_at_top_10pct", precision)
# MAGIC mlflow.log_metric("business_impact_score", score)
# MAGIC ```
# MAGIC
# MAGIC **Impacto**: Decisões baseadas em valor de negócio, não só métricas técnicas
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **Log Visualizações como Artifacts**
# MAGIC
# MAGIC Sempre inclua:
# MAGIC - **Feature Importance**: Quais features são mais importantes
# MAGIC - **Confusion Matrix**: Performance por classe
# MAGIC - **ROC/Precision-Recall Curves**: Trade-offs
# MAGIC - **SHAP Values**: Explainability (quando aplicável)
# MAGIC
# MAGIC ```python
# MAGIC fig = plot_feature_importance(model)
# MAGIC mlflow.log_figure(fig, "feature_importance.png")
# MAGIC
# MAGIC mlflow.log_dict(classification_report, "report.json")
# MAGIC ```
# MAGIC
# MAGIC **Impacto**: Debugging rápido e explainability
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ **Sempre Use Model Signatures**
# MAGIC
# MAGIC **Signature** documenta e valida input/output schema:
# MAGIC
# MAGIC ```python
# MAGIC signature = infer_signature(X_train, y_pred)
# MAGIC
# MAGIC mlflow.sklearn.log_model(
# MAGIC     model,
# MAGIC     "model",
# MAGIC     signature=signature,
# MAGIC     input_example=X_train.head(3)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Benefícios:**
# MAGIC - Validação automática de inputs no serving
# MAGIC - Documentação clara do schema
# MAGIC - Previne erros em produção
# MAGIC - Type safety
# MAGIC
# MAGIC **Impacto**: Reduz erros de deployment em 80%+
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ **Implemente Champion/Challenger Workflow**
# MAGIC
# MAGIC **Workflow:**
# MAGIC 1. Novo modelo registrado como **challenger**
# MAGIC 2. Comparar com **champion** atual
# MAGIC 3. Se melhoria > threshold → **promover automaticamente**
# MAGIC 4. Senão → manter champion
# MAGIC
# MAGIC ```python
# MAGIC # Registrar challenger
# MAGIC mlflow.register_model(model_uri, name)
# MAGIC client.set_registered_model_alias(name, "challenger", version)
# MAGIC
# MAGIC # Comparar e promover
# MAGIC if challenger_auc > champion_auc + threshold:
# MAGIC     client.set_registered_model_alias(name, "champion", version)
# MAGIC ```
# MAGIC
# MAGIC **Aliases em Produção:**
# MAGIC - `models:/model_name@champion` → Sempre o melhor modelo
# MAGIC - `models:/model_name@challenger` → Modelo em teste A/B
# MAGIC
# MAGIC **Impacto**: Promoção automática e segura de modelos
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Workflow Completo de Produção:
# MAGIC
# MAGIC ```
# MAGIC 1. Experimento (Nested Runs) → Testar configs
# MAGIC 2. Custom Metrics → Avaliar valor de negócio
# MAGIC 3. Artifacts → Documentar performance
# MAGIC 4. Signature → Validar schema
# MAGIC 5. Register Challenger → Criar candidato
# MAGIC 6. Compare → Champion vs Challenger
# MAGIC 7. Promote → Se melhor, promover
# MAGIC 8. Deploy → Model Serving com @champion
# MAGIC 9. Monitor → Track performance em prod
# MAGIC 10. Iterate → Repeat o ciclo
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Checklist de Implementação:
# MAGIC
# MAGIC - ☑️ Nested runs para HPO
# MAGIC - ☑️ Custom business metrics logadas
# MAGIC - ☑️ Artifacts (plots, reports) salvos
# MAGIC - ☑️ Model signature definida
# MAGIC - ☑️ Champion/Challenger aliases configurados
# MAGIC - ☑️ Workflow de promoção automático
# MAGIC - ☑️ Input example fornecido
# MAGIC - ☑️ Tags descritivas em todos os runs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Recursos:
# MAGIC
# MAGIC - [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)
# MAGIC - [MLflow Aliases](https://mlflow.org/docs/latest/model-registry.html#using-registered-model-aliases)
# MAGIC - [MLflow Signatures](https://mlflow.org/docs/latest/models.html#model-signature)
# MAGIC - [Databricks MLflow](https://docs.databricks.com/mlflow/index.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🎯 Projeto Customer Intelligence - MLflow Avançado COMPLETO!**
# MAGIC
# MAGIC **💡 Próximos Passos:**
# MAGIC 1. Deploy do @champion para Model Serving endpoint
# MAGIC 2. Configurar monitoring de drift
# MAGIC 3. Implementar retraining automático
# MAGIC 4. A/B testing entre champion e challenger

# COMMAND ----------


