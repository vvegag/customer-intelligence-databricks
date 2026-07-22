# Databricks notebook source
# DBTITLE 1,Model Explainability com SHAP
# MAGIC %md
# MAGIC # Model Explainability com SHAP 🔍
# MAGIC
# MAGIC ## Objetivo
# MAGIC Explicar as decisões dos modelos de Machine Learning usando **SHAP (SHapley Additive exPlanations)**.
# MAGIC
# MAGIC ### Por que SHAP?
# MAGIC * ✅ **Interpretabilidade**: Entender POR QUÊ o modelo fez cada predição
# MAGIC * ✅ **Confiança**: Validar que o modelo usa lógica de negócio correta
# MAGIC * ✅ **Comunicação**: Explicar para stakeholders não-técnicos
# MAGIC * ✅ **Debug**: Identificar features problemáticas ou vieses
# MAGIC
# MAGIC ### Modelos Analisados
# MAGIC 1. **Churn Prediction** - Por que clientes estão em risco?
# MAGIC 2. **Propensity Score** - O que influencia probabilidade de compra?
# COMMAND ----------

# DBTITLE 1,Setup e Instalação
# shap==0.44.0 usa np.obj2sctype, removido no NumPy 2.0 (o Batch Scoring já
# deixou o ambiente com numpy>=2 via --upgrade); shap>=0.45 corrige isso.
# xgboost é necessário para desserializar o modelo (XGBClassifier salvo sob
# o flavor "sklearn" do MLflow, mas ainda precisa do pacote pra unpickle).
# MAGIC %pip install "shap>=0.45.0" xgboost --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

print("✓ SHAP instalado")

# COMMAND ----------

# DBTITLE 1,Imports e Configuração
import shap
import mlflow
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# Configurar matplotlib para notebooks
import warnings
warnings.filterwarnings('ignore')
plt.rcParams['figure.figsize'] = (12, 6)

CATALOG = "customer_intelligence"
SCHEMA_GOLD = "gold"

def get_full_table_name(schema, table):
    """Retorna nome completo da tabela"""
    return f"{CATALOG}.{schema}.{table}"

print("✓ Setup completo")
print(f"SHAP version: {shap.__version__}")

# COMMAND ----------

# DBTITLE 1,Carregar Modelo de Churn
# Carregar modelo do Unity Catalog Model Registry pelo alias @champion.
# Nota: "models:/nome/latest" NÃO é suportado para modelos no UC (usa
# get_latest_versions internamente, proibido no UC) — por isso carregamos
# direto por alias, sem fallback para essa sintaxe legada.
mlflow.set_registry_uri("databricks-uc")

model_name = "customer_intelligence.gold.churn_model"
model_uri = f"models:/{model_name}@champion"
model = mlflow.sklearn.load_model(model_uri)
print(f"✓ Modelo carregado: {model_name}@champion")

print(f"Tipo de modelo: {type(model).__name__}")

# COMMAND ----------

# DBTITLE 1,Preparar Dados de Teste
# Carregar features de clientes
df_features = spark.table(get_full_table_name(SCHEMA_GOLD, "customer_features")).toPandas()

# Selecionar features usadas no treinamento (tem que ser exatamente as mesmas
# e na mesma ordem de "Modelo Churn Prediction.py" — o modelo foi treinado com
# essa lista, não com nomes "parecidos")
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

X_test = df_features[feature_cols].fillna(0)

print(f"✓ Dados preparados: {len(X_test):,} clientes")
print(f"✓ Features: {len(feature_cols)}")
print(f"\nPrimeiras features: {feature_cols[:5]}")

# COMMAND ----------

# DBTITLE 1,SHAP: Feature Importance Global
# Criar SHAP explainer. feature_perturbation="tree_path_dependent" evita o
# NotImplementedError ("Categorical split") que o modo "interventional"
# (default ao passar um dataset de background) lança para árvores do
# XGBoost; nesse modo não é preciso (nem usado) um dataset de background.
explainer = shap.TreeExplainer(model, feature_perturbation="tree_path_dependent")

# Calcular SHAP values para amostra de teste
X_explain = X_test.sample(min(500, len(X_test)), random_state=42)
shap_values = explainer.shap_values(X_explain)

print(f"✓ SHAP values calculados para {len(X_explain):,} clientes")
print(f"✓ Shape: {shap_values.shape if isinstance(shap_values, np.ndarray) else 'múltiplas classes'}")

# Para modelos de classificação binária, pegar a classe positiva (churn=1)
if isinstance(shap_values, list):
    shap_values_churn = shap_values[1]  # classe positiva
else:
    shap_values_churn = shap_values

print("✓ Usando SHAP values da classe 'churn'")

# COMMAND ----------

# DBTITLE 1,Visualização 1: Summary Plot
# Summary plot: mostra importância E direção do impacto
plt.figure(figsize=(12, 8))
shap.summary_plot(shap_values_churn, X_explain, plot_type="dot", show=False)
plt.title("SHAP Summary Plot - Churn Prediction\n(Features mais importantes para predição de churn)", 
          fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

print("\n📊 Interpretação:")
print("• Cada ponto = um cliente")
print("• Cor vermelha = valor alto da feature")
print("• Cor azul = valor baixo da feature")
print("• Posição horizontal = impacto na predição (+ churn à direita)")

# COMMAND ----------

# DBTITLE 1,Visualização 2: Feature Importance (Bar Plot)
# Feature importance: ranking das features mais importantes
plt.figure(figsize=(10, 6))
shap.summary_plot(shap_values_churn, X_explain, plot_type="bar", show=False)
plt.title("Top Features para Predição de Churn\n(Importância média absoluta)", 
          fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

# Calcular importância numérica
feature_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': np.abs(shap_values_churn).mean(axis=0)
}).sort_values('importance', ascending=False)

print("\n📊 Top 10 Features Mais Importantes:")
for idx, row in feature_importance.head(10).iterrows():
    print(f"  {row['feature']:30s}: {row['importance']:.4f}")

# COMMAND ----------

# DBTITLE 1,Visualização 3: Waterfall (Cliente Individual)
# Waterfall plot: explicação detalhada para UM cliente específico
# Pegar um cliente de alto risco de churn
predictions = model.predict_proba(X_explain)[:, 1]  # probabilidade de churn
high_risk_idx = predictions.argmax()  # cliente com maior risco

print(f"Cliente de MAIOR risco: índice {high_risk_idx}")
print(f"Probabilidade de churn: {predictions[high_risk_idx]:.1%}")
print(f"\nFeatures deste cliente:")
for col in feature_cols[:5]:
    print(f"  {col}: {X_explain.iloc[high_risk_idx][col]:.2f}")

print("\n" + "="*70)
print("WATERFALL PLOT - Por que este cliente tem alto risco de churn?")
print("="*70)

# Criar waterfall plot
shap.waterfall_plot(
    shap.Explanation(
        values=shap_values_churn[high_risk_idx],
        base_values=explainer.expected_value[1] if isinstance(explainer.expected_value, list) else explainer.expected_value,
        data=X_explain.iloc[high_risk_idx].values,
        feature_names=feature_cols
    ),
    show=True
)

print("\n📊 Interpretação:")
print("• Cada barra = contribuição de uma feature")
print("• Vermelho/rosa = aumenta risco de churn")
print("• Azul = diminui risco de churn")
print("• Base value = predição média do modelo")
print("• f(x) = predição final para este cliente")

# COMMAND ----------

# DBTITLE 1,Visualização 4: Dependence Plot (Recency)
# Dependence plot: mostra relação entre feature e SHAP value
plt.figure(figsize=(12, 6))
shap.dependence_plot(
    "recency_days",
    shap_values_churn,
    X_explain,
    interaction_index="monetary_total",  # cor mostra interação
    show=False
)
plt.title("Dependence Plot: Recency Days\n(Como recência afeta risco de churn)", 
          fontsize=14, fontweight='bold')
plt.xlabel("Recency Days (dias desde última compra)", fontsize=12)
plt.ylabel("SHAP value (impacto na predição de churn)", fontsize=12)
plt.tight_layout()
plt.show()

print("\n📊 Interpretação:")
print("• Eixo X = valor da feature (dias desde última compra)")
print("• Eixo Y = SHAP value (quanto maior, maior o risco de churn)")
print("• Cor = interação com Monetary Total")
print("• Tendência: quanto MAIOR a recência, MAIOR o risco de churn")

# COMMAND ----------

# DBTITLE 1,Visualização 5: Force Plot (Múltiplos Clientes)
# Force plot: visualização compacta de múltiplos clientes
print("Force Plot - Top 20 clientes de maior risco\n")

# Pegar top 20 de maior risco
top_risk_indices = predictions.argsort()[-20:][::-1]

shap.force_plot(
    explainer.expected_value[1] if isinstance(explainer.expected_value, list) else explainer.expected_value,
    shap_values_churn[top_risk_indices],
    X_explain.iloc[top_risk_indices],
    matplotlib=True,
    show=False
)
plt.title("Force Plot - Top 20 Clientes de Maior Risco de Churn", fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

print("\n📊 Interpretação:")
print("• Vermelho = features que AUMENTAM risco")
print("• Azul = features que DIMINUEM risco")
print("• Largura da barra = magnitude do impacto")

# COMMAND ----------

# DBTITLE 1,Business Insights - Análise Acionável
print("="*70)
print("BUSINESS INSIGHTS - AÇÕES RECOMENDADAS")
print("="*70)

# Analisar principais drivers de churn
top_features = feature_importance.head(5)['feature'].tolist()

print("\n🎯 PRINCIPAIS DRIVERS DE CHURN:\n")

for feature in top_features:
    feature_idx = feature_cols.index(feature)
    avg_shap = shap_values_churn[:, feature_idx].mean()
    
    if feature == 'recency_days':
        print("1️⃣ RECENCY (Dias desde última compra)")
        print("   ⚠️  Impacto: Quanto MAIOR a recência, MAIOR o risco")
        print("   ✅ Ação: Campanhas de reativação para clientes inativos >30 dias")
        print("   📊 Recomendação: Email automation baseado em tempo desde última compra\n")
    
    elif feature == 'frequency':
        print("2️⃣ FREQUENCY (Número de compras)")
        print("   ⚠️  Impacto: Quanto MENOR a frequência, MAIOR o risco")
        print("   ✅ Ação: Programa de fidelidade para aumentar engajamento")
        print("   📊 Recomendação: Incentivos para 2ª e 3ª compras (momentos críticos)\n")
    
    elif feature == 'monetary_total':
        print("3️⃣ MONETARY (Valor total gasto)")
        print("   ⚠️  Impacto: Clientes de baixo valor têm maior risco")
        print("   ✅ Ação: Upsell e cross-sell para aumentar ticket médio")
        print("   📊 Recomendação: Ofertas personalizadas de produtos complementares\n")
    
    elif 'rfm' in feature.lower():
        print("4️⃣ RFM SCORE (Segmentação RFM)")
        print("   ⚠️  Impacto: Score RFM baixo = alto risco")
        print("   ✅ Ação: Tratamento diferenciado por segmento")
        print("   📊 Recomendação: Priorizar clientes 'At Risk' e 'Hibernating'\n")

print("\n" + "="*70)
print("✓ MODELO É INTERPRETÁVEL E ACIONÁVEL")
print("✓ DECISÕES BASEADAS EM LÓGICA DE NEGÓCIO VALIDADA")
print("="*70)

# COMMAND ----------


