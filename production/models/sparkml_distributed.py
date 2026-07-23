# Databricks notebook source
# DBTITLE 1,Overview - SparkML Distributed Training
# MAGIC %md
# MAGIC # ⚡ PySpark MLlib - Distributed Machine Learning
# MAGIC
# MAGIC ## Implementação Production-Ready de Distributed Training
# MAGIC
# MAGIC Este notebook demonstra **PySpark MLlib** para machine learning distribuído em produção.
# MAGIC
# MAGIC ### 📋 Conteúdo:
# MAGIC 1. **ML Pipelines** - Encadeamento de transformações
# MAGIC 2. **VectorAssembler** - Feature engineering distribuído
# MAGIC 3. **StandardScaler** - Normalização distribuída
# MAGIC 4. **Distributed Training** - Random Forest, GBT, Logistic Regression
# MAGIC 5. **CrossValidator** - Hyperparameter tuning distribuído
# MAGIC 6. **Model Persistence** - Save/load pipelines
# MAGIC 7. **MLflow Integration** - Tracking distribuído
# MAGIC 8. **Distributed Predictions** - Inference em larga escala
# MAGIC
# MAGIC ### 🎯 Vantagens do PySpark MLlib:
# MAGIC ```
# MAGIC ✅ DISTRIBUTED: Treina em clusters com TB de dados
# MAGIC ✅ SCALABLE: Adicione nodes para aumentar capacidade
# MAGIC ✅ PIPELINE: Encadeamento declarativo de transformações
# MAGIC ✅ PARALLEL: Hyperparameter tuning paralelo
# MAGIC ✅ FAULT-TOLERANT: Resiliente a falhas de nodes
# MAGIC ✅ IN-MEMORY: Processamento otimizado com cache
# MAGIC ```
# MAGIC
# MAGIC ### 🔄 ML Pipeline Architecture:
# MAGIC ```
# MAGIC [Raw DataFrame]
# MAGIC     ↓ StringIndexer (categorical → numeric)
# MAGIC [Indexed DataFrame]
# MAGIC     ↓ VectorAssembler (features → vector)
# MAGIC [Feature Vector]
# MAGIC     ↓ StandardScaler (normalization)
# MAGIC [Scaled Features]
# MAGIC     ↓ Model (RandomForest, GBT, LogisticRegression)
# MAGIC [Predictions]
# MAGIC ```
# MAGIC
# MAGIC ### 📊 Dataset:
# MAGIC Customer Churn Prediction (distributed)
# MAGIC
# MAGIC ### 🚀 Performance:
# MAGIC - **Local (pandas/sklearn)**: ~1M rows, single machine
# MAGIC - **Distributed (Spark ML)**: Billions of rows, cluster parallelism
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **💡 Use Case:** Quando os dados não cabem em memória de uma máquina OU quando você precisa treinar rápido em grandes datasets.

# COMMAND ----------

# DBTITLE 1,Setup e Imports
# PySpark MLlib - Distributed ML
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    VectorAssembler, 
    StandardScaler
)
from pyspark.ml.classification import (
    RandomForestClassifier
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)
from pyspark.ml.tuning import (
    CrossValidator,
    ParamGridBuilder
)

# PySpark SQL
from pyspark.sql.functions import *
from pyspark.sql.types import *

# MLflow para tracking
import mlflow
import mlflow.spark

# Python standard
import time

print("✅ Imports carregados!")
print(f"📦 Spark version: {spark.version}")
print(f"⚡ Execution mode: {'DISTRIBUTED' if spark.sparkContext.defaultParallelism > 1 else 'LOCAL'}")
print(f"🔢 Parallelism: {spark.sparkContext.defaultParallelism}")

# COMMAND ----------

# DBTITLE 1,Load Data - Distributed
# Carregar dados com PySpark (distribuído)
print("📊 Carregando dados distribuídos...")

catalog = "customer_intelligence"
schema = "gold"

# Load customer features (distributed DataFrame)
df = spark.table(f"{catalog}.{schema}.customer_features")

# Select features para churn prediction
feature_columns = [
    "recency_days",
    "frequency",
    "monetary_total",
    "avg_response_value",
    "total_response_value",
    "conversion_rate",
    "event_count_30d",
    "engagement_score_30d"
]

# Preparar dataset
df_ml = df.select(
    *feature_columns,
    # Label: churned se recency > 90 dias E frequency < 3
    when(
        (col("recency_days") > 90) & (col("frequency") < 3), 1
    ).otherwise(0).cast("double").alias("label")
)

# Cache para performance (mantém em memória)
df_ml = df_ml.cache()

print("\n✅ Dados carregados!")
print(f"   📊 Total rows: {df_ml.count():,}")
print(f"   📋 Features: {len(feature_columns)}")
print(f"   🎯 Churn rate: {df_ml.filter('label = 1').count() / df_ml.count():.2%}")
print("\n📈 Schema:")
df_ml.printSchema()

# Display sample
print("\n🔍 Sample (primeiras 5 rows):")
display(df_ml.limit(5))

# COMMAND ----------

# DBTITLE 1,Train/Test Split - Distributed
# Train/Test Split (distribuído)
print("🔀 Fazendo Train/Test Split distribuído...")

# Split 70/30 (seed para reproducibilidade)
train_df, test_df = df_ml.randomSplit([0.7, 0.3], seed=42)

# Cache ambos
train_df = train_df.cache()
test_df = test_df.cache()

# Force cache (trigger action)
train_count = train_df.count()
test_count = test_df.count()

print("\n✅ Split completo!")
print(f"   📈 Train: {train_count:,} rows")
print(f"   📉 Test: {test_count:,} rows")
print(f"   🎯 Train churn rate: {train_df.filter('label = 1').count() / train_count:.2%}")
print(f"   🎯 Test churn rate: {test_df.filter('label = 1').count() / test_count:.2%}")

print("\n💡 Dados cached em memória distribuída para performance!")

# COMMAND ----------

# DBTITLE 1,Build ML Pipeline
# Construir ML Pipeline (encadeamento de transformações)
print("🔧 Construindo ML Pipeline...")
print("=" * 60)

# Stage 1: VectorAssembler - Combina features em um único vector
print("\n1️⃣ VectorAssembler: features → vector")
assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features_raw",
    handleInvalid="skip"  # Skip rows com NaN
)

# Stage 2: StandardScaler - Normalização distribuída
print("2️⃣ StandardScaler: normalização (mean=0, std=1)")
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withMean=True,
    withStd=True
)

# Stage 3: Random Forest Classifier - Modelo distribuído
print("3️⃣ RandomForestClassifier: distributed training")
rf = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    numTrees=100,
    maxDepth=10,
    seed=42,
    # Distributed parameters
    subsamplingRate=0.8,
    featureSubsetStrategy="auto"
)

# Pipeline completo: assembler → scaler → model
pipeline = Pipeline(stages=[assembler, scaler, rf])

print("\n✅ Pipeline criado!")
print("\n📋 Pipeline Stages:")
for i, stage in enumerate(pipeline.getStages(), 1):
    print(f"   {i}. {stage.__class__.__name__}")

print("\n💡 Pipeline é um grafo computacional distribuído!")
print("   - Cada stage processa dados em paralelo")
print("   - Transformações são lazy (só executam quando necessário)")
print("   - Pipeline pode ser serializado e reutilizado")

# COMMAND ----------

# DBTITLE 1,Train Model - Distributed
# Treinar modelo (distribuído)
print("⚡ Treinando modelo distribuído...")
print("=" * 60)

start_time = time.time()

# Fit pipeline (transforma dados + treina modelo)
# Esta operação é distribuída pelo cluster
with mlflow.start_run(run_name="sparkml_random_forest") as run:
    
    # Tags
    mlflow.set_tag("model_type", "spark_ml")
    mlflow.set_tag("distributed", "true")
    mlflow.set_tag("algorithm", "random_forest")
    
    # Train
    print("\n🚀 Training iniciado (distribuído)...")
    pipeline_model = pipeline.fit(train_df)
    
    training_time = time.time() - start_time
    
    print("\n✅ Training completo!")
    print(f"   ⏱️ Tempo: {training_time:.2f}s")
    print(f"   📊 Rows processadas: {train_count:,}")
    print(f"   ⚡ Throughput: {train_count/training_time:,.0f} rows/sec")
    
    # Predictions no test set (distribuído)
    print("\n🔮 Gerando predictions (distribuído)...")
    predictions = pipeline_model.transform(test_df)
    predictions = predictions.cache()
    
    # Evaluate
    print("\n📊 Avaliando modelo...")
    
    # Binary evaluator (AUC-ROC)
    evaluator_auc = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    auc = evaluator_auc.evaluate(predictions)
    
    # Multiclass evaluator (Accuracy, F1)
    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )
    accuracy = evaluator_acc.evaluate(predictions)
    
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1"
    )
    f1 = evaluator_f1.evaluate(predictions)
    
    # Log metrics no MLflow
    mlflow.log_metric("auc_roc", auc)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("training_time_sec", training_time)
    mlflow.log_metric("throughput_rows_per_sec", train_count/training_time)
    
    # Log model (Spark ML)
    mlflow.spark.log_model(
        pipeline_model,
        "model",
        registered_model_name=f"{catalog}.{schema}.churn_sparkml"
    )
    
    print("\n🎯 Métricas:")
    print(f"   📈 AUC-ROC: {auc:.4f}")
    print(f"   🎯 Accuracy: {accuracy:.4f}")
    print(f"   📊 F1-Score: {f1:.4f}")
    
    print("\n✅ Modelo logado no MLflow!")
    print(f"   🎯 Run ID: {run.info.run_id}")
    print(f"   📦 Model: {catalog}.{schema}.churn_sparkml")

# Display predictions sample
print("\n🔍 Predictions Sample:")
display(
    predictions.select(
        *feature_columns[:3],
        "label",
        "prediction",
        "probability"
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 1,CrossValidator - Distributed HPO
# CrossValidator: Hyperparameter Tuning Distribuído
print("🎯 CrossValidator - Distributed Hyperparameter Tuning")
print("=" * 60)

# Build pipeline (sem modelo ainda)
assembler_cv = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features_raw",
    handleInvalid="skip"
)

scaler_cv = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withMean=True,
    withStd=True
)

rf_cv = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    seed=42
)

pipeline_cv = Pipeline(stages=[assembler_cv, scaler_cv, rf_cv])

# Parameter Grid - Testar múltiplas combinações
print("\n📊 Criando Parameter Grid...")
paramGrid = ParamGridBuilder() \
    .addGrid(rf_cv.numTrees, [50, 100]) \
    .addGrid(rf_cv.maxDepth, [5, 10]) \
    .addGrid(rf_cv.subsamplingRate, [0.7, 0.8]) \
    .build()

print(f"   📊 Total combinations: {len(paramGrid)}")
for i, params in enumerate(paramGrid, 1):
    print(f"   {i}. {params}")

# CrossValidator - 3-fold CV (distribuído)
print("\n🔄 Configurando CrossValidator...")
evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    metricName="areaUnderROC"
)

crossval = CrossValidator(
    estimator=pipeline_cv,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=3,  # 3-fold cross-validation
    parallelism=2,  # Parallelismo para folds
    seed=42
)

print(f"   ✅ 3-fold CV com {len(paramGrid)} combinações")
print(f"   ⚡ Total runs: {len(paramGrid) * 3} (distribuídos)")
print("   🔢 Parallelism: 2 (folds processados em paralelo)")

# Train com CrossValidator (DEMORADO - distributed HPO)
print("\n🚀 Iniciando distributed hyperparameter tuning...")
print("⚠️  Isso pode demorar - são múltiplas combinações testadas!\n")

start_time_cv = time.time()

with mlflow.start_run(run_name="sparkml_crossvalidator") as run_cv:
    
    mlflow.set_tag("tuning_method", "cross_validator")
    mlflow.set_tag("num_folds", 3)
    mlflow.set_tag("num_param_combinations", len(paramGrid))
    
    # Fit (distributed)
    cv_model = crossval.fit(train_df)
    
    cv_time = time.time() - start_time_cv
    
    print("\n✅ CrossValidator completo!")
    print(f"   ⏱️ Tempo total: {cv_time:.2f}s ({cv_time/60:.1f} min)")
    print(f"   🔢 Runs executados: {len(paramGrid) * 3}")
    print(f"   ⚡ Avg time per run: {cv_time/(len(paramGrid)*3):.1f}s")
    
    # Best model
    best_model = cv_model.bestModel
    best_rf = best_model.stages[-1]  # Último stage (RF)
    
    print("\n🏆 Best Model Parameters:")
    print(f"   🌳 numTrees: {best_rf.getNumTrees}")
    print(f"   📊 maxDepth: {best_rf.getMaxDepth()}")
    print(f"   🎯 subsamplingRate: {best_rf.getSubsamplingRate()}")
    
    # Evaluate best model
    predictions_cv = cv_model.transform(test_df)
    auc_cv = evaluator.evaluate(predictions_cv)
    
    # Accuracy
    evaluator_acc_cv = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )
    accuracy_cv = evaluator_acc_cv.evaluate(predictions_cv)
    
    print("\n🎯 Best Model Performance:")
    print(f"   📈 AUC-ROC: {auc_cv:.4f}")
    print(f"   🎯 Accuracy: {accuracy_cv:.4f}")
    
    # Log no MLflow
    mlflow.log_metric("best_auc_roc", auc_cv)
    mlflow.log_metric("best_accuracy", accuracy_cv)
    mlflow.log_metric("cv_time_sec", cv_time)
    mlflow.log_param("best_numTrees", best_rf.getNumTrees)
    mlflow.log_param("best_maxDepth", best_rf.getMaxDepth())
    mlflow.log_param("best_subsamplingRate", best_rf.getSubsamplingRate())
    
    # Log model
    mlflow.spark.log_model(
        cv_model.bestModel,
        "best_model"
    )
    
    print("\n✅ Best model logado no MLflow!")
    print(f"   🎯 Run ID: {run_cv.info.run_id}")

# COMMAND ----------

# DBTITLE 1,Model Persistence - Save/Load
# Model Persistence: Salvar e carregar pipelines
print("💾 Model Persistence - Save & Load")
print("=" * 60)

# Path para salvar modelo
model_path = f"/tmp/{catalog}_{schema}_sparkml_pipeline"

print("\n💾 Salvando pipeline completo...")
print(f"   📁 Path: {model_path}")

# Save pipeline (serialização completa)
pipeline_model.write().overwrite().save(model_path)

print("\n✅ Pipeline salvo!")
print("\n📊 Conteúdo do pipeline:")
print("   1. VectorAssembler (features)")
print("   2. StandardScaler (normalização)")
print("   3. RandomForestClassifier (modelo treinado)")

# Load pipeline
print("\n📂 Carregando pipeline salvo...")
loaded_pipeline = PipelineModel.load(model_path)

print("\n✅ Pipeline carregado!")
print(f"   🔢 Stages: {len(loaded_pipeline.stages)}")

# Test loaded pipeline
print("\n🧪 Testando pipeline carregado...")
test_predictions = loaded_pipeline.transform(test_df.limit(100))
test_auc = evaluator_auc.evaluate(test_predictions)

print("\n✅ Pipeline carregado funciona!")
print(f"   🎯 AUC (100 rows): {test_auc:.4f}")

# Demonstrar inference em batch (distributed)
print("\n🚀 Inference em larga escala (distributed)...")

# Criar batch grande para inference
large_batch = test_df  # Todo o test set

start_inference = time.time()
batch_predictions = loaded_pipeline.transform(large_batch)
batch_count = batch_predictions.count()  # Trigger action
inference_time = time.time() - start_inference

print("\n✅ Inference completa!")
print(f"   📊 Rows: {batch_count:,}")
print(f"   ⏱️ Tempo: {inference_time:.2f}s")
print(f"   ⚡ Throughput: {batch_count/inference_time:,.0f} rows/sec")
print("\n💡 Inference distribuído = Escalabilidade ilimitada!")

# Display inference results
print("\n🔍 Inference Results Sample:")
display(
    batch_predictions.select(
        "recency_days",
        "frequency",
        "label",
        "prediction",
        "probability"
    ).limit(5)
)

# COMMAND ----------

# DBTITLE 1,Feature Importance - Distributed
# Feature Importance (Distributed)
print("📈 Feature Importance - Random Forest")
print("=" * 60)

# Extrair Random Forest do pipeline
rf_model = pipeline_model.stages[-1]  # Último stage

# Get feature importances
importances = rf_model.featureImportances.toArray()

# Criar DataFrame com importances
importance_df = spark.createDataFrame(
    [(feature_columns[i], float(importances[i])) for i in range(len(feature_columns))],
    ["feature", "importance"]
).orderBy(col("importance").desc())

print("\n🎯 Feature Importance Ranking:")
importance_pandas = importance_df.toPandas()
for idx, row in importance_pandas.iterrows():
    print(f"   {idx+1}. {row['feature']:<25} {row['importance']:.4f}")

# Visualização
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(importance_pandas['feature'], importance_pandas['importance'])
ax.set_xlabel('Importance', fontsize=12)
ax.set_title('Feature Importance - Spark ML Random Forest', fontsize=14, fontweight='bold')
ax.grid(axis='x', alpha=0.3)
plt.tight_layout()

print("\n📉 Visualização:")
display(fig)

# Top 3 features
top_3 = importance_pandas.head(3)
print("\n🏆 Top 3 Features:")
for idx, row in top_3.iterrows():
    print(f"   {idx+1}. {row['feature']}: {row['importance']:.4f}")

# COMMAND ----------

# DBTITLE 1,Best Practices - SparkML
# MAGIC %md
# MAGIC # 🏆 Best Practices - PySpark MLlib
# MAGIC
# MAGIC ## 💡 5 Best Practices Essenciais:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1️⃣ **Use ML Pipelines para Encadeamento de Transformações**
# MAGIC
# MAGIC **Pipeline** encadeia transformações e modelo em um único grafo:
# MAGIC
# MAGIC ```python
# MAGIC assembler = VectorAssembler(inputCols=features, outputCol="features")
# MAGIC scaler = StandardScaler(inputCol="features", outputCol="scaled")
# MAGIC model = RandomForestClassifier(featuresCol="scaled")
# MAGIC
# MAGIC pipeline = Pipeline(stages=[assembler, scaler, model])
# MAGIC pipeline_model = pipeline.fit(train_df)
# MAGIC ```
# MAGIC
# MAGIC **Benefícios:**
# MAGIC - **Encapsulamento**: Todas as transformações em um objeto
# MAGIC - **Serialização**: Salva e carrega tudo junto
# MAGIC - **Reprodução**: Garante mesmas transformações em produção
# MAGIC - **Manutenibilidade**: Código limpo e organizado
# MAGIC
# MAGIC **Impacto**: Elimina bugs de preprocessing em produção
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Cache DataFrames Intermediários**
# MAGIC
# MAGIC **Cache** mantém DataFrames em memória distribuída:
# MAGIC
# MAGIC ```python
# MAGIC # Antes de usar múltiplas vezes
# MAGIC train_df = train_df.cache()
# MAGIC test_df = test_df.cache()
# MAGIC
# MAGIC # Force cache (trigger action)
# MAGIC train_df.count()
# MAGIC
# MAGIC # Remover cache quando não precisar mais
# MAGIC train_df.unpersist()
# MAGIC ```
# MAGIC
# MAGIC **Quando usar:**
# MAGIC - DataFrames usados múltiplas vezes (train, evaluate, transform)
# MAGIC - Iterative algorithms (CrossValidator, hyperparameter tuning)
# MAGIC - Exploratory analysis
# MAGIC
# MAGIC **Quando NÃO usar:**
# MAGIC - DataFrames muito grandes (> memória do cluster)
# MAGIC - DataFrames usados apenas uma vez
# MAGIC
# MAGIC **Impacto**: 10-100x speedup em workloads iterativos
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **Use CrossValidator para Hyperparameter Tuning Distribuído**
# MAGIC
# MAGIC **CrossValidator** paraliza HPO pelo cluster:
# MAGIC
# MAGIC ```python
# MAGIC paramGrid = ParamGridBuilder() \
# MAGIC     .addGrid(rf.numTrees, [50, 100, 200]) \
# MAGIC     .addGrid(rf.maxDepth, [5, 10, 15]) \
# MAGIC     .build()
# MAGIC
# MAGIC crossval = CrossValidator(
# MAGIC     estimator=pipeline,
# MAGIC     estimatorParamMaps=paramGrid,
# MAGIC     evaluator=evaluator,
# MAGIC     numFolds=3,
# MAGIC     parallelism=4  # Folds em paralelo
# MAGIC )
# MAGIC
# MAGIC cv_model = crossval.fit(train_df)
# MAGIC best_model = cv_model.bestModel
# MAGIC ```
# MAGIC
# MAGIC **Features:**
# MAGIC - Distribuição automática de combinações
# MAGIC - K-fold CV distribuído
# MAGIC - Retorna best model automaticamente
# MAGIC
# MAGIC **Alternativa mais rápida:**
# MAGIC ```python
# MAGIC TrainValidationSplit  # Single split, mais rápido que CV
# MAGIC ```
# MAGIC
# MAGIC **Impacto**: HPO em minutos ao invés de horas
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ **Persista Pipelines Completos, Não Apenas Modelos**
# MAGIC
# MAGIC **Sempre salve o pipeline inteiro:**
# MAGIC
# MAGIC ```python
# MAGIC # ✅ CORRETO: Salva pipeline completo
# MAGIC pipeline_model.write().overwrite().save(path)
# MAGIC loaded = PipelineModel.load(path)
# MAGIC
# MAGIC # ❌ ERRADO: Salvar apenas modelo
# MAGIC model.write().overwrite().save(path)  # Falta preprocessing!
# MAGIC ```
# MAGIC
# MAGIC **Por quê pipeline completo?**
# MAGIC - Inclui preprocessing (VectorAssembler, Scaler, etc)
# MAGIC - Garante mesmas transformações em produção
# MAGIC - Evita train/serve skew
# MAGIC - Serialização determinística
# MAGIC
# MAGIC **Deploy pattern:**
# MAGIC ```python
# MAGIC # Training
# MAGIC pipeline_model = pipeline.fit(train)
# MAGIC pipeline_model.save("s3://models/churn_v1")
# MAGIC
# MAGIC # Production
# MAGIC loaded = PipelineModel.load("s3://models/churn_v1")
# MAGIC predictions = loaded.transform(new_data)  # Tudo funciona!
# MAGIC ```
# MAGIC
# MAGIC **Impacto**: Elimina bugs de preprocessing em produção
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ **Monitore Partitioning e Shuffle**
# MAGIC
# MAGIC **Partitioning** impacta diretamente performance:
# MAGIC
# MAGIC ```python
# MAGIC # Ver partitioning
# MAGIC print(f"Partitions: {df.rdd.getNumPartitions()}")
# MAGIC
# MAGIC # Repartition se necessário
# MAGIC df = df.repartition(200)  # Aumentar paralelismo
# MAGIC df = df.coalesce(10)      # Reduzir (sem shuffle)
# MAGIC
# MAGIC # Particionar por chave antes de joins
# MAGIC df = df.repartition("customer_id")
# MAGIC ```
# MAGIC
# MAGIC **Guidelines:**
# MAGIC - **Default partitions**: `spark.sql.shuffle.partitions` (default 200)
# MAGIC - **Rule of thumb**: 128 MB - 1 GB por partição
# MAGIC - **Small data**: Reduzir partitions (menos overhead)
# MAGIC - **Large data**: Aumentar partitions (mais paralelismo)
# MAGIC
# MAGIC **Shuffle otimização:**
# MAGIC ```python
# MAGIC # Evite múltiplos shuffles
# MAGIC df.repartition(100).groupBy("key").count()  # 1 shuffle
# MAGIC
# MAGIC # Vs.
# MAGIC df.groupBy("key").count().repartition(100)  # 2 shuffles!
# MAGIC ```
# MAGIC
# MAGIC **Impacto**: 2-10x speedup em joins e aggregations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Workflow Completo de Produção:
# MAGIC
# MAGIC ```
# MAGIC 1. Load data (distributed) → Spark DataFrame
# MAGIC 2. Cache (if reused) → In-memory
# MAGIC 3. Build Pipeline → Stages encadeados
# MAGIC 4. CrossValidator → Distributed HPO
# MAGIC 5. Train best model → Distributed training
# MAGIC 6. Evaluate → Test set
# MAGIC 7. Save pipeline → Persist complete pipeline
# MAGIC 8. MLflow log → Tracking
# MAGIC 9. Deploy → Load pipeline em produção
# MAGIC 10. Batch inference → Distributed predictions
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Checklist de Implementação:
# MAGIC
# MAGIC - ☑️ Pipeline com múltiplos stages
# MAGIC - ☑️ VectorAssembler para features
# MAGIC - ☑️ StandardScaler para normalização
# MAGIC - ☑️ CrossValidator para HPO
# MAGIC - ☑️ Cache em DataFrames reutilizados
# MAGIC - ☑️ Pipeline persistence (save/load)
# MAGIC - ☑️ MLflow integration
# MAGIC - ☑️ Feature importance analysis
# MAGIC - ☑️ Distributed inference tested
# MAGIC - ☑️ Partitioning otimizado
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Quando Usar Spark ML vs. Scikit-Learn:
# MAGIC
# MAGIC ### **Use Spark ML quando:**
# MAGIC - ✅ Dados > 1 GB (não cabem em memória)
# MAGIC - ✅ Precisa de distributed training
# MAGIC - ✅ Inference em larga escala (milhões de rows)
# MAGIC - ✅ Já tem pipeline Spark (ETL)
# MAGIC
# MAGIC ### **Use Scikit-Learn quando:**
# MAGIC - ✅ Dados < 1 GB (cabem em memória)
# MAGIC - ✅ Precisa de algoritmos mais avançados
# MAGIC - ✅ Prototipagem rápida
# MAGIC - ✅ Ecosystem mais rico (SHAP, etc)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔗 Recursos:
# MAGIC
# MAGIC - [PySpark MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html)
# MAGIC - [ML Pipelines](https://spark.apache.org/docs/latest/ml-pipeline.html)
# MAGIC - [Tuning Guide](https://spark.apache.org/docs/latest/ml-tuning.html)
# MAGIC - [Databricks ML Guide](https://docs.databricks.com/machine-learning/index.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🎯 Projeto Customer Intelligence - SparkML COMPLETO!**
# MAGIC
# MAGIC **💡 Próximos Passos:**
# MAGIC 1. Deploy pipeline para Model Serving
# MAGIC 2. Implement online inference
# MAGIC 3. Monitor model drift
# MAGIC 4. Retrain pipeline automaticamente

# COMMAND ----------

# DBTITLE 1,Summary & Next Steps
# MAGIC %md
# MAGIC # 🚀 Summary - SparkML Distributed Training
# MAGIC
# MAGIC ## ✅ O Que Foi Implementado:
# MAGIC
# MAGIC ### **1️⃣ ML Pipeline Completo (3 Stages)**
# MAGIC - **VectorAssembler**: Features → Vector
# MAGIC - **StandardScaler**: Normalização distribuída
# MAGIC - **RandomForestClassifier**: Modelo distribuído
# MAGIC
# MAGIC ### **2️⃣ Distributed Training**
# MAGIC - Training distribuído em cluster
# MAGIC - Throughput: ~X,XXX rows/sec
# MAGIC - MLflow tracking integrado
# MAGIC
# MAGIC ### **3️⃣ CrossValidator (Distributed HPO)**
# MAGIC - 8 combinações de parâmetros testadas
# MAGIC - 3-fold cross-validation
# MAGIC - Total: 24 runs distribuídos
# MAGIC - Best model selecionado automaticamente
# MAGIC
# MAGIC ### **4️⃣ Model Persistence**
# MAGIC - Pipeline completo serializado
# MAGIC - Save & Load testado
# MAGIC - Inference distribuído validado
# MAGIC
# MAGIC ### **5️⃣ Feature Importance**
# MAGIC - Extraído do Random Forest
# MAGIC - Visualizado com matplotlib
# MAGIC - Top features identificadas
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Performance:
# MAGIC
# MAGIC ```
# MAGIC ⚡ Training:     X,XXX rows/sec
# MAGIC ⚡ Inference:    X,XXX rows/sec
# MAGIC 🔢 Parallelism:  Y cores
# MAGIC 🎯 AUC-ROC:      0.XXXX
# MAGIC 🎯 Accuracy:     XX.XX%
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Diferenças: Spark ML vs. Scikit-Learn:
# MAGIC
# MAGIC | Feature | Scikit-Learn | Spark ML |
# MAGIC |---------|--------------|----------|
# MAGIC | **Scale** | Single machine | Distributed cluster |
# MAGIC | **Data size** | < 1 GB (memory) | TB+ (distributed) |
# MAGIC | **Training** | Single core | Multi-node parallel |
# MAGIC | **Inference** | Sequential | Distributed batch |
# MAGIC | **Pipeline** | Pipeline API | ML Pipeline (stages) |
# MAGIC | **HPO** | GridSearchCV | CrossValidator |
# MAGIC | **Persistence** | pickle/joblib | Native Spark save |
# MAGIC | **Integration** | Pandas/NumPy | Spark DataFrame |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Deploy para Produção:
# MAGIC
# MAGIC ### **Opção 1: Batch Inference (Spark)**
# MAGIC
# MAGIC ```python
# MAGIC # Load pipeline
# MAGIC pipeline = PipelineModel.load("s3://models/churn_v1")
# MAGIC
# MAGIC # Load new data (distributed)
# MAGIC new_data = spark.read.parquet("s3://data/new_customers")
# MAGIC
# MAGIC # Distributed inference
# MAGIC predictions = pipeline.transform(new_data)
# MAGIC
# MAGIC # Save results (distributed write)
# MAGIC predictions.write.parquet("s3://predictions/churn_daily")
# MAGIC ```
# MAGIC
# MAGIC ### **Opção 2: Real-Time Inference (Model Serving)**
# MAGIC
# MAGIC ```python
# MAGIC # Registrar no MLflow
# MAGIC mlflow.spark.log_model(
# MAGIC     pipeline_model,
# MAGIC     "model",
# MAGIC     registered_model_name="customer_intelligence.gold.churn_sparkml"
# MAGIC )
# MAGIC
# MAGIC # Deploy via Databricks Model Serving
# MAGIC # UI: Machine Learning → Models → Serve Model
# MAGIC ```
# MAGIC
# MAGIC ### **Opção 3: Scheduled Job**
# MAGIC
# MAGIC ```python
# MAGIC # Databricks Job que roda este notebook daily
# MAGIC # Workflows → Jobs → Create Job
# MAGIC # Schedule: Daily at 2 AM
# MAGIC # Cluster: Job cluster (auto-scale)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Monitoramento:
# MAGIC
# MAGIC ### **Metrics para Track:**
# MAGIC 1. **Training metrics**: AUC, Accuracy, F1
# MAGIC 2. **Performance**: Training time, throughput
# MAGIC 3. **Data drift**: Input distribution changes
# MAGIC 4. **Model drift**: Prediction distribution changes
# MAGIC 5. **Business metrics**: Churn cost savings
# MAGIC
# MAGIC ### **Alerts:**
# MAGIC - AUC drops < threshold
# MAGIC - Training fails
# MAGIC - Data quality issues
# MAGIC - Inference latency spikes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Frase para Entrevista:
# MAGIC
# MAGIC *"Implementei distributed machine learning com PySpark MLlib usando ML Pipelines para encadeamento de transformações (VectorAssembler, StandardScaler, RandomForest), CrossValidator para hyperparameter tuning distribuído com 3-fold CV em paralelo, model persistence completo do pipeline serializado, e inference distribuído em larga escala com throughput de milhares de rows por segundo, tudo integrado com MLflow para tracking e Unity Catalog para model registry."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Key Capabilities Demonstradas:
# MAGIC
# MAGIC - ☑️ **Distributed Training** - Cluster parallelism
# MAGIC - ☑️ **ML Pipelines** - Encadeamento de stages
# MAGIC - ☑️ **CrossValidator** - Distributed HPO
# MAGIC - ☑️ **Model Persistence** - Save/load pipelines
# MAGIC - ☑️ **Feature Engineering** - VectorAssembler, Scaler
# MAGIC - ☑️ **Distributed Inference** - Batch predictions
# MAGIC - ☑️ **MLflow Integration** - Tracking
# MAGIC - ☑️ **Feature Importance** - Explainability
# MAGIC - ☑️ **Cache Optimization** - Performance tuning
# MAGIC - ☑️ **Production-Ready** - Complete workflow
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Próximos Passos:
# MAGIC
# MAGIC 1. ☐ **Deploy para Model Serving**
# MAGIC 2. ☐ **Configure monitoring & alerts**
# MAGIC 3. ☐ **Implement A/B testing**
# MAGIC 4. ☐ **Set up retraining pipeline**
# MAGIC 5. ☐ **Add feature store integration**
# MAGIC 6. ☐ **Optimize cluster sizing**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🎯 Projeto Customer Intelligence - SparkML Distributed Training COMPLETO!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Recursos:
# MAGIC
# MAGIC - [PySpark MLlib](https://spark.apache.org/docs/latest/ml-guide.html)
# MAGIC - [Databricks ML](https://docs.databricks.com/machine-learning/index.html)
# MAGIC - [Model Serving](https://docs.databricks.com/machine-learning/model-serving/index.html)
# MAGIC - [MLflow Spark](https://mlflow.org/docs/latest/python_api/mlflow.spark.html)

# COMMAND ----------


