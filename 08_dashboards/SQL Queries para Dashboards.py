# Databricks notebook source
# DBTITLE 1,SQL Queries para Dashboards
# MAGIC %md
# MAGIC # SQL Queries para Dashboards
# MAGIC
# MAGIC QueriesSQL prontas para criar dashboards executivos.
# MAGIC
# MAGIC ## Dashboards Recomendados:
# MAGIC 1. **CRM Dashboard**: Visão geral de clientes e churn
# MAGIC 2. **Campaign Performance**: Performance de campanhas
# MAGIC 3. **Growth Dashboard**: Métricas de crescimento
# MAGIC 4. **Executive Summary**: KPIs principais
# MAGIC
# MAGIC Copie as queries abaixo para criar dashboards no Databricks SQL.

# COMMAND ----------

# DBTITLE 1,1. Overview de Churn Risk
# MAGIC %sql
# MAGIC -- Dashboard 1: Churn Risk Overview
# MAGIC -- Top clientes com maior risco de churn
# MAGIC
# MAGIC SELECT 
# MAGIC   cs.customer_id,
# MAGIC   c.segment,
# MAGIC   c.customer_age_days,
# MAGIC   cf.recency_days,
# MAGIC   cf.frequency,
# MAGIC   cf.monetary_total,
# MAGIC   cs.churn_probability,
# MAGIC   cs.churn_risk_category,
# MAGIC   cs.customer_value_score,
# MAGIC   cs.engagement_score
# MAGIC FROM workspace.customer_intelligence_gold.customer_scores cs
# MAGIC INNER JOIN workspace.customer_intelligence_silver.customers c ON cs.customer_id = c.customer_id
# MAGIC INNER JOIN workspace.customer_intelligence_gold.customer_features cf ON cs.customer_id = cf.customer_id
# MAGIC WHERE cs.churn_risk_category = 'High'
# MAGIC   AND c.is_active = 1
# MAGIC ORDER BY cs.churn_probability DESC, cs.customer_value_score DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,2. Distribuição de Risco de Churn
# MAGIC %sql
# MAGIC -- Dashboard 2: Distribuição de Risco por Segmento
# MAGIC
# MAGIC SELECT 
# MAGIC   c.segment,
# MAGIC   cs.churn_risk_category,
# MAGIC   COUNT(*) as customer_count,
# MAGIC   AVG(cs.churn_probability) as avg_churn_prob,
# MAGIC   AVG(cs.customer_value_score) as avg_value_score,
# MAGIC   SUM(cf.monetary_total) as total_revenue_at_risk
# MAGIC FROM workspace.customer_intelligence_gold.customer_scores cs
# MAGIC INNER JOIN workspace.customer_intelligence_silver.customers c ON cs.customer_id = c.customer_id
# MAGIC INNER JOIN workspace.customer_intelligence_gold.customer_features cf ON cs.customer_id = cf.customer_id
# MAGIC GROUP BY c.segment, cs.churn_risk_category
# MAGIC ORDER BY c.segment, cs.churn_risk_category;

# COMMAND ----------

# DBTITLE 1,3. Campaign Performance Summary
# MAGIC %sql
# MAGIC -- Dashboard 3: Campaign Performance
# MAGIC
# MAGIC SELECT 
# MAGIC   campaign_id,
# MAGIC   campaign_name,
# MAGIC   Control_conversion_rate,
# MAGIC   Treatment_conversion_rate,
# MAGIC   lift_pct,
# MAGIC   uplift_absolute,
# MAGIC   incremental_revenue,
# MAGIC   is_significant_chi2 as is_significant,
# MAGIC   result_category,
# MAGIC   CASE 
# MAGIC     WHEN result_category = 'Strong Positive' THEN '🟢'
# MAGIC     WHEN result_category = 'Positive' THEN '🟡'
# MAGIC     WHEN result_category = 'Negative' THEN '🔴'
# MAGIC     ELSE '⚪'
# MAGIC   END as status_icon
# MAGIC FROM workspace.customer_intelligence_gold.campaign_ab_test_results
# MAGIC ORDER BY lift_pct DESC;

# COMMAND ----------

# DBTITLE 1,4. ROAS Analysis
# MAGIC %sql
# MAGIC -- Dashboard 4: ROAS (Return on Ad Spend)
# MAGIC
# MAGIC SELECT 
# MAGIC   campaign_id,
# MAGIC   campaign_name,
# MAGIC   budget,
# MAGIC   incremental_revenue,
# MAGIC   roas,
# MAGIC   roi_pct,
# MAGIC   result_category,
# MAGIC   CASE 
# MAGIC     WHEN roas >= 3 THEN 'Excellent (3x+)'
# MAGIC     WHEN roas >= 2 THEN 'Good (2-3x)'
# MAGIC     WHEN roas >= 1 THEN 'Breakeven (1-2x)'
# MAGIC     ELSE 'Loss (<1x)'
# MAGIC   END as roas_category
# MAGIC FROM workspace.customer_intelligence_gold.campaign_roas
# MAGIC WHERE result_category IN ('Positive', 'Strong Positive')
# MAGIC ORDER BY roas DESC;

# COMMAND ----------

# DBTITLE 1,5. KPIs Executivos
# MAGIC %sql
# MAGIC -- Dashboard 5: Executive KPIs
# MAGIC
# MAGIC SELECT 
# MAGIC   'Total Customers' as metric,
# MAGIC   total_customers as value,
# MAGIC   '' as change_pct
# MAGIC FROM workspace.customer_intelligence_gold.business_kpis_history
# MAGIC WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM workspace.customer_intelligence_gold.business_kpis_history)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Active Customers',
# MAGIC   active_customers,
# MAGIC   CAST(ROUND((active_customers * 100.0 / total_customers), 1) AS STRING) || '%'
# MAGIC FROM workspace.customer_intelligence_gold.business_kpis_history
# MAGIC WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM workspace.customer_intelligence_gold.business_kpis_history)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Churn Rate',
# MAGIC   churned_customers,
# MAGIC   CAST(churn_rate_pct AS STRING) || '%'
# MAGIC FROM workspace.customer_intelligence_gold.business_kpis_history
# MAGIC WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM workspace.customer_intelligence_gold.business_kpis_history)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Total Revenue',
# MAGIC   CAST(total_revenue AS BIGINT),
# MAGIC   ''
# MAGIC FROM workspace.customer_intelligence_gold.business_kpis_history
# MAGIC WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM workspace.customer_intelligence_gold.business_kpis_history);

# COMMAND ----------

# DBTITLE 1,6. Customer Segmentation RFM
# MAGIC %sql
# MAGIC -- Dashboard 6: RFM Segmentation
# MAGIC
# MAGIC SELECT 
# MAGIC   c.segment,
# MAGIC   COUNT(*) as customer_count,
# MAGIC   AVG(rf.recency_days) as avg_recency,
# MAGIC   AVG(rf.frequency) as avg_frequency,
# MAGIC   AVG(rf.monetary_total) as avg_monetary,
# MAGIC   SUM(rf.monetary_total) as total_revenue,
# MAGIC   AVG(cs.churn_probability) as avg_churn_risk,
# MAGIC   AVG(cs.engagement_score) as avg_engagement
# MAGIC FROM workspace.customer_intelligence_silver.customers c
# MAGIC INNER JOIN workspace.customer_intelligence_gold.rfm_features rf ON c.customer_id = rf.customer_id
# MAGIC INNER JOIN workspace.customer_intelligence_gold.customer_scores cs ON c.customer_id = cs.customer_id
# MAGIC WHERE c.is_active = 1
# MAGIC GROUP BY c.segment
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,7. Monthly Trends
# MAGIC %sql
# MAGIC -- Dashboard 7: Monthly Trends
# MAGIC
# MAGIC SELECT 
# MAGIC   year_month,
# MAGIC   total_exposures,
# MAGIC   total_conversions,
# MAGIC   ROUND(conversion_rate, 2) as conversion_rate_pct,
# MAGIC   LAG(conversion_rate) OVER (ORDER BY year_month) as prev_month_rate,
# MAGIC   ROUND(conversion_rate - LAG(conversion_rate) OVER (ORDER BY year_month), 2) as rate_change
# MAGIC FROM workspace.customer_intelligence_gold.campaign_performance_trend
# MAGIC ORDER BY year_month DESC
# MAGIC LIMIT 12;

# COMMAND ----------

