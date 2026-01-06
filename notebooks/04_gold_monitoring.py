# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Monitoring & Drift Detection
# MAGIC 
# MAGIC **Purpose:** Comprehensive data quality monitoring and drift detection for Gold Layer.
# MAGIC 
# MAGIC **Frequency:** Run daily after Gold Layer refresh (via Databricks Workflow)
# MAGIC 
# MAGIC **Author:** Senior Analytics Engineer  
# MAGIC **Date:** 2025-01-06

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.getOrCreate()

# Import monitoring utilities
sys.path.insert(0, "/Workspace/Users/diego.mayorgacapera@gmail.com/.bundle/BI_Market_Visibility/dev/files")
from src.utils.gold_quality_monitoring import (
    run_gold_layer_monitoring,
    print_monitoring_report,
    dq_check_null_rates,
    dq_check_value_ranges,
    dq_check_uniqueness,
    dd_detect_kpi_anomalies,
    fm_check_data_freshness,
    br_validate_market_share_sums,
    br_validate_price_competitiveness,
    br_validate_stock_health,
    xl_reconcile_aggregates
)

SCHEMA = "workspace.default"
print("Setup complete - Monitoring utilities loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run Comprehensive Monitoring Suite

# COMMAND ----------

# Execute full monitoring suite
monitoring_results = run_gold_layer_monitoring(spark, schema=SCHEMA)

# Print formatted report
print_monitoring_report(monitoring_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Save Results to Delta (Audit Trail)

# COMMAND ----------

import json
from pyspark.sql.functions import lit, current_timestamp

# Convert results to DataFrame for audit trail
results_json = json.dumps(monitoring_results)

audit_df = spark.createDataFrame([{
    "run_id": monitoring_results["monitoring_run_id"],
    "overall_status": monitoring_results.get("overall_status", "UNKNOWN"),
    "total_checks": monitoring_results.get("summary", {}).get("total_checks", 0),
    "passed": monitoring_results.get("summary", {}).get("passed", 0),
    "warnings": monitoring_results.get("summary", {}).get("warnings", 0),
    "failed": monitoring_results.get("summary", {}).get("failed", 0),
    "results_json": results_json
}]).withColumn("execution_timestamp", current_timestamp())

# Append to audit table
(
    audit_df
    .write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{SCHEMA}.gold_monitoring_audit")
)

print(f"Monitoring results saved to {SCHEMA}.gold_monitoring_audit")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Alert on Failures (Optional: Integrate with Slack/Email)

# COMMAND ----------

overall_status = monitoring_results.get("overall_status", "UNKNOWN")

if overall_status == "FAIL":
    # In production: Send alert to Slack/Email/PagerDuty
    print("ALERT: Gold Layer monitoring detected FAILURES!")
    print("   Action required: Review monitoring report above")
    
    # Raise exception to fail the job (triggers Databricks alert)
    # raise Exception("Gold Layer monitoring FAILED - see report for details")
    
elif overall_status == "WARN":
    print("WARNING: Gold Layer monitoring detected issues")
    print("   Review recommended but not critical")
    
else:
    print("Gold Layer monitoring PASSED - all checks healthy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Historical Trend Analysis

# COMMAND ----------

# Query audit history
audit_history = spark.sql(f"""
    SELECT 
        date(execution_timestamp) as run_date,
        overall_status,
        total_checks,
        passed,
        warnings,
        failed,
        round(passed / total_checks * 100, 1) as pass_rate_pct
    FROM {SCHEMA}.gold_monitoring_audit
    ORDER BY execution_timestamp DESC
    LIMIT 30
""")

display(audit_history)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Individual Check Deep Dive (Optional)

# COMMAND ----------

# Example: Run individual checks for deeper analysis
gold_fact_sell_in = spark.read.table(f"{SCHEMA}.gold_fact_sell_in")

# Detailed null check
null_results = dq_check_null_rates(
    gold_fact_sell_in,
    ["date_sk", "product_sk", "pdv_sk", "quantity_sell_in", "value_sell_in"],
    "gold_fact_sell_in",
    threshold_pct=0.5  # Stricter threshold
)

print("Detailed Null Rate Analysis:")
for detail in null_results.get("details", []):
    print(f"  {detail['column']}: {detail['null_pct']}% nulls ({detail['status']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cross-Layer Reconciliation Details

# COMMAND ----------

# Reconcile Silver vs Gold aggregates
silver_sell_in = spark.read.table(f"{SCHEMA}.silver_sell_in")
gold_fact_sell_in = spark.read.table(f"{SCHEMA}.gold_fact_sell_in")

reconciliation = xl_reconcile_aggregates(
    silver_sell_in,
    gold_fact_sell_in,
    "quantity",
    "quantity_sell_in",
    "silver_sell_in",
    "gold_fact_sell_in",
    tolerance_pct=0.1  # Very strict for financial data
)

print("Cross-Layer Reconciliation:")
print(f"  Silver Sum: {reconciliation['silver_sum']:,.0f}")
print(f"  Gold Sum: {reconciliation['gold_sum']:,.0f}")
print(f"  Variance: {reconciliation['variance_pct']:.4f}%")
print(f"  Status: {reconciliation['status']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **End of Gold Layer Monitoring Notebook**
