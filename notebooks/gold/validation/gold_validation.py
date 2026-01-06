# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Validation (Read-Only)
# MAGIC 
# MAGIC **Purpose:** Post-execution validation of Gold Layer tables using Delta metadata and lightweight queries. Designed for operational monitoring and alerting.
# MAGIC 
# MAGIC **Author:** Diego Mayorga  
# MAGIC **Date:** 2026-01-06  
# MAGIC **Project:** BI Market Visibility Analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Design Philosophy
# MAGIC 
# MAGIC **This notebook is READ-ONLY.**
# MAGIC 
# MAGIC No writes, no modifications - only validation checks:
# MAGIC 
# MAGIC 1. **Metadata Validation** - Delta History (zero compute)
# MAGIC 2. **Referential Integrity** - FK relationships
# MAGIC 3. **Business Rules** - KPI sanity checks
# MAGIC 4. **Freshness Checks** - Data recency
# MAGIC 
# MAGIC **Serverless Optimizations:**
# MAGIC - Primary validation via Delta History (no DataFrame scans)
# MAGIC - Sampled checks where necessary
# MAGIC - Early exit on critical failures

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql.functions import col, max as spark_max, min as spark_min, count as spark_count
from datetime import datetime, timedelta

# Unity Catalog
CATALOG = "workspace"
SCHEMA = "default"

# Gold Layer Tables
GOLD_TABLES = {
    # Dimensions
    "dim_date": f"{CATALOG}.{SCHEMA}.gold_dim_date",
    "dim_product": f"{CATALOG}.{SCHEMA}.gold_dim_product",
    "dim_pdv": f"{CATALOG}.{SCHEMA}.gold_dim_pdv",
    # Facts
    "fact_sell_in": f"{CATALOG}.{SCHEMA}.gold_fact_sell_in",
    "fact_price_audit": f"{CATALOG}.{SCHEMA}.gold_fact_price_audit",
    "fact_stock": f"{CATALOG}.{SCHEMA}.gold_fact_stock",
    # KPIs
    "kpi_market_visibility": f"{CATALOG}.{SCHEMA}.gold_kpi_market_visibility_daily",
    "kpi_market_share": f"{CATALOG}.{SCHEMA}.gold_kpi_market_share"
}

# Validation thresholds
FRESHNESS_THRESHOLD_HOURS = 24
MIN_EXPECTED_ROWS = {
    "dim_date": 1000,
    "dim_product": 10,
    "dim_pdv": 10,
    "fact_sell_in": 100,
    "fact_price_audit": 100
}

# Results accumulator
validation_results = []

print("🔍 Gold Layer Validation Started")
print(f"   Tables to validate: {len(GOLD_TABLES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Metadata Validation (Zero-Compute)

# COMMAND ----------

def validate_table_exists(table_name, table_path):
    """Check if table exists using catalog."""
    try:
        exists = spark._jsparkSession.catalog().tableExists(table_path)
        return {"table": table_name, "check": "exists", "passed": exists, "details": table_path}
    except Exception as e:
        return {"table": table_name, "check": "exists", "passed": False, "details": str(e)}


def validate_table_freshness(table_name, table_path, threshold_hours):
    """Check table freshness using Delta History."""
    try:
        history = spark.sql(f"DESCRIBE HISTORY {table_path} LIMIT 1").first()
        last_update = history["timestamp"]
        
        # Check if update is within threshold
        threshold_time = datetime.now() - timedelta(hours=threshold_hours)
        is_fresh = last_update > threshold_time
        
        return {
            "table": table_name, 
            "check": "freshness", 
            "passed": is_fresh,
            "details": f"Last update: {last_update}, Threshold: {threshold_hours}h"
        }
    except Exception as e:
        return {"table": table_name, "check": "freshness", "passed": False, "details": str(e)}


def validate_row_count(table_name, table_path, min_rows):
    """Check minimum row count using Delta History metrics."""
    try:
        history = spark.sql(f"DESCRIBE HISTORY {table_path} LIMIT 1").first()
        metrics = history["operationMetrics"] or {}
        rows = int(metrics.get("numOutputRows", metrics.get("numRows", 0)))
        
        passed = rows >= min_rows
        return {
            "table": table_name,
            "check": "row_count",
            "passed": passed,
            "details": f"Rows: {rows}, Min expected: {min_rows}"
        }
    except Exception as e:
        return {"table": table_name, "check": "row_count", "passed": False, "details": str(e)}

# Run metadata validations
print("=" * 60)
print("METADATA VALIDATION (Zero-Compute)")
print("=" * 60)

for name, path in GOLD_TABLES.items():
    # Existence check
    result = validate_table_exists(name, path)
    validation_results.append(result)
    status = "✅" if result["passed"] else "❌"
    print(f"{status} {name}: exists = {result['passed']}")
    
    if result["passed"]:
        # Freshness check
        fresh_result = validate_table_freshness(name, path, FRESHNESS_THRESHOLD_HOURS)
        validation_results.append(fresh_result)
        status = "✅" if fresh_result["passed"] else "⚠️"
        print(f"   {status} freshness: {fresh_result['details']}")
        
        # Row count check (if threshold defined)
        if name in MIN_EXPECTED_ROWS:
            count_result = validate_row_count(name, path, MIN_EXPECTED_ROWS[name])
            validation_results.append(count_result)
            status = "✅" if count_result["passed"] else "❌"
            print(f"   {status} row_count: {count_result['details']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Referential Integrity Checks

# COMMAND ----------

def check_referential_integrity(fact_table, dim_table, fact_fk, dim_pk, sample_size=1000):
    """
    Check if FK values in fact table exist in dimension table.
    Uses sampling to avoid full table scans.
    """
    try:
        # Sample fact table
        df_fact_sample = spark.read.table(fact_table) \
            .select(fact_fk).distinct().limit(sample_size)
        
        # Get dimension PKs
        df_dim = spark.read.table(dim_table).select(dim_pk)
        
        # Find orphans
        df_orphans = df_fact_sample.join(
            df_dim,
            df_fact_sample[fact_fk] == df_dim[dim_pk],
            "left_anti"
        )
        
        orphan_count = df_orphans.count()
        passed = orphan_count == 0
        
        return {
            "check": f"RI: {fact_table.split('.')[-1]}.{fact_fk} -> {dim_table.split('.')[-1]}.{dim_pk}",
            "passed": passed,
            "details": f"Orphan records: {orphan_count} (sampled {sample_size})"
        }
    except Exception as e:
        return {
            "check": f"RI: {fact_fk} -> {dim_pk}",
            "passed": False,
            "details": str(e)
        }

print("\n" + "=" * 60)
print("REFERENTIAL INTEGRITY CHECKS")
print("=" * 60)

# Define RI checks
ri_checks = [
    (GOLD_TABLES["fact_sell_in"], GOLD_TABLES["dim_date"], "date_sk", "date_sk"),
    (GOLD_TABLES["fact_sell_in"], GOLD_TABLES["dim_product"], "product_sk", "product_sk"),
    (GOLD_TABLES["fact_sell_in"], GOLD_TABLES["dim_pdv"], "pdv_sk", "pdv_sk"),
    (GOLD_TABLES["fact_price_audit"], GOLD_TABLES["dim_date"], "date_sk", "date_sk"),
    (GOLD_TABLES["fact_price_audit"], GOLD_TABLES["dim_product"], "product_sk", "product_sk"),
]

for fact_table, dim_table, fact_fk, dim_pk in ri_checks:
    try:
        result = check_referential_integrity(fact_table, dim_table, fact_fk, dim_pk)
        validation_results.append(result)
        status = "✅" if result["passed"] else "⚠️"
        print(f"{status} {result['check']}: {result['details']}")
    except Exception as e:
        print(f"❌ RI check failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Business Rule Validation

# COMMAND ----------

def validate_kpi_ranges(table_path, column, min_val, max_val, sample_size=1000):
    """Check if KPI values are within expected business ranges."""
    try:
        df = spark.read.table(table_path).select(column).limit(sample_size)
        
        agg = df.agg(
            spark_min(column).alias("min_val"),
            spark_max(column).alias("max_val")
        ).first()
        
        actual_min = agg["min_val"]
        actual_max = agg["max_val"]
        
        passed = (actual_min >= min_val or actual_min is None) and \
                 (actual_max <= max_val or actual_max is None)
        
        return {
            "check": f"KPI range: {column}",
            "passed": passed,
            "details": f"Range: [{actual_min}, {actual_max}], Expected: [{min_val}, {max_val}]"
        }
    except Exception as e:
        return {"check": f"KPI range: {column}", "passed": False, "details": str(e)}

print("\n" + "=" * 60)
print("BUSINESS RULE VALIDATION")
print("=" * 60)

# Define business rules
business_rules = [
    # KPI Market Visibility
    (GOLD_TABLES["kpi_market_visibility"], "availability_rate", 0, 100),
    (GOLD_TABLES["kpi_market_visibility"], "price_competitiveness_index", 0, 500),
    (GOLD_TABLES["kpi_market_visibility"], "stock_health_score", 0, 100),
    
    # KPI Market Share
    (GOLD_TABLES["kpi_market_share"], "portfolio_share_qty_pct", 0, 100),
    (GOLD_TABLES["kpi_market_share"], "portfolio_share_value_pct", 0, 100),
    
    # Fact Price Audit
    (GOLD_TABLES["fact_price_audit"], "price_index", 0, 500),
]

for table, column, min_val, max_val in business_rules:
    try:
        result = validate_kpi_ranges(table, column, min_val, max_val)
        validation_results.append(result)
        status = "✅" if result["passed"] else "⚠️"
        print(f"{status} {result['check']}: {result['details']}")
    except Exception as e:
        print(f"❌ Rule check failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validation Summary

# COMMAND ----------

# Calculate summary statistics
total_checks = len(validation_results)
passed_checks = sum(1 for r in validation_results if r["passed"])
failed_checks = total_checks - passed_checks
pass_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0

print("\n" + "=" * 60)
print("VALIDATION SUMMARY")
print("=" * 60)
print(f"Total Checks:  {total_checks}")
print(f"Passed:        {passed_checks} ✅")
print(f"Failed:        {failed_checks} ❌")
print(f"Pass Rate:     {pass_rate:.1f}%")
print("=" * 60)

# Overall status
if failed_checks == 0:
    print("\n🎉 ALL VALIDATIONS PASSED - Gold Layer is healthy!")
    overall_status = "HEALTHY"
elif failed_checks <= 2:
    print("\n⚠️ WARNINGS DETECTED - Review failed checks")
    overall_status = "WARNING"
else:
    print("\n🚨 CRITICAL FAILURES - Immediate investigation required")
    overall_status = "CRITICAL"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Failed Checks Detail

# COMMAND ----------

failed_results = [r for r in validation_results if not r["passed"]]

if failed_results:
    print("=" * 60)
    print("FAILED CHECKS DETAIL")
    print("=" * 60)
    for result in failed_results:
        table = result.get("table", "N/A")
        check = result.get("check", "Unknown")
        details = result.get("details", "No details")
        print(f"\n❌ {table} - {check}")
        print(f"   Details: {details}")
else:
    print("\n✅ No failed checks to report")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Export Validation Results (Optional)

# COMMAND ----------

# Create DataFrame from results for potential storage/alerting
from pyspark.sql import Row

df_results = spark.createDataFrame([
    Row(
        table=r.get("table", ""),
        check_type=r.get("check", ""),
        passed=r["passed"],
        details=r["details"],
        validation_timestamp=datetime.now().isoformat(),
        overall_status=overall_status
    )
    for r in validation_results
])

# Display results
df_results.display()

# Optionally write to a validation log table
# df_results.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_validation_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Alerting Integration
# MAGIC 
# MAGIC ### Slack/Teams Integration (Example)
# MAGIC 
# MAGIC ```python
# MAGIC # Uncomment and configure for production alerting
# MAGIC import requests
# MAGIC 
# MAGIC if overall_status == "CRITICAL":
# MAGIC     webhook_url = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
# MAGIC     message = {
# MAGIC         "text": f"🚨 Gold Layer Validation CRITICAL\n"
# MAGIC                 f"Failed checks: {failed_checks}/{total_checks}\n"
# MAGIC                 f"Details: {[r['check'] for r in failed_results]}"
# MAGIC     }
# MAGIC     requests.post(webhook_url, json=message)
# MAGIC ```
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Author:** Diego Mayorga | **Date:** 2026-01-06
