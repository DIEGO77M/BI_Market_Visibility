# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Schema Drift Detection
# MAGIC 
# MAGIC **Purpose:** Autonomous schema drift monitoring for Bronze tables using Delta History as single source of truth. Zero coupling with ingestion pipeline.
# MAGIC 
# MAGIC **Author:** Diego Mayorga  
# MAGIC **Date:** 2025-12-31  
# MAGIC **Project:** BI Market Visibility Analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Architecture: Zero-Coupling Design
# MAGIC 
# MAGIC **Philosophy:** Drift monitoring is a **read-only observer** that never modifies data pipelines.
# MAGIC 
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────────┐
# MAGIC │  Bronze Ingestion (notebooks/01_bronze_ingestion.py)      │
# MAGIC │  - Ingests data from CSV/Excel                            │
# MAGIC │  - Writes Delta tables                                    │
# MAGIC │  - NO schema logging code                                 │
# MAGIC │  - NO dependency on monitoring                            │
# MAGIC └─────────────────────┬──────────────────────────────────────┘
# MAGIC                       │
# MAGIC                       │ Writes to Delta Lake
# MAGIC                       ▼
# MAGIC ┌────────────────────────────────────────────────────────────┐
# MAGIC │  Delta Lake (Storage Layer)                               │
# MAGIC │  - Delta tables: bronze_master_pdv, bronze_price_audit... │
# MAGIC │  - Transaction logs (_delta_log/)                         │
# MAGIC │  - Schema evolution metadata                              │
# MAGIC └─────────────────────┬──────────────────────────────────────┘
# MAGIC                       │
# MAGIC                       │ Reads Delta History (metadata-only)
# MAGIC                       ▼
# MAGIC ┌────────────────────────────────────────────────────────────┐
# MAGIC │  Drift Monitoring (this notebook)                         │
# MAGIC │  - DESCRIBE HISTORY → extract schemas                     │
# MAGIC │  - Compare consecutive versions                           │
# MAGIC │  - Classify severity (HIGH/MEDIUM/LOW)                    │
# MAGIC │  - Write alerts → bronze_schema_alerts                    │
# MAGIC └────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC 
# MAGIC **Key Benefits:**
# MAGIC - ✅ **Zero Coupling:** Bronze doesn't know monitoring exists
# MAGIC - ✅ **Single Source of Truth:** Delta History is authoritative
# MAGIC - ✅ **Non-Blocking:** Monitoring failure doesn't affect ingestion
# MAGIC - ✅ **Self-Healing:** Monitoring can backfill history from Delta logs
# MAGIC - ✅ **Zero Additional Cost:** Uses existing Delta metadata
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Drift Detection Strategy
# MAGIC 
# MAGIC ### What is Monitored
# MAGIC 
# MAGIC | Table | Critical Columns | Alert Logic |
# MAGIC |-------|------------------|-------------|
# MAGIC | `bronze_price_audit` | PDV_Code, Product_SKU, Audit_Date, Observed_Price | NEW columns = LOW, REMOVED columns = HIGH |
# MAGIC | `bronze_sell_in` | Year, Product, PDV, Quantity, Amount | NEW columns = LOW, REMOVED columns = HIGH |
# MAGIC | `bronze_master_pdv` | PDV_Code, PDV_Name | NEW columns = LOW, REMOVED columns = MEDIUM |
# MAGIC | `bronze_master_products` | Product_SKU, Product_Name | NEW columns = LOW, REMOVED columns = MEDIUM |
# MAGIC 
# MAGIC ### Severity Classification
# MAGIC 
# MAGIC - **HIGH 🚨:** Critical column removed (breaks Silver transformations)
# MAGIC - **MEDIUM ⚠️:** Non-critical column removed (potential data loss)
# MAGIC - **LOW ℹ️:** New columns added (schema extension, no breakage)
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Execution Schedule
# MAGIC 
# MAGIC **Recommended:** Daily at 3:05 AM (5 minutes after Bronze ingestion)
# MAGIC 
# MAGIC ```yaml
# MAGIC Databricks Job: Bronze_Pipeline_Daily
# MAGIC Task 1: bronze_ingestion (3:00 AM)
# MAGIC Task 2: drift_monitoring_bronze (depends on Task 1 SUCCESS)
# MAGIC ```
# MAGIC 
# MAGIC **Runtime:** ~1-2 minutes (metadata operations only, no data scanning)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, current_timestamp, array, size
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, ArrayType
import json

# Unity Catalog configuration
CATALOG = "workspace"
SCHEMA = "default"

# Bronze tables to monitor
BRONZE_TABLES = [
    "bronze_master_pdv",
    "bronze_master_products", 
    "bronze_price_audit",
    "bronze_sell_in"
]

# Critical columns by table (used for severity classification)
CRITICAL_COLUMNS = {
    "bronze_price_audit": ["PDV_Code", "Product_SKU", "Audit_Date", "Observed_Price"],
    "bronze_sell_in": ["Year", "Product", "PDV", "Quantity", "Amount"],
    "bronze_master_pdv": ["PDV_Code", "PDV_Name"],
    "bronze_master_products": ["Product_SKU", "Product_Name"]
}

# Alert table
ALERT_TABLE = f"{CATALOG}.{SCHEMA}.bronze_schema_alerts"

# Lookback period for drift detection (compare current vs previous version)
LOOKBACK_VERSIONS = 2  # Compare last 2 versions

print("🔍 Drift Monitoring Configuration")
print(f"   Catalog: {CATALOG}")
print(f"   Schema: {SCHEMA}")
print(f"   Tables: {len(BRONZE_TABLES)}")
print(f"   Alert Table: {ALERT_TABLE}")
print(f"   Lookback: {LOOKBACK_VERSIONS} versions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Alert Table (Idempotent)

# COMMAND ----------

# Create alert table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ALERT_TABLE} (
    alert_id STRING COMMENT 'Unique alert identifier',
    table_name STRING COMMENT 'Bronze table name',
    version_current BIGINT COMMENT 'Current Delta version',
    version_previous BIGINT COMMENT 'Previous Delta version compared',
    new_columns ARRAY<STRING> COMMENT 'Columns added in current version',
    removed_columns ARRAY<STRING> COMMENT 'Columns removed from previous version',
    detected_timestamp TIMESTAMP COMMENT 'When drift was detected',
    alert_type STRING COMMENT 'NEW_COLUMNS or REMOVED_COLUMNS',
    severity STRING COMMENT 'HIGH, MEDIUM, or LOW',
    critical_columns_affected BOOLEAN COMMENT 'Whether critical columns were impacted',
    action_required STRING COMMENT 'Recommended action for data engineers',
    layer STRING COMMENT 'Medallion layer (always bronze)',
    notified BOOLEAN COMMENT 'Whether alert notification was sent (future)'
)
USING DELTA
COMMENT 'Schema drift alerts for Bronze layer tables'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality.dimension' = 'schema_stability',
    'monitoring.type' = 'drift_detection'
)
""")

print(f"✅ Alert table ready: {ALERT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema Extraction Functions

# COMMAND ----------

def get_table_schema_from_delta(table_name, catalog, schema):
    """
    Extract current schema directly from Delta table metadata.
    Uses DESCRIBE TABLE (zero-compute operation).
    
    Args:
        table_name: Table name without catalog/schema prefix
        catalog: Unity Catalog name
        schema: Schema name
        
    Returns:
        dict with schema metadata
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    try:
        # Get current schema from DESCRIBE TABLE
        schema_info = spark.sql(f"DESCRIBE TABLE {full_table_name}").collect()
        
        # Filter business columns (exclude metadata and partition columns)
        business_columns = sorted([
            row.col_name 
            for row in schema_info 
            if not row.col_name.startswith('_metadata_') 
            and row.col_name not in ['year', 'year_month', '# Partition Information', 'Part 0']
            and not row.col_name.startswith('#')
        ])
        
        # Get latest version from Delta History
        history = spark.sql(f"DESCRIBE HISTORY {full_table_name} LIMIT 1").collect()
        current_version = history[0].version if history else 0
        current_timestamp = history[0].timestamp if history else datetime.now()
        
        return {
            "table_name": table_name,
            "full_table_name": full_table_name,
            "version": current_version,
            "timestamp": current_timestamp,
            "column_count": len(business_columns),
            "columns": business_columns
        }
        
    except Exception as e:
        print(f"⚠️  Error extracting schema for {table_name}: {str(e)}")
        return None


def get_previous_schema_from_delta_history(table_name, catalog, schema, current_version):
    """
    Extract previous schema version from Delta History.
    Uses transaction log metadata (zero-compute).
    
    Args:
        table_name: Table name without catalog/schema prefix
        catalog: Unity Catalog name
        schema: Schema name
        current_version: Current version number
        
    Returns:
        dict with previous schema metadata or None
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    try:
        # Get previous version from history
        history = spark.sql(f"""
            DESCRIBE HISTORY {full_table_name} 
            WHERE version < {current_version}
            ORDER BY version DESC
            LIMIT 1
        """).collect()
        
        if not history:
            print(f"   ℹ️  No previous version found for {table_name} (likely first run)")
            return None
        
        prev_version = history[0].version
        
        # Restore table to previous version temporarily to get schema
        temp_df = spark.read.format("delta").option("versionAsOf", prev_version).table(full_table_name)
        
        # Extract business columns
        business_columns = sorted([
            c for c in temp_df.columns 
            if not c.startswith('_metadata_') 
            and c not in ['year', 'year_month']
        ])
        
        return {
            "table_name": table_name,
            "version": prev_version,
            "timestamp": history[0].timestamp,
            "column_count": len(business_columns),
            "columns": business_columns
        }
        
    except Exception as e:
        print(f"   ⚠️  Error extracting previous schema for {table_name}: {str(e)}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Drift Detection Logic

# COMMAND ----------

def detect_schema_drift(current_schema, previous_schema):
    """
    Compare two schema snapshots and identify drift.
    
    Args:
        current_schema: dict from get_table_schema_from_delta()
        previous_schema: dict from get_previous_schema_from_delta_history()
        
    Returns:
        dict with drift analysis
    """
    if not previous_schema:
        return {
            "has_drift": False,
            "reason": "no_previous_version"
        }
    
    current_cols = set(current_schema["columns"])
    previous_cols = set(previous_schema["columns"])
    
    new_columns = sorted(list(current_cols - previous_cols))
    removed_columns = sorted(list(previous_cols - current_cols))
    
    has_drift = len(new_columns) > 0 or len(removed_columns) > 0
    
    return {
        "has_drift": has_drift,
        "new_columns": new_columns,
        "removed_columns": removed_columns,
        "current_version": current_schema["version"],
        "previous_version": previous_schema["version"],
        "current_timestamp": current_schema["timestamp"],
        "column_count_current": current_schema["column_count"],
        "column_count_previous": previous_schema["column_count"]
    }


def classify_drift_severity(drift_info, table_name, critical_columns_config):
    """
    Classify drift severity based on impact to critical columns.
    
    Args:
        drift_info: dict from detect_schema_drift()
        table_name: Name of the table
        critical_columns_config: dict mapping table names to critical columns
        
    Returns:
        tuple (severity: str, action_required: str, critical_affected: bool)
    """
    removed = set(drift_info.get("removed_columns", []))
    critical_cols = set(critical_columns_config.get(table_name, []))
    
    critical_removed = removed.intersection(critical_cols)
    
    if critical_removed:
        # Critical column removed = HIGH severity
        severity = "HIGH"
        action_required = f"URGENT: Critical columns removed: {list(critical_removed)}. Verify source system change and update Silver layer validation."
        critical_affected = True
        
    elif removed:
        # Non-critical column removed = MEDIUM severity
        severity = "MEDIUM"
        action_required = f"WARNING: Columns removed: {list(removed)}. Check Silver layer dependencies and verify change is intentional."
        critical_affected = False
        
    else:
        # Only new columns = LOW severity
        severity = "LOW"
        action_required = f"INFO: New columns added: {drift_info.get('new_columns', [])}. Document in data dictionary and consider Silver layer extension."
        critical_affected = False
    
    return severity, action_required, critical_affected

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Alert Generation

# COMMAND ----------

def generate_alert_record(table_name, drift_info, severity, action_required, critical_affected):
    """
    Create alert record for insertion into bronze_schema_alerts table.
    
    Args:
        table_name: Name of the table
        drift_info: dict from detect_schema_drift()
        severity: str (HIGH/MEDIUM/LOW)
        action_required: str with recommended actions
        critical_affected: bool indicating critical column impact
        
    Returns:
        dict representing alert record
    """
    alert_id = f"{table_name}_{drift_info['current_version']}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Determine primary alert type
    if drift_info.get("removed_columns"):
        alert_type = "REMOVED_COLUMNS"
    else:
        alert_type = "NEW_COLUMNS"
    
    return {
        "alert_id": alert_id,
        "table_name": table_name,
        "version_current": drift_info["current_version"],
        "version_previous": drift_info["previous_version"],
        "new_columns": drift_info.get("new_columns", []),
        "removed_columns": drift_info.get("removed_columns", []),
        "detected_timestamp": datetime.now(),
        "alert_type": alert_type,
        "severity": severity,
        "critical_columns_affected": critical_affected,
        "action_required": action_required,
        "layer": "bronze",
        "notified": False  # Future: Slack/email integration
    }


def write_alert(alert_record, alert_table):
    """
    Write alert to Delta table.
    
    Args:
        alert_record: dict from generate_alert_record()
        alert_table: Full table name for alerts
    """
    alert_df = spark.createDataFrame([alert_record])
    
    alert_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(alert_table)
    
    severity_emoji = {"HIGH": "🚨", "MEDIUM": "⚠️", "LOW": "ℹ️"}.get(alert_record["severity"], "📋")
    print(f"   {severity_emoji} Alert logged: {alert_record['severity']} - {alert_record['alert_type']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Main Monitoring Loop

# COMMAND ----------

print("=" * 80)
print("🔍 BRONZE LAYER - SCHEMA DRIFT MONITORING")
print("=" * 80)
print(f"⏰ Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📊 Monitoring {len(BRONZE_TABLES)} Bronze tables")
print("-" * 80)

drift_detected_count = 0
alerts_generated = 0

for table_name in BRONZE_TABLES:
    print(f"\n🔎 Analyzing: {table_name}")
    print("-" * 60)
    
    # Step 1: Get current schema
    current_schema = get_table_schema_from_delta(table_name, CATALOG, SCHEMA)
    
    if not current_schema:
        print(f"   ⚠️  Skipping {table_name} (schema extraction failed)")
        continue
    
    print(f"   📌 Current Version: {current_schema['version']}")
    print(f"   📋 Current Columns: {current_schema['column_count']} business columns")
    
    # Step 2: Get previous schema
    previous_schema = get_previous_schema_from_delta_history(
        table_name, CATALOG, SCHEMA, current_schema['version']
    )
    
    if not previous_schema:
        print(f"   ℹ️  No drift detection (first run or no previous version)")
        continue
    
    print(f"   📌 Previous Version: {previous_schema['version']}")
    print(f"   📋 Previous Columns: {previous_schema['column_count']} business columns")
    
    # Step 3: Detect drift
    drift_info = detect_schema_drift(current_schema, previous_schema)
    
    if not drift_info["has_drift"]:
        print(f"   ✅ No drift detected (schema stable)")
        continue
    
    # Drift detected!
    drift_detected_count += 1
    print(f"   🔔 DRIFT DETECTED!")
    
    if drift_info["new_columns"]:
        print(f"      ➕ New Columns: {drift_info['new_columns']}")
    
    if drift_info["removed_columns"]:
        print(f"      ➖ Removed Columns: {drift_info['removed_columns']}")
    
    # Step 4: Classify severity
    severity, action_required, critical_affected = classify_drift_severity(
        drift_info, table_name, CRITICAL_COLUMNS
    )
    
    print(f"   📊 Severity: {severity}")
    print(f"   📝 Action: {action_required[:80]}...")
    
    # Step 5: Generate and write alert
    alert_record = generate_alert_record(
        table_name, drift_info, severity, action_required, critical_affected
    )
    
    write_alert(alert_record, ALERT_TABLE)
    alerts_generated += 1

print("\n" + "=" * 80)
print("📊 MONITORING SUMMARY")
print("=" * 80)
print(f"✅ Tables Analyzed: {len(BRONZE_TABLES)}")
print(f"🔔 Drift Detected: {drift_detected_count} table(s)")
print(f"📝 Alerts Generated: {alerts_generated}")
print(f"⏰ Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Operational Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recent Alerts (Last 7 Days)

# COMMAND ----------

spark.sql(f"""
SELECT 
    table_name,
    severity,
    alert_type,
    CASE 
        WHEN size(new_columns) > 0 THEN concat('Added: ', array_join(new_columns, ', '))
        ELSE ''
    END as columns_added,
    CASE 
        WHEN size(removed_columns) > 0 THEN concat('Removed: ', array_join(removed_columns, ', '))
        ELSE ''
    END as columns_removed,
    detected_timestamp,
    action_required
FROM {ALERT_TABLE}
WHERE detected_timestamp >= current_date() - INTERVAL 7 DAYS
  AND layer = 'bronze'
ORDER BY severity DESC, detected_timestamp DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables Needing Attention

# COMMAND ----------

spark.sql(f"""
SELECT 
    table_name,
    COUNT(*) as unresolved_alerts,
    MAX(severity) as highest_severity,
    MAX(detected_timestamp) as last_alert_time
FROM {ALERT_TABLE}
WHERE notified = false 
  AND layer = 'bronze'
GROUP BY table_name
HAVING COUNT(*) > 0
ORDER BY 
    CASE MAX(severity) 
        WHEN 'HIGH' THEN 1 
        WHEN 'MEDIUM' THEN 2 
        ELSE 3 
    END,
    COUNT(*) DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Stability Score (Last 30 Days)

# COMMAND ----------

spark.sql(f"""
WITH drift_counts AS (
    SELECT 
        table_name,
        COUNT(*) as total_alerts,
        SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) as high_alerts,
        SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) as medium_alerts,
        SUM(CASE WHEN severity = 'LOW' THEN 1 ELSE 0 END) as low_alerts
    FROM {ALERT_TABLE}
    WHERE detected_timestamp >= current_date() - INTERVAL 30 DAYS
      AND layer = 'bronze'
    GROUP BY table_name
)
SELECT 
    table_name,
    total_alerts,
    high_alerts,
    medium_alerts,
    low_alerts,
    CASE 
        WHEN total_alerts = 0 THEN 100
        WHEN high_alerts > 0 THEN 50
        WHEN medium_alerts > 2 THEN 70
        ELSE 90
    END as stability_score
FROM drift_counts
ORDER BY stability_score ASC, total_alerts DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Future Enhancements
# MAGIC 
# MAGIC ### Phase 2: Notifications
# MAGIC 
# MAGIC ```python
# MAGIC # Slack Integration
# MAGIC if severity == "HIGH":
# MAGIC     webhook_url = dbutils.secrets.get("monitoring", "slack_webhook")
# MAGIC     requests.post(webhook_url, json={
# MAGIC         "text": f"🚨 Critical schema drift in {table_name}",
# MAGIC         "attachments": [{
# MAGIC             "color": "danger",
# MAGIC             "fields": [
# MAGIC                 {"title": "Table", "value": table_name, "short": True},
# MAGIC                 {"title": "Removed Columns", "value": str(removed_columns), "short": True}
# MAGIC             ]
# MAGIC         }]
# MAGIC     })
# MAGIC ```
# MAGIC 
# MAGIC ### Phase 3: Auto-Remediation
# MAGIC 
# MAGIC - For LOW severity (new columns): Auto-update data dictionary
# MAGIC - For MEDIUM severity: Create Jira ticket for review
# MAGIC - For HIGH severity: Page on-call engineer
# MAGIC 
# MAGIC ### Phase 4: ML-Based Anomaly Detection
# MAGIC 
# MAGIC - Track typical drift frequency per table
# MAGIC - Alert when drift pattern is unusual (e.g., 5 changes in 1 week)
# MAGIC - Predict likely drift based on source system update cycles
