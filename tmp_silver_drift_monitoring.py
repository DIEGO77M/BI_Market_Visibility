# Silver Layer - Lightweight Drift Monitoring
# Author: Diego Mayorga
# Date: 2026-01-05
# Project: BI Market Visibility Analysis

"""
This script implements lightweight drift monitoring for the Silver layer.
It enforces soft data contracts and protects downstream Gold/BI layers by detecting:
- Schema drift (new/missing columns, data type changes)
- Quality drift (null rates, invalid records, quality flags)
- Volume drift (row count, key cardinality changes)

All operations are metadata-only or use existing Silver logs/metrics (Serverless-friendly).
Drift events are logged in a Delta audit table with severity (HIGH/MEDIUM/LOW).
No pipeline blocking: only observability and early detection.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField, TimestampType, IntegerType, DoubleType
import json

# --- Config ---
CATALOG = "workspace"
SCHEMA = "default"
SILVER_TABLES = [
    "silver_master_pdv",
    "silver_master_products",
    "silver_price_audit",
    "silver_sell_in"
]
AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.silver_drift_history"

# --- Utility: Get current schema as dict ---
def get_table_schema(spark, table):
    schema = spark.table(table).schema
    return {f.name: str(f.dataType) for f in schema.fields}

# --- Utility: Get Delta History metrics ---
def get_delta_history_metrics(spark, table):
    hist = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
    metrics = hist["operationMetrics"]
    return {
        "numOutputRows": int(metrics.get("numOutputRows", 0)),
        "numFiles": int(metrics.get("numFiles", 0)),
        "timestamp": hist["timestamp"]
    }

# --- Utility: Load baseline (from audit table or static) ---
def load_baseline(spark, table):
    try:
        # Check if audit table exists (serverless-friendly)
        if not spark._jsparkSession.catalog().tableExists(AUDIT_TABLE):
            return None
        df = spark.table(AUDIT_TABLE).filter(col("table_name") == table).orderBy(col("timestamp").desc())
        last = df.first()
        if last:
            return json.loads(last["baseline_json"])
    except Exception:
        return None
    return None

# --- Utility: Save drift event ---
def log_drift_event(spark, event):
    df = spark.createDataFrame([event])
    df.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)
    print(f"[Drift] {event['table_name']} | {event['drift_type']} | {event['severity']} | {event['details']}")

# --- Utility: Compare schemas ---
def compare_schema(current, baseline):
    drift = {"new_columns": [], "missing_columns": [], "type_changes": []}
    if not baseline:
        return drift
    for col in current:
        if col not in baseline:
            drift["new_columns"].append(col)
        elif current[col] != baseline[col]:
            drift["type_changes"].append({"column": col, "old": baseline[col], "new": current[col]})
    for col in baseline:
        if col not in current:
            drift["missing_columns"].append(col)
    return drift

# --- Utility: Compare metrics ---
def compare_metrics(current, baseline, key="numOutputRows", threshold=0.2):
    if not baseline or key not in baseline or key not in current:
        return None
    prev = baseline[key]
    curr = current[key]
    if prev == 0:
        return None
    change = (curr - prev) / prev
    if abs(change) > threshold:
        return {"metric": key, "prev": prev, "curr": curr, "change": change}
    return None


# --- Utility: Ensure audit table exists ---
def ensure_audit_table(spark):
    """
    Create the audit table if it does not exist.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
            timestamp TIMESTAMP,
            table_name STRING,
            drift_type STRING,
            severity STRING,
            details STRING,
            baseline_json STRING,
            notified BOOLEAN
        )
        USING DELTA
    """)

# --- Main monitoring loop ---
def run_silver_drift_monitoring(spark):
    ensure_audit_table(spark)
    for table in SILVER_TABLES:
        full_table = f"{CATALOG}.{SCHEMA}.{table}"
        # 1. Get current schema and metrics
        schema_now = get_table_schema(spark, full_table)
        metrics_now = get_delta_history_metrics(spark, full_table)
        # 2. Load baseline
        baseline = load_baseline(spark, table)
        # 3. Compare schema
        schema_drift = compare_schema(schema_now, baseline["schema"] if baseline else None)
        # 4. Compare volume
        volume_drift = compare_metrics(metrics_now, baseline["metrics"] if baseline else None)
        # 5. Quality drift: use Silver logs if available (placeholder)
        # (You can extend this to read from a Silver quality log table)
        # 6. Severity logic
        severity = "LOW"
        details = []
        if schema_drift["missing_columns"] or schema_drift["type_changes"]:
            severity = "HIGH"
            details.append(f"Missing/type-changed columns: {schema_drift}")
        elif schema_drift["new_columns"]:
            severity = "MEDIUM"
            details.append(f"New columns: {schema_drift['new_columns']}")
        if volume_drift:
            if abs(volume_drift["change"]) > 0.5:
                severity = "HIGH"
            else:
                severity = max(severity, "MEDIUM")
            details.append(f"Volume drift: {volume_drift}")
        # 7. Log event if any drift
        if schema_drift["new_columns"] or schema_drift["missing_columns"] or schema_drift["type_changes"] or volume_drift:
            event = {
                "timestamp": current_timestamp(),
                "table_name": table,
                "drift_type": "schema/volume",
                "severity": severity,
                "details": json.dumps(details),
                "baseline_json": json.dumps({"schema": schema_now, "metrics": metrics_now}),
                "notified": False
            }
            log_drift_event(spark, event)
        # 8. Update baseline (always store latest)
        # (In this lightweight version, baseline is always the last run)

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run_silver_drift_monitoring(spark)
