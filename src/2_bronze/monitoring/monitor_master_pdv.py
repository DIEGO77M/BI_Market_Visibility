"""
Bronze Monitoring Script: master_pdv
Author: Diego Mayorga

Purpose:
    Monitor and audit post-ingestion data quality for workspace.bronze.master_pdv.
    Calculates quality metrics, detects schema drift/anomalies, generates alerts, 
    and outputs JSON for n8n orchestration.
    Fully vectorized, single-pass, Serverless-optimized.
"""

from pyspark.sql import SparkSession, functions as F, Row
from pyspark.sql.types import StructType, StructField, StringType
from functools import reduce
from datetime import datetime

# --- Configuration ---
BRONZE_TABLE = "workspace.bronze.master_pdv"
VALIDATION_TABLE = "workspace.bronze.master_pdv_validation"
FULLY_EMPTY_ROWS_THRESHOLD = 0.05  # 5% threshold

# --- Initialize SparkSession ---
spark = SparkSession.builder.getOrCreate()
df = spark.table(BRONZE_TABLE)

# --- Define columns ---
BUSINESS_COLUMNS = [
    "Code (eLeader)", "Store Name", "Channel", "Sub Channel", "Chain",
    "Neighborhood", "City", "Parish", "Country", "Latitude", "Longitude",
    "Type of Service", "Status", "Supervisor Code", "Supervisor Name",
    "Merchandiser Code", "Merchandiser Name", "CODE PO",
    "Aditional_Exhibitions", "Commercial Activities", "Planograms",
    "Store SAP Code", "Sales Rep"
]
TECHNICAL_COLUMNS = ["_ingestion_timestamp", "_load_date", "_source_file", "_batch_id"]
EXPECTED_COLUMNS = BUSINESS_COLUMNS + TECHNICAL_COLUMNS

# --- Column consistency ---
actual_columns = df.columns
missing_columns = [c for c in EXPECTED_COLUMNS if c not in actual_columns]
extra_columns = [c for c in actual_columns if c not in EXPECTED_COLUMNS]
schema_match = (len(missing_columns) == 0 and len(extra_columns) == 0)

# --- Vectorized aggregations ---
agg_exprs = []
for col_name in BUSINESS_COLUMNS:
    agg_exprs.append(F.count(F.when(F.col(col_name).isNull(), 1)).alias(f"null_count_{col_name}"))
    agg_exprs.append(F.count(F.when((F.col(col_name) == "") | (F.length(F.col(col_name)) == 0), 1)).alias(f"empty_count_{col_name}"))

# Fully empty rows
fully_empty_condition = reduce(
    lambda acc, c: acc & ((F.col(c).isNull()) | (F.col(c) == "") | (F.length(F.col(c)) == 0)),
    BUSINESS_COLUMNS[1:],
    (F.col(BUSINESS_COLUMNS[0]).isNull()) | (F.col(BUSINESS_COLUMNS[0]) == "") | (F.length(F.col(BUSINESS_COLUMNS[0])) == 0)
)
agg_exprs.append(F.count(F.lit(1)).alias("total_records"))
agg_exprs.append(F.sum(F.when(fully_empty_condition, 1).otherwise(0)).alias("fully_empty_rows"))

agg_result = df.agg(*agg_exprs).first()

# --- Extract latest batch info without count() ---
latest_row = df.select("_batch_id", "_source_file").orderBy(F.col("_ingestion_timestamp").desc()).limit(1).first()
batch_id = latest_row["_batch_id"] if latest_row else None
source_file = latest_row["_source_file"] if latest_row else None
timestamp = datetime.utcnow().isoformat()

# --- Assemble metrics ---
metrics = [
    Row(metric="total_records", value=str(agg_result["total_records"]), detail=""),
    Row(metric="fully_empty_rows", value=str(agg_result["fully_empty_rows"]), detail=""),
    Row(metric="missing_columns", value=str(len(missing_columns)), detail=", ".join(missing_columns) if missing_columns else "None"),
    Row(metric="extra_columns", value=str(len(extra_columns)), detail=", ".join(extra_columns) if extra_columns else "None"),
    Row(metric="schema_match", value=str(schema_match), detail="true = schema aligned with expected contract")
]

for col_name in BUSINESS_COLUMNS:
    metrics.append(Row(metric=f"null_count_{col_name}", value=str(agg_result[f"null_count_{col_name}"]), detail=""))
    metrics.append(Row(metric=f"empty_count_{col_name}", value=str(agg_result[f"empty_count_{col_name}"]), detail=""))

# --- Assemble alerts (fixed schema) ---
alerts = []
fully_empty_ratio = (agg_result["fully_empty_rows"] / agg_result["total_records"]) if agg_result["total_records"] > 0 else 0
if fully_empty_ratio > FULLY_EMPTY_ROWS_THRESHOLD:
    alerts.append(Row(alert="fully_empty_rows_high", value=str(fully_empty_ratio), threshold=str(FULLY_EMPTY_ROWS_THRESHOLD), message="High ratio of fully empty rows detected"))
if not schema_match:
    alerts.append(Row(alert="schema_mismatch", value="False", threshold="True", message="Schema does not match expected contract"))

# --- Define explicit schema for alerts to avoid empty dataset errors ---
alerts_schema = StructType([
    StructField("alert", StringType(), True),
    StructField("value", StringType(), True),
    StructField("threshold", StringType(), True),
    StructField("message", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("monitoring_timestamp", StringType(), True)
])

# --- Create DataFrames ---
metrics_df = spark.createDataFrame(metrics).withColumn("batch_id", F.lit(batch_id)) \
                                            .withColumn("source_file", F.lit(source_file)) \
                                            .withColumn("monitoring_timestamp", F.lit(timestamp))

if alerts:
    alerts_df = spark.createDataFrame([
        (a.alert, a.value, a.threshold, a.message, batch_id, source_file, timestamp)
        for a in alerts
    ], schema=alerts_schema)
else:
    alerts_df = spark.createDataFrame([], schema=alerts_schema)

# --- Persist to Delta ---
metrics_df.write.format("delta").mode("append").saveAsTable(VALIDATION_TABLE)
if alerts:
    alerts_df.write.format("delta").mode("append").saveAsTable(VALIDATION_TABLE + "_alerts")

# --- Output for n8n (JSON) ---
metrics_json = [row.asDict() for row in metrics_df.collect()]
alerts_json = [row.asDict() for row in alerts_df.collect()] if alerts else []

# --- Example return for orchestration ---
print("Metrics JSON for n8n:", metrics_json)
print("Alerts JSON for n8n:", alerts_json)

