"""
Bronze Monitoring Script: master_products
Author: Diego Mayorga

Purpose:
    Monitor and audit post-ingestion data quality for workspace.bronze.master_products.
    Calculates quality metrics, detects schema drift/anomalies, generates alerts, 
    and outputs JSON for n8n orchestration.
    Fully vectorized, single-pass, Serverless-optimized.
"""

from pyspark.sql import SparkSession, functions as F, Row
from pyspark.sql.types import StructType, StructField, StringType
from functools import reduce
from datetime import datetime

def run_monitoring(
    bronze_table: str = "workspace.bronze.master_products",
    validation_table: str = "workspace.bronze.master_products_validation",
    fully_empty_threshold: float = 0.05,
    env: str = "dev"
):
    """
    Perform monitoring and audit of a Bronze Delta table after ingestion.
    
    Args:
        bronze_table (str): Bronze Delta table to monitor.
        validation_table (str): Delta table to store validation metrics.
        fully_empty_threshold (float): Threshold ratio for fully empty rows to trigger alerts.
        env (str): Environment label (dev/staging/prod) for logging.
    
    Returns:
        Tuple of two lists:
            - metrics_json: List of dictionaries with metrics for orchestration.
            - alerts_json: List of dictionaries with alerts for orchestration.
    """

    # --- Initialize SparkSession ---
    spark = SparkSession.builder.getOrCreate()
    df = spark.table(bronze_table)

    # --- Define columns ---
    BUSINESS_COLUMNS = [
        "Product_Code", "Product_Name", "Brand", "Segment", "Subsegment", "Category", "Subcategory"
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

    fully_empty_condition = reduce(
        lambda acc, c: acc & ((F.col(c).isNull()) | (F.col(c) == "") | (F.length(F.col(c)) == 0)),
        BUSINESS_COLUMNS[1:],
        (F.col(BUSINESS_COLUMNS[0]).isNull()) | (F.col(BUSINESS_COLUMNS[0]) == "") | (F.length(F.col(BUSINESS_COLUMNS[0])) == 0)
    )
    agg_exprs.append(F.count(F.lit(1)).alias("total_records"))
    agg_exprs.append(F.sum(F.when(fully_empty_condition, 1).otherwise(0)).alias("fully_empty_rows"))

    agg_result = df.agg(*agg_exprs).first()

    # --- Latest batch info ---
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

    # --- Alerts ---
    alerts = []
    fully_empty_ratio = (agg_result["fully_empty_rows"] / agg_result["total_records"]) if agg_result["total_records"] > 0 else 0
    if fully_empty_ratio > fully_empty_threshold:
        alerts.append(Row(alert="fully_empty_rows_high", value=str(fully_empty_ratio), threshold=str(fully_empty_threshold), message="High ratio of fully empty rows detected"))
    if not schema_match:
        alerts.append(Row(alert="schema_mismatch", value="False", threshold="True", message="Schema does not match expected contract"))

    # --- Alerts schema ---
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
    metrics_df.write.format("delta").mode("append").saveAsTable(validation_table)
    if alerts:
        alerts_df.write.format("delta").mode("append").saveAsTable(validation_table + "_alerts")

    # --- Output JSON for orchestration ---
    metrics_json = [row.asDict() for row in metrics_df.collect()]
    alerts_json = [row.asDict() for row in alerts_df.collect()] if alerts else []

    # --- Optional debug ---
    print("Metrics JSON for n8n:", metrics_json)
    print("Alerts JSON for n8n:", alerts_json)

    return metrics_json, alerts_json


# --- CLI / Notebook execution ---
if __name__ == "__main__":
    metrics, alerts = run_monitoring()
    print("Monitoring completed.")
