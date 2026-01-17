"""
Bronze Validation Script: sell_in
Author: Diego Mayorga

Purpose:
    Post-ingestion technical validation for workspace.bronze.sell_in.
    Observational only: does NOT modify, filter, or correct data.
    Designed for auditability, monitoring, and reproducibility.

Technical Context:
    - Databricks Serverless (Unity Catalog)
    - Source: workspace.bronze.sell_in (Delta, managed)
    - All business columns are STRING
    - Technical columns: _ingestion_timestamp, _load_date, _source_file, _batch_id

Validation Principles:
    - Only technical, non-intrusive checks
    - No RDDs, UDFs, or Pandas
    - Single-pass, vectorized aggregations for Serverless optimization
    - Output: DataFrame of quality metrics ready for monitoring or Silver-gate ingestion
"""

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from functools import reduce

def run_validation(
    bronze_table: str = "workspace.bronze.sell_in",
    batch_id: str = None,  # Optional: validate only latest batch
    load_date: str = None, # Optional: validate by date
    env: str = "dev"
):
    """
    Perform post-ingestion validation for a Bronze Delta table.
    Supports scalable validation by batch_id or load_date to avoid full-table scans.

    Args:
        bronze_table (str): Target Bronze Delta table to validate.
        batch_id (str, optional): Validate only the specified batch.
        load_date (str, optional): Validate only the specified load date (YYYY-MM-DD).
        env (str): Environment label (dev/staging/prod) for logging.

    Returns:
        metrics_df (DataFrame): DataFrame with validation metrics.
    """

    # --- Initialize SparkSession ---
    spark = SparkSession.builder.getOrCreate()

    # --- Load Bronze Table (scalable: filter by batch_id or load_date) ---
    df = spark.table(bronze_table)
    # Parameter existence validation
    if batch_id:
        batch_exists = df.filter(F.col("_batch_id") == batch_id).limit(1).count() > 0
        if not batch_exists:
            metrics = [Row(metric="batch_id_not_found", value=str(batch_id), detail="No records found for batch_id.")]
            metrics_df = spark.createDataFrame(metrics)
            metrics_df.show(truncate=False)
            return metrics_df
        df = df.filter(F.col("_batch_id") == batch_id)
    elif load_date:
        date_exists = df.filter(F.col("_load_date") == load_date).limit(1).count() > 0
        if not date_exists:
            metrics = [Row(metric="load_date_not_found", value=str(load_date), detail="No records found for load_date.")]
            metrics_df = spark.createDataFrame(metrics)
            metrics_df.show(truncate=False)
            return metrics_df
        df = df.filter(F.col("_load_date") == load_date)

    # --- Define expected columns ---
    BUSINESS_COLUMNS = [
        "Year", "Month", "PDV_Code", "Product_Code",
        "Opening_Stock_Units", "Sell_In_Units", "Returns_Units", "Closing_Stock_Units",
        "Days_of_Inventory", "Inventory_Turnover", "Replenishment_Flag", "Stock_Risk_Level"
    ]
    TECHNICAL_COLUMNS = ["_ingestion_timestamp", "_load_date", "_source_file", "_batch_id"]
    EXPECTED_COLUMNS = BUSINESS_COLUMNS + TECHNICAL_COLUMNS

    # --- 1. Column Consistency Check ---
    actual_columns = df.columns
    missing_columns = [c for c in EXPECTED_COLUMNS if c not in actual_columns]
    extra_columns = [c for c in actual_columns if c not in EXPECTED_COLUMNS]

    # --- 2. Vectorized Aggregations ---
    agg_exprs = []
    for col_name in BUSINESS_COLUMNS:
        agg_exprs.append(
            F.count(F.when(F.col(col_name).isNull(), 1)).alias(f"null_count_{col_name}")
        )
        agg_exprs.append(
            F.count(F.when((F.col(col_name) == "") | (F.length(F.col(col_name)) == 0), 1)).alias(f"empty_count_{col_name}")
        )

    # Helper function for empty column check
    def is_empty(col_name):
        return (F.col(col_name).isNull()) | (F.col(col_name) == "") | (F.length(F.col(col_name)) == 0)

    # Fully empty rows condition (pythonic, readable)
    fully_empty_condition = reduce(
        lambda a, b: a & b,
        [is_empty(c) for c in BUSINESS_COLUMNS]
    )

    agg_exprs.append(F.count(F.lit(1)).alias("total_records"))
    agg_exprs.append(F.sum(F.when(fully_empty_condition, 1).otherwise(0)).alias("fully_empty_rows"))

    agg_result = df.agg(*agg_exprs).first()

    # --- 3. Assemble Quality Metrics ---
    metrics = []

    metrics.append(Row(metric="total_records", value=str(agg_result["total_records"]), detail=""))
    # Add validation_scope metric for monitoring/debugging
    scope_value = batch_id if batch_id else (load_date if load_date else "full_table")
    scope_detail = f"batch_id={batch_id}, load_date={load_date}"
    metrics.append(Row(metric="validation_scope", value=scope_value, detail=scope_detail))
    metrics.append(Row(metric="missing_columns", value=str(len(missing_columns)), detail=", ".join(missing_columns) if missing_columns else "None"))
    metrics.append(Row(metric="extra_columns", value=str(len(extra_columns)), detail=", ".join(extra_columns) if extra_columns else "None"))
    metrics.append(Row(metric="fully_empty_rows", value=str(agg_result["fully_empty_rows"]), detail=""))

    schema_match = (len(missing_columns) == 0 and len(extra_columns) == 0)
    metrics.append(Row(metric="schema_match", value=str(schema_match), detail="true = schema aligned with expected contract"))

    for col_name in BUSINESS_COLUMNS:
        metrics.append(Row(metric=f"null_count_{col_name}", value=str(agg_result[f"null_count_{col_name}"]), detail=""))
        metrics.append(Row(metric=f"empty_count_{col_name}", value=str(agg_result[f"empty_count_{col_name}"]), detail=""))

    metrics.append(Row(metric="validation_timestamp", value=spark.sql("SELECT current_timestamp()").first()[0].isoformat(), detail=""))

    # --- 4. Metrics DataFrame ---
    metrics_df = spark.createDataFrame(metrics)

    # Optional debug output
    metrics_df.show(truncate=False)

    return metrics_df

# --- Allow local testing via CLI ---
if __name__ == "__main__":
    metrics_df = run_validation()
    print("Validation completed.")
