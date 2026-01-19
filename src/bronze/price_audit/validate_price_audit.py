"""
Bronze Validation Script: price_audit
Author: Diego Mayorga

Purpose:
    Post-ingestion technical validation for workspace.bronze.price_audit.
    Observational only: does NOT modify, filter, or correct data.
    Designed for auditability, monitoring, and reproducibility.

Technical Context:
    - Databricks Serverless (Unity Catalog)
    - Source: workspace.bronze.price_audit (Delta, managed)
    - All business columns are STRING
    - Technical columns: _ingestion_timestamp, _load_date, _source_file, _batch_id

Validation Principles:
    - Only technical, non-intrusive checks
    - No RDDs, UDFs, o Pandas
    - Single-pass, vectorized aggregations for Serverless optimization
    - Output: DataFrame of quality metrics ready for monitoring or Silver-gate ingestion
"""

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from functools import reduce

def run_validation(
    bronze_table: str = "workspace.bronze.price_audit",
    env: str = "dev"
):
    """
    Perform post-ingestion validation for a Bronze Delta table.
    Args:
        bronze_table (str): Target Bronze Delta table to validate.
        env (str): Environment label (dev/staging/prod) for logging.
    Returns:
        metrics_df (DataFrame): DataFrame with validation metrics.
    """
    spark = SparkSession.builder.getOrCreate()
    df = spark.table(bronze_table)

    BUSINESS_COLUMNS = [
        "Fecha", "Nombre del punto de venta", "Cod_PDV", "Nombre_Producto", "Cod_Producto", "Precio",
        "Tiene este producto una promocion?", "Promotional Price", "Comentarios", "Competitive_Group"
    ]
    TECHNICAL_COLUMNS = ["_ingestion_timestamp", "_load_date", "_source_file", "_batch_id"]
    EXPECTED_COLUMNS = BUSINESS_COLUMNS + TECHNICAL_COLUMNS

    # 1. Column Consistency Check
    actual_columns = df.columns
    missing_columns = [c for c in EXPECTED_COLUMNS if c not in actual_columns]
    extra_columns = [c for c in actual_columns if c not in EXPECTED_COLUMNS]

    # 2. Vectorized Aggregations
    agg_exprs = []
    for col_name in BUSINESS_COLUMNS:
        agg_exprs.append(
            F.count(F.when(F.col(col_name).isNull(), 1)).alias(f"null_count_{col_name}")
        )
        agg_exprs.append(
            F.count(F.when((F.col(col_name) == "") | (F.length(F.col(col_name)) == 0), 1)).alias(f"empty_count_{col_name}")
        )

    # Business Key Nulls (Fecha, Cod_PDV, Cod_Producto)
    for key_col in ["Fecha", "Cod_PDV", "Cod_Producto"]:
        agg_exprs.append(
            F.count(F.when(F.col(key_col).isNull(), 1)).alias(f"null_count_business_key_{key_col}")
        )
        agg_exprs.append(
            F.count(F.when((F.col(key_col) == "") | (F.length(F.col(key_col)) == 0), 1)).alias(f"empty_count_business_key_{key_col}")
        )

    # Helper for empty column condition
    def is_empty(c):
        return F.col(c).isNull() | (F.col(c) == "") | (F.length(F.col(c)) == 0)

    # Fully empty rows
    fully_empty_condition = reduce(
        lambda acc, c: acc & is_empty(c),
        BUSINESS_COLUMNS[1:],
        is_empty(BUSINESS_COLUMNS[0])
    )
    agg_exprs.append(F.count(F.lit(1)).alias("total_records"))
    agg_exprs.append(F.sum(F.when(fully_empty_condition, 1).otherwise(0)).alias("fully_empty_rows"))

    agg_result = df.agg(*agg_exprs).first()

    # 3. Assemble Quality Metrics
    metrics = []
    metrics.append(Row(metric="total_records", value=str(agg_result["total_records"]), detail=""))
    for key_col in ["Fecha", "Cod_PDV", "Cod_Producto"]:
        metrics.append(Row(metric=f"null_count_business_key_{key_col}", value=str(agg_result[f"null_count_business_key_{key_col}"]), detail=f"Nulls in business key: {key_col}"))
        metrics.append(Row(metric=f"empty_count_business_key_{key_col}", value=str(agg_result[f"empty_count_business_key_{key_col}"]), detail=f"Empty values in business key: {key_col}"))
    metrics.append(Row(metric="missing_columns", value=str(len(missing_columns)), detail=", ".join(missing_columns) if missing_columns else "None"))
    metrics.append(Row(metric="extra_columns", value=str(len(extra_columns)), detail=", ".join(extra_columns) if extra_columns else "None"))
    metrics.append(Row(metric="fully_empty_rows", value=str(agg_result["fully_empty_rows"]), detail=""))
    schema_match = (len(missing_columns) == 0 and len(extra_columns) == 0)
    metrics.append(Row(metric="schema_match", value=str(schema_match), detail="true = schema aligned with expected contract"))
    for col_name in BUSINESS_COLUMNS:
        metrics.append(Row(metric=f"null_count_{col_name}", value=str(agg_result[f"null_count_{col_name}"]), detail=""))
        metrics.append(Row(metric=f"empty_count_{col_name}", value=str(agg_result[f"empty_count_{col_name}"]), detail=""))
    from datetime import datetime
    validation_ts = datetime.utcnow().isoformat()
    metrics.append(Row(metric="validation_timestamp", value=validation_ts, detail=""))

    # 4. Metrics DataFrame
    metrics_df = spark.createDataFrame(metrics)
    metrics_df.show(truncate=False)
    return metrics_df

if __name__ == "__main__":
    metrics_df = run_validation()
    print("Validation completed.")
