"""
Bronze Ingestion Script: master_products
Author: Diego Mayorga

Purpose:
    Ingest raw CSV files from Unity Catalog Volumes into a managed Bronze Delta table for master_products.
    All business columns are stored as STRING to maintain flexibility and simplicity in Bronze layer.
    Adds technical columns for traceability: ingestion timestamp, load date, batch_id, and source reference.

Technical Context:
    - Databricks Serverless (Unity Catalog)
    - Target Delta Table: workspace.bronze.master_products
    - Source path: /Volumes/workspace/raw_data/master_products
    - CSV format: header=True, delimiter=",", UTF-8 encoding
    - Fail-fast mode to stop on corrupt files
    - Snapshot ingestion: Bronze table is overwritten each batch

Usage:
    1. Local testing:
        python ingest_master_products.py
    2. Notebook or Job orchestration:
        from ingest_master_products import run_ingestion
        batch_id, rows = run_ingestion()
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, current_date, lit
import uuid
import json

def run_ingestion(
    source_path: str = "/Volumes/workspace/raw_data/master_products",
    delta_table: str = "workspace.bronze.master_products",
    delimiter: str = ",",
    encoding: str = "UTF-8",
    env: str = "dev"
):
    """
    Executes Bronze ingestion for the master_products dataset in Databricks Serverless.

    Parameters:
        source_path (str): Path to CSV files in Unity Catalog.
        delta_table (str): Target Delta table for Bronze layer.
        delimiter (str): CSV delimiter.
        encoding (str): CSV encoding.
        env (str): Environment label (dev/staging/prod), useful for logging and monitoring.

    Returns:
        tuple: batch_id (str), rows_ingested (int)
    """

    # --- Define business schema: all columns as STRING for flexibility in Bronze ---
    # 'Product_Code' is the business primary key for master_products. It uniquely identifies each product in the dataset.
    BUSINESS_COLUMNS = [
        "Product_Code",  # Business primary key (unique product identifier)
        "Product_Name", "Brand", "Segment", "Subsegment", "Category", "Subcategory"
    ]
    SCHEMA = StructType([StructField(c, StringType(), True) for c in BUSINESS_COLUMNS])

    # --- Initialize SparkSession for Serverless environment ---
    spark = SparkSession.builder.getOrCreate()
    batch_id = str(uuid.uuid4())  # Unique batch identifier for traceability

    # --- Read CSV files from Unity Catalog Volume ---
    df = (
        spark.read
        .format("csv")
        .option("delimiter", delimiter)
        .option("header", True)
        .option("encoding", encoding)
        .option("mode", "FAILFAST")
        .schema(SCHEMA)
        .load(source_path)
    )

    # --- Validate that the source has data ---
    rows_ingested = df.count()
    if rows_ingested == 0:
        raise RuntimeError(f"[{env}] No data found at source path: {source_path}")

    # --- Add technical columns for traceability ---
    df = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_load_date", current_date())
        .withColumn("_source_file", lit(source_path))  # Safe for Serverless, replaces _metadata
        .withColumn("_batch_id", lit(batch_id))
    )

    # --- Write to Delta (overwrite snapshot for Bronze) ---
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("delta.autoOptimize.optimizeWrite", "true") \
        .option("delta.autoOptimize.autoCompact", "true") \
        .saveAsTable(delta_table)

    # --- Structured logging for orchestration and monitoring ---
    log_info = {
        "env": env,
        "source_path": source_path,
        "delta_table": delta_table,
        "batch_id": batch_id,
        "rows_ingested": rows_ingested
    }
    print(json.dumps(log_info))  # Orchestrators can parse JSON logs

    return batch_id, rows_ingested


# --- Allow local testing via CLI ---
if __name__ == "__main__":
    batch_id, rows = run_ingestion()
    print(f"Batch processed: {batch_id} | Rows inserted: {rows}")
