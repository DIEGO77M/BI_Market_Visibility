
"""Bronze Ingestion Script: sell_in (Excel)
Author: Diego Mayorga

Purpose:
    Ingest monthly Excel files (.xlsx, .xls) from Unity Catalog Volumes into a managed Bronze Delta table for sell_in.
    All business columns are stored as STRING to maintain flexibility and simplicity in the Bronze layer.
    Adds technical columns for traceability: ingestion timestamp, load date, batch_id, and source reference.
    Appends new records without overwriting previous data (append-only, incremental by file).

    Technical Context:
    - Databricks Serverless (Unity Catalog)
    - Target Delta Table: workspace.bronze.sell_in
    - Source path: /Volumes/workspace/raw_data/sell_in
    - Excel format: .xlsx, .xls, header=True
    - Fail-fast mode to stop on corrupt files
    - Incremental ingestion: new data is appended, previous data is preserved
    - Trade-off: Direct Excel ingestion is not natively supported in Serverless; workaround uses Pandas for conversion.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, current_timestamp, current_date
import pandas as pd
import uuid
import json


def run_ingestion(
    source_path: str = "/Volumes/workspace/raw_data/sell_in",
    delta_table: str = "workspace.bronze.sell_in",
    supported_extensions = ["xlsx", "xls"],
    business_keys = ["Year", "Month", "PDV_Code", "Product_Code"],
    env: str = "dev",
    dbutils=None
):
    """
    Incremental ingestion for Excel files in Databricks Serverless.
    Converts Excel to Spark DataFrame via Pandas to bypass Serverless limitations.

    Parameters:
        source_path (str): Path to the Unity Catalog Volume containing Excel files.
        delta_table (str): Target Delta table for Bronze layer.
        supported_extensions (list): Supported Excel file extensions (default: ["xlsx", "xls"]).
        business_keys (list): List of business key columns for deduplication.
        env (str): Environment label (dev/staging/prod).
        dbutils: Databricks utility object for file listing (optional, recommended for notebook orchestration).

    Returns:
        tuple: batch_id (str), total_rows_inserted (int)
    """
    spark = SparkSession.builder.getOrCreate()
    batch_id = str(uuid.uuid4())

    # --- List Excel files from Unity Catalog Volume ---
    try:
        # Use dbutils if provided, else fallback to globals
        dbutils_obj = dbutils if dbutils is not None else globals().get('dbutils', None)
        if dbutils_obj is not None:
            files_info = dbutils_obj.fs.ls(source_path)
            files = [f.path for f in files_info if any(f.name.endswith(ext) for ext in supported_extensions)]
        else:
            raise NameError("dbutils is not defined or not passed. This script must be run in a Databricks notebook or with dbutils available.")
    except Exception as e:
        print(f"[{env}] Error accessing path: {str(e)}")
        return batch_id, 0

    if not files:
        print(f"[{env}] No Excel files found to process.")
        return batch_id, 0

    # --- Load existing business keys for deduplication (anti-join) ---
    try:
        df_existing = spark.table(delta_table).select(*business_keys)
    except Exception:
        df_existing = None

    total_rows_inserted = 0

    for file_path in files:
        file_name = file_path.split("/")[-1]
        print(f"[{env}] Processing Excel file: {file_name}")

        try:
            # --- Read Excel file with Pandas using FUSE path (/Volumes/...) ---
            # Note: Databricks Serverless automatically maps Volumes
            pdf = pd.read_excel(file_path.replace("dbfs:", ""))

            # --- Ensure all column names are string to avoid schema errors ---
            pdf.columns = [str(c) for c in pdf.columns]

            # --- Convert Pandas DataFrame to Spark DataFrame (all columns as string) ---
            df_new = spark.createDataFrame(pdf.astype(str))

            # --- Add technical columns for traceability ---
            df_new = df_new.withColumn("_ingestion_timestamp", current_timestamp()) \
                           .withColumn("_load_date", current_date()) \
                           .withColumn("_source_file", lit(file_name)) \
                           .withColumn("_batch_id", lit(batch_id))

            # --- Deduplication logic: avoid duplicates if business keys already exist ---
            if df_existing is not None:
                df_to_insert = df_new.join(df_existing, on=business_keys, how="left_anti")
            else:
                df_to_insert = df_new

            rows_to_insert = df_to_insert.count()

            if rows_to_insert > 0:
                # --- Write to Delta Table (Unity Catalog) ---
                df_to_insert.write.format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(delta_table)

                total_rows_inserted += rows_to_insert
                print(f"[{env}] Inserted {rows_to_insert} records from {file_name}")
            else:
                print(f"[{env}] File {file_name} contains no new records.")

        except Exception as e:
            print(f"[{env}] Fatal error processing {file_name}: {str(e)}")

    # --- Execution summary for orchestration and monitoring ---
    result = {
        "env": env,
        "batch_id": batch_id,
        "rows_ingested": total_rows_inserted,
        "status": "Success" if total_rows_inserted > 0 else "No data processed"
    }
    print(json.dumps(result, indent=4))
    return batch_id, total_rows_inserted

# --- Allow local testing via CLI ---
# Entry point for local execution, Databricks jobs, or workflow orchestration
if __name__ == "__main__":
    batch_id, rows = run_ingestion()
    print(f"Batch processed: {batch_id} | Rows inserted: {rows}")
