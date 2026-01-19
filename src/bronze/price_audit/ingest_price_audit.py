"""
Bronze Ingestion Script: price_audit
Author: Diego Mayorga

Purpose:
    Ingest monthly CSV files from Unity Catalog Volumes into a managed Bronze Delta table for price_audit.
    All business columns are stored as STRING to maintain flexibility and simplicity in the Bronze layer.
    Adds technical columns for traceability: ingestion timestamp, load date, batch_id, and source reference.
    Appends new records and updates existing ones if the same file is reprocessed (upsert by file + business keys).

Technical Context:
    - Databricks Serverless (Unity Catalog)
    - Target Delta Table: workspace.bronze.price_audit
    - Source path: /Volumes/workspace/raw_data/price_audit
    - CSV format: header=True, delimiter=",", UTF-8 encoding
    - Fail-fast mode to stop on corrupt files
    - Incremental ingestion: new data is appended, previous data is preserved unless updated by file reprocessing
    - Upsert logic: if a file is reprocessed, records from that file are replaced (idempotent per file)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, current_timestamp, current_date
from pyspark.sql.types import StructType, StructField, StringType
import uuid
import json
import os

def run_ingestion(
    source_path: str = "/Volumes/workspace/raw_data/price_audit",
    delta_table: str = "workspace.bronze.price_audit",
    env: str = "dev"
):
    """
    Incremental ingestion for monthly CSV files in Databricks Serverless.
    Upserts by file: if a file is reprocessed, its previous records are replaced.

    Parameters:
        source_path (str): Path to the Unity Catalog Volume containing CSV files.
        delta_table (str): Target Delta table for Bronze layer.
        env (str): Environment label (dev/staging/prod).

    Returns:
        tuple: batch_id (str), total_rows_inserted (int)
    """
    spark = SparkSession.builder.getOrCreate()
    batch_id = str(uuid.uuid4())

    # --- Define schema: all columns as STRING ---
    BUSINESS_COLUMNS = [
        "Fecha", "Nombre del punto de venta", "Cod_PDV", "Nombre_Producto", "Cod_Producto", "Precio",
        "Tiene este producto una promocion?", "Promotional Price", "Comentarios", "Competitive_Group"
    ]
    SCHEMA = StructType([StructField(c, StringType(), True) for c in BUSINESS_COLUMNS])

    # --- List CSV files from Unity Catalog Volume ---
    import os
    try:
        files = [os.path.join(source_path, f) for f in os.listdir(source_path) if f.endswith('.csv')]
    except Exception as e:
        print(f"[{env}] Error accessing path: {str(e)}")
        return batch_id, 0

    if not files:
        print(f"[{env}] No CSV files found to process.")
        return batch_id, 0


    from pyspark.sql.functions import col, regexp_extract

    # --- Read all CSV files in batch ---
    df = (
        spark.read
        .format("csv")
        .option("header", True)
        .option("delimiter", ",")
        .option("encoding", "UTF-8")
        .option("mode", "FAILFAST")
        .schema(SCHEMA)
        .load(files)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_load_date", current_date())
        .withColumn("_source_file", regexp_extract(col("_metadata.file_path"), r"([^/]+$)", 1))
        .withColumn("_batch_id", lit(batch_id))
    )

    # --- Get list of file names to delete ---
    file_names = [f.split("/")[-1] for f in files]
    file_names_str = ",".join([f"'" + fn + "'" for fn in file_names])

    # --- Upsert logic: DELETE + APPEND (efficient, scalable) ---
    try:
        spark.sql(f"""
            DELETE FROM {delta_table}
            WHERE _source_file IN ({file_names_str})
        """)
    except Exception:
        pass  # Table may not exist yet; append will create it if needed

    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .option("delta.autoOptimize.optimizeWrite", "true") \
        .option("delta.autoOptimize.autoCompact", "true") \
        .saveAsTable(delta_table)

    total_rows_inserted = df.count()
    print(f"[{env}] Upserted {total_rows_inserted} records from {len(files)} files.")

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
if __name__ == "__main__":
    batch_id, rows = run_ingestion()
    print(f"Batch processed: {batch_id} | Rows inserted: {rows}")