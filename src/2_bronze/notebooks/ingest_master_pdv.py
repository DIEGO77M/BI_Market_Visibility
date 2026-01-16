"""
Bronze Ingestion Script: master_pdv
Author: Diego Mayorga

Purpose:
    Ingest raw CSVs from Unity Catalog Volumes into a managed Bronze Delta table, preserving all business columns as STRING.

Technical Context:
    - Databricks Serverless (Unity Catalog)
    - Table: workspace.bronze.master_pdv
    - Source: /Volumes/workspace/raw_data/master_pdv (CSV, explicit delimiter, UTF-8)

Key Design:
    - No validation, no business logic, no type casting
    - All columns as STRING, names as in source
    - FAILFAST mode: fail early on corrupt files
    - No idempotency: every run appends all files (for demo, see previous versions for idempotent logic)
"""


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType
from pyspark.sql.functions import current_timestamp, current_date, lit, col
import uuid

# --- Configuration ---
SOURCE_PATH = "/Volumes/workspace/raw_data/master_pdv"
DELTA_TABLE = "workspace.bronze.master_pdv"
DELIMITER = ";"
ENCODING = "UTF-8"

# --- Schema: all columns as STRING, no inference ---
BUSINESS_COLUMNS = [
    "Code (eLeader)", "Store Name", "Channel", "Sub Channel", "Chain", 
    "Neighborhood", "City", "Parish", "Country", "Latitude", "Longitude", 
    "Type of Service", "Status", "Supervisor Code", "Supervisor Name", 
    "Merchandiser Code", "Merchandiser Name", "CODE PO", 
    "Aditional_Exhibitions", "Commercial Activities", "Planograms", 
    "Store SAP Code", "Sales Rep"
]

SCHEMA = StructType([
    StructField(name, StringType(), True) for name in BUSINESS_COLUMNS
])

# --- Initialize Spark ---
spark = SparkSession.builder.getOrCreate()
batch_id = str(uuid.uuid4())

# --- Read CSV (fail fast on corrupt files) ---
df = (spark.read
    .format("csv")
    .option("delimiter", DELIMITER)
    .option("header", True)
    .option("encoding", ENCODING)
    .option("mode", "FAILFAST")
    .schema(SCHEMA)
    .load(SOURCE_PATH))

# --- Add technical columns for traceability and partitioning ---
df = (df
      .withColumn("_ingestion_timestamp", current_timestamp())
      .withColumn("_load_date", current_date())
      .withColumn("_source_file", col("_metadata.file_path"))
      .withColumn("_batch_id", lit(batch_id)))


# --- Write to Delta (overwrite: Bronze is snapshot, table replaced on each ingestion) ---
if df.limit(1).count() == 0:
    raise RuntimeError("No data to ingest")
else:
    (df.write
     .format("delta")
     .mode("overwrite")  # Bronze is snapshot: table replaced on each ingestion
     .option("overwriteSchema", "true")
     .option("delta.autoOptimize.optimizeWrite", "true")
     .option("delta.autoOptimize.autoCompact", "true")
     .saveAsTable(DELTA_TABLE))

# --- Minimal output for orchestration/logging ---
print(batch_id)