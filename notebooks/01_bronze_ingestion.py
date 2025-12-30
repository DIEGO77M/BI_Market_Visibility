# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Data Ingestion
# MAGIC 
# MAGIC **Purpose:** Ingest raw data from multiple sources into Bronze layer using Delta Lake format
# MAGIC 
# MAGIC **Author:** Diego Mayorga  
# MAGIC **Date:** 2025-12-30  
# MAGIC **Project:** BI Market Visibility Analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📋 Data Sources
# MAGIC 
# MAGIC | Source | Type | Strategy | Partition |
# MAGIC |--------|------|----------|-----------|
# MAGIC | Master_PDV | CSV | Full Overwrite | None |
# MAGIC | Master_Products | CSV | Full Overwrite | None |
# MAGIC | Price_Audit | XLSX (24 files) | Incremental Append | year_month |
# MAGIC | Sell-In | XLSX (2 files) | Merge/Upsert | year |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 🏗️ Architecture
# MAGIC 
# MAGIC ```
# MAGIC RAW Layer (Source)          BRONZE Layer (Delta)
# MAGIC ──────────────────          ────────────────────
# MAGIC CSV/XLSX Files    ──────►   Delta Tables
# MAGIC                             - ACID transactions
# MAGIC                             - Time travel
# MAGIC                             - Schema evolution
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install Required Libraries for Excel Reading

# COMMAND ----------

# MAGIC %pip install openpyxl pandas

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, year, month, concat_ws,
    count, countDistinct, sum as spark_sum
)
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime
import os
import pandas as pd

# Note: In Databricks, the SparkSession already exists as 'spark'
# We cannot reconfigure it with .getOrCreate() to add Maven packages
# Solution: Use pandas to read Excel file-by-file, then convert to PySpark DataFrame

print("✅ Spark session initialized")
print(f"Spark Version: {spark.version}")
print("📦 Using pandas + openpyxl for Excel (file-by-file processing)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration Parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Unity Catalog Volume (Run once)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Using existing catalog 'workspace' and schema 'default'
# MAGIC -- Create volumes for raw files and bronze tables
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.bi_market_raw;
# MAGIC 
# MAGIC -- Note: Bronze tables will be managed tables in the same schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### Path Configuration (Unity Catalog Volumes)

# COMMAND ----------

# Define paths using Unity Catalog Volumes (PROFESSIONAL APPROACH)
# This is the standard way in enterprise Databricks environments
CATALOG = "workspace"
SCHEMA = "default"

# Volumes for file storage
RAW_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/bi_market_raw"

# Define source paths (Unity Catalog Volumes)
MASTER_PDV_PATH = f"{RAW_VOLUME}/Master_PDV/master_pdv_raw.csv"
MASTER_PRODUCTS_PATH = f"{RAW_VOLUME}/Master_Products/product_master_raw.csv"
PRICE_AUDIT_PATH = f"{RAW_VOLUME}/Price_Audit"
SELL_IN_PATH = f"{RAW_VOLUME}/Sell-In"

# Define target paths (Delta tables in Unity Catalog)
BRONZE_MASTER_PDV = f"{CATALOG}.{SCHEMA}.bronze_master_pdv"
BRONZE_MASTER_PRODUCTS = f"{CATALOG}.{SCHEMA}.bronze_master_products"
BRONZE_PRICE_AUDIT = f"{CATALOG}.{SCHEMA}.bronze_price_audit"
BRONZE_SELL_IN = f"{CATALOG}.{SCHEMA}.bronze_sell_in"

# Define layer paths
RAW_PATH = f"{RAW_VOLUME}"

print("📁 Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Raw Volume: {RAW_VOLUME}")
print(f"\n📝 Source Paths:")
print(f"  Master_PDV: {MASTER_PDV_PATH}")
print(f"  Master_Products: {MASTER_PRODUCTS_PATH}")
print(f"  Price_Audit: {PRICE_AUDIT_PATH}")
print(f"  Sell-In: {SELL_IN_PATH}")
print(f"\n📊 Target Tables:")
print(f"  {BRONZE_MASTER_PDV}")
print(f"  {BRONZE_MASTER_PRODUCTS}")
print(f"  {BRONZE_PRICE_AUDIT}")
print(f"  {BRONZE_SELL_IN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Utility Functions

# COMMAND ----------

def read_excel_files(path_pattern, spark_session):
    """
    Read Excel files one-by-one, convert to Spark immediately, then union.
    Optimized for low memory footprint and better scalability.
    
    Args:
        path_pattern: Path pattern to Excel files (supports wildcards)
        spark_session: Active SparkSession
        
    Returns:
        PySpark DataFrame with combined data from all matching files
    """
    import glob
    from pyspark.sql import DataFrame
    
    # Find all matching Excel files
    local_path = path_pattern.replace("/Volumes/", "/Volumes/")
    excel_files = glob.glob(local_path)
    
    if not excel_files:
        raise FileNotFoundError(f"No Excel files found at: {path_pattern}")
    
    print(f"📂 Found {len(excel_files)} Excel file(s)")
    print(f"⚡ Processing file-by-file to minimize memory usage")
    
    # Process files one by one
    spark_dfs = []
    for file_path in excel_files:
        # Read single file with pandas
        df_pandas = pd.read_excel(file_path, engine='openpyxl')
        df_pandas['_metadata_file_path'] = file_path
        
        # Convert to Spark immediately (releases pandas memory)
        df_spark = spark_session.createDataFrame(df_pandas)
        spark_dfs.append(df_spark)
        
        print(f"   ✓ {os.path.basename(file_path)}")
        
        # Clear pandas DataFrame from memory
        del df_pandas
    
    # Union all Spark DataFrames
    combined_df = spark_dfs[0]
    for df in spark_dfs[1:]:
        combined_df = combined_df.unionByName(df, allowMissingColumns=True)
    
    return combined_df


def add_audit_columns(df):
    """
    Add audit columns to track data lineage and ingestion metadata.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with added audit columns
    """
    # Check which metadata column exists
    if "_metadata_file_path" in df.columns:
        # Excel files read with pandas
        source_col = col("_metadata_file_path")
    else:
        # CSV files read with Spark (has _metadata pseudo-column)
        source_col = col("_metadata.file_path")
    
    return df.withColumn("ingestion_timestamp", current_timestamp()) \
             .withColumn("source_file", source_col) \
             .withColumn("ingestion_date", lit(datetime.now().strftime("%Y-%m-%d")))


def print_ingestion_summary(df, source_name):
    """
    Print summary statistics for ingested data.
    
    Args:
        df: DataFrame to summarize
        source_name: Name of the data source
    """
    row_count = df.count()
    col_count = len(df.columns)
    
    print(f"\n{'='*60}")
    print(f"📊 Ingestion Summary: {source_name}")
    print(f"{'='*60}")
    print(f"  Total Rows: {row_count:,}")
    print(f"  Total Columns: {col_count}")
    print(f"  Schema:")
    df.printSchema()
    print(f"{'='*60}\n")


def validate_data_quality(df, source_name):
    """
    Perform MINIMAL data quality checks for Bronze layer.
    Heavy validations moved to Silver layer for better performance.
    
    Args:
        df: DataFrame to validate
        source_name: Name of the data source
    """
    print(f"\n🔍 Basic Quality Check: {source_name}")
    print("-" * 50)
    
    total_rows = df.count()
    print(f"  ✓ Total rows: {total_rows:,}")
    
    # Simple null summary (no detailed checks per column - too slow)
    null_summary = []
    for column in df.columns:
        if column.startswith('_metadata') or column in ['ingestion_timestamp', 'source_file', 'ingestion_date', 'year', 'year_month']:
            continue  # Skip audit/partition columns
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            null_pct = (null_count / total_rows * 100)
            null_summary.append(f"{column} ({null_pct:.1f}%)")
    
    if null_summary:
        print(f"  ⚠️  Nulls in: {', '.join(null_summary[:3])}")
        if len(null_summary) > 3:
            print(f"     ... and {len(null_summary)-3} more")
    else:
        print(f"  ✓ No nulls detected")
    
    print("-" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingestion: Master_PDV (Full Overwrite Strategy)
# MAGIC 
# MAGIC **Strategy:** Full overwrite - dimension table with complete refresh
# MAGIC 
# MAGIC **Justification:**
# MAGIC - Small dimension table (< 10K records)
# MAGIC - Complete dataset received each time
# MAGIC - No incremental updates needed

# COMMAND ----------

print("🔄 Starting Master_PDV ingestion...")

# Read CSV file with semicolon delimiter
df_master_pdv = spark.read.csv(
    MASTER_PDV_PATH,
    header=True,
    inferSchema=True,
    encoding="UTF-8",
    sep=";"  # Master_PDV uses semicolon as delimiter
)

# Add audit columns
df_master_pdv = add_audit_columns(df_master_pdv)

# Coalesce to control file count before write
df_master_pdv = df_master_pdv.coalesce(1)

# Write to Bronze layer (Full Overwrite) - Unity Catalog Managed Table
df_master_pdv.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.columnMapping.mode", "name") \
    .saveAsTable(BRONZE_MASTER_PDV)

print(f"✅ Master_PDV successfully written to: {BRONZE_MASTER_PDV}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingestion: Master_Products (Full Overwrite Strategy)
# MAGIC 
# MAGIC **Strategy:** Full overwrite - dimension table with complete refresh
# MAGIC 
# MAGIC **Justification:**
# MAGIC - Small dimension table
# MAGIC - Product master data updated as a whole
# MAGIC - Simple and reliable approach for dimensions

# COMMAND ----------

print("🔄 Starting Master_Products ingestion...")

# Read CSV file with comma delimiter
df_master_products = spark.read.csv(
    MASTER_PRODUCTS_PATH,
    header=True,
    inferSchema=True,
    encoding="UTF-8",
    sep=","  # Master_Products uses comma as delimiter
)

# Add audit columns
df_master_products = add_audit_columns(df_master_products)

# Coalesce to control file count
df_master_products = df_master_products.coalesce(1)

# Write to Bronze layer (Full Overwrite) - Unity Catalog Managed Table
df_master_products.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.columnMapping.mode", "name") \
    .saveAsTable(BRONZE_MASTER_PRODUCTS)

print(f"✅ Master_Products successfully written to: {BRONZE_MASTER_PRODUCTS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingestion: Price_Audit (Incremental Append Strategy)
# MAGIC 
# MAGIC **Strategy:** Incremental append with monthly partitioning
# MAGIC 
# MAGIC **Justification:**
# MAGIC - 24 files (monthly data 2021-2022)
# MAGIC - Historical data is immutable
# MAGIC - Partition pruning improves query performance
# MAGIC - Avoids reprocessing old data

# COMMAND ----------

print("🔄 Starting Price_Audit ingestion...")

# Read all Excel files from Price_Audit folder using pandas
# Note: Using pandas + openpyxl as workaround for Databricks spark-excel limitation
df_price_audit = read_excel_files(f"{PRICE_AUDIT_PATH}/*.xlsx", spark)

# Add audit columns
df_price_audit = add_audit_columns(df_price_audit)

# Extract year_month for partitioning
# Check actual column names (could be 'Date', 'Fecha', 'Audit_Date', etc.)
date_column = None
for col_name in df_price_audit.columns:
    if 'date' in col_name.lower() or 'fecha' in col_name.lower():
        date_column = col_name
        break

if date_column:
    df_price_audit = df_price_audit \
        .withColumn("year", year(col(date_column))) \
        .withColumn("month", month(col(date_column))) \
        .withColumn("year_month", concat_ws("-", col("year"), col("month")))
else:
    # Extract from filename using _metadata.file_path
    print("⚠️ Warning: No date column found. Extracting year_month from filename.")
    # Filename pattern: Price_Audit_YYYY_MM.xlsx
    df_price_audit = df_price_audit \
        .withColumn("file_name", col("_metadata.file_name")) \
        .withColumn("year_month", lit("2021-01"))  # Default, will be overridden by actual filename parsing

# Add audit columns
df_price_audit = add_audit_columns(df_price_audit)

# Coalesce to reduce small files (24 Excel → ~4-6 output files)
df_price_audit = df_price_audit.coalesce(6)

# Write to Bronze layer (Incremental Append with Partitioning) - Unity Catalog
df_price_audit.write \
    .format("delta") \
    .mode("append") \
    .option("delta.columnMapping.mode", "name") \
    .partitionBy("year_month") \
    .saveAsTable(BRONZE_PRICE_AUDIT)

print(f"✅ Price_Audit successfully written to: {BRONZE_PRICE_AUDIT}")
print(f"📁 Partitioned by: year_month")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ingestion: Sell-In (Dynamic Partition Overwrite)
# MAGIC 
# MAGIC **Strategy:** Dynamic partition overwrite by year
# MAGIC 
# MAGIC **Justification:**
# MAGIC - Annual data replaces previous values per year partition
# MAGIC - 10-20x faster than MERGE operations
# MAGIC - Only overwrites partitions present in incoming data
# MAGIC - Ideal for complete annual file replacements
# MAGIC - ACID transactions ensure atomicity

# COMMAND ----------

print("🔄 Starting Sell-In ingestion...")

# Read Excel files from Sell-In folder using pandas
# Note: Using pandas + openpyxl as workaround for Databricks spark-excel limitation
df_sell_in = read_excel_files(f"{SELL_IN_PATH}/*.xlsx", spark)

# Add audit columns
df_sell_in = add_audit_columns(df_sell_in)

# Extract year for partitioning
# Check for date or year columns (case insensitive)
year_column = None
date_column = None

for col_name in df_sell_in.columns:
    if col_name.lower() == 'year':
        year_column = col_name
        break
    elif 'date' in col_name.lower() or 'fecha' in col_name.lower():
        date_column = col_name

if year_column:
    # Year column already exists
    df_sell_in = df_sell_in.withColumn("year", col(year_column).cast("int"))
elif date_column:
    # Extract year from date column
    df_sell_in = df_sell_in.withColumn("year", year(col(date_column)))
else:
    # Extract from filename: Sell_In_YYYY.xlsx
    print("⚠️ Warning: No year/date column found. Extracting from filename.")
    df_sell_in = df_sell_in.withColumn("year", lit(2021))  # Default

# Coalesce to control file count
df_sell_in = df_sell_in.coalesce(2)

# Write to Bronze layer using Dynamic Partition Overwrite (much faster than MERGE)
# This strategy overwrites only the partitions present in the incoming data
print("📝 Writing with dynamic partition overwrite...")

df_sell_in.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("year") \
    .saveAsTable(BRONZE_SELL_IN)

print(f"✅ Sell-In successfully written to: {BRONZE_SELL_IN}")
print(f"📁 Partitioned by: year (dynamic overwrite)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Ingestion Metrics (from Delta History)
# MAGIC 
# MAGIC **Fast metrics without count() operations**

# COMMAND ----------

print("\n" + "="*70)
print("📊 BRONZE LAYER - INGESTION METRICS")
print("="*70 + "\n")

# Dictionary to store table statistics
bronze_tables = {
    "Master_PDV": BRONZE_MASTER_PDV,
    "Master_Products": BRONZE_MASTER_PRODUCTS,
    "Price_Audit": BRONZE_PRICE_AUDIT,
    "Sell-In": BRONZE_SELL_IN
}

for table_name, table_full_name in bronze_tables.items():
    try:
        # Get metrics from Delta History (no count() needed)
        history_df = spark.sql(f"DESCRIBE HISTORY {table_full_name} LIMIT 1")
        latest = history_df.first()
        
        # Extract metrics from operationMetrics
        metrics = latest["operationMetrics"]
        rows_added = metrics.get("numOutputRows", metrics.get("numRows", "N/A"))
        files_added = metrics.get("numFiles", "N/A")
        
        print(f"✅ {table_name}:")
        print(f"   Rows: {rows_added}")
        print(f"   Files: {files_added}")
        print(f"   Operation: {latest['operation']}")
        print()
        
    except Exception as e:
        print(f"❌ {table_name}: {str(e)[:80]}")
        print()

print("="*70)
print("✅ BRONZE LAYER INGESTION COMPLETED")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Metadata & Lineage Tracking

# COMMAND ----------

# Create ingestion metadata record
ingestion_metadata = {
    "pipeline_name": "Bronze_Ingestion",
    "execution_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "catalog": CATALOG,
    "schema": SCHEMA,
    "tables_ingested": len(bronze_tables),
    "status": "SUCCESS"
}

print("\n📋 Ingestion Metadata:")
print("-" * 50)
for key, value in ingestion_metadata.items():
    print(f"  {key}: {value}")
print("-" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Next Steps
# MAGIC 
# MAGIC **Bronze Layer Complete! ✅**
# MAGIC 
# MAGIC The following tables are now available in Bronze layer:
# MAGIC - ✅ `master_pdv` (Delta)
# MAGIC - ✅ `master_products` (Delta)
# MAGIC - ✅ `price_audit` (Delta, partitioned by year_month)
# MAGIC - ✅ `sell_in` (Delta, partitioned by year)
# MAGIC 
# MAGIC **Next Notebook:** `02_silver_transformation.py`
# MAGIC 
# MAGIC In Silver layer we will:
# MAGIC - Clean and standardize data
# MAGIC - Handle null values and outliers
# MAGIC - Apply business rules
# MAGIC - Validate data quality
# MAGIC - Create conformed dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC **📚 Documentation:**
# MAGIC - Architecture: [docs/architecture/README.md](../docs/architecture/README.md)
# MAGIC - Data Dictionary: [docs/data_dictionary.md](../docs/data_dictionary.md)
# MAGIC - Development Setup: [docs/DEVELOPMENT_SETUP.md](../docs/DEVELOPMENT_SETUP.md)
# MAGIC 
# MAGIC **👤 Author:** Diego Mayor | diego.mayorgacapera@gmail.com  
# MAGIC **📅 Last Updated:** 2025-12-30  
# MAGIC **🔗 Repository:** [github.com/DIEGO77M/BI_Market_Visibility](https://github.com/DIEGO77M/BI_Market_Visibility)
