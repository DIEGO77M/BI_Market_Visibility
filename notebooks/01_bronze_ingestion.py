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

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, year, month, concat_ws,
    input_file_name, count, countDistinct, sum as spark_sum
)
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze_Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("✅ Spark session initialized")
print(f"Spark Version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration Parameters

# COMMAND ----------

# Define paths - Adapt these paths based on your environment
# For Databricks workspace (bundle deployment)
BASE_PATH_WORKSPACE = "/Workspace/Users/diego.mayorgacapera@gmail.com/.bundle/BI_Market_Visibility/dev/files"

# For DBFS (if available)
BASE_PATH_DBFS = "dbfs:/mnt/bi_market_visibility"

# For local testing
BASE_PATH_LOCAL = "."

# Select active path (change based on environment)
ENVIRONMENT = "workspace"  # Options: "workspace", "dbfs", "local"

if ENVIRONMENT == "workspace":
    BASE_PATH = BASE_PATH_WORKSPACE
elif ENVIRONMENT == "dbfs":
    BASE_PATH = BASE_PATH_DBFS
else:
    BASE_PATH = BASE_PATH_LOCAL

# Define layer paths
RAW_PATH = f"{BASE_PATH}/data/raw"
BRONZE_PATH = f"{BASE_PATH}/data/bronze"

# Define source paths
MASTER_PDV_PATH = f"{RAW_PATH}/Master_PDV/master_pdv_raw.csv"
MASTER_PRODUCTS_PATH = f"{RAW_PATH}/Master_Products/product_master_raw.csv"
PRICE_AUDIT_PATH = f"{RAW_PATH}/Price_Audit"
SELL_IN_PATH = f"{RAW_PATH}/Sell-In"

# Define target paths
BRONZE_MASTER_PDV = f"{BRONZE_PATH}/master_pdv"
BRONZE_MASTER_PRODUCTS = f"{BRONZE_PATH}/master_products"
BRONZE_PRICE_AUDIT = f"{BRONZE_PATH}/price_audit"
BRONZE_SELL_IN = f"{BRONZE_PATH}/sell_in"

print("📁 Configuration:")
print(f"  Environment: {ENVIRONMENT}")
print(f"  Base Path: {BASE_PATH}")
print(f"  Raw Path: {RAW_PATH}")
print(f"  Bronze Path: {BRONZE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Utility Functions

# COMMAND ----------

def add_audit_columns(df):
    """
    Add audit columns to track data lineage and ingestion metadata.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with added audit columns
    """
    return df.withColumn("ingestion_timestamp", current_timestamp()) \
             .withColumn("source_file", input_file_name()) \
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


def validate_data_quality(df, source_name, key_columns):
    """
    Perform basic data quality checks.
    
    Args:
        df: DataFrame to validate
        source_name: Name of the data source
        key_columns: List of key columns to check for nulls
        
    Returns:
        Boolean indicating if validation passed
    """
    print(f"\n🔍 Data Quality Validation: {source_name}")
    print("-" * 50)
    
    # Check for null values in key columns
    null_checks = []
    for column in key_columns:
        null_count = df.filter(col(column).isNull()).count()
        null_pct = (null_count / df.count() * 100) if df.count() > 0 else 0
        status = "✅ PASS" if null_count == 0 else f"⚠️ WARN ({null_pct:.2f}%)"
        print(f"  {column}: {null_count:,} nulls - {status}")
        null_checks.append(null_count == 0)
    
    # Check for duplicates
    total_rows = df.count()
    distinct_rows = df.dropDuplicates(key_columns).count()
    duplicate_count = total_rows - distinct_rows
    dup_status = "✅ PASS" if duplicate_count == 0 else f"⚠️ WARN ({duplicate_count:,} duplicates)"
    print(f"  Duplicates: {dup_status}")
    
    print("-" * 50)
    
    # Overall validation result
    validation_passed = all(null_checks) and duplicate_count == 0
    return validation_passed

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

# Read CSV file
df_master_pdv = spark.read.csv(
    MASTER_PDV_PATH,
    header=True,
    inferSchema=True,
    encoding="UTF-8"
)

# Add audit columns
df_master_pdv = add_audit_columns(df_master_pdv)

# Print summary
print_ingestion_summary(df_master_pdv, "Master_PDV")

# Validate data quality (assuming 'pdv_id' is the primary key)
# Adjust key columns based on actual schema
key_columns_pdv = ["pdv_id"] if "pdv_id" in df_master_pdv.columns else df_master_pdv.columns[:1]
validate_data_quality(df_master_pdv, "Master_PDV", key_columns_pdv)

# Write to Bronze layer (Full Overwrite)
df_master_pdv.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(BRONZE_MASTER_PDV)

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

# Read CSV file
df_master_products = spark.read.csv(
    MASTER_PRODUCTS_PATH,
    header=True,
    inferSchema=True,
    encoding="UTF-8"
)

# Add audit columns
df_master_products = add_audit_columns(df_master_products)

# Print summary
print_ingestion_summary(df_master_products, "Master_Products")

# Validate data quality
key_columns_products = ["product_id"] if "product_id" in df_master_products.columns else df_master_products.columns[:1]
validate_data_quality(df_master_products, "Master_Products", key_columns_products)

# Write to Bronze layer (Full Overwrite)
df_master_products.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(BRONZE_MASTER_PRODUCTS)

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

# Read all Excel files from Price_Audit folder
df_price_audit = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{PRICE_AUDIT_PATH}/*.xlsx")

# Add audit columns
df_price_audit = add_audit_columns(df_price_audit)

# Extract year_month from filename or date column for partitioning
# Assuming there's a 'date' or 'audit_date' column
# Adjust based on actual schema
if "date" in df_price_audit.columns:
    df_price_audit = df_price_audit \
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("year_month", concat_ws("-", col("year"), col("month")))
else:
    # If no date column, extract from filename
    # This is a fallback - adjust based on actual schema
    print("⚠️ Warning: No 'date' column found. Using current date for partitioning.")
    df_price_audit = df_price_audit \
        .withColumn("year_month", lit(datetime.now().strftime("%Y-%m")))

# Print summary
print_ingestion_summary(df_price_audit, "Price_Audit")

# Validate data quality
key_columns_price = ["product_id", "pdv_id", "date"] if all(c in df_price_audit.columns for c in ["product_id", "pdv_id", "date"]) else df_price_audit.columns[:2]
validate_data_quality(df_price_audit, "Price_Audit", key_columns_price)

# Write to Bronze layer (Incremental Append with Partitioning)
df_price_audit.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("year_month") \
    .save(BRONZE_PRICE_AUDIT)

print(f"✅ Price_Audit successfully written to: {BRONZE_PRICE_AUDIT}")
print(f"📁 Partitioned by: year_month")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ingestion: Sell-In (Merge/Upsert Strategy)
# MAGIC 
# MAGIC **Strategy:** Merge (Upsert) by year to handle corrections
# MAGIC 
# MAGIC **Justification:**
# MAGIC - Annual data may receive corrections/updates
# MAGIC - Prevents duplicate records
# MAGIC - Maintains data integrity with ACID transactions
# MAGIC - Time travel enables audit of changes

# COMMAND ----------

print("🔄 Starting Sell-In ingestion...")

# Read Excel files from Sell-In folder
df_sell_in = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{SELL_IN_PATH}/*.xlsx")

# Add audit columns
df_sell_in = add_audit_columns(df_sell_in)

# Extract year for partitioning
# Adjust based on actual schema
if "date" in df_sell_in.columns:
    df_sell_in = df_sell_in.withColumn("year", year(col("date")))
elif "year" not in df_sell_in.columns:
    print("⚠️ Warning: No 'date' or 'year' column found. Using current year.")
    df_sell_in = df_sell_in.withColumn("year", lit(datetime.now().year))

# Print summary
print_ingestion_summary(df_sell_in, "Sell-In")

# Validate data quality
key_columns_sellin = ["product_id", "year"] if all(c in df_sell_in.columns for c in ["product_id", "year"]) else df_sell_in.columns[:2]
validate_data_quality(df_sell_in, "Sell-In", key_columns_sellin)

# Check if Bronze table exists
try:
    # If table exists, perform MERGE (Upsert)
    deltaTable = DeltaTable.forPath(spark, BRONZE_SELL_IN)
    
    print("📝 Performing MERGE operation...")
    
    # Define merge condition (adjust based on your business keys)
    merge_condition = "target.product_id = source.product_id AND target.year = source.year"
    
    deltaTable.alias("target").merge(
        df_sell_in.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    print(f"✅ Sell-In successfully merged to: {BRONZE_SELL_IN}")
    
except Exception as e:
    # If table doesn't exist, create it
    print(f"📝 Table doesn't exist. Creating new table...")
    
    df_sell_in.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year") \
        .save(BRONZE_SELL_IN)
    
    print(f"✅ Sell-In successfully written to: {BRONZE_SELL_IN}")

print(f"📁 Partitioned by: year")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Quality Checks & Validation

# COMMAND ----------

print("\n" + "="*70)
print("📊 BRONZE LAYER - FINAL DATA QUALITY REPORT")
print("="*70 + "\n")

# Dictionary to store table statistics
bronze_tables = {
    "Master_PDV": BRONZE_MASTER_PDV,
    "Master_Products": BRONZE_MASTER_PRODUCTS,
    "Price_Audit": BRONZE_PRICE_AUDIT,
    "Sell-In": BRONZE_SELL_IN
}

summary_stats = []

for table_name, table_path in bronze_tables.items():
    try:
        df_check = spark.read.format("delta").load(table_path)
        row_count = df_check.count()
        col_count = len(df_check.columns)
        
        # Check for partition columns
        partitions = "None"
        if "year_month" in df_check.columns:
            partitions = f"year_month ({df_check.select('year_month').distinct().count()} partitions)"
        elif "year" in df_check.columns:
            partitions = f"year ({df_check.select('year').distinct().count()} partitions)"
        
        summary_stats.append({
            "Table": table_name,
            "Rows": f"{row_count:,}",
            "Columns": col_count,
            "Partitions": partitions,
            "Status": "✅ Success"
        })
        
        print(f"✅ {table_name}: {row_count:,} rows, {col_count} columns")
        
    except Exception as e:
        summary_stats.append({
            "Table": table_name,
            "Rows": "0",
            "Columns": 0,
            "Partitions": "N/A",
            "Status": f"❌ Error: {str(e)[:50]}"
        })
        print(f"❌ {table_name}: Error reading table")

print("\n" + "="*70)
print("✅ BRONZE LAYER INGESTION COMPLETED")
print("="*70)

# Display summary as DataFrame
df_summary = spark.createDataFrame(summary_stats)
display(df_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Metadata & Lineage Tracking

# COMMAND ----------

# Create ingestion metadata record
ingestion_metadata = {
    "pipeline_name": "Bronze_Ingestion",
    "execution_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "environment": ENVIRONMENT,
    "tables_ingested": len(bronze_tables),
    "status": "SUCCESS",
    "bronze_path": BRONZE_PATH
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
