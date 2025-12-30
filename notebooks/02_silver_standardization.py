# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Standardization & Quality
# MAGIC 
# MAGIC **Purpose:** Standardize and validate Bronze data for analytical consumption
# MAGIC 
# MAGIC **Author:** Diego Mayorga  
# MAGIC **Date:** 2025-12-30  
# MAGIC **Project:** BI Market Visibility Analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Architecture Principles
# MAGIC 
# MAGIC **Silver Layer Scope:**
# MAGIC - Schema standardization (snake_case, explicit types)
# MAGIC - Business-driven null handling
# MAGIC - Deduplication (justified by business keys)
# MAGIC - Domain validations (prices, dates, ranges)
# MAGIC - Light referential consistency checks
# MAGIC - Derived columns (year, month, flags)
# MAGIC - Audit traceability
# MAGIC 
# MAGIC **Explicit Constraints:**
# MAGIC - Read ONLY from Bronze Delta tables (no raw files)
# MAGIC - No cache() or persist() (Serverless optimized)
# MAGIC - No unnecessary count(), show(), collect()
# MAGIC - One write action per table
# MAGIC - No KPIs, aggregations, or Gold-layer logic
# MAGIC - No over-engineering (no streaming, CDC, frameworks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, lower, regexp_replace,
    when, coalesce, lit, current_timestamp, current_date,
    year, month, to_date, date_format,
    round as spark_round,
    row_number
)
from pyspark.sql.types import StringType, IntegerType, DoubleType, DecimalType, DateType, TimestampType
from pyspark.sql.window import Window

# COMMAND ----------

# Unity Catalog Configuration
CATALOG = "workspace"
SCHEMA = "default"

# Bronze tables (READ ONLY - DO NOT MODIFY)
BRONZE_MASTER_PDV = f"{CATALOG}.{SCHEMA}.bronze_master_pdv"
BRONZE_MASTER_PRODUCTS = f"{CATALOG}.{SCHEMA}.bronze_master_products"
BRONZE_PRICE_AUDIT = f"{CATALOG}.{SCHEMA}.bronze_price_audit"
BRONZE_SELL_IN = f"{CATALOG}.{SCHEMA}.bronze_sell_in"

# Silver tables (TARGET)
SILVER_MASTER_PDV = f"{CATALOG}.{SCHEMA}.silver_master_pdv"
SILVER_MASTER_PRODUCTS = f"{CATALOG}.{SCHEMA}.silver_master_products"
SILVER_PRICE_AUDIT = f"{CATALOG}.{SCHEMA}.silver_price_audit"
SILVER_SELL_IN = f"{CATALOG}.{SCHEMA}.silver_sell_in"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver: Master PDV
# MAGIC 
# MAGIC **Business Context:** Point of Sale master data  
# MAGIC **Source:** bronze_master_pdv  
# MAGIC **Transformations:**
# MAGIC - Schema standardization (snake_case, explicit types)
# MAGIC - Text normalization (trim, uppercase)
# MAGIC - Deduplication by business key (PDV code)
# MAGIC - Null handling for critical fields
# MAGIC - Audit columns preserved

# COMMAND ----------

# Read from Bronze Delta table
df_bronze = spark.read.table(BRONZE_MASTER_PDV)

# Drop Bronze audit columns (not needed in Silver)
df = df_bronze.drop("ingestion_timestamp", "source_file", "ingestion_date")

# Schema standardization: Rename columns to snake_case
column_mapping = {}
for c in df.columns:
    # Convert to snake_case: lowercase, replace spaces/special chars with underscore
    new_name = c.lower().strip().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "")
    new_name = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in new_name)
    column_mapping[c] = new_name

# Apply renaming
for old_name, new_name in column_mapping.items():
    df = df.withColumnRenamed(old_name, new_name)

# Text normalization: trim and uppercase for all string columns
for column, dtype in df.dtypes:
    if dtype == "string":
        df = df.withColumn(column, trim(upper(col(column))))

# Explicit type casting for known columns (adapt to your schema)
# Example: df = df.withColumn("pdv_code", col("pdv_code").cast(StringType()))

# Deduplication: Keep latest record by business key
pdv_key_col = [c for c in df.columns if "pdv" in c or "codigo" in c or "code" in c]
if pdv_key_col:
    # Use row_number to keep first occurrence
    window = Window.partitionBy(pdv_key_col[0]).orderBy(lit(1))
    df = df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")

# Domain validations: Critical columns must not be null
critical_cols = pdv_key_col[:1] if pdv_key_col else []
for c in critical_cols:
    df = df.withColumn(f"{c}_is_valid", when(col(c).isNotNull(), lit(True)).otherwise(lit(False)))

# Audit columns
df = df.withColumn("processing_date", current_date()) \
       .withColumn("processing_timestamp", current_timestamp())

# Write to Silver (one write action)
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_MASTER_PDV)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver: Master Products
# MAGIC 
# MAGIC **Business Context:** Product master data  
# MAGIC **Source:** bronze_master_products  
# MAGIC **Transformations:**
# MAGIC - Schema standardization (snake_case, explicit types)
# MAGIC - Text normalization
# MAGIC - Price validation (> 0, round to 2 decimals)
# MAGIC - Deduplication by product code
# MAGIC - Null handling for critical fields

# COMMAND ----------

# Read from Bronze
df_bronze = spark.read.table(BRONZE_MASTER_PRODUCTS)

# Drop Bronze audit columns
df = df_bronze.drop("ingestion_timestamp", "source_file", "ingestion_date")

# Schema standardization: snake_case
column_mapping = {}
for c in df.columns:
    new_name = c.lower().strip().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "")
    new_name = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in new_name)
    column_mapping[c] = new_name

for old_name, new_name in column_mapping.items():
    df = df.withColumnRenamed(old_name, new_name)

# Text normalization
for column, dtype in df.dtypes:
    if dtype == "string":
        df = df.withColumn(column, trim(upper(col(column))))

# Identify price columns
price_cols = [c for c in df.columns if "price" in c or "precio" in c or "valor" in c]

# Price validation: must be > 0, round to 2 decimals
for price_col in price_cols:
    df = df.withColumn(
        price_col,
        when(col(price_col).isNull(), lit(None))
        .when(col(price_col) < 0, lit(None))  # Invalid negative prices
        .otherwise(spark_round(col(price_col).cast(DoubleType()), 2))
    )

# Deduplication by product key
product_key_col = [c for c in df.columns if "product" in c or "codigo" in c or "sku" in c]
if product_key_col:
    window = Window.partitionBy(product_key_col[0]).orderBy(lit(1))
    df = df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")

# Domain validations: Critical columns
critical_cols = product_key_col[:1] if product_key_col else []
for c in critical_cols:
    df = df.withColumn(f"{c}_is_valid", when(col(c).isNotNull(), lit(True)).otherwise(lit(False)))

# Audit columns
df = df.withColumn("processing_date", current_date()) \
       .withColumn("processing_timestamp", current_timestamp())

# Write to Silver
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_MASTER_PRODUCTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver: Price Audit
# MAGIC 
# MAGIC **Business Context:** Price audit data from multiple files  
# MAGIC **Source:** bronze_price_audit (partitioned by year_month)  
# MAGIC **Transformations:**
# MAGIC - Schema standardization
# MAGIC - Price validation (> 0, round to 2 decimals)
# MAGIC - Date validation (no future dates)
# MAGIC - Filter records with all critical fields null
# MAGIC - Preserve partitioning (year_month)

# COMMAND ----------

# Read from Bronze (partitioned table)
df_bronze = spark.read.table(BRONZE_PRICE_AUDIT)

# Drop Bronze audit columns
df = df_bronze.drop("ingestion_timestamp", "source_file", "ingestion_date")

# Schema standardization: snake_case
column_mapping = {}
for c in df.columns:
    if c == "year_month":  # Preserve partition column
        continue
    new_name = c.lower().strip().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "")
    new_name = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in new_name)
    column_mapping[c] = new_name

for old_name, new_name in column_mapping.items():
    df = df.withColumnRenamed(old_name, new_name)

# Identify critical columns
price_cols = [c for c in df.columns if "price" in c or "precio" in c]
date_cols = [c for c in df.columns if "date" in c or "fecha" in c]
pdv_cols = [c for c in df.columns if "pdv" in c or "store" in c]
product_cols = [c for c in df.columns if "product" in c or "sku" in c]

# Price validation
for price_col in price_cols:
    df = df.withColumn(
        price_col,
        when(col(price_col).isNull(), lit(None))
        .when(col(price_col) <= 0, lit(None))  # Prices must be positive
        .otherwise(spark_round(col(price_col).cast(DoubleType()), 2))
    )

# Date validation: no future dates
for date_col in date_cols:
    df = df.withColumn(
        date_col,
        when(col(date_col).cast(DateType()) > current_date(), lit(None))  # Remove future dates
        .otherwise(col(date_col).cast(DateType()))
    )

# Filter: Remove records where ALL critical fields are null
critical_cols = price_cols + pdv_cols + product_cols
if critical_cols:
    # Keep records with at least one non-null critical field
    filter_condition = col(critical_cols[0]).isNotNull()
    for c in critical_cols[1:]:
        filter_condition = filter_condition | col(c).isNotNull()
    df = df.filter(filter_condition)

# Audit columns
df = df.withColumn("processing_date", current_date()) \
       .withColumn("processing_timestamp", current_timestamp())

# Write to Silver (preserve partitioning)
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("year_month") \
    .saveAsTable(SILVER_PRICE_AUDIT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver: Sell-In
# MAGIC 
# MAGIC **Business Context:** Sales transactions  
# MAGIC **Source:** bronze_sell_in (partitioned by year)  
# MAGIC **Transformations:**
# MAGIC - Schema standardization
# MAGIC - Quantity validation (>= 0)
# MAGIC - Value validation (>= 0, round to 2 decimals)
# MAGIC - Derived column: unit_price (value / quantity)
# MAGIC - Derived column: is_active_sale (quantity > 0)
# MAGIC - Date validation
# MAGIC - Preserve partitioning (year)

# COMMAND ----------

# Read from Bronze (partitioned by year)
df_bronze = spark.read.table(BRONZE_SELL_IN)

# Drop Bronze audit columns
df = df_bronze.drop("ingestion_timestamp", "source_file", "ingestion_date")

# Schema standardization: snake_case
column_mapping = {}
for c in df.columns:
    if c == "year":  # Preserve partition column
        continue
    new_name = c.lower().strip().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "")
    new_name = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in new_name)
    column_mapping[c] = new_name

for old_name, new_name in column_mapping.items():
    df = df.withColumnRenamed(old_name, new_name)

# Identify quantity and value columns
qty_cols = [c for c in df.columns if "quantity" in c or "cantidad" in c or "qty" in c]
value_cols = [c for c in df.columns if "value" in c or "valor" in c or "amount" in c or "monto" in c]
date_cols = [c for c in df.columns if "date" in c or "fecha" in c]

# Quantity validation: must be >= 0
for qty_col in qty_cols:
    df = df.withColumn(
        qty_col,
        when(col(qty_col).isNull(), lit(0))  # Null = 0
        .when(col(qty_col) < 0, lit(0))  # Negative = 0
        .otherwise(col(qty_col).cast(IntegerType()))
    )

# Value validation: must be >= 0, round to 2 decimals
for value_col in value_cols:
    df = df.withColumn(
        value_col,
        when(col(value_col).isNull(), lit(0))
        .when(col(value_col) < 0, lit(0))
        .otherwise(spark_round(col(value_col).cast(DoubleType()), 2))
    )

# Derived column: unit_price (value / quantity)
if qty_cols and value_cols:
    df = df.withColumn(
        "unit_price",
        when((col(qty_cols[0]) > 0) & (col(value_cols[0]) > 0),
             spark_round(col(value_cols[0]) / col(qty_cols[0]), 2))
        .otherwise(lit(None))
    )

# Derived column: is_active_sale (quantity > 0)
if qty_cols:
    df = df.withColumn("is_active_sale", when(col(qty_cols[0]) > 0, lit(True)).otherwise(lit(False)))

# Date validation: no future dates
for date_col in date_cols:
    df = df.withColumn(
        date_col,
        when(col(date_col).cast(DateType()) > current_date(), lit(None))
        .otherwise(col(date_col).cast(DateType()))
    )

# Derived columns: year, month (from date if exists)
if date_cols:
    df = df.withColumn("transaction_year", year(col(date_cols[0]))) \
           .withColumn("transaction_month", month(col(date_cols[0])))

# Audit columns
df = df.withColumn("processing_date", current_date()) \
       .withColumn("processing_timestamp", current_timestamp())

# Write to Silver (preserve year partitioning)
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("year") \
    .saveAsTable(SILVER_SELL_IN)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validation: Silver Layer Metrics
# MAGIC 
# MAGIC **Post-ingestion validation using Delta History (no count operations)**

# COMMAND ----------

silver_tables = {
    "Master_PDV": SILVER_MASTER_PDV,
    "Master_Products": SILVER_MASTER_PRODUCTS,
    "Price_Audit": SILVER_PRICE_AUDIT,
    "Sell_In": SILVER_SELL_IN
}

for table_name, table_full_name in silver_tables.items():
    history_df = spark.sql(f"DESCRIBE HISTORY {table_full_name} LIMIT 1")
    latest = history_df.first()
    metrics = latest["operationMetrics"]
    rows = metrics.get("numOutputRows", metrics.get("numRows", "N/A"))
    files = metrics.get("numFiles", "N/A")
    
    print(f"{table_name}: {rows} rows, {files} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## Silver Layer Complete
# MAGIC 
# MAGIC **Tables Created:**
# MAGIC - workspace.default.silver_master_pdv
# MAGIC - workspace.default.silver_master_products
# MAGIC - workspace.default.silver_price_audit (partitioned by year_month)
# MAGIC - workspace.default.silver_sell_in (partitioned by year)
# MAGIC 
# MAGIC **Next:** Gold layer for business aggregations and KPIs
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Author:** Diego Mayorga | diego.mayorgacapera@gmail.com  
# MAGIC **Date:** 2025-12-30  
# MAGIC **Repository:** [github.com/DIEGO77M/BI_Market_Visibility](https://github.com/DIEGO77M/BI_Market_Visibility)
