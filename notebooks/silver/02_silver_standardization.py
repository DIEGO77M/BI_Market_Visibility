# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Standardization & Quality
# MAGIC 
# MAGIC **Purpose:** Transform Bronze snapshots into analysis-ready datasets with consistent schema, validated domains, and deterministic quality controls. All columns are standardized to English and snake_case, following the project data dictionary.
# MAGIC 
# MAGIC **Author:** Diego Mayorga  
# MAGIC **Date:** 2025-12-30  
# MAGIC **Project:** BI Market Visibility Analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Design Philosophy
# MAGIC 
# MAGIC Silver Layer implements **curated standardization** with minimal yet sufficient transformations for analytical consumption. This layer bridges raw ingestion (Bronze) and business logic (Gold) by establishing data contracts through schema normalization, domain validation, and deterministic deduplication. All column names are converted to English and snake_case for cross-platform compatibility and business clarity.
# MAGIC 
# MAGIC ### Architectural Decisions
# MAGIC 
# MAGIC **1. Snapshot-Based Processing**
# MAGIC - Reads complete Bronze Delta tables (no incremental complexity)
# MAGIC - Simplifies lineage and replay scenarios
# MAGIC - Enables deterministic deduplication with temporal ordering
# MAGIC 
# MAGIC **2. Serverless-First Design**
# MAGIC - No `cache()` or `persist()` operations (incompatible with Databricks Serverless)
# MAGIC - One write action per dataset (atomic operations)
# MAGIC - Dynamic partition overwrite for fact tables (optimized rewrites)
# MAGIC 
# MAGIC **3. Schema Stability**
# MAGIC - Explicit type casting for business keys and numeric fields
# MAGIC - `mergeSchema=false` on master tables (controlled evolution)
# MAGIC - snake_case standardization for cross-platform compatibility
# MAGIC 
# MAGIC **4. Intentional Null Preservation**
# MAGIC - Nulls represent missing data (not default values)
# MAGIC - Completeness flags signal data quality without imputation
# MAGIC - Enables transparent downstream analysis decisions
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Silver Layer Scope
# MAGIC 
# MAGIC **Included Transformations:**
# MAGIC - Schema standardization (snake_case, explicit types)
# MAGIC - Text normalization (trim, uppercase for dimensions)
# MAGIC - Deduplication by business keys with deterministic ordering
# MAGIC - Domain validations (price positivity, date ranges, quantity sanity)
# MAGIC - Derived technical columns (month extraction, completeness flags)
# MAGIC - Temporal lineage preservation (Bronze ingestion timestamp)
# MAGIC 
# MAGIC **Explicitly Excluded:**
# MAGIC - KPIs and business metrics (Gold Layer responsibility)
# MAGIC - Cross-dataset joins (Gold Layer aggregation)
# MAGIC - Complex aggregations or window functions beyond deduplication
# MAGIC - Data quality frameworks (over-engineering for portfolio scale)
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Data Quality Strategy
# MAGIC 
# MAGIC **Validation Approach:**
# MAGIC - **Master Data:** Strict filtering (null business keys rejected)
# MAGIC - **Transactional Data:** Preserve nulls + completeness flags
# MAGIC - **Price/Value Fields:** Unified criteria (strictly positive)
# MAGIC - **Dates:** Future dates nullified (historical analysis assumption)
# MAGIC 
# MAGIC **Metrics via Delta History:**
# MAGIC - Post-write validation uses `DESCRIBE HISTORY` (zero compute cost)
# MAGIC - No `count()` operations (Serverless optimization)
# MAGIC - Metrics extracted from operation metadata

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

# --- Utility: Standardize column names to snake_case ---
def standardize_column_names(df, exclude_cols=None):
    """
    Convert all column names to snake_case and English.
    Args:
        df: DataFrame to standardize
        exclude_cols: List of columns to preserve (e.g., partition keys)
    Returns:
        DataFrame with standardized column names in English
    """
    exclude_cols = exclude_cols or []
    # Spanish to English mapping (extend as needed)
    spanish_to_english = {
        "precio": "price",
        "fecha": "date",
        "pdv": "store_code",
        "codigo_pdv": "store_code",
        "producto": "product_code",
        "canal": "channel",
        "motivo": "reason",
        "observacion": "observation",
        "stock": "stock",
        "venta": "sales",
        "marca": "brand",
        "categoria": "category",
        "subcategoria": "subcategory",
        "region": "region",
        "zona": "zone",
        "ciudad": "city",
        "estado": "state",
        "pais": "country",
        "nombre": "name"
    }
    for old_name in df.columns:
        if old_name in exclude_cols:
            continue
        new_name = old_name.lower().strip()
        new_name = new_name.replace(" ", "_").replace("-", "_")
        new_name = new_name.replace("(", "").replace(")", "")
        new_name = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in new_name)
        # Map Spanish to English if applicable
        if new_name in spanish_to_english:
            new_name = spanish_to_english[new_name]
        df = df.withColumnRenamed(old_name, new_name)
    return df
    return df

from pyspark.sql.types import StringType, IntegerType, DoubleType, DecimalType, DateType, TimestampType
from pyspark.sql.window import Window

# --- Data Quality Metrics Logging ---
def log_data_quality(df, table_name, price_cols=None, date_cols=None):
    """
    Log basic data quality metrics for Silver layer.
    Args:
        df: DataFrame to analyze
        table_name: Logical table name (for printout)
        price_cols: List of price/value columns (optional)
        date_cols: List of date columns (optional)
    """
    total_records = df.count()
    invalid_prices = 0
    invalid_dates = 0
    if price_cols:
        invalid_prices = df.filter(
            sum([col(c).isNull().cast("int") for c in price_cols]) > 0
        ).count()
    if date_cols:
        invalid_dates = df.filter(
            sum([col(c).isNull().cast("int") for c in date_cols]) > 0
        ).count()
    print(f"✅ {table_name} Quality Metrics:")
    print(f"   Total: {total_records:,}")
    if price_cols:
        print(f"   Invalid Prices: {invalid_prices:,} ({invalid_prices/total_records*100 if total_records else 0:.1f}%)")
    if date_cols:
        print(f"   Invalid Dates: {invalid_dates:,} ({invalid_dates/total_records*100 if total_records else 0:.1f}%)")


def clean_european_number_format(df, column_name):
    """
    Clean European number format (180.562.475) to standard double format.
    
    European format uses:
    - Dots (.) as thousand separators  
    - Comma (,) as decimal separator
    
    This function handles both cases:
    - "180.562.475" (integer with thousand separators) -> 180562475.0
    - "180.562,475" (decimal with thousand separators) -> 180562.475
    
    Args:
        df: Input DataFrame
        column_name: Column to clean
        
    Returns:
        DataFrame with cleaned column as DoubleType
    """
    temp_col = f"{column_name}_cleaned"
    
    return df.withColumn(
        temp_col,
        # Step 1: Replace comma with placeholder (preserve decimal comma)
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    col(column_name).cast("string"),
                    ",", "##DECIMAL##"  # Preserve decimal comma
                ),
                "\\.", ""  # Remove all dots (thousand separators)
            ),
            "##DECIMAL##", "."  # Restore decimal as dot
        ).cast("double")
    ).drop(column_name).withColumnRenamed(temp_col, column_name)

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
# MAGIC **Business Context:** Point of Sale (PDV) master dimension containing store attributes, geographic coordinates, and organizational hierarchy.
# MAGIC 
# MAGIC **Source:** `bronze_master_pdv` (full snapshot, semicolon-delimited CSV origin)
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### Design Decisions
# MAGIC 
# MAGIC **Deduplication Strategy:**
# MAGIC - **Business Key:** `code_eleader` (standardized from "Code (eLeader)")
# MAGIC - **Selection Criteria:** Most recent record by `bronze_ingestion_timestamp`
# MAGIC - **Rationale:** Temporal ordering ensures reproducibility across pipeline runs
# MAGIC 
# MAGIC **Type Enforcement:**
# MAGIC - `code_eleader` → `StringType` (business identifier)
# MAGIC - `latitude`, `longitude` → `DoubleType` (geographic precision)
# MAGIC - Prevents schema drift and type inference inconsistencies
# MAGIC 
# MAGIC **Text Normalization:**
# MAGIC - Uppercase transformation for dimensional attributes (e.g., store names, channels)
# MAGIC - Facilitates case-insensitive joins and reduces duplicate detection surface
# MAGIC 
# MAGIC **Quality Control:**
# MAGIC - Records with null `code_eleader` filtered out (invalid dimension members)
# MAGIC - Bronze lineage preserved via `bronze_ingestion_timestamp` for audit trail
# MAGIC 
# MAGIC **Output Schema:** Standardized snake_case, explicit types, processing metadata

# COMMAND ----------


# Read from Bronze Delta table
df_bronze = spark.read.table(BRONZE_MASTER_PDV)
# Preserve Bronze ingestion timestamp for lineage, drop other audit columns
df = df_bronze.withColumnRenamed("_metadata_ingestion_timestamp", "bronze_ingestion_timestamp") \
              .drop("_metadata_source_file", "_metadata_ingestion_date", "_metadata_batch_id")
# Deduplication: Keep most recent record by PDV code (deterministic)
if "code_eleader" in df.columns and "bronze_ingestion_timestamp" in df.columns:
    window = Window.partitionBy("code_eleader").orderBy(col("bronze_ingestion_timestamp").desc_nulls_last())
    df = df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
elif "code_eleader" in df.columns:
    window = Window.partitionBy("code_eleader").orderBy([col(c) for c in sorted(df.columns) if c != "code_eleader"])
    df = df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
# Schema standardization
df = standardize_column_names(df)
# Text normalization: trim and uppercase for all string columns
for column, dtype in df.dtypes:
    if dtype == "string":
        df = df.withColumn(column, trim(upper(col(column))))
# Explicit type casting for critical columns
if "code_eleader" in df.columns:
    df = df.withColumn("code_eleader", col("code_eleader").cast(StringType()))
# Clean European number format for latitude/longitude (e.g., "180.562.475" -> 180562.475)
if "latitude" in df.columns:
    df = clean_european_number_format(df, "latitude")
if "longitude" in df.columns:
    df = clean_european_number_format(df, "longitude")
# Filter: Remove records where PDV code is null (critical business field)
if "code_eleader" in df.columns:
    df = df.filter(col("code_eleader").isNotNull())

# Audit columns
df = df.withColumn("processing_date", current_date()) \
    .withColumn("processing_timestamp", current_timestamp())

# Logging (no price/date columns in PDV, so only total)
log_data_quality(df, "silver_master_pdv")

# Write to Silver (one write action)
# Note: overwriteSchema=false for schema stability in production
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("mergeSchema", "false") \
    .saveAsTable(SILVER_MASTER_PDV)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver: Master Products
# MAGIC 
# MAGIC **Business Context:** Product master dimension with hierarchical attributes (brand, segment, category) and optional pricing metadata.
# MAGIC 
# MAGIC **Source:** `bronze_master_products` (full snapshot, comma-delimited CSV origin)
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### Design Decisions
# MAGIC 
# MAGIC **Deduplication Strategy:**
# MAGIC - **Business Key:** `product_code`
# MAGIC - **Selection Criteria:** Most recent record by `bronze_ingestion_timestamp`
# MAGIC - **Fallback:** Deterministic column-based ordering (ensures stability if timestamp absent)
# MAGIC 
# MAGIC **Price Validation (if present):**
# MAGIC - **Criteria:** Values ≤ 0 nullified (prices must be strictly positive)
# MAGIC - **Precision:** Rounded to 2 decimals (monetary standard)
# MAGIC - **Rationale:** Unified validation rule across all price fields (consistent with Price Audit)
# MAGIC 
# MAGIC **Type Enforcement:**
# MAGIC - `product_code` → `StringType` (prevents numeric type confusion)
# MAGIC - Price fields → `DoubleType` with explicit rounding
# MAGIC 
# MAGIC **Text Normalization:**
# MAGIC - Uppercase for categorical attributes (brand, segment, category)
# MAGIC - Enables case-insensitive lookups in downstream joins
# MAGIC 
# MAGIC **Quality Control:**
# MAGIC - Null `product_code` records rejected (invalid dimension members)
# MAGIC - Bronze lineage preserved for temporal audit
# MAGIC 
# MAGIC **Output Schema:** Standardized snake_case, explicit types, processing metadata

# COMMAND ----------


# Read from Bronze
df_bronze = spark.read.table(BRONZE_MASTER_PRODUCTS)
# Preserve Bronze ingestion timestamp for lineage, drop other audit columns
df = df_bronze.withColumnRenamed("_metadata_ingestion_timestamp", "bronze_ingestion_timestamp") \
              .drop("_metadata_source_file", "_metadata_ingestion_date", "_metadata_batch_id")
# Deduplication: Keep most recent record by product code (deterministic)
if "product_code" in df.columns and "bronze_ingestion_timestamp" in df.columns:
    window = Window.partitionBy("product_code").orderBy(col("bronze_ingestion_timestamp").desc_nulls_last())
    df = df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
elif "product_code" in df.columns:
    window = Window.partitionBy("product_code").orderBy([col(c) for c in sorted(df.columns) if c != "product_code"])
    df = df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
# Schema standardization
df = standardize_column_names(df)
# Text normalization
for column, dtype in df.dtypes:
    if dtype == "string":
        df = df.withColumn(column, trim(upper(col(column))))
# Explicit type casting for product key
if "product_code" in df.columns:
    df = df.withColumn("product_code", col("product_code").cast(StringType()))
# Identify price columns (if any exist in product master)
price_cols = [c for c in df.columns if "price" in c or "precio" in c or "valor" in c]
# Price validation: must be > 0 (unified criteria), round to 2 decimals
for price_col in price_cols:
    df = df.withColumn(
        price_col,
        when(col(price_col).isNull(), lit(None))
        .when(col(price_col) <= 0, lit(None))  # Strictly positive for all
        .otherwise(spark_round(col(price_col).cast(DoubleType()), 2))
    )
# Filter: Remove records where product code is null (critical business field)
if "product_code" in df.columns:
    df = df.filter(col("product_code").isNotNull())

# Audit columns
df = df.withColumn("processing_date", current_date()) \
    .withColumn("processing_timestamp", current_timestamp())

# Logging
log_data_quality(df, "silver_master_products", price_cols=price_cols)

# Write to Silver
# Note: overwriteSchema=false for schema stability in production
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("mergeSchema", "false") \
    .saveAsTable(SILVER_MASTER_PRODUCTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver: Price Audit
# MAGIC 
# MAGIC **Business Context:** Historical price observations collected from point-of-sale audits (24 monthly Excel files consolidated).
# MAGIC 
# MAGIC **Source:** `bronze_price_audit` (partitioned by `year_month`)
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### Design Decisions
# MAGIC 
# MAGIC **Partitioning Strategy:**
# MAGIC - **Partition Key:** `year_month` (preserved from Bronze)
# MAGIC - **Write Mode:** Dynamic partition overwrite (rewrites only incoming month partitions)
# MAGIC - **Rationale:** Optimizes query performance for time-series analysis and enables incremental updates
# MAGIC 
# MAGIC **Price Validation:**
# MAGIC - **Criteria:** Values ≤ 0 nullified (unified with Product master validation)
# MAGIC - **Precision:** 2 decimal places (monetary standard)
# MAGIC - **Philosophy:** Invalid prices preserved as null for downstream investigation
# MAGIC 
# MAGIC **Date Validation:**
# MAGIC - **Rule:** Future dates nullified (historical analysis assumption)
# MAGIC - **Design Choice:** Preserves records but flags temporal anomalies
# MAGIC 
# MAGIC **Completeness Filtering:**
# MAGIC - **Criteria:** Records with ALL critical fields null are excluded
# MAGIC - **Critical Fields:** price, PDV identifiers, product identifiers
# MAGIC - **Rationale:** Empty records provide no analytical value (reduce storage and compute)
# MAGIC 
# MAGIC **Schema Handling:**
# MAGIC - Dynamic partition overwrite requires stable schema (no `overwriteSchema`)
# MAGIC - `year_month` partition column excluded from standardization
# MAGIC 
# MAGIC **Output Schema:** Standardized snake_case, validated domains, monthly partitions

# COMMAND ----------

# Read from Bronze (partitioned table)
df_bronze = spark.read.table(BRONZE_PRICE_AUDIT)


# Drop Bronze audit columns
df = df_bronze.drop("_metadata_ingestion_timestamp", "_metadata_source_file", "_metadata_ingestion_date", "_metadata_batch_id")
# Schema standardization: snake_case
df = standardize_column_names(df, exclude_cols=["year_month"])

# Explicit column identification (avoid dynamic detection risks)
# Known columns after standardization: price, date, pdv/store codes, product codes
price_cols = [c for c in df.columns if "price" in c or "precio" in c]
date_cols = [c for c in df.columns if "date" in c or "fecha" in c]
pdv_cols = [c for c in df.columns if "pdv" in c or "store" in c or "code_eleader" in c]
product_cols = [c for c in df.columns if "product" in c or "sku" in c]

# Price validation: must be > 0 (unified criteria with Products master)
for price_col in price_cols:
    df = df.withColumn(
        price_col,
        when(col(price_col).isNull(), lit(None))
        .when(col(price_col) <= 0, lit(None))  # Prices must be strictly positive (unified)
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

# Logging
log_data_quality(df, "silver_price_audit", price_cols=price_cols, date_cols=date_cols)

# Write to Silver (preserve partitioning)
# Note: overwriteSchema removed - incompatible with dynamic partition overwrite
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("year_month") \
    .saveAsTable(SILVER_PRICE_AUDIT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver: Sell-In
# MAGIC 
# MAGIC **Business Context:** Sales transactions (sell-in from manufacturer to distributors/retailers) with quantity, value, and temporal attributes.
# MAGIC 
# MAGIC **Source:** `bronze_sell_in` (partitioned by `year`)
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### Design Decisions
# MAGIC 
# MAGIC **Partitioning Strategy:**
# MAGIC - **Partition Key:** `year` (preserved from Bronze)
# MAGIC - **Write Mode:** Dynamic partition overwrite (rewrites only incoming year partitions)
# MAGIC - **Rationale:** Annual partitioning optimizes long-term historical queries and aligns with business reporting cycles
# MAGIC 
# MAGIC **Null Preservation Philosophy:**
# MAGIC - **Quantities/Values:** Nulls preserved (represent missing data, not zero)
# MAGIC - **Quality Flag:** `is_complete_transaction` identifies records with both quantity and value present
# MAGIC - **Rationale:** Enables transparent downstream decisions (filter incomplete vs impute)
# MAGIC 
# MAGIC **Domain Validation:**
# MAGIC - **Quantities:** Negative values nullified (invalid transactions)
# MAGIC - **Values:** Negative values nullified, rounded to 2 decimals
# MAGIC - **Dates:** Future dates nullified (historical transaction assumption)
# MAGIC 
# MAGIC **Derived Technical Columns:**
# MAGIC 
# MAGIC | Column | Logic | Purpose |
# MAGIC |--------|-------|--------|
# MAGIC | `unit_price` | `value / quantity` (when both > 0) | Enable price analysis without Gold aggregation |
# MAGIC | `is_active_sale` | `quantity > 0` | Quick filter for actual vs cancelled/returned transactions |
# MAGIC | `transaction_month` | `month(date)` | Temporal analysis (year already exists as partition) |
# MAGIC | `is_complete_transaction` | Both quantity and value non-null | Data quality signal |
# MAGIC 
# MAGIC **Design Philosophy:**
# MAGIC - Derived columns are **technical attributes** (not business KPIs)
# MAGIC - `unit_price` enables row-level filtering without joins (Silver scope)
# MAGIC - Aggregated metrics remain in Gold Layer
# MAGIC 
# MAGIC **Output Schema:** Standardized snake_case, null-preserved quantities, quality flags, annual partitions

# COMMAND ----------

# Read from Bronze (partitioned by year)
df_bronze = spark.read.table(BRONZE_SELL_IN)


# Drop Bronze audit columns
df = df_bronze.drop("_metadata_ingestion_timestamp", "_metadata_source_file", "_metadata_ingestion_date", "_metadata_batch_id")
# Schema standardization: snake_case
df = standardize_column_names(df, exclude_cols=["year"])

# Identify quantity and value columns
qty_cols = [c for c in df.columns if "quantity" in c or "cantidad" in c or "qty" in c]
value_cols = [c for c in df.columns if "value" in c or "valor" in c or "amount" in c or "monto" in c]
date_cols = [c for c in df.columns if "date" in c or "fecha" in c]

# Quantity validation: must be >= 0, reject negatives but preserve nulls with flag
for qty_col in qty_cols:
    df = df.withColumn(
        qty_col,
        when(col(qty_col).isNull(), lit(None))  # Keep null explicit (missing data)
        .when(col(qty_col) < 0, lit(None))  # Reject invalid negatives
        .otherwise(col(qty_col).cast(IntegerType()))
    )

# Value validation: must be >= 0, round to 2 decimals, reject negatives
for value_col in value_cols:
    df = df.withColumn(
        value_col,
        when(col(value_col).isNull(), lit(None))  # Keep null explicit
        .when(col(value_col) < 0, lit(None))  # Reject invalid negatives
        .otherwise(spark_round(col(value_col).cast(DoubleType()), 2))
    )

# Data completeness flag: transaction has valid quantity and value
if qty_cols and value_cols:
    df = df.withColumn(
        "is_complete_transaction",
        when((col(qty_cols[0]).isNotNull()) & (col(value_cols[0]).isNotNull()), lit(True))
        .otherwise(lit(False))
    )

# Derived column: unit_price (value / quantity) with null checks
if qty_cols and value_cols:
    df = df.withColumn(
        "unit_price",
        when(
            col(qty_cols[0]).isNotNull() & col(value_cols[0]).isNotNull() &
            (col(qty_cols[0]) > 0) & (col(value_cols[0]) > 0),
            spark_round(col(value_cols[0]) / col(qty_cols[0]), 2)
        ).otherwise(lit(None))
    )
# --- Data Quality Metrics Logging ---
def log_data_quality(df, table_name, price_cols=None, date_cols=None):
    total_records = df.count()
    invalid_prices = 0
    invalid_dates = 0
    if price_cols:
        invalid_prices = df.filter(
            sum([col(c).isNull().cast("int") for c in price_cols]) > 0
        ).count()
    if date_cols:
        invalid_dates = df.filter(
            sum([col(c).isNull().cast("int") for c in date_cols]) > 0
        ).count()
    print(f"✅ {table_name} Quality Metrics:")
    print(f"   Total: {total_records:,}")
    if price_cols:
        print(f"   Invalid Prices: {invalid_prices:,} ({invalid_prices/total_records*100 if total_records else 0:.1f}%)")
    if date_cols:
        print(f"   Invalid Dates: {invalid_dates:,} ({invalid_dates/total_records*100 if total_records else 0:.1f}%)")

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

# Derived column: transaction_month (year already exists as partition column)
if date_cols:
    df = df.withColumn("transaction_month", month(col(date_cols[0])))


# Audit columns
df = df.withColumn("processing_date", current_date()) \
    .withColumn("processing_timestamp", current_timestamp())

# Logging
log_data_quality(df, "silver_sell_in", price_cols=value_cols, date_cols=date_cols)

# Write to Silver (preserve year partitioning)
# Note: overwriteSchema removed - incompatible with dynamic partition overwrite
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("year") \
    .saveAsTable(SILVER_SELL_IN)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validation: Post-Write Metrics
# MAGIC 
# MAGIC **Validation Strategy:** Leverage Delta Lake transaction logs for zero-compute metrics extraction.
# MAGIC 
# MAGIC ### Design Rationale
# MAGIC 
# MAGIC **Why Delta History:**
# MAGIC - `DESCRIBE HISTORY` queries metadata (no DataFrame scans)
# MAGIC - Serverless-friendly (no cluster overhead)
# MAGIC - Provides row counts, file counts, and operation type
# MAGIC 
# MAGIC **What This Validates:**
# MAGIC - Write operations completed successfully
# MAGIC - Expected number of output files (partition efficiency)
# MAGIC - Operation type matches intent (WRITE vs MERGE)
# MAGIC 
# MAGIC **What This Does NOT Validate:**
# MAGIC - Business logic correctness (handled by explicit transformations)
# MAGIC - Data quality metrics (handled by domain validations)
# MAGIC - Cross-table referential integrity (deferred to Gold Layer)
# MAGIC 
# MAGIC This approach balances validation coverage with operational cost.

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
# MAGIC ### Output Assets
# MAGIC 
# MAGIC | Table | Type | Partition | Records Source | Purpose |
# MAGIC |-------|------|-----------|----------------|--------|
# MAGIC | `silver_master_pdv` | Dimension | None | ~50 stores | Geographic and organizational attributes |
# MAGIC | `silver_master_products` | Dimension | None | ~200 products | Product hierarchy and attributes |
# MAGIC | `silver_price_audit` | Fact | `year_month` | Historical audits | Point-in-time price observations |
# MAGIC | `silver_sell_in` | Fact | `year` | Transactional | Manufacturer sales to retailers |
# MAGIC 
# MAGIC ### Architectural Role
# MAGIC 
# MAGIC **Silver Layer Responsibilities:**
# MAGIC - ✅ Schema standardization and type safety
# MAGIC - ✅ Domain validation (prices, dates, quantities)
# MAGIC - ✅ Deterministic deduplication by business keys
# MAGIC - ✅ Null preservation with quality signals
# MAGIC - ✅ Temporal lineage to Bronze
# MAGIC 
# MAGIC **Handed Off to Gold Layer:**
# MAGIC - Business KPIs (sales velocity, price indices, market share)
# MAGIC - Cross-dataset joins (PDV × Products × Transactions)
# MAGIC - Time-series aggregations (monthly trends, YoY growth)
# MAGIC - Analytical models (Star schema, conformed dimensions)
# MAGIC 
# MAGIC ### Technical Characteristics
# MAGIC 
# MAGIC - **Format:** Delta Lake with Unity Catalog governance
# MAGIC - **Write Mode:** Overwrite (masters), Dynamic Partition Overwrite (facts)
# MAGIC - **Schema Evolution:** Controlled (no automatic merges on masters)
# MAGIC - **Optimization:** Coalesced files, partition pruning enabled
# MAGIC - **Serverless Compatible:** No cache/persist, single write per table
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Next Stage:** Gold Layer (`03_gold_aggregation.py`) will consume Silver tables to produce:
# MAGIC - Star schema dimensional model
# MAGIC - Pre-aggregated KPI tables
# MAGIC - Power BI-optimized datasets
# MAGIC 
# MAGIC ---
# MAGIC 

# ---
# Post-write: Run Silver Drift Monitoring (Serverless-friendly, metadata-only)
import sys
sys.path.append("../monitoring")
try:
    from silver_drift_monitoring import run_silver_drift_monitoring
    run_silver_drift_monitoring(spark)
    print("✅ Silver drift monitoring completed.")
except Exception as e:
    print(f"⚠️ Drift monitoring failed: {e}")

# MAGIC **Author:** Diego Mayorga | diego.mayorgacapera@gmail.com  
# MAGIC **Date:** 2025-12-30  
# MAGIC **Repository:** [github.com/DIEGO77M/BI_Market_Visibility](https://github.com/DIEGO77M/BI_Market_Visibility)