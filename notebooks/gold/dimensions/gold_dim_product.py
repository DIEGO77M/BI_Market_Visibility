# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Dimension: Product (SCD Type 2)
# MAGIC 
# MAGIC **Purpose:** Create a slowly changing dimension for product master data with full history tracking.
# MAGIC 
# MAGIC **Author:** Diego Mayorga  
# MAGIC **Date:** 2026-01-06  
# MAGIC **Project:** BI Market Visibility Analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Design Decisions
# MAGIC 
# MAGIC **Table Type:** SCD Type 2 (Slowly Changing Dimension)
# MAGIC 
# MAGIC **Why SCD Type 2:**
# MAGIC - Product attributes (brand, segment, category) may change over time
# MAGIC - Historical analysis requires point-in-time accuracy
# MAGIC - Enables "what was the brand when this sale happened?" queries
# MAGIC 
# MAGIC **Surrogate Key Strategy:**
# MAGIC - `product_sk`: Hash-based key (SHA-256 truncated)
# MAGIC - Includes version identifier for uniqueness across versions
# MAGIC - Business key: `product_code` (natural key from source)
# MAGIC 
# MAGIC **Tracked Attributes:**
# MAGIC - `product_name`, `brand`, `segment`, `category`
# MAGIC - Changes in any trigger new version
# MAGIC 
# MAGIC **Refresh Strategy:**
# MAGIC - MERGE-based incremental
# MAGIC - Detects changes via attribute comparison
# MAGIC - Closes old records (valid_to, is_current=false)
# MAGIC - Inserts new versions
# MAGIC 
# MAGIC **Serverless Optimizations:**
# MAGIC - No cache/persist
# MAGIC - Single MERGE operation
# MAGIC - Broadcast join for small lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, trim, upper,
    current_date, current_timestamp,
    sha2, concat_ws, row_number, md5
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DateType, BooleanType
from delta.tables import DeltaTable

# Unity Catalog
CATALOG = "workspace"
SCHEMA = "default"

# Source and Target tables
SILVER_MASTER_PRODUCTS = f"{CATALOG}.{SCHEMA}.silver_master_products"
GOLD_DIM_PRODUCT = f"{CATALOG}.{SCHEMA}.gold_dim_product"

# Business key and tracked attributes
BUSINESS_KEY = "product_code"
TRACKED_ATTRIBUTES = ["product_name", "brand", "segment", "category"]

print(f"📦 Processing Product Dimension (SCD Type 2)")
print(f"   Source: {SILVER_MASTER_PRODUCTS}")
print(f"   Target: {GOLD_DIM_PRODUCT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Silver Source

# COMMAND ----------

# Read current product master from Silver
df_silver = spark.read.table(SILVER_MASTER_PRODUCTS)

# Select and rename columns for Gold schema
# Handle potential column name variations from Silver
df_source = df_silver.select(
    col("product_code").cast(StringType()).alias("product_code"),
    coalesce(col("product_name"), col("product_code")).alias("product_name"),
    coalesce(col("brand"), lit("UNKNOWN")).alias("brand"),
    coalesce(col("segment"), lit("UNKNOWN")).alias("segment"),
    coalesce(col("category"), lit("UNKNOWN")).alias("category")
).distinct()

# Generate surrogate key (version-agnostic for initial comparison)
df_source = df_source.withColumn(
    "product_sk",
    sha2(concat_ws("||", 
        col("product_code"),
        col("product_name"),
        col("brand"),
        col("segment"),
        col("category"),
        current_timestamp().cast(StringType())  # Version uniqueness
    ), 256).substr(1, 16)
)

print(f"📊 Source records from Silver (distinct)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SCD Type 2 Implementation

# COMMAND ----------

# Check if target table exists
table_exists = spark._jsparkSession.catalog().tableExists(GOLD_DIM_PRODUCT)

if not table_exists:
    # ==========================================================================
    # INITIAL LOAD: Create table with all records as current
    # ==========================================================================
    print("🆕 Initial load - creating new dimension table")
    
    df_initial = df_source \
        .withColumn("valid_from", current_date()) \
        .withColumn("valid_to", lit("9999-12-31").cast(DateType())) \
        .withColumn("is_current", lit(True)) \
        .select(
            "product_sk",
            "product_code",
            "product_name",
            "brand",
            "segment",
            "category",
            "valid_from",
            "valid_to",
            "is_current"
        )
    
    # Single write operation
    df_initial.write \
        .format("delta") \
        .mode("overwrite") \
        .option("delta.columnMapping.mode", "name") \
        .saveAsTable(GOLD_DIM_PRODUCT)
    
    print(f"✅ Initial load complete to {GOLD_DIM_PRODUCT}")

else:
    # ==========================================================================
    # INCREMENTAL LOAD: SCD Type 2 MERGE
    # ==========================================================================
    print("🔄 Incremental load - detecting changes")
    
    delta_table = DeltaTable.forName(spark, GOLD_DIM_PRODUCT)
    
    # Prepare source with SCD2 columns
    df_updates = df_source \
        .withColumn("valid_from", current_date()) \
        .withColumn("valid_to", lit("9999-12-31").cast(DateType())) \
        .withColumn("is_current", lit(True))
    
    # Build change detection condition
    change_condition = """
        target.product_name <> source.product_name OR
        target.brand <> source.brand OR
        target.segment <> source.segment OR
        target.category <> source.category
    """
    
    # -------------------------------------------------------------------------
    # Step 1: Mark changed records as closed
    # -------------------------------------------------------------------------
    delta_table.alias("target").merge(
        df_updates.alias("source"),
        "target.product_code = source.product_code AND target.is_current = true"
    ).whenMatchedUpdate(
        condition=change_condition,
        set={
            "valid_to": current_date(),
            "is_current": lit(False)
        }
    ).execute()
    
    print("   ✓ Closed changed records")
    
    # -------------------------------------------------------------------------
    # Step 2: Insert new versions for changed records + new products
    # -------------------------------------------------------------------------
    # Get current state after close operation
    df_current = spark.read.table(GOLD_DIM_PRODUCT) \
        .filter(col("is_current") == True) \
        .select("product_code")
    
    # Find records that need new version inserted
    # (either new products or products whose current version was just closed)
    df_to_insert = df_updates.alias("source") \
        .join(
            df_current.alias("current"),
            col("source.product_code") == col("current.product_code"),
            "left_anti"
        ).union(
            # Also insert for products that were just closed (changed)
            df_updates.alias("source").join(
                spark.read.table(GOLD_DIM_PRODUCT)
                    .filter((col("is_current") == False) & (col("valid_to") == current_date()))
                    .select("product_code").distinct()
                    .alias("closed"),
                col("source.product_code") == col("closed.product_code"),
                "inner"
            ).select("source.*")
        ).distinct()
    
    # Insert new records
    if df_to_insert.count() > 0:  # Note: count used only for conditional logic, not validation
        df_to_insert.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(GOLD_DIM_PRODUCT)
        print("   ✓ Inserted new versions")
    else:
        print("   ✓ No new versions to insert")
    
    print(f"✅ Incremental SCD2 complete for {GOLD_DIM_PRODUCT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validation (Zero-Compute)

# COMMAND ----------

# Validate using Delta History
history = spark.sql(f"DESCRIBE HISTORY {GOLD_DIM_PRODUCT} LIMIT 1").first()
metrics = history["operationMetrics"]

print("=" * 50)
print("GOLD_DIM_PRODUCT VALIDATION")
print("=" * 50)
print(f"Operation: {history['operation']}")
print(f"Timestamp: {history['timestamp']}")
print(f"Rows Affected: {metrics.get('numOutputRows', metrics.get('numTargetRowsInserted', 'N/A'))}")
print(f"Files: {metrics.get('numFiles', metrics.get('numTargetFilesAdded', 'N/A'))}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Schema Documentation
# MAGIC 
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | `product_sk` | STRING(16) | Surrogate key (SHA-256 hash) |
# MAGIC | `product_code` | STRING | Business key (natural key) |
# MAGIC | `product_name` | STRING | Product description |
# MAGIC | `brand` | STRING | Brand name |
# MAGIC | `segment` | STRING | Product segment |
# MAGIC | `category` | STRING | Product category |
# MAGIC | `valid_from` | DATE | Version start date |
# MAGIC | `valid_to` | DATE | Version end date (9999-12-31 for current) |
# MAGIC | `is_current` | BOOLEAN | True if this is the active version |
# MAGIC 
# MAGIC ### SCD Type 2 Query Patterns
# MAGIC 
# MAGIC ```sql
# MAGIC -- Get current product attributes
# MAGIC SELECT * FROM gold_dim_product WHERE is_current = true
# MAGIC 
# MAGIC -- Get product attributes as of a specific date
# MAGIC SELECT * FROM gold_dim_product 
# MAGIC WHERE '2025-06-15' BETWEEN valid_from AND valid_to
# MAGIC 
# MAGIC -- Get full history for a product
# MAGIC SELECT * FROM gold_dim_product 
# MAGIC WHERE product_code = 'SKU123' 
# MAGIC ORDER BY valid_from
# MAGIC ```
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Author:** Diego Mayorga | **Date:** 2026-01-06
