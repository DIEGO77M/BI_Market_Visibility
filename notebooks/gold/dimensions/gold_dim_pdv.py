# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Dimension: PDV (SCD Type 2)
# MAGIC 
# MAGIC **Purpose:** Create a slowly changing dimension for Point of Sale (PDV) master data with full history tracking.
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
# MAGIC - Store attributes (channel, region, format) may change over time
# MAGIC - Store closures/reopenings need tracking
# MAGIC - Regional reassignments should preserve historical accuracy
# MAGIC 
# MAGIC **Surrogate Key Strategy:**
# MAGIC - `pdv_sk`: Hash-based key (SHA-256 truncated)
# MAGIC - Business key: `pdv_code` (code_eleader from Silver)
# MAGIC 
# MAGIC **Tracked Attributes:**
# MAGIC - `pdv_name`, `channel`, `region`, `city`, `format`
# MAGIC - Geographic coordinates (`latitude`, `longitude`) - informational only
# MAGIC 
# MAGIC **Refresh Strategy:**
# MAGIC - MERGE-based incremental
# MAGIC - Same pattern as product dimension
# MAGIC 
# MAGIC **Serverless Optimizations:**
# MAGIC - No cache/persist
# MAGIC - Single MERGE operation
# MAGIC - Efficient change detection

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, trim, upper,
    current_date, current_timestamp,
    sha2, concat_ws, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DateType, BooleanType, DoubleType
from delta.tables import DeltaTable

# Unity Catalog
CATALOG = ""
SCHEMA = "default"

# Source and Target tables
SILVER_MASTER_PDV = f"{SCHEMA}.silver_master_pdv"
GOLD_DIM_PDV = f"{SCHEMA}.gold_dim_pdv"

# Business key and tracked attributes
BUSINESS_KEY = "code_eleader"
TRACKED_ATTRIBUTES = ["pdv_name", "channel", "region", "city", "format"]

print(f"🏪 Processing PDV Dimension (SCD Type 2)")
print(f"   Source: {SILVER_MASTER_PDV}")
print(f"   Target: {GOLD_DIM_PDV}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Silver Source

# COMMAND ----------

# Read current PDV master from Silver
df_silver = spark.read.table(SILVER_MASTER_PDV)

# Map Silver columns to Gold schema
# Handle column name variations and provide defaults
df_source = df_silver.select(
    col("code_eleader").cast(StringType()).alias("pdv_code"),
    coalesce(
        col("nombre_pdv") if "nombre_pdv" in df_silver.columns else lit(None),
        col("pdv_name") if "pdv_name" in df_silver.columns else lit(None),
        col("name") if "name" in df_silver.columns else lit(None),
        col("code_eleader")
    ).alias("pdv_name"),
    coalesce(
        col("canal") if "canal" in df_silver.columns else lit(None),
        col("channel") if "channel" in df_silver.columns else lit(None),
        lit("UNKNOWN")
    ).alias("channel"),
    coalesce(
        col("region") if "region" in df_silver.columns else lit(None),
        col("zona") if "zona" in df_silver.columns else lit(None),
        lit("UNKNOWN")
    ).alias("region"),
    coalesce(
        col("ciudad") if "ciudad" in df_silver.columns else lit(None),
        col("city") if "city" in df_silver.columns else lit(None),
        lit("UNKNOWN")
    ).alias("city"),
    coalesce(
        col("formato") if "formato" in df_silver.columns else lit(None),
        col("format") if "format" in df_silver.columns else lit(None),
        lit("STANDARD")
    ).alias("format"),
    # Geographic coordinates (informational, not tracked for SCD)
    col("latitude").cast(DoubleType()) if "latitude" in df_silver.columns else lit(None).alias("latitude"),
    col("longitude").cast(DoubleType()) if "longitude" in df_silver.columns else lit(None).alias("longitude")
).distinct()

# Generate surrogate key
df_source = df_source.withColumn(
    "pdv_sk",
    sha2(concat_ws("||", 
        col("pdv_code"),
        col("pdv_name"),
        col("channel"),
        col("region"),
        col("city"),
        col("format"),
        current_timestamp().cast(StringType())
    ), 256).substr(1, 16)
)

print(f"📊 Source records from Silver (distinct)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SCD Type 2 Implementation

# COMMAND ----------

# Check if target table exists
table_exists = spark._jsparkSession.catalog().tableExists(GOLD_DIM_PDV)

if not table_exists:
    # ==========================================================================
    # INITIAL LOAD
    # ==========================================================================
    print("🆕 Initial load - creating new dimension table")
    
    df_initial = df_source \
        .withColumn("valid_from", current_date()) \
        .withColumn("valid_to", lit("9999-12-31").cast(DateType())) \
        .withColumn("is_current", lit(True)) \
        .select(
            "pdv_sk",
            "pdv_code",
            "pdv_name",
            "channel",
            "region",
            "city",
            "format",
            "latitude",
            "longitude",
            "valid_from",
            "valid_to",
            "is_current"
        )
    
    df_initial.write \
        .format("delta") \
        .mode("overwrite") \
        .option("delta.columnMapping.mode", "name") \
        .saveAsTable(GOLD_DIM_PDV)
    
    print(f"✅ Initial load complete to {GOLD_DIM_PDV}")

else:
    # ==========================================================================
    # INCREMENTAL LOAD: SCD Type 2 MERGE
    # ==========================================================================
    print("🔄 Incremental load - detecting changes")
    
    delta_table = DeltaTable.forName(spark, GOLD_DIM_PDV)
    
    # Prepare source with SCD2 columns
    df_updates = df_source \
        .withColumn("valid_from", current_date()) \
        .withColumn("valid_to", lit("9999-12-31").cast(DateType())) \
        .withColumn("is_current", lit(True))
    
    # Change detection condition (tracked attributes only)
    change_condition = """
        target.pdv_name <> source.pdv_name OR
        target.channel <> source.channel OR
        target.region <> source.region OR
        target.city <> source.city OR
        target.format <> source.format
    """
    
    # Step 1: Close changed records
    delta_table.alias("target").merge(
        df_updates.alias("source"),
        "target.pdv_code = source.pdv_code AND target.is_current = true"
    ).whenMatchedUpdate(
        condition=change_condition,
        set={
            "valid_to": current_date(),
            "is_current": lit(False)
        }
    ).execute()
    
    print("   ✓ Closed changed records")
    
    # Step 2: Insert new versions
    df_current = spark.read.table(GOLD_DIM_PDV) \
        .filter(col("is_current") == True) \
        .select("pdv_code")
    
    df_closed_today = spark.read.table(GOLD_DIM_PDV) \
        .filter((col("is_current") == False) & (col("valid_to") == current_date())) \
        .select("pdv_code").distinct()
    
    # New PDVs (not in current)
    df_new = df_updates.alias("source").join(
        df_current.alias("current"),
        col("source.pdv_code") == col("current.pdv_code"),
        "left_anti"
    )
    
    # Changed PDVs (closed today, need new version)
    df_changed = df_updates.alias("source").join(
        df_closed_today.alias("closed"),
        col("source.pdv_code") == col("closed.pdv_code"),
        "inner"
    ).select("source.*")
    
    df_to_insert = df_new.union(df_changed).distinct()
    
    # Insert if records exist
    insert_count = df_to_insert.count()
    if insert_count > 0:
        df_to_insert.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(GOLD_DIM_PDV)
        print(f"   ✓ Inserted {insert_count} new versions")
    else:
        print("   ✓ No new versions to insert")
    
    print(f"✅ Incremental SCD2 complete for {GOLD_DIM_PDV}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validation (Zero-Compute)

# COMMAND ----------

# Validate using Delta History
history = spark.sql(f"DESCRIBE HISTORY {GOLD_DIM_PDV} LIMIT 1").first()
metrics = history["operationMetrics"]

print("=" * 50)
print("GOLD_DIM_PDV VALIDATION")
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
# MAGIC | `pdv_sk` | STRING(16) | Surrogate key (SHA-256 hash) |
# MAGIC | `pdv_code` | STRING | Business key (code_eleader) |
# MAGIC | `pdv_name` | STRING | Store name |
# MAGIC | `channel` | STRING | Sales channel (Traditional, Modern, etc.) |
# MAGIC | `region` | STRING | Geographic region |
# MAGIC | `city` | STRING | City |
# MAGIC | `format` | STRING | Store format |
# MAGIC | `latitude` | DOUBLE | Geographic latitude |
# MAGIC | `longitude` | DOUBLE | Geographic longitude |
# MAGIC | `valid_from` | DATE | Version start date |
# MAGIC | `valid_to` | DATE | Version end date |
# MAGIC | `is_current` | BOOLEAN | Active version flag |
# MAGIC 
# MAGIC ### Usage Examples
# MAGIC 
# MAGIC ```sql
# MAGIC -- Current active stores by channel
# MAGIC SELECT channel, COUNT(*) as store_count
# MAGIC FROM gold_dim_pdv 
# MAGIC WHERE is_current = true
# MAGIC GROUP BY channel
# MAGIC 
# MAGIC -- Store history
# MAGIC SELECT * FROM gold_dim_pdv 
# MAGIC WHERE pdv_code = 'PDV001' 
# MAGIC ORDER BY valid_from
# MAGIC ```
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Author:** Diego Mayorga | **Date:** 2026-01-06
