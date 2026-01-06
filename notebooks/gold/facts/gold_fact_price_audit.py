# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Fact: Price Audit
# MAGIC 
# MAGIC **Purpose:** Create a fact table for price observations from point-of-sale audits, enabling price visibility and competitiveness analysis.
# MAGIC 
# MAGIC **Author:** Diego Mayorga  
# MAGIC **Date:** 2026-01-06  
# MAGIC **Project:** BI Market Visibility Analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Design Decisions
# MAGIC 
# MAGIC **Grain:** One row per audit_date × product × pdv
# MAGIC 
# MAGIC **Business Purpose:**
# MAGIC - Track observed prices at retail points
# MAGIC - Calculate price variance vs market average
# MAGIC - Compute price competitiveness index
# MAGIC - Identify pricing anomalies
# MAGIC 
# MAGIC **Metrics Calculated:**
# MAGIC - `observed_price`: Actual price at PDV
# MAGIC - `avg_market_price`: Average price across all PDVs (same product, same period)
# MAGIC - `price_variance`: Difference from market average
# MAGIC - `price_index`: Index (100 = market average)
# MAGIC 
# MAGIC **Serverless Optimizations:**
# MAGIC - Partitioned by date for efficient pruning
# MAGIC - Single-pass market average calculation
# MAGIC - No complex recursive windows

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, 
    current_date, current_timestamp, to_date, date_format,
    sum as spark_sum, count as spark_count, avg as spark_avg,
    round as spark_round, broadcast
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType

# Unity Catalog
CATALOG = "workspace"
SCHEMA = "default"

# Source tables
SILVER_PRICE_AUDIT = f"{CATALOG}.{SCHEMA}.silver_price_audit"
GOLD_DIM_DATE = f"{CATALOG}.{SCHEMA}.gold_dim_date"
GOLD_DIM_PRODUCT = f"{CATALOG}.{SCHEMA}.gold_dim_product"
GOLD_DIM_PDV = f"{CATALOG}.{SCHEMA}.gold_dim_pdv"

# Target table
GOLD_FACT_PRICE_AUDIT = f"{CATALOG}.{SCHEMA}.gold_fact_price_audit"

# Business constants
PRICE_INDEX_BASELINE = 100.0

print(f"💰 Processing Fact Price Audit")
print(f"   Source: {SILVER_PRICE_AUDIT}")
print(f"   Target: {GOLD_FACT_PRICE_AUDIT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Silver Source

# COMMAND ----------

# Read Silver price audit
df_price = spark.read.table(SILVER_PRICE_AUDIT)

# Identify key columns
date_col = next((c for c in df_price.columns if "date" in c.lower() and "process" not in c.lower()), None)
product_col = next((c for c in df_price.columns if "product" in c.lower() or "sku" in c.lower()), None)
pdv_col = next((c for c in df_price.columns if "pdv" in c.lower() or "eleader" in c.lower() or "store" in c.lower()), None)
price_col = next((c for c in df_price.columns if "price" in c.lower() or "precio" in c.lower()), None)

print(f"   Detected columns:")
print(f"   - Date: {date_col}")
print(f"   - Product: {product_col}")
print(f"   - PDV: {pdv_col}")
print(f"   - Price: {price_col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Dimension Lookups

# COMMAND ----------

# Load dimensions with broadcast
df_dim_date = broadcast(
    spark.read.table(GOLD_DIM_DATE)
    .select("date_sk", "date", "year_month")
)

df_dim_product = broadcast(
    spark.read.table(GOLD_DIM_PRODUCT)
    .filter(col("is_current") == True)
    .select(
        col("product_sk"),
        col("product_code").alias("dim_product_code")
    )
)

df_dim_pdv = broadcast(
    spark.read.table(GOLD_DIM_PDV)
    .filter(col("is_current") == True)
    .select(
        col("pdv_sk"),
        col("pdv_code").alias("dim_pdv_code")
    )
)

print("✓ Dimension lookups loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transform & Calculate Market Metrics

# COMMAND ----------

# Prepare audit data
df_audit = df_price.select(
    to_date(col(date_col)).alias("audit_date"),
    col(product_col).cast(StringType()).alias("product_code"),
    col(pdv_col).cast(StringType()).alias("pdv_code"),
    col(price_col).cast(DoubleType()).alias("observed_price")
).filter(
    col("audit_date").isNotNull() &
    col("product_code").isNotNull() &
    col("pdv_code").isNotNull() &
    col("observed_price").isNotNull() &
    (col("observed_price") > 0)
)

# Aggregate to grain: audit_date × product × pdv
# (average if multiple observations per day)
df_grain = df_audit.groupBy(
    "audit_date",
    "product_code",
    "pdv_code"
).agg(
    spark_avg("observed_price").alias("observed_price")
)

# Calculate market average per product per month
# Single-pass calculation using window function
df_with_period = df_grain.withColumn(
    "year_month",
    date_format(col("audit_date"), "yyyy-MM")
)

# Calculate avg market price per product per month
df_market_avg = df_with_period.groupBy(
    "year_month",
    "product_code"
).agg(
    spark_avg("observed_price").alias("avg_market_price"),
    spark_count("*").alias("market_observations")
)

# Join market average back to grain data
df_with_market = df_with_period.join(
    broadcast(df_market_avg),
    ["year_month", "product_code"],
    "left"
)

# Calculate price metrics
df_metrics = df_with_market.withColumn(
    "price_variance",
    spark_round(col("observed_price") - col("avg_market_price"), 2)
).withColumn(
    "price_index",
    when(col("avg_market_price") > 0,
         spark_round((col("observed_price") / col("avg_market_price")) * PRICE_INDEX_BASELINE, 2))
    .otherwise(lit(None))
).withColumn(
    "is_above_market",
    col("observed_price") > col("avg_market_price")
).withColumn(
    "is_below_market",
    col("observed_price") < col("avg_market_price")
)

print("✓ Market metrics calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Join Dimension Keys

# COMMAND ----------

# Join with Date dimension
df_with_date = df_metrics.join(
    df_dim_date,
    df_metrics["audit_date"] == df_dim_date["date"],
    "left"
).drop("date")

# Join with Product dimension
df_with_product = df_with_date.join(
    df_dim_product,
    df_with_date["product_code"] == df_dim_product["dim_product_code"],
    "left"
).drop("dim_product_code")

# Join with PDV dimension
df_fact = df_with_product.join(
    df_dim_pdv,
    df_with_product["pdv_code"] == df_dim_pdv["dim_pdv_code"],
    "left"
).drop("dim_pdv_code")

# Final selection
df_fact_final = df_fact.select(
    col("date_sk"),
    col("product_sk"),
    col("pdv_sk"),
    col("audit_date"),
    spark_round(col("observed_price"), 2).alias("observed_price"),
    spark_round(col("avg_market_price"), 2).alias("avg_market_price"),
    col("price_variance"),
    col("price_index"),
    col("is_above_market"),
    col("is_below_market"),
    col("market_observations"),
    # Degenerate dimensions
    col("product_code"),
    col("pdv_code"),
    df_fact["year_month"]
).withColumn(
    "processing_timestamp", current_timestamp()
)

# Handle orphan dates
df_fact_final = df_fact_final.withColumn(
    "date_sk",
    coalesce(col("date_sk"), date_format(col("audit_date"), "yyyyMMdd").cast(IntegerType()))
)

print("✓ Dimension keys joined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write to Gold Layer

# COMMAND ----------

# Write partitioned by date
df_fact_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("date_sk") \
    .saveAsTable(GOLD_FACT_PRICE_AUDIT)

print(f"✅ Written to {GOLD_FACT_PRICE_AUDIT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {GOLD_FACT_PRICE_AUDIT} LIMIT 1").first()
metrics = history["operationMetrics"]

print("=" * 50)
print("GOLD_FACT_PRICE_AUDIT VALIDATION")
print("=" * 50)
print(f"Operation: {history['operation']}")
print(f"Timestamp: {history['timestamp']}")
print(f"Rows Written: {metrics.get('numOutputRows', 'N/A')}")
print(f"Files Written: {metrics.get('numFiles', 'N/A')}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Schema Documentation
# MAGIC 
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | `date_sk` | INT | FK to gold_dim_date (partition key) |
# MAGIC | `product_sk` | STRING | FK to gold_dim_product |
# MAGIC | `pdv_sk` | STRING | FK to gold_dim_pdv |
# MAGIC | `audit_date` | DATE | Date of price observation |
# MAGIC | `observed_price` | DOUBLE | Actual price at PDV |
# MAGIC | `avg_market_price` | DOUBLE | Market average (same product, same month) |
# MAGIC | `price_variance` | DOUBLE | observed - avg_market |
# MAGIC | `price_index` | DOUBLE | (observed/avg) × 100 |
# MAGIC | `is_above_market` | BOOLEAN | Price above market average |
# MAGIC | `is_below_market` | BOOLEAN | Price below market average |
# MAGIC | `market_observations` | INT | Number of market observations |
# MAGIC | `product_code` | STRING | Natural key |
# MAGIC | `pdv_code` | STRING | Natural key |
# MAGIC | `year_month` | STRING | Year-Month for grouping |
# MAGIC | `processing_timestamp` | TIMESTAMP | ETL timestamp |
# MAGIC 
# MAGIC ### Price Index Interpretation
# MAGIC 
# MAGIC | Index Range | Interpretation |
# MAGIC |-------------|----------------|
# MAGIC | < 85 | Significantly below market (potential underpricing) |
# MAGIC | 85-95 | Below market (competitive) |
# MAGIC | 95-105 | At market (aligned) |
# MAGIC | 105-115 | Above market (premium positioning) |
# MAGIC | > 115 | Significantly above market (price anomaly risk) |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Author:** Diego Mayorga | **Date:** 2026-01-06
