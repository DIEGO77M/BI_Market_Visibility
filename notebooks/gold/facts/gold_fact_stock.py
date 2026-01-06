# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Fact: Stock (Estimated)
# MAGIC 
# MAGIC **Purpose:** Create an estimated stock fact table derived from sell-in patterns, enabling stock-out risk and overstock detection.
# MAGIC 
# MAGIC **Author:** Diego Mayorga  
# MAGIC **Date:** 2026-01-06  
# MAGIC **Project:** BI Market Visibility Analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Design Decisions
# MAGIC 
# MAGIC **⚠️ IMPORTANT: Stock Estimation Model**
# MAGIC 
# MAGIC This table uses an **estimated stock model** based on sell-in patterns. Key assumptions:
# MAGIC 
# MAGIC 1. **No actual inventory data available** - Stock is derived from sell-in cumulative patterns
# MAGIC 2. **Sell-Out estimation** - Assumed constant sell-out rate based on historical sell-in
# MAGIC 3. **Lead time consideration** - 7-day assumption for stock replenishment
# MAGIC 
# MAGIC **Business Purpose:**
# MAGIC - Identify stock-out risk (projected days of stock = 0)
# MAGIC - Flag overstock situations (>45 days inventory)
# MAGIC - Enable proactive replenishment alerts
# MAGIC - Support lost sales estimation
# MAGIC 
# MAGIC **Grain:** One row per date × product × pdv (daily stock position)
# MAGIC 
# MAGIC **Estimation Formula:**
# MAGIC ```
# MAGIC estimated_closing_stock = cumulative_sell_in - estimated_cumulative_sell_out
# MAGIC stock_days = estimated_closing_stock / avg_daily_sell_out
# MAGIC ```
# MAGIC 
# MAGIC **Serverless Optimizations:**
# MAGIC - Simplified window functions (bounded)
# MAGIC - Single write operation
# MAGIC - Partitioned output

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, 
    current_date, current_timestamp, to_date, date_format,
    sum as spark_sum, count as spark_count, avg as spark_avg,
    max as spark_max, min as spark_min,
    round as spark_round, broadcast,
    datediff, lag, lead
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, BooleanType

# Unity Catalog
CATALOG = "workspace"
SCHEMA = "default"

# Source tables
GOLD_FACT_SELL_IN = f"{CATALOG}.{SCHEMA}.gold_fact_sell_in"
GOLD_DIM_DATE = f"{CATALOG}.{SCHEMA}.gold_dim_date"
GOLD_DIM_PRODUCT = f"{CATALOG}.{SCHEMA}.gold_dim_product"
GOLD_DIM_PDV = f"{CATALOG}.{SCHEMA}.gold_dim_pdv"

# Target table
GOLD_FACT_STOCK = f"{CATALOG}.{SCHEMA}.gold_fact_stock"

# Business assumptions (documented)
SELL_OUT_RATE_ASSUMPTION = 0.85  # 85% of sell-in converts to sell-out within period
STOCK_OUT_THRESHOLD_DAYS = 0     # Days <= 0 = stock-out
OVERSTOCK_THRESHOLD_DAYS = 45    # Days > 45 = overstock
LOOKBACK_DAYS_AVG = 30           # Days for average calculation

print(f"📦 Processing Fact Stock (Estimated)")
print(f"   Source: {GOLD_FACT_SELL_IN}")
print(f"   Target: {GOLD_FACT_STOCK}")
print(f"   ⚠️ Stock is ESTIMATED based on sell-in patterns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Source Data

# COMMAND ----------

# Read sell-in fact
df_sell_in = spark.read.table(GOLD_FACT_SELL_IN)

# Load dimension lookups
df_dim_date = broadcast(spark.read.table(GOLD_DIM_DATE).select("date_sk", "date"))

print(f"✓ Source data loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Calculate Stock Metrics

# COMMAND ----------

# Define windows for stock calculations
# Partitioned by product × pdv, ordered by date
window_cumulative = Window.partitionBy("product_sk", "pdv_sk") \
    .orderBy("date_sk") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

window_rolling_30d = Window.partitionBy("product_sk", "pdv_sk") \
    .orderBy("date_sk") \
    .rowsBetween(-30, -1)  # Previous 30 days (excluding current)

# Calculate cumulative sell-in and rolling metrics
df_stock_base = df_sell_in \
    .withColumn("cumulative_sell_in", 
        spark_sum("quantity_sell_in").over(window_cumulative)) \
    .withColumn("avg_daily_sell_in_30d",
        spark_avg("quantity_sell_in").over(window_rolling_30d)) \
    .withColumn("total_sell_in_30d",
        spark_sum("quantity_sell_in").over(window_rolling_30d))

# Estimate sell-out (assumption-based)
df_stock_base = df_stock_base.withColumn(
    "estimated_cumulative_sell_out",
    spark_round(col("cumulative_sell_in") * SELL_OUT_RATE_ASSUMPTION, 0)
).withColumn(
    "avg_daily_sell_out_estimated",
    spark_round(col("avg_daily_sell_in_30d") * SELL_OUT_RATE_ASSUMPTION, 2)
)

# Calculate estimated stock position
df_stock = df_stock_base.withColumn(
    "estimated_closing_stock",
    col("cumulative_sell_in") - col("estimated_cumulative_sell_out")
).withColumn(
    "estimated_opening_stock",
    coalesce(
        lag("estimated_closing_stock").over(
            Window.partitionBy("product_sk", "pdv_sk").orderBy("date_sk")
        ),
        lit(0)
    )
)

# Calculate stock days (days of inventory remaining)
df_stock = df_stock.withColumn(
    "stock_days",
    when(col("avg_daily_sell_out_estimated") > 0,
         spark_round(col("estimated_closing_stock") / col("avg_daily_sell_out_estimated"), 1))
    .otherwise(lit(999))  # No sell-out = infinite days (or dead stock)
)

# Flag stock-out and overstock
df_stock = df_stock.withColumn(
    "stock_out_flag",
    (col("estimated_closing_stock") <= 0) | (col("stock_days") <= STOCK_OUT_THRESHOLD_DAYS)
).withColumn(
    "overstock_flag",
    col("stock_days") > OVERSTOCK_THRESHOLD_DAYS
).withColumn(
    "is_healthy_stock",
    (col("stock_days") > STOCK_OUT_THRESHOLD_DAYS) & 
    (col("stock_days") <= OVERSTOCK_THRESHOLD_DAYS)
)

print("✓ Stock metrics calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Final Selection

# COMMAND ----------

# Select final columns
df_fact_stock = df_stock.select(
    col("date_sk"),
    col("product_sk"),
    col("pdv_sk"),
    col("transaction_date").alias("stock_date"),
    
    # Stock position
    col("estimated_opening_stock").cast(IntegerType()).alias("opening_stock"),
    col("estimated_closing_stock").cast(IntegerType()).alias("closing_stock"),
    col("stock_days"),
    
    # Flags
    col("stock_out_flag").cast(BooleanType()),
    col("overstock_flag").cast(BooleanType()),
    col("is_healthy_stock").cast(BooleanType()),
    
    # Supporting metrics
    col("cumulative_sell_in").cast(IntegerType()),
    col("estimated_cumulative_sell_out").cast(IntegerType()).alias("cumulative_sell_out_estimated"),
    col("avg_daily_sell_out_estimated"),
    col("quantity_sell_in").alias("daily_sell_in"),
    
    # Degenerate dimensions
    col("product_code"),
    col("pdv_code")
).withColumn(
    "processing_timestamp", current_timestamp()
).withColumn(
    # Document estimation method
    "estimation_method", 
    lit(f"SELL_OUT_RATE={SELL_OUT_RATE_ASSUMPTION}")
)

print("✓ Final columns selected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Gold Layer

# COMMAND ----------

# Write partitioned by date
df_fact_stock.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("date_sk") \
    .saveAsTable(GOLD_FACT_STOCK)

print(f"✅ Written to {GOLD_FACT_STOCK}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validation

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {GOLD_FACT_STOCK} LIMIT 1").first()
metrics = history["operationMetrics"]

print("=" * 50)
print("GOLD_FACT_STOCK VALIDATION")
print("=" * 50)
print(f"Operation: {history['operation']}")
print(f"Timestamp: {history['timestamp']}")
print(f"Rows Written: {metrics.get('numOutputRows', 'N/A')}")
print(f"Files Written: {metrics.get('numFiles', 'N/A')}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Schema Documentation
# MAGIC 
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | `date_sk` | INT | FK to gold_dim_date (partition key) |
# MAGIC | `product_sk` | STRING | FK to gold_dim_product |
# MAGIC | `pdv_sk` | STRING | FK to gold_dim_pdv |
# MAGIC | `stock_date` | DATE | Stock position date |
# MAGIC | `opening_stock` | INT | Estimated opening stock (units) |
# MAGIC | `closing_stock` | INT | Estimated closing stock (units) |
# MAGIC | `stock_days` | DOUBLE | Estimated days of inventory |
# MAGIC | `stock_out_flag` | BOOLEAN | True if stock-out detected |
# MAGIC | `overstock_flag` | BOOLEAN | True if overstock (>45 days) |
# MAGIC | `is_healthy_stock` | BOOLEAN | True if stock in healthy range |
# MAGIC | `cumulative_sell_in` | INT | Cumulative sell-in to date |
# MAGIC | `cumulative_sell_out_estimated` | INT | Estimated cumulative sell-out |
# MAGIC | `avg_daily_sell_out_estimated` | DOUBLE | 30-day avg daily sell-out |
# MAGIC | `daily_sell_in` | INT | Daily sell-in quantity |
# MAGIC | `product_code` | STRING | Natural key |
# MAGIC | `pdv_code` | STRING | Natural key |
# MAGIC | `estimation_method` | STRING | Documents the estimation formula |
# MAGIC | `processing_timestamp` | TIMESTAMP | ETL timestamp |
# MAGIC 
# MAGIC ### ⚠️ Estimation Assumptions
# MAGIC 
# MAGIC | Assumption | Value | Rationale |
# MAGIC |------------|-------|-----------|
# MAGIC | Sell-out Rate | 85% | Industry standard for FMCG sell-through |
# MAGIC | Stock-out Threshold | 0 days | Zero inventory = stock-out |
# MAGIC | Overstock Threshold | 45 days | >6 weeks inventory = excess |
# MAGIC | Lookback Period | 30 days | Monthly average for stability |
# MAGIC 
# MAGIC ### Query Patterns
# MAGIC 
# MAGIC ```sql
# MAGIC -- Current stock-out risk by product
# MAGIC SELECT 
# MAGIC     p.product_name,
# MAGIC     p.brand,
# MAGIC     COUNT(CASE WHEN s.stock_out_flag THEN 1 END) as stock_outs,
# MAGIC     COUNT(*) as total_pdvs
# MAGIC FROM gold_fact_stock s
# MAGIC JOIN gold_dim_product p ON s.product_sk = p.product_sk
# MAGIC WHERE s.date_sk = (SELECT MAX(date_sk) FROM gold_fact_stock)
# MAGIC GROUP BY p.product_name, p.brand
# MAGIC ORDER BY stock_outs DESC
# MAGIC ```
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Author:** Diego Mayorga | **Date:** 2026-01-06
