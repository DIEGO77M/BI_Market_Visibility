# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Fact: Sell-In
# MAGIC 
# MAGIC **Purpose:** Create a fact table for sell-in transactions (manufacturer → retailer/distributor) with pre-joined dimension keys for star schema consumption.
# MAGIC 
# MAGIC **Author:** Diego Mayorga  
# MAGIC **Date:** 2026-01-06  
# MAGIC **Project:** BI Market Visibility Analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Design Decisions
# MAGIC 
# MAGIC **Grain:** One row per date × product × pdv transaction
# MAGIC 
# MAGIC **Business Purpose:**
# MAGIC - Measure commercial push (empuje comercial)
# MAGIC - Track manufacturer sales to retail points
# MAGIC - Enable sell-in vs sell-out comparison (with estimated sell-out)
# MAGIC 
# MAGIC **Strategy:**
# MAGIC - Incremental by date partition
# MAGIC - Append-only design (no updates to historical facts)
# MAGIC - Pre-joined dimension surrogate keys
# MAGIC - Pre-aggregated at grain level (sum quantities/values per grain)
# MAGIC 
# MAGIC **Serverless Optimizations:**
# MAGIC - Partitioned by `date_sk` for efficient pruning
# MAGIC - Dynamic partition overwrite
# MAGIC - No complex window functions
# MAGIC - Single write operation

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
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType

# Unity Catalog
CATALOG = "workspace"
SCHEMA = "default"

# Source tables
SILVER_SELL_IN = f"{CATALOG}.{SCHEMA}.silver_sell_in"
GOLD_DIM_DATE = f"{CATALOG}.{SCHEMA}.gold_dim_date"
GOLD_DIM_PRODUCT = f"{CATALOG}.{SCHEMA}.gold_dim_product"
GOLD_DIM_PDV = f"{CATALOG}.{SCHEMA}.gold_dim_pdv"

# Target table
GOLD_FACT_SELL_IN = f"{CATALOG}.{SCHEMA}.gold_fact_sell_in"

print(f"📊 Processing Fact Sell-In")
print(f"   Source: {SILVER_SELL_IN}")
print(f"   Target: {GOLD_FACT_SELL_IN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Silver Source

# COMMAND ----------

# Read Silver sell-in transactions
df_sell_in = spark.read.table(SILVER_SELL_IN)

# Identify key columns dynamically
# Common patterns: date, product_code/sku, pdv_code/code_eleader, quantity, value
date_col = next((c for c in df_sell_in.columns if "date" in c.lower() and "process" not in c.lower()), None)
product_col = next((c for c in df_sell_in.columns if "product" in c.lower() or "sku" in c.lower()), None)
pdv_col = next((c for c in df_sell_in.columns if "pdv" in c.lower() or "eleader" in c.lower() or "store" in c.lower()), None)
qty_col = next((c for c in df_sell_in.columns if "quantity" in c.lower() or "qty" in c.lower() or "cantidad" in c.lower()), None)
value_col = next((c for c in df_sell_in.columns if "value" in c.lower() or "valor" in c.lower() or "amount" in c.lower()), None)

print(f"   Detected columns:")
print(f"   - Date: {date_col}")
print(f"   - Product: {product_col}")
print(f"   - PDV: {pdv_col}")
print(f"   - Quantity: {qty_col}")
print(f"   - Value: {value_col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Dimension Lookups

# COMMAND ----------

# Load dimension tables (broadcast for small tables)
df_dim_date = broadcast(
    spark.read.table(GOLD_DIM_DATE)
    .select("date_sk", "date")
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

print("✓ Dimension lookups loaded (broadcast)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transform & Aggregate

# COMMAND ----------

# Prepare transaction data
df_transactions = df_sell_in.select(
    to_date(col(date_col)).alias("transaction_date"),
    col(product_col).cast(StringType()).alias("product_code"),
    col(pdv_col).cast(StringType()).alias("pdv_code"),
    coalesce(col(qty_col), lit(0)).cast(IntegerType()).alias("quantity"),
    coalesce(col(value_col), lit(0.0)).cast(DoubleType()).alias("value")
).filter(
    col("transaction_date").isNotNull() &
    col("product_code").isNotNull() &
    col("pdv_code").isNotNull()
)

# Aggregate at grain level: date × product × pdv
df_aggregated = df_transactions.groupBy(
    "transaction_date",
    "product_code",
    "pdv_code"
).agg(
    spark_sum("quantity").alias("quantity_sell_in"),
    spark_sum("value").alias("value_sell_in"),
    spark_count("*").alias("transactions_count")
)

# Calculate unit price
df_aggregated = df_aggregated.withColumn(
    "unit_price_sell_in",
    when(col("quantity_sell_in") > 0,
         spark_round(col("value_sell_in") / col("quantity_sell_in"), 2))
    .otherwise(lit(None))
)

print("✓ Transactions aggregated at grain level")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Join Dimension Keys

# COMMAND ----------

# Join with Date dimension
df_with_date = df_aggregated.join(
    df_dim_date,
    df_aggregated["transaction_date"] == df_dim_date["date"],
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

# Final column selection
df_fact_final = df_fact.select(
    col("date_sk"),
    col("product_sk"),
    col("pdv_sk"),
    col("transaction_date").alias("transaction_date"),  # Keep for partitioning
    col("quantity_sell_in"),
    col("value_sell_in"),
    col("unit_price_sell_in"),
    col("transactions_count"),
    # Degenerate dimensions (natural keys for debugging)
    col("product_code"),
    col("pdv_code")
).withColumn(
    "processing_timestamp", current_timestamp()
)

# Handle orphan records (no dimension match)
df_fact_final = df_fact_final.withColumn(
    "date_sk",
    coalesce(col("date_sk"), date_format(col("transaction_date"), "yyyyMMdd").cast(IntegerType()))
)

print("✓ Dimension keys joined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write to Gold Layer

# COMMAND ----------

# Write with partition by date_sk for efficient pruning
df_fact_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("date_sk") \
    .saveAsTable(GOLD_FACT_SELL_IN)

print(f"✅ Written to {GOLD_FACT_SELL_IN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation

# COMMAND ----------

# Zero-compute validation
history = spark.sql(f"DESCRIBE HISTORY {GOLD_FACT_SELL_IN} LIMIT 1").first()
metrics = history["operationMetrics"]

print("=" * 50)
print("GOLD_FACT_SELL_IN VALIDATION")
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
# MAGIC | `transaction_date` | DATE | Transaction date |
# MAGIC | `quantity_sell_in` | INT | Total units sold (aggregated) |
# MAGIC | `value_sell_in` | DOUBLE | Total value in currency |
# MAGIC | `unit_price_sell_in` | DOUBLE | Avg unit price (value/qty) |
# MAGIC | `transactions_count` | INT | Number of transactions aggregated |
# MAGIC | `product_code` | STRING | Natural key (degenerate) |
# MAGIC | `pdv_code` | STRING | Natural key (degenerate) |
# MAGIC | `processing_timestamp` | TIMESTAMP | ETL timestamp |
# MAGIC 
# MAGIC ### Query Patterns
# MAGIC 
# MAGIC ```sql
# MAGIC -- Sell-in by brand and month
# MAGIC SELECT 
# MAGIC     d.year_month,
# MAGIC     p.brand,
# MAGIC     SUM(f.quantity_sell_in) as total_qty,
# MAGIC     SUM(f.value_sell_in) as total_value
# MAGIC FROM gold_fact_sell_in f
# MAGIC JOIN gold_dim_date d ON f.date_sk = d.date_sk
# MAGIC JOIN gold_dim_product p ON f.product_sk = p.product_sk
# MAGIC GROUP BY d.year_month, p.brand
# MAGIC ORDER BY d.year_month
# MAGIC ```
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Author:** Diego Mayorga | **Date:** 2026-01-06
