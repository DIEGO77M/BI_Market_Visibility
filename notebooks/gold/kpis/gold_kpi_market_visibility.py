# Databricks notebook source
# MAGIC %md
# MAGIC # Gold KPI: Market Visibility Daily
# MAGIC 
# MAGIC **Purpose:** Create a denormalized, pre-aggregated KPI table for daily market visibility operations. Designed for direct consumption by Power BI and Streamlit without complex DAX calculations.
# MAGIC 
# MAGIC **Author:** Diego Mayorga  
# MAGIC **Date:** 2026-01-06  
# MAGIC **Project:** BI Market Visibility Analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Design Philosophy
# MAGIC 
# MAGIC **This table breaks the star schema intentionally.**
# MAGIC 
# MAGIC Why? Because operational dashboards need:
# MAGIC - Pre-calculated KPIs (no runtime computation)
# MAGIC - Single table queries (no joins)
# MAGIC - Immediate alerting capability
# MAGIC - Sub-second Power BI response
# MAGIC 
# MAGIC **KPIs Materialized:**
# MAGIC 
# MAGIC | KPI | Formula | Business Meaning |
# MAGIC |-----|---------|------------------|
# MAGIC | `availability_rate` | PDVs with stock / Total PDVs | Product distribution health |
# MAGIC | `price_competitiveness_index` | Avg price index across market | Pricing alignment |
# MAGIC | `sell_in_sell_out_ratio` | Sell-in / Estimated Sell-out | Push vs Pull balance |
# MAGIC | `lost_sales_estimated` | Stock-out days × Avg daily demand | Revenue at risk |
# MAGIC | `stock_health_score` | Weighted score (0-100) | Overall inventory health |
# MAGIC 
# MAGIC **Grain:** date × product × channel (not PDV level - aggregated)
# MAGIC 
# MAGIC **Serverless Optimizations:**
# MAGIC - Single-pass aggregation
# MAGIC - Pre-joined dimensions (denormalized)
# MAGIC - No complex window functions
# MAGIC - Partitioned by date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, 
    current_date, current_timestamp, to_date, date_format,
    sum as spark_sum, count as spark_count, avg as spark_avg,
    max as spark_max, min as spark_min, countDistinct,
    round as spark_round, broadcast,
    expr
)
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType

# Unity Catalog
CATALOG = "workspace"
SCHEMA = "default"

# Source tables (Gold Layer)
GOLD_FACT_SELL_IN = f"{CATALOG}.{SCHEMA}.gold_fact_sell_in"
GOLD_FACT_PRICE_AUDIT = f"{CATALOG}.{SCHEMA}.gold_fact_price_audit"
GOLD_FACT_STOCK = f"{CATALOG}.{SCHEMA}.gold_fact_stock"
GOLD_DIM_DATE = f"{CATALOG}.{SCHEMA}.gold_dim_date"
GOLD_DIM_PRODUCT = f"{CATALOG}.{SCHEMA}.gold_dim_product"
GOLD_DIM_PDV = f"{CATALOG}.{SCHEMA}.gold_dim_pdv"

# Target table
GOLD_KPI_MARKET_VISIBILITY = f"{CATALOG}.{SCHEMA}.gold_kpi_market_visibility_daily"

# Business assumptions
SELL_OUT_RATE = 0.85
LOST_SALES_AVG_PRICE_ASSUMPTION = 10.0  # Default avg unit price if unknown

print(f"📊 Processing KPI: Market Visibility Daily")
print(f"   Target: {GOLD_KPI_MARKET_VISIBILITY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Source Facts

# COMMAND ----------

# Load fact tables
df_sell_in = spark.read.table(GOLD_FACT_SELL_IN)
df_price = spark.read.table(GOLD_FACT_PRICE_AUDIT)
df_stock = spark.read.table(GOLD_FACT_STOCK)

# Load dimensions (for denormalization)
df_dim_date = broadcast(spark.read.table(GOLD_DIM_DATE))
df_dim_product = broadcast(
    spark.read.table(GOLD_DIM_PRODUCT)
    .filter(col("is_current") == True)
)
df_dim_pdv = broadcast(
    spark.read.table(GOLD_DIM_PDV)
    .filter(col("is_current") == True)
)

print("✓ Source tables loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build Sell-In Metrics

# COMMAND ----------

# Aggregate sell-in by date × product × channel
df_sell_in_metrics = df_sell_in \
    .join(df_dim_product, df_sell_in["product_sk"] == df_dim_product["product_sk"], "left") \
    .join(df_dim_pdv, df_sell_in["pdv_sk"] == df_dim_pdv["pdv_sk"], "left") \
    .groupBy(
        df_sell_in["date_sk"],
        df_sell_in["product_sk"],
        df_dim_product["product_code"],
        df_dim_product["product_name"],
        df_dim_product["brand"],
        df_dim_product["category"],
        df_dim_pdv["channel"]
    ).agg(
        spark_sum("quantity_sell_in").alias("total_sell_in_qty"),
        spark_sum("value_sell_in").alias("total_sell_in_value"),
        spark_avg("unit_price_sell_in").alias("avg_unit_price_sell_in"),
        countDistinct(df_sell_in["pdv_sk"]).alias("pdv_count_with_sell_in"),
        spark_count("*").alias("transaction_count")
    )

# Estimate sell-out from sell-in
df_sell_in_metrics = df_sell_in_metrics.withColumn(
    "estimated_sell_out_qty",
    spark_round(col("total_sell_in_qty") * SELL_OUT_RATE, 0)
).withColumn(
    "sell_in_sell_out_ratio",
    when(col("estimated_sell_out_qty") > 0,
         spark_round(col("total_sell_in_qty") / col("estimated_sell_out_qty"), 2))
    .otherwise(lit(None))
)

print("✓ Sell-in metrics aggregated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build Price Metrics

# COMMAND ----------

# Aggregate price by date × product × channel
df_price_metrics = df_price \
    .join(df_dim_pdv, df_price["pdv_sk"] == df_dim_pdv["pdv_sk"], "left") \
    .groupBy(
        df_price["date_sk"],
        df_price["product_sk"],
        df_dim_pdv["channel"]
    ).agg(
        spark_avg("observed_price").alias("avg_observed_price"),
        spark_avg("avg_market_price").alias("market_avg_price"),
        spark_avg("price_index").alias("price_competitiveness_index"),
        spark_min("price_index").alias("min_price_index"),
        spark_max("price_index").alias("max_price_index"),
        spark_sum(when(col("is_above_market"), 1).otherwise(0)).alias("pdv_above_market"),
        spark_sum(when(col("is_below_market"), 1).otherwise(0)).alias("pdv_below_market"),
        spark_count("*").alias("price_observation_count")
    )

print("✓ Price metrics aggregated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build Stock Metrics

# COMMAND ----------

# Aggregate stock by date × product × channel
df_stock_metrics = df_stock \
    .join(df_dim_pdv, df_stock["pdv_sk"] == df_dim_pdv["pdv_sk"], "left") \
    .groupBy(
        df_stock["date_sk"],
        df_stock["product_sk"],
        df_dim_pdv["channel"]
    ).agg(
        spark_sum("closing_stock").alias("total_closing_stock"),
        spark_avg("stock_days").alias("avg_stock_days"),
        countDistinct(df_stock["pdv_sk"]).alias("total_pdv_count"),
        spark_sum(when(col("stock_out_flag"), 1).otherwise(0)).alias("pdv_with_stock_out"),
        spark_sum(when(col("overstock_flag"), 1).otherwise(0)).alias("pdv_with_overstock"),
        spark_sum(when(col("is_healthy_stock"), 1).otherwise(0)).alias("pdv_with_healthy_stock"),
        spark_avg("avg_daily_sell_out_estimated").alias("avg_daily_demand")
    )

# Calculate availability rate
df_stock_metrics = df_stock_metrics.withColumn(
    "availability_rate",
    when(col("total_pdv_count") > 0,
         spark_round((col("total_pdv_count") - col("pdv_with_stock_out")) / col("total_pdv_count") * 100, 2))
    .otherwise(lit(None))
)

# Estimate lost sales (stock-out days × avg daily demand × avg price)
df_stock_metrics = df_stock_metrics.withColumn(
    "lost_sales_qty_estimated",
    col("pdv_with_stock_out") * col("avg_daily_demand")
)

print("✓ Stock metrics aggregated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Combine All Metrics

# COMMAND ----------

# Join metrics on date × product × channel
df_combined = df_sell_in_metrics.alias("s") \
    .join(
        df_price_metrics.alias("p"),
        (col("s.date_sk") == col("p.date_sk")) &
        (col("s.product_sk") == col("p.product_sk")) &
        (col("s.channel") == col("p.channel")),
        "full_outer"
    ).join(
        df_stock_metrics.alias("st"),
        (coalesce(col("s.date_sk"), col("p.date_sk")) == col("st.date_sk")) &
        (coalesce(col("s.product_sk"), col("p.product_sk")) == col("st.product_sk")) &
        (coalesce(col("s.channel"), col("p.channel")) == col("st.channel")),
        "full_outer"
    )

# Coalesce keys and build final output
df_kpi = df_combined.select(
    # Keys
    coalesce(col("s.date_sk"), col("p.date_sk"), col("st.date_sk")).alias("date_sk"),
    coalesce(col("s.product_sk"), col("p.product_sk"), col("st.product_sk")).alias("product_sk"),
    coalesce(col("s.channel"), col("p.channel"), col("st.channel")).alias("channel"),
    
    # Denormalized product attributes
    col("s.product_code"),
    col("s.product_name"),
    col("s.brand"),
    col("s.category"),
    
    # Sell-In KPIs
    col("s.total_sell_in_qty"),
    col("s.total_sell_in_value"),
    col("s.avg_unit_price_sell_in"),
    col("s.estimated_sell_out_qty"),
    col("s.sell_in_sell_out_ratio"),
    col("s.pdv_count_with_sell_in"),
    col("s.transaction_count"),
    
    # Price KPIs
    col("p.avg_observed_price"),
    col("p.market_avg_price"),
    col("p.price_competitiveness_index"),
    col("p.min_price_index"),
    col("p.max_price_index"),
    col("p.pdv_above_market"),
    col("p.pdv_below_market"),
    col("p.price_observation_count"),
    
    # Stock KPIs
    col("st.total_closing_stock"),
    col("st.avg_stock_days"),
    col("st.total_pdv_count"),
    col("st.pdv_with_stock_out"),
    col("st.pdv_with_overstock"),
    col("st.pdv_with_healthy_stock"),
    col("st.availability_rate"),
    col("st.lost_sales_qty_estimated"),
    col("st.avg_daily_demand")
)

print("✓ Metrics combined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Calculate Composite Scores

# COMMAND ----------

# Add date dimension attributes for easy filtering
df_kpi_enriched = df_kpi.join(
    df_dim_date.select("date_sk", "date", "year_month", "year", "quarter", "is_weekend"),
    "date_sk",
    "left"
)

# Calculate Stock Health Score (0-100)
# Weighted: 40% availability, 30% days-of-stock health, 30% no overstock
df_kpi_enriched = df_kpi_enriched.withColumn(
    "stock_health_score",
    spark_round(
        (coalesce(col("availability_rate"), lit(0)) * 0.40) +
        (when(col("avg_stock_days").between(7, 45), lit(100))
         .when(col("avg_stock_days").between(1, 7), lit(50))
         .when(col("avg_stock_days") > 45, lit(30))
         .otherwise(lit(0)) * 0.30) +
        (when(col("total_pdv_count") > 0,
              (1 - col("pdv_with_overstock") / col("total_pdv_count")) * 100)
         .otherwise(lit(0)) * 0.30),
        2
    )
)

# Calculate Lost Sales Value
df_kpi_enriched = df_kpi_enriched.withColumn(
    "lost_sales_value_estimated",
    spark_round(
        col("lost_sales_qty_estimated") * 
        coalesce(col("avg_observed_price"), col("avg_unit_price_sell_in"), lit(LOST_SALES_AVG_PRICE_ASSUMPTION)),
        2
    )
)

# Add alert flags for operational monitoring
df_kpi_enriched = df_kpi_enriched \
    .withColumn("alert_stock_out", col("pdv_with_stock_out") > 0) \
    .withColumn("alert_price_anomaly", 
        (col("price_competitiveness_index") < 85) | (col("price_competitiveness_index") > 115)) \
    .withColumn("alert_overstock", col("pdv_with_overstock") > col("total_pdv_count") * 0.3) \
    .withColumn("alert_low_availability", col("availability_rate") < 80)

# Add metadata
df_final = df_kpi_enriched.withColumn(
    "processing_timestamp", current_timestamp()
)

print("✓ Composite scores and alerts calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write to Gold Layer

# COMMAND ----------

# Write partitioned by date_sk
df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("date_sk") \
    .saveAsTable(GOLD_KPI_MARKET_VISIBILITY)

print(f"✅ Written to {GOLD_KPI_MARKET_VISIBILITY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validation

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {GOLD_KPI_MARKET_VISIBILITY} LIMIT 1").first()
metrics = history["operationMetrics"]

print("=" * 60)
print("GOLD_KPI_MARKET_VISIBILITY_DAILY VALIDATION")
print("=" * 60)
print(f"Operation: {history['operation']}")
print(f"Timestamp: {history['timestamp']}")
print(f"Rows Written: {metrics.get('numOutputRows', 'N/A')}")
print(f"Files Written: {metrics.get('numFiles', 'N/A')}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Schema Documentation
# MAGIC 
# MAGIC ### Primary Keys & Grain
# MAGIC 
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | `date_sk` | INT | Date surrogate key (partition) |
# MAGIC | `product_sk` | STRING | Product surrogate key |
# MAGIC | `channel` | STRING | Sales channel |
# MAGIC 
# MAGIC ### Denormalized Attributes
# MAGIC 
# MAGIC | Column | Type | Source |
# MAGIC |--------|------|--------|
# MAGIC | `product_code` | STRING | dim_product |
# MAGIC | `product_name` | STRING | dim_product |
# MAGIC | `brand` | STRING | dim_product |
# MAGIC | `category` | STRING | dim_product |
# MAGIC | `date` | DATE | dim_date |
# MAGIC | `year_month` | STRING | dim_date |
# MAGIC 
# MAGIC ### Sell-In KPIs
# MAGIC 
# MAGIC | Column | Type | Business Meaning |
# MAGIC |--------|------|------------------|
# MAGIC | `total_sell_in_qty` | INT | Units pushed to channel |
# MAGIC | `total_sell_in_value` | DOUBLE | Revenue from push |
# MAGIC | `sell_in_sell_out_ratio` | DOUBLE | Push/Pull balance (1.0 = balanced) |
# MAGIC | `estimated_sell_out_qty` | INT | Estimated consumer sales |
# MAGIC 
# MAGIC ### Price KPIs
# MAGIC 
# MAGIC | Column | Type | Business Meaning |
# MAGIC |--------|------|------------------|
# MAGIC | `price_competitiveness_index` | DOUBLE | 100 = market avg |
# MAGIC | `pdv_above_market` | INT | Stores pricing above market |
# MAGIC | `pdv_below_market` | INT | Stores pricing below market |
# MAGIC 
# MAGIC ### Stock KPIs
# MAGIC 
# MAGIC | Column | Type | Business Meaning |
# MAGIC |--------|------|------------------|
# MAGIC | `availability_rate` | DOUBLE | % stores with stock |
# MAGIC | `stock_health_score` | DOUBLE | Composite score (0-100) |
# MAGIC | `lost_sales_value_estimated` | DOUBLE | Revenue at risk |
# MAGIC 
# MAGIC ### Alert Flags (Boolean)
# MAGIC 
# MAGIC | Column | Trigger Condition |
# MAGIC |--------|-------------------|
# MAGIC | `alert_stock_out` | Any store has stock-out |
# MAGIC | `alert_price_anomaly` | Price index <85 or >115 |
# MAGIC | `alert_overstock` | >30% stores overstocked |
# MAGIC | `alert_low_availability` | Availability <80% |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Author:** Diego Mayorga | **Date:** 2026-01-06
