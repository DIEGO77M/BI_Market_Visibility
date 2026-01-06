# Databricks notebook source
# MAGIC %md
# MAGIC # Gold KPI: Market Share
# MAGIC 
# MAGIC **Purpose:** Create a denormalized KPI table for market share analysis, enabling competitive positioning insights and trend detection.
# MAGIC 
# MAGIC **Author:** Diego Mayorga  
# MAGIC **Date:** 2026-01-06  
# MAGIC **Project:** BI Market Visibility Analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Design Philosophy
# MAGIC 
# MAGIC **Market Share Estimation Model**
# MAGIC 
# MAGIC Since we only have our own sell-in data (no competitor data), market share is estimated using:
# MAGIC 
# MAGIC 1. **Relative Share by Brand** - Share within our portfolio
# MAGIC 2. **Share by Channel** - Distribution across channels
# MAGIC 3. **Share by Region** - Geographic concentration
# MAGIC 4. **Trend Analysis** - Period-over-period changes
# MAGIC 
# MAGIC **⚠️ Limitation:** True market share requires competitor data. This table provides **internal portfolio share** which is still valuable for:
# MAGIC - Portfolio optimization
# MAGIC - Brand performance tracking
# MAGIC - Channel strategy
# MAGIC - Regional focus
# MAGIC 
# MAGIC **Grain:** year_month × product/brand/category × channel × region
# MAGIC 
# MAGIC **Aggregation Level:** Monthly (weekly too granular, daily too noisy)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, 
    current_date, current_timestamp, date_format,
    sum as spark_sum, count as spark_count, avg as spark_avg,
    max as spark_max, min as spark_min, countDistinct,
    round as spark_round, broadcast,
    lag, first, last
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType

# Unity Catalog
CATALOG = "workspace"
SCHEMA = "default"

# Source tables
GOLD_KPI_MARKET_VISIBILITY = f"{CATALOG}.{SCHEMA}.gold_kpi_market_visibility_daily"
GOLD_DIM_DATE = f"{CATALOG}.{SCHEMA}.gold_dim_date"
GOLD_DIM_PDV = f"{CATALOG}.{SCHEMA}.gold_dim_pdv"

# Target table
GOLD_KPI_MARKET_SHARE = f"{CATALOG}.{SCHEMA}.gold_kpi_market_share"

print(f"📈 Processing KPI: Market Share")
print(f"   Target: {GOLD_KPI_MARKET_SHARE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Source Data

# COMMAND ----------

# Load market visibility KPIs (already pre-aggregated)
df_visibility = spark.read.table(GOLD_KPI_MARKET_VISIBILITY)

# Load dimensions
df_dim_date = broadcast(
    spark.read.table(GOLD_DIM_DATE)
    .select("date_sk", "year_month", "year", "quarter")
)

df_dim_pdv = broadcast(
    spark.read.table(GOLD_DIM_PDV)
    .filter(col("is_current") == True)
    .select("pdv_code", "channel", "region", "city")
)

print("✓ Source data loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Calculate Monthly Aggregations

# COMMAND ----------

# Aggregate to monthly level by brand × channel
df_monthly_brand_channel = df_visibility \
    .join(df_dim_date, "date_sk", "left") \
    .groupBy(
        "year_month",
        "brand",
        "category",
        "channel"
    ).agg(
        spark_sum("total_sell_in_qty").alias("monthly_sell_in_qty"),
        spark_sum("total_sell_in_value").alias("monthly_sell_in_value"),
        spark_sum("estimated_sell_out_qty").alias("monthly_sell_out_qty"),
        spark_avg("price_competitiveness_index").alias("avg_price_index"),
        spark_avg("availability_rate").alias("avg_availability"),
        spark_sum("lost_sales_value_estimated").alias("monthly_lost_sales"),
        countDistinct("product_sk").alias("active_products"),
        spark_count("*").alias("observation_count")
    )

print("✓ Monthly brand×channel aggregation complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Calculate Share Metrics

# COMMAND ----------

# Total market by month (for share calculation)
df_monthly_total = df_monthly_brand_channel.groupBy("year_month").agg(
    spark_sum("monthly_sell_in_qty").alias("total_market_qty"),
    spark_sum("monthly_sell_in_value").alias("total_market_value")
)

# Join totals for share calculation
df_with_totals = df_monthly_brand_channel.join(
    broadcast(df_monthly_total),
    "year_month",
    "left"
)

# Calculate share metrics
df_share = df_with_totals \
    .withColumn(
        "share_by_qty",
        when(col("total_market_qty") > 0,
             spark_round(col("monthly_sell_in_qty") / col("total_market_qty") * 100, 2))
        .otherwise(lit(0))
    ) \
    .withColumn(
        "share_by_value",
        when(col("total_market_value") > 0,
             spark_round(col("monthly_sell_in_value") / col("total_market_value") * 100, 2))
        .otherwise(lit(0))
    )

print("✓ Share metrics calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Calculate Trends (Period-over-Period)

# COMMAND ----------

# Window for trend calculation (previous month)
window_trend = Window.partitionBy("brand", "category", "channel").orderBy("year_month")

df_with_trends = df_share \
    .withColumn(
        "prev_month_share_qty",
        lag("share_by_qty", 1).over(window_trend)
    ) \
    .withColumn(
        "prev_month_share_value",
        lag("share_by_value", 1).over(window_trend)
    ) \
    .withColumn(
        "prev_month_sell_in",
        lag("monthly_sell_in_qty", 1).over(window_trend)
    )

# Calculate changes
df_with_trends = df_with_trends \
    .withColumn(
        "share_change_qty_pp",  # percentage points change
        spark_round(col("share_by_qty") - col("prev_month_share_qty"), 2)
    ) \
    .withColumn(
        "share_change_value_pp",
        spark_round(col("share_by_value") - col("prev_month_share_value"), 2)
    ) \
    .withColumn(
        "sell_in_growth_pct",
        when(col("prev_month_sell_in") > 0,
             spark_round((col("monthly_sell_in_qty") - col("prev_month_sell_in")) / col("prev_month_sell_in") * 100, 2))
        .otherwise(lit(None))
    )

# Trend direction
df_with_trends = df_with_trends \
    .withColumn(
        "share_trend",
        when(col("share_change_qty_pp") > 1, lit("GAINING"))
        .when(col("share_change_qty_pp") < -1, lit("LOSING"))
        .otherwise(lit("STABLE"))
    )

print("✓ Trend calculations complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Add Brand-Level Rollups

# COMMAND ----------

# Brand-level totals (across all channels)
df_brand_total = df_monthly_brand_channel.groupBy("year_month", "brand").agg(
    spark_sum("monthly_sell_in_qty").alias("brand_total_qty"),
    spark_sum("monthly_sell_in_value").alias("brand_total_value")
)

# Join brand totals
df_enriched = df_with_trends.join(
    broadcast(df_brand_total),
    ["year_month", "brand"],
    "left"
)

# Calculate channel share within brand
df_enriched = df_enriched.withColumn(
    "channel_share_of_brand",
    when(col("brand_total_qty") > 0,
         spark_round(col("monthly_sell_in_qty") / col("brand_total_qty") * 100, 2))
    .otherwise(lit(0))
)

print("✓ Brand-level rollups complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Final Selection

# COMMAND ----------

df_final = df_enriched.select(
    # Keys
    col("year_month"),
    col("brand"),
    col("category"),
    col("channel"),
    
    # Absolute metrics
    col("monthly_sell_in_qty"),
    col("monthly_sell_in_value"),
    col("monthly_sell_out_qty"),
    col("monthly_lost_sales"),
    col("active_products"),
    
    # Share metrics (portfolio share, not true market share)
    col("share_by_qty").alias("portfolio_share_qty_pct"),
    col("share_by_value").alias("portfolio_share_value_pct"),
    col("channel_share_of_brand").alias("channel_share_of_brand_pct"),
    
    # Trend metrics
    col("share_change_qty_pp"),
    col("share_change_value_pp"),
    col("sell_in_growth_pct"),
    col("share_trend"),
    
    # Context metrics
    col("avg_price_index"),
    col("avg_availability"),
    col("total_market_qty"),
    col("total_market_value"),
    col("brand_total_qty"),
    col("brand_total_value"),
    
    # Totals for reference
    col("observation_count")
).withColumn(
    "processing_timestamp", current_timestamp()
)

print("✓ Final columns selected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write to Gold Layer

# COMMAND ----------

# Write partitioned by year_month for efficient time-series queries
df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("year_month") \
    .saveAsTable(GOLD_KPI_MARKET_SHARE)

print(f"✅ Written to {GOLD_KPI_MARKET_SHARE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validation

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {GOLD_KPI_MARKET_SHARE} LIMIT 1").first()
metrics = history["operationMetrics"]

print("=" * 60)
print("GOLD_KPI_MARKET_SHARE VALIDATION")
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
# MAGIC | `year_month` | STRING | Period (YYYY-MM), partition key |
# MAGIC | `brand` | STRING | Product brand |
# MAGIC | `category` | STRING | Product category |
# MAGIC | `channel` | STRING | Sales channel |
# MAGIC 
# MAGIC ### Share Metrics (Portfolio Share)
# MAGIC 
# MAGIC | Column | Type | Business Meaning |
# MAGIC |--------|------|------------------|
# MAGIC | `portfolio_share_qty_pct` | DOUBLE | % of total portfolio units |
# MAGIC | `portfolio_share_value_pct` | DOUBLE | % of total portfolio value |
# MAGIC | `channel_share_of_brand_pct` | DOUBLE | % of brand sold in channel |
# MAGIC 
# MAGIC ### Trend Metrics
# MAGIC 
# MAGIC | Column | Type | Business Meaning |
# MAGIC |--------|------|------------------|
# MAGIC | `share_change_qty_pp` | DOUBLE | MoM share change (pp) |
# MAGIC | `share_change_value_pp` | DOUBLE | MoM value share change |
# MAGIC | `sell_in_growth_pct` | DOUBLE | MoM sell-in growth % |
# MAGIC | `share_trend` | STRING | GAINING/STABLE/LOSING |
# MAGIC 
# MAGIC ### Interpretation Guide
# MAGIC 
# MAGIC | Trend | share_change_qty_pp | Action |
# MAGIC |-------|---------------------|--------|
# MAGIC | GAINING | > +1pp | Capitalize - increase investment |
# MAGIC | STABLE | -1pp to +1pp | Maintain current strategy |
# MAGIC | LOSING | < -1pp | Investigate - root cause analysis |
# MAGIC 
# MAGIC ### Query Patterns
# MAGIC 
# MAGIC ```sql
# MAGIC -- Top gaining brands this month
# MAGIC SELECT brand, portfolio_share_qty_pct, share_change_qty_pp, share_trend
# MAGIC FROM gold_kpi_market_share
# MAGIC WHERE year_month = '2026-01'
# MAGIC   AND share_trend = 'GAINING'
# MAGIC ORDER BY share_change_qty_pp DESC
# MAGIC 
# MAGIC -- Brand trend over time
# MAGIC SELECT year_month, portfolio_share_qty_pct
# MAGIC FROM gold_kpi_market_share
# MAGIC WHERE brand = 'BRAND_X'
# MAGIC ORDER BY year_month
# MAGIC ```
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Author:** Diego Mayorga | **Date:** 2026-01-06
