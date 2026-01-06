# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Dimension: Date
# MAGIC 
# MAGIC **Purpose:** Generate a static calendar dimension table for time-based analysis.
# MAGIC 
# MAGIC **Author:** Diego Mayorga  
# MAGIC **Date:** 2026-01-06  
# MAGIC **Project:** BI Market Visibility Analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Design Decisions
# MAGIC 
# MAGIC **Table Type:** Static dimension (no SCD required)
# MAGIC 
# MAGIC **Grain:** One row per calendar date
# MAGIC 
# MAGIC **Surrogate Key Strategy:**
# MAGIC - `date_sk`: Integer in YYYYMMDD format (e.g., 20260106)
# MAGIC - Enables efficient integer joins vs. date comparisons
# MAGIC - Self-documenting: humans can read the key
# MAGIC 
# MAGIC **Refresh Strategy:**
# MAGIC - Full refresh (small table, ~2K rows for 5 years)
# MAGIC - Run annually or when extending date range
# MAGIC 
# MAGIC **Serverless Optimizations:**
# MAGIC - No partitioning (small table)
# MAGIC - Single write operation
# MAGIC - No window functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, 
    year, month, quarter, weekofyear, dayofweek, dayofmonth,
    date_format, to_date, last_day, expr,
    current_date, current_timestamp,
    sequence, explode
)
from pyspark.sql.types import DateType, IntegerType, StringType, BooleanType
from datetime import date

# Unity Catalog
CATALOG = "workspace"
SCHEMA = "default"

# Target table
GOLD_DIM_DATE = f"{CATALOG}.{SCHEMA}.gold_dim_date"

# Date range configuration
DATE_START = "2022-01-01"
DATE_END = "2026-12-31"

print(f"📅 Generating date dimension from {DATE_START} to {DATE_END}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Date Dimension

# COMMAND ----------

# Generate date sequence
df_dates = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{DATE_START}'), 
        to_date('{DATE_END}'), 
        interval 1 day
    )) AS date
""")

# Add date attributes
df_dim_date = df_dates \
    .withColumn("date_sk", date_format(col("date"), "yyyyMMdd").cast(IntegerType())) \
    .withColumn("year", year(col("date"))) \
    .withColumn("quarter", quarter(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .withColumn("week", weekofyear(col("date"))) \
    .withColumn("day_of_month", dayofmonth(col("date"))) \
    .withColumn("day_of_week", dayofweek(col("date"))) \
    .withColumn("year_month", date_format(col("date"), "yyyy-MM")) \
    .withColumn("year_quarter", 
        expr("concat(year(date), '-Q', quarter(date))")) \
    .withColumn("month_name", date_format(col("date"), "MMMM")) \
    .withColumn("month_name_short", date_format(col("date"), "MMM")) \
    .withColumn("day_name", date_format(col("date"), "EEEE")) \
    .withColumn("day_name_short", date_format(col("date"), "EEE")) \
    .withColumn("is_weekend", 
        when(dayofweek(col("date")).isin(1, 7), lit(True)).otherwise(lit(False))) \
    .withColumn("is_month_end", 
        when(col("date") == last_day(col("date")), lit(True)).otherwise(lit(False))) \
    .withColumn("is_month_start",
        when(dayofmonth(col("date")) == 1, lit(True)).otherwise(lit(False))) \
    .withColumn("fiscal_year",  # Assuming fiscal year = calendar year
        year(col("date"))) \
    .withColumn("fiscal_quarter",
        quarter(col("date")))

# Select final columns in order
df_dim_date = df_dim_date.select(
    "date_sk",
    "date",
    "year",
    "quarter",
    "month",
    "week",
    "day_of_month",
    "day_of_week",
    "year_month",
    "year_quarter",
    "month_name",
    "month_name_short",
    "day_name",
    "day_name_short",
    "is_weekend",
    "is_month_end",
    "is_month_start",
    "fiscal_year",
    "fiscal_quarter"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Gold Layer

# COMMAND ----------

# Write to Delta (single write operation)
df_dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_DIM_DATE)

print(f"✅ Written to {GOLD_DIM_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validation (Zero-Compute)

# COMMAND ----------

# Validate using Delta History (no DataFrame scan)
history = spark.sql(f"DESCRIBE HISTORY {GOLD_DIM_DATE} LIMIT 1").first()
metrics = history["operationMetrics"]

print("=" * 50)
print("GOLD_DIM_DATE VALIDATION")
print("=" * 50)
print(f"Operation: {history['operation']}")
print(f"Timestamp: {history['timestamp']}")
print(f"Rows Written: {metrics.get('numOutputRows', 'N/A')}")
print(f"Files Written: {metrics.get('numFiles', 'N/A')}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Schema Documentation
# MAGIC 
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | `date_sk` | INT | Surrogate key (YYYYMMDD format) |
# MAGIC | `date` | DATE | Calendar date |
# MAGIC | `year` | INT | Calendar year |
# MAGIC | `quarter` | INT | Quarter (1-4) |
# MAGIC | `month` | INT | Month (1-12) |
# MAGIC | `week` | INT | ISO week of year |
# MAGIC | `day_of_month` | INT | Day of month (1-31) |
# MAGIC | `day_of_week` | INT | Day of week (1=Sunday, 7=Saturday) |
# MAGIC | `year_month` | STRING | Year-Month (YYYY-MM) |
# MAGIC | `year_quarter` | STRING | Year-Quarter (YYYY-Qn) |
# MAGIC | `month_name` | STRING | Full month name |
# MAGIC | `month_name_short` | STRING | Abbreviated month name |
# MAGIC | `day_name` | STRING | Full day name |
# MAGIC | `day_name_short` | STRING | Abbreviated day name |
# MAGIC | `is_weekend` | BOOLEAN | True if Saturday or Sunday |
# MAGIC | `is_month_end` | BOOLEAN | True if last day of month |
# MAGIC | `is_month_start` | BOOLEAN | True if first day of month |
# MAGIC | `fiscal_year` | INT | Fiscal year (= calendar year) |
# MAGIC | `fiscal_quarter` | INT | Fiscal quarter |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Author:** Diego Mayorga | **Date:** 2026-01-06
