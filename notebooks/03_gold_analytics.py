# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Market Visibility Analytics
# MAGIC 
# MAGIC **Purpose:** Transform Silver curated data into business-consumable analytics through dimensional modeling (star schema) and KPI materialization.
# MAGIC 
# MAGIC **Architecture:** Medallion (Bronze → Silver → Gold)  
# MAGIC **Designer:** Senior Analytics Engineer  
# MAGIC **Date:** 2025-01-06  
# MAGIC **Project:** BI Market Visibility
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook implements a **star schema** with:
# MAGIC 
# MAGIC ### Dimensions (Conformed)
# MAGIC - `gold_dim_date`: Calendar dimension (immutable)
# MAGIC - `gold_dim_product`: Product SCD Type 2 (tracks changes)
# MAGIC - `gold_dim_pdv`: Point-of-Sale SCD Type 2 (tracks location/channel changes)
# MAGIC 
# MAGIC ### Facts (Append-Only, Incremental)
# MAGIC - `gold_fact_sell_in`: Daily sell-in by product × PDV
# MAGIC - `gold_fact_price_audit`: Daily price observations
# MAGIC - `gold_fact_stock`: Stock levels & days-of-supply
# MAGIC 
# MAGIC ### KPI Tables (Pre-Aggregated for Power BI)
# MAGIC - `gold_kpi_market_visibility_daily`: Consolidated daily metrics
# MAGIC - `gold_kpi_market_share`: Market share & penetration analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Design Principles
# MAGIC 
# MAGIC 1. **Serverless-First:** No `cache()`, `persist()`, or unnecessary `count()`
# MAGIC 2. **Append-Only Facts:** All facts immutable, insert-only (no deletes)
# MAGIC 3. **Incremental Refresh:** Partition-based upserts (DPO for Databricks)
# MAGIC 4. **One Write Per Dataset:** Atomic operations prevent inconsistency
# MAGIC 5. **Pre-Computed KPIs:** All logic in Gold (minimal DAX for Power BI)
# MAGIC 6. **Deterministic Keys:** Surrogate keys via hash functions (reproducible)
# MAGIC 7. **Full Lineage:** `_metadata_*` columns for auditability
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date, 
    sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    when, coalesce, row_number, lag,
    to_date, date_format, year, month, quarter, week_of_year, dayofweek, dayofmonth,
    datediff, days_between, date_add, date_sub,
    round as spark_round, md5, concat_ws, sha2,
    row_number, dense_rank,
    Window
)
from datetime import datetime, timedelta
import sys

# Initialize Spark
spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.adaptive.enabled = true")
spark.sql("SET spark.sql.adaptive.skewJoin.enabled = true")

# Configuration
GOLD_SCHEMA = "default"
SILVER_SCHEMA = "default"
BATCH_ID = datetime.now().strftime("%Y%m%d_%H%M%S") + "_gold_analytics"
EXECUTION_TIMESTAMP = current_timestamp()

# Import Gold utilities
sys.path.insert(0, "/Workspace/Users/diego.mayorgacapera@gmail.com/.bundle/BI_Market_Visibility/dev/files")
from src.utils.gold_layer_utils import (
    generate_dense_surrogate_key,
    generate_surrogate_key_udf,
    detect_scd2_changes,
    aggregate_sell_in,
    aggregate_price_audit,
    calculate_market_visibility_kpis,
    calculate_market_share_kpis,
    validate_surrogate_key_uniqueness,
    validate_scd2_current_only,
    validate_referential_integrity,
    validate_kpi_consistency,
)

print(f"✅ Gold Layer Initialization Complete")
print(f"   Batch ID: {BATCH_ID}")
print(f"   Timestamp: {EXECUTION_TIMESTAMP}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Silver Layer Sources

# COMMAND ----------

# Read Silver tables (append-only, immutable)
silver_master_pdv = spark.read.table(f"{SILVER_SCHEMA}.silver_master_pdv")
silver_master_products = spark.read.table(f"{SILVER_SCHEMA}.silver_master_products")
silver_price_audit = spark.read.table(f"{SILVER_SCHEMA}.silver_price_audit")
silver_sell_in = spark.read.table(f"{SILVER_SCHEMA}.silver_sell_in")

print(f"✅ Silver Layer Loaded")
print(f"   silver_master_pdv: {silver_master_pdv.count():,} rows")
print(f"   silver_master_products: {silver_master_products.count():,} rows")
print(f"   silver_price_audit: {silver_price_audit.count():,} rows (partitioned by year_month)")
print(f"   silver_sell_in: {silver_sell_in.count():,} rows (partitioned by year)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold Dimension 1: gold_dim_date
# MAGIC 
# MAGIC **Purpose:** Calendar dimension (10 years: 2020-2030)  
# MAGIC **Grain:** 1 row per day  
# MAGIC **Partitioning:** None (small, ~3KB)  
# MAGIC **Refresh:** One-time (idempotent: skip if already exists)

# COMMAND ----------

def create_date_dimension(start_year: int = 2020, end_year: int = 2030) -> "DataFrame":
    """
    Create calendar dimension for 10-year span.
    
    Includes:
    - Year, Quarter, Month, Week, Day of Month, Day of Week
    - Is-Month-End and Is-Weekend flags
    - YYYYMM partition key
    """
    # Generate date range
    date_range_df = spark.sql(f"""
        WITH date_series AS (
            SELECT sequence(to_date('{start_year}-01-01'), to_date('{end_year}-12-31'), interval 1 day) as dates
        )
        SELECT explode(dates) as date_val FROM date_series
    """)
    
    dim_date = (
        date_range_df
        .withColumnRenamed("date_val", "date")
        .withColumn("date_sk", row_number().over(Window.orderBy("date")))
        .withColumn("year", year(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("week", week_of_year(col("date")))
        .withColumn("day_of_month", dayofmonth(col("date")))
        .withColumn("day_of_week", dayofweek(col("date")) - 1)  # 0=Monday
        .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
        .withColumn(
            "is_month_end",
            col("date") == last_day(col("date"))
        )
        .withColumn("is_weekend", col("day_of_week").isin(5, 6))
        .withColumn(
            "is_quarter_end",
            col("date") == last_day(col("date")) & col("month").isin(3, 6, 9, 12)
        )
        .withColumn("_metadata_created_date", current_date())
        .select(
            "date_sk", "date", "year", "quarter", "month", "week",
            "day_of_month", "day_of_week", "year_month",
            "is_month_end", "is_weekend", "is_quarter_end",
            "_metadata_created_date"
        )
    )
    
    return dim_date

# Create or skip
try:
    existing_date_dim = spark.read.table(f"{GOLD_SCHEMA}.gold_dim_date")
    print(f"⏭️  gold_dim_date already exists ({existing_date_dim.count():,} rows), skipping creation")
    gold_dim_date = existing_date_dim
except:
    print(f"📝 Creating gold_dim_date...")
    gold_dim_date = create_date_dimension()
    
    # Write (one-time, skip merge logic)
    (
        gold_dim_date
        .write
        .mode("overwrite")
        .option("mergeSchema", "false")
        .saveAsTable(f"{GOLD_SCHEMA}.gold_dim_date", path=f"/user/hive/warehouse/gold_dim_date")
    )
    
    # Validate
    assert validate_surrogate_key_uniqueness(gold_dim_date, "date_sk", "gold_dim_date")
    print(f"✅ gold_dim_date created ({gold_dim_date.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Dimension 2: gold_dim_product (SCD Type 2)
# MAGIC 
# MAGIC **Purpose:** Product master with historical tracking  
# MAGIC **Grain:** 1 row per product version  
# MAGIC **Refresh:** Incremental (daily SCD2 check)  
# MAGIC **Strategy:** MERGE statement (upsert on product_sku + hash)

# COMMAND ----------

def prepare_product_dimension(silver_products_df: "DataFrame") -> "DataFrame":
    """
    Prepare product dimension with SCD Type 2 logic.
    
    Adds surrogate keys, hash for change detection, and SCD flags.
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import LongType
    
    # Generate product SK via UDF
    product_sk_udf = generate_surrogate_key_udf("product")
    
    dim_product = (
        silver_products_df
        .select(
            col("product_sku"),
            col("product_name"),
            col("brand"),
            col("segment"),
            col("category"),
            col("sub_category"),
        )
        .distinct()
        .withColumn("product_sk", product_sk_udf(col("product_sku")))
        .withColumn(
            "attribute_hash",
            sha2(concat_ws("|", col("product_name"), col("brand"), col("segment"), col("category")), 256)
        )
        .withColumn("valid_from", current_date())
        .withColumn("valid_to", lit(None))
        .withColumn("is_current", lit(True))
        .withColumn("_metadata_source_batch_id", lit(BATCH_ID))
        .withColumn("_metadata_created_date", current_date())
        .select(
            "product_sk", "product_sku", "product_name", "brand", "segment",
            "category", "sub_category",
            "valid_from", "valid_to", "is_current",
            "_metadata_source_batch_id", "_metadata_created_date"
        )
    )
    
    return dim_product

print(f"📝 Preparing gold_dim_product (SCD Type 2)...")
gold_dim_product_new = prepare_product_dimension(silver_master_products)

# MERGE or CREATE
try:
    existing_product_dim = spark.read.table(f"{GOLD_SCHEMA}.gold_dim_product")
    
    # MERGE: Update old versions if hash changed, insert new
    spark.sql(f"""
        MERGE INTO {GOLD_SCHEMA}.gold_dim_product tgt
        USING (
            SELECT * FROM gold_dim_product_new
        ) src
        ON tgt.product_sku = src.product_sku AND tgt.is_current = True
        WHEN MATCHED AND tgt.attribute_hash <> src.attribute_hash THEN
            UPDATE SET tgt.valid_to = date_sub(current_date(), 1), tgt.is_current = False
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    print(f"✅ gold_dim_product merged")
    
except:
    # First time: create table
    print(f"📝 Creating gold_dim_product...")
    (
        gold_dim_product_new
        .write
        .mode("overwrite")
        .option("mergeSchema", "false")
        .saveAsTable(f"{GOLD_SCHEMA}.gold_dim_product", path=f"/user/hive/warehouse/gold_dim_product")
    )
    print(f"✅ gold_dim_product created ({gold_dim_product_new.count():,} rows)")

gold_dim_product = spark.read.table(f"{GOLD_SCHEMA}.gold_dim_product")
assert validate_scd2_current_only(gold_dim_product, "product_sku", "gold_dim_product")
assert validate_surrogate_key_uniqueness(gold_dim_product.filter(col("is_current") == True), "product_sku", "gold_dim_product current versions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold Dimension 3: gold_dim_pdv (SCD Type 2)
# MAGIC 
# MAGIC **Purpose:** Point-of-Sale master with location & channel history  
# MAGIC **Grain:** 1 row per PDV version  
# MAGIC **Refresh:** Incremental (daily SCD2 check)  
# MAGIC **Strategy:** MERGE statement

# COMMAND ----------

def prepare_pdv_dimension(silver_pdv_df: "DataFrame") -> "DataFrame":
    """
    Prepare PDV dimension with SCD Type 2 logic.
    
    Adds surrogate keys, hash for change detection, and SCD flags.
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import LongType
    
    pdv_sk_udf = generate_surrogate_key_udf("pdv")
    
    dim_pdv = (
        silver_pdv_df
        .select(
            col("pdv_code"),
            col("pdv_name"),
            col("channel"),
            col("sub_channel"),
            col("chain"),
            col("region"),
            col("city"),
            col("parish"),
            col("format"),
            col("latitude"),
            col("longitude"),
            col("sales_rep"),
        )
        .distinct()
        .withColumn("pdv_sk", pdv_sk_udf(col("pdv_code")))
        .withColumn(
            "attribute_hash",
            sha2(concat_ws("|", col("pdv_name"), col("channel"), col("region"), col("city")), 256)
        )
        .withColumn("valid_from", current_date())
        .withColumn("valid_to", lit(None))
        .withColumn("is_current", lit(True))
        .withColumn("_metadata_source_batch_id", lit(BATCH_ID))
        .withColumn("_metadata_created_date", current_date())
        .select(
            "pdv_sk", "pdv_code", "pdv_name", "channel", "sub_channel",
            "chain", "region", "city", "parish", "format",
            "latitude", "longitude", "sales_rep",
            "valid_from", "valid_to", "is_current",
            "_metadata_source_batch_id", "_metadata_created_date"
        )
    )
    
    return dim_pdv

print(f"📝 Preparing gold_dim_pdv (SCD Type 2)...")
gold_dim_pdv_new = prepare_pdv_dimension(silver_master_pdv)

# MERGE or CREATE
try:
    existing_pdv_dim = spark.read.table(f"{GOLD_SCHEMA}.gold_dim_pdv")
    
    # MERGE
    spark.sql(f"""
        MERGE INTO {GOLD_SCHEMA}.gold_dim_pdv tgt
        USING (
            SELECT * FROM gold_dim_pdv_new
        ) src
        ON tgt.pdv_code = src.pdv_code AND tgt.is_current = True
        WHEN MATCHED AND tgt.attribute_hash <> src.attribute_hash THEN
            UPDATE SET tgt.valid_to = date_sub(current_date(), 1), tgt.is_current = False
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    print(f"✅ gold_dim_pdv merged")
    
except:
    print(f"📝 Creating gold_dim_pdv...")
    (
        gold_dim_pdv_new
        .write
        .mode("overwrite")
        .option("mergeSchema", "false")
        .saveAsTable(f"{GOLD_SCHEMA}.gold_dim_pdv", path=f"/user/hive/warehouse/gold_dim_pdv")
    )
    print(f"✅ gold_dim_pdv created ({gold_dim_pdv_new.count():,} rows)")

gold_dim_pdv = spark.read.table(f"{GOLD_SCHEMA}.gold_dim_pdv")
assert validate_scd2_current_only(gold_dim_pdv, "pdv_code", "gold_dim_pdv")
assert validate_surrogate_key_uniqueness(gold_dim_pdv.filter(col("is_current") == True), "pdv_code", "gold_dim_pdv current versions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gold Fact 1: gold_fact_sell_in
# MAGIC 
# MAGIC **Purpose:** Daily sell-in by product × PDV  
# MAGIC **Grain:** (date, product, pdv)  
# MAGIC **Partitioning:** By `year` (DPO strategy)  
# MAGIC **Refresh:** Incremental per year  
# MAGIC **Mode:** Dynamic Partition Overwrite (fast, idempotent)

# COMMAND ----------

def prepare_sell_in_facts(
    silver_sell_in_df: "DataFrame",
    gold_dim_date_df: "DataFrame",
    gold_dim_product_df: "DataFrame",
    gold_dim_pdv_df: "DataFrame",
    batch_id: str
) -> "DataFrame":
    """
    Transform silver_sell_in into gold_fact_sell_in.
    
    Aggregates to daily grain, adds surrogate keys, and joins dimensions.
    """
    
    # Aggregate to daily grain (date, product_sku, pdv_code)
    fact_agg = (
        silver_sell_in_df
        .groupBy("sale_date", "product_sku", "pdv_code")
        .agg(
            spark_sum("quantity").alias("quantity_sell_in"),
            spark_sum("value").alias("value_sell_in"),
            count("*").alias("transactions_count"),
            spark_max("is_complete_transaction").alias("is_complete_transaction")
        )
        .withColumn("year", year(col("sale_date")))
    )
    
    # Derive unit price
    fact_with_price = (
        fact_agg
        .withColumn(
            "unit_price_sell_in",
            when(col("quantity_sell_in") > 0, col("value_sell_in") / col("quantity_sell_in"))
            .otherwise(None)
        )
    )
    
    # Join with dimensions to get surrogate keys
    fact_with_dims = (
        fact_with_price
        .join(
            gold_dim_date_df.select("date_sk", "date"),
            fact_with_price.sale_date == gold_dim_date_df.date,
            how="inner"
        )
        .join(
            gold_dim_product_df
            .filter(col("is_current") == True)
            .select("product_sk", "product_sku"),
            on="product_sku",
            how="inner"
        )
        .join(
            gold_dim_pdv_df
            .filter(col("is_current") == True)
            .select("pdv_sk", "pdv_code"),
            on="pdv_code",
            how="inner"
        )
    )
    
    # Add metadata and surrogate key
    fact_sk_udf = generate_surrogate_key_udf("fact_sell_in")
    
    fact_final = (
        fact_with_dims
        .withColumn("fact_sell_in_id", fact_sk_udf(concat_ws("_", col("date_sk"), col("product_sk"), col("pdv_sk"))))
        .withColumn("_metadata_silver_batch_id", lit(batch_id))
        .withColumn("_metadata_created_timestamp", current_timestamp())
        .select(
            "fact_sell_in_id",
            "date_sk", "product_sk", "pdv_sk",
            "quantity_sell_in", "value_sell_in", "unit_price_sell_in",
            "transactions_count", "is_complete_transaction",
            "year",
            "_metadata_silver_batch_id", "_metadata_created_timestamp"
        )
    )
    
    return fact_final

print(f"📝 Preparing gold_fact_sell_in...")
gold_fact_sell_in_new = prepare_sell_in_facts(
    silver_sell_in, gold_dim_date, gold_dim_product, gold_dim_pdv, BATCH_ID
)

# Dynamic Partition Overwrite (DPO) by year
try:
    (
        gold_fact_sell_in_new
        .write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("year")
        .option("mergeSchema", "false")
        .saveAsTable(f"{GOLD_SCHEMA}.gold_fact_sell_in", path=f"/user/hive/warehouse/gold_fact_sell_in")
    )
    print(f"✅ gold_fact_sell_in created/updated ({gold_fact_sell_in_new.count():,} rows)")
    
except Exception as e:
    print(f"⚠️  gold_fact_sell_in write failed: {e}")
    raise

gold_fact_sell_in = spark.read.table(f"{GOLD_SCHEMA}.gold_fact_sell_in")
assert validate_referential_integrity(gold_fact_sell_in, gold_dim_date, "date_sk", "date_sk", "gold_fact_sell_in → gold_dim_date")
assert validate_referential_integrity(gold_fact_sell_in, gold_dim_product.filter(col("is_current")==True), "product_sk", "product_sk", "gold_fact_sell_in → gold_dim_product")
assert validate_referential_integrity(gold_fact_sell_in, gold_dim_pdv.filter(col("is_current")==True), "pdv_sk", "pdv_sk", "gold_fact_sell_in → gold_dim_pdv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold Fact 2: gold_fact_price_audit
# MAGIC 
# MAGIC **Purpose:** Daily price observations with market comparison  
# MAGIC **Grain:** (date, product, pdv)  
# MAGIC **Partitioning:** By `year_month`  
# MAGIC **Refresh:** Incremental per month  
# MAGIC **Metrics:** Price variance, index, outlier detection

# COMMAND ----------

def prepare_price_audit_facts(
    silver_price_audit_df: "DataFrame",
    gold_dim_date_df: "DataFrame",
    gold_dim_product_df: "DataFrame",
    gold_dim_pdv_df: "DataFrame",
    batch_id: str
) -> "DataFrame":
    """
    Transform silver_price_audit into gold_fact_price_audit.
    
    Calculates market average, variance, index, and outlier flags.
    """
    
    # Filter nulls
    price_clean = silver_price_audit_df.filter(col("observed_price").isNotNull())
    
    # Calculate market average per (date, product)
    window_market = Window.partitionBy("audit_date", "product_sku")
    
    price_with_market = (
        price_clean
        .withColumn("avg_market_price", avg(col("observed_price")).over(window_market))
    )
    
    # Calculate variance and index
    price_with_metrics = (
        price_with_market
        .withColumn(
            "price_variance_pct",
            spark_round(
                ((col("observed_price") - col("avg_market_price")) / col("avg_market_price")) * 100,
                2
            )
        )
        .withColumn(
            "price_index",
            spark_round(
                (col("observed_price") / col("avg_market_price")) * 100,
                4
            )
        )
        .withColumn(
            "is_outlier_price",
            abs(col("price_variance_pct")) > 10
        )
        .withColumn("year_month", col("audit_date").substr(1, 7))
    )
    
    # Aggregate to daily grain (deduplicate if multiple observations per day)
    fact_agg = (
        price_with_metrics
        .groupBy("audit_date", "product_sku", "pdv_code", "year_month")
        .agg(
            avg(col("observed_price")).alias("observed_price"),
            spark_max(col("avg_market_price")).alias("avg_market_price"),
            spark_round(avg(col("price_variance_pct")), 2).alias("price_variance_pct"),
            spark_round(avg(col("price_index")), 4).alias("price_index"),
            spark_max(col("is_outlier_price")).alias("is_outlier_price")
        )
    )
    
    # Join dimensions
    fact_with_dims = (
        fact_agg
        .join(
            gold_dim_date_df.select("date_sk", "date"),
            fact_agg.audit_date == gold_dim_date_df.date,
            how="inner"
        )
        .join(
            gold_dim_product_df
            .filter(col("is_current") == True)
            .select("product_sk", "product_sku"),
            on="product_sku",
            how="inner"
        )
        .join(
            gold_dim_pdv_df
            .filter(col("is_current") == True)
            .select("pdv_sk", "pdv_code"),
            on="pdv_code",
            how="inner"
        )
    )
    
    # Add surrogate key and metadata
    fact_sk_udf = generate_surrogate_key_udf("fact_price_audit")
    
    fact_final = (
        fact_with_dims
        .withColumn("fact_price_audit_id", fact_sk_udf(concat_ws("_", col("date_sk"), col("product_sk"), col("pdv_sk"))))
        .withColumn("_metadata_silver_batch_id", lit(batch_id))
        .withColumn("_metadata_created_timestamp", current_timestamp())
        .select(
            "fact_price_audit_id",
            "date_sk", "product_sk", "pdv_sk",
            "observed_price", "avg_market_price",
            "price_variance_pct", "price_index", "is_outlier_price",
            "year_month",
            "_metadata_silver_batch_id", "_metadata_created_timestamp"
        )
    )
    
    return fact_final

print(f"📝 Preparing gold_fact_price_audit...")
gold_fact_price_audit_new = prepare_price_audit_facts(
    silver_price_audit, gold_dim_date, gold_dim_product, gold_dim_pdv, BATCH_ID
)

# Dynamic Partition Overwrite by year_month
try:
    (
        gold_fact_price_audit_new
        .write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("year_month")
        .option("mergeSchema", "false")
        .saveAsTable(f"{GOLD_SCHEMA}.gold_fact_price_audit", path=f"/user/hive/warehouse/gold_fact_price_audit")
    )
    print(f"✅ gold_fact_price_audit created/updated ({gold_fact_price_audit_new.count():,} rows)")
    
except Exception as e:
    print(f"⚠️  gold_fact_price_audit write failed: {e}")
    raise

gold_fact_price_audit = spark.read.table(f"{GOLD_SCHEMA}.gold_fact_price_audit")
assert validate_referential_integrity(gold_fact_price_audit, gold_dim_date, "date_sk", "date_sk", "gold_fact_price_audit → gold_dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Gold Fact 3: gold_fact_stock (Estimated via Sell-In Proxy)
# MAGIC 
# MAGIC **⚠️ IMPORTANT:** No direct stock source available.  
# MAGIC **Strategy:** Estimate closing stock from sell-in movements with assumptions.
# MAGIC 
# MAGIC **Assumptions:**
# MAGIC 1. Sell-In ≈ Sell-Out (within 1 day)
# MAGIC 2. Replenishment cycles: Weekly on Fridays
# MAGIC 3. Safety stock: 10 days minimum
# MAGIC 4. Initial stock: 30-day supply based on avg sell-in
# MAGIC 
# MAGIC **Limitations:**
# MAGIC - Unsuitable for slow-movers or seasonal products
# MAGIC - Requires monthly audits for validation
# MAGIC - Suitable for FMCG (fast-moving consumer goods)

# COMMAND ----------

def prepare_stock_facts(
    gold_fact_sell_in_df: "DataFrame",
    gold_dim_date_df: "DataFrame",
    gold_dim_product_df: "DataFrame",
    gold_dim_pdv_df: "DataFrame",
    batch_id: str
) -> "DataFrame":
    """
    Estimate stock levels from sell-in proxy model.
    
    Assumptions:
    - Sell-in ≈ daily depletion
    - Weekly replenishment (Fridays)
    - Initial stock: 30-day average sell-in
    - Safety stock: 10 days minimum
    """
    
    # Join with dimensions for date context
    fact_with_date = (
        gold_fact_sell_in_df
        .join(gold_dim_date_df, on="date_sk", how="inner")
    )
    
    # Window: partitioned by (product, pdv), ordered by date
    window_product_pdv = Window.partitionBy("product_sk", "pdv_sk").orderBy("date_sk")
    window_30d = Window.partitionBy("product_sk", "pdv_sk").orderBy("date_sk").rangeBetween(-30, 0)
    
    # Calculate 30-day rolling average for initial stock estimation
    fact_with_rolling = (
        fact_with_date
        .withColumn("avg_daily_sell_in_30d", avg("quantity_sell_in").over(window_30d))
    )
    
    # Estimate opening stock on day 1, then accumulate
    fact_with_stock = (
        fact_with_rolling
        .withColumn(
            "estimated_replenishment",
            when(col("day_of_week") == 4, col("avg_daily_sell_in_30d") * 7)  # Friday: weekly rep
            .otherwise(0)
        )
        .withColumn(
            "opening_stock_units",
            when(row_number().over(window_product_pdv) == 1, col("avg_daily_sell_in_30d") * 30)  # Day 1: 30-day stock
            .otherwise(
                lag("closing_stock_units", 1).over(window_product_pdv) + col("estimated_replenishment") - col("quantity_sell_in")
            )
        )
        .withColumn(
            "closing_stock_units",
            when(col("opening_stock_units") - col("quantity_sell_in") < 0, 0)
            .otherwise(col("opening_stock_units") - col("quantity_sell_in"))
        )
        .withColumn(
            "stock_movement_units",
            col("closing_stock_units") - col("opening_stock_units")
        )
        .withColumn(
            "stock_days_available",
            when(col("avg_daily_sell_in_30d") > 0, col("closing_stock_units") / col("avg_daily_sell_in_30d"))
            .otherwise(0)
        )
    )
    
    # Stock health flags
    fact_with_health = (
        fact_with_stock
        .withColumn("stock_out_flag", col("closing_stock_units") == 0)
        .withColumn("overstock_flag", col("stock_days_available") > 30)
        .withColumn(
            "stock_health_score",
            when(
                (col("stock_days_available") >= 5) & (col("stock_days_available") <= 30),
                100
            )
            .when(
                (col("stock_days_available") >= 3) & (col("stock_days_available") <= 40),
                75
            )
            .otherwise(
                spark_round(
                    spark_max(
                        lit(0),
                        50 - (abs(col("stock_days_available") - 15) / 15 * 25)
                    ),
                    0
                )
            )
        )
    )
    
    # Add surrogate key and metadata
    fact_sk_udf = generate_surrogate_key_udf("fact_stock")
    
    fact_final = (
        fact_with_health
        .withColumn("fact_stock_id", fact_sk_udf(concat_ws("_", col("date_sk"), col("product_sk"), col("pdv_sk"))))
        .withColumn("year", year(col("date")))
        .withColumn("_metadata_estimation_note", lit("Estimated via sell-in proxy (daily movement assumption)"))
        .withColumn("_metadata_source_batch_id", lit(batch_id))
        .withColumn("_metadata_created_timestamp", current_timestamp())
        .select(
            "fact_stock_id",
            "date_sk", "product_sk", "pdv_sk",
            "opening_stock_units", "closing_stock_units", "stock_movement_units",
            "stock_days_available", "stock_out_flag", "overstock_flag", "stock_health_score",
            "year",
            "_metadata_estimation_note", "_metadata_source_batch_id", "_metadata_created_timestamp"
        )
    )
    
    return fact_final

print(f"📝 Preparing gold_fact_stock (estimated)...")
gold_fact_stock_new = prepare_stock_facts(
    gold_fact_sell_in_new, gold_dim_date, gold_dim_product, gold_dim_pdv, BATCH_ID
)

# Dynamic Partition Overwrite by year
try:
    (
        gold_fact_stock_new
        .write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("year")
        .option("mergeSchema", "false")
        .saveAsTable(f"{GOLD_SCHEMA}.gold_fact_stock", path=f"/user/hive/warehouse/gold_fact_stock")
    )
    print(f"✅ gold_fact_stock created/updated ({gold_fact_stock_new.count():,} rows)")
    print(f"   ⚠️  REMINDER: Stock is estimated via sell-in proxy. Monthly audits recommended.")
    
except Exception as e:
    print(f"⚠️  gold_fact_stock write failed: {e}")
    raise

gold_fact_stock = spark.read.table(f"{GOLD_SCHEMA}.gold_fact_stock")
assert validate_referential_integrity(gold_fact_stock, gold_dim_date, "date_sk", "date_sk", "gold_fact_stock → gold_dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Gold KPI Table 1: gold_kpi_market_visibility_daily
# MAGIC 
# MAGIC **Purpose:** Consolidated daily metrics for Power BI  
# MAGIC **Grain:** (date, product, pdv)  
# MAGIC **Partitioning:** By `year`  
# MAGIC **Refresh:** Incremental per date  
# MAGIC **Metrics:** Sell-In, Price, Stock, and derived KPIs

# COMMAND ----------

def prepare_market_visibility_kpi(
    gold_fact_sell_in_df: "DataFrame",
    gold_fact_price_audit_df: "DataFrame",
    gold_fact_stock_df: "DataFrame",
    batch_id: str
) -> "DataFrame":
    """
    Materialize consolidated daily market visibility KPIs.
    
    Joins all facts and pre-calculates rolling metrics, efficiency scores.
    """
    
    # Join all facts on (date_sk, product_sk, pdv_sk)
    kpi_base = (
        gold_fact_sell_in_df
        .join(
            gold_fact_price_audit_df.select(
                "date_sk", "product_sk", "pdv_sk",
                "observed_price", "avg_market_price",
                "price_variance_pct", "price_index"
            ),
            on=["date_sk", "product_sk", "pdv_sk"],
            how="left"
        )
        .join(
            gold_fact_stock_df.select(
                "date_sk", "product_sk", "pdv_sk",
                "closing_stock_units", "stock_days_available", "stock_out_flag", "stock_health_score"
            ),
            on=["date_sk", "product_sk", "pdv_sk"],
            how="left"
        )
    )
    
    # Window: 30-day rolling
    window_30d = Window.partitionBy("product_sk", "pdv_sk").orderBy("date_sk").rangeBetween(-30, 0)
    
    # Calculate rolling metrics
    kpi_with_rolling = (
        kpi_base
        .withColumn("avg_sell_in_30d", avg("quantity_sell_in").over(window_30d))
    )
    
    # Price Competitiveness Index
    kpi_with_price_index = (
        kpi_with_rolling
        .withColumn(
            "price_competitiveness_index",
            coalesce(col("price_index"), lit(100))  # Default to 100 if no price data
        )
    )
    
    # Sell-In / Sell-Out Ratio
    kpi_with_ratio = (
        kpi_with_price_index
        .withColumn(
            "sell_in_sell_out_ratio",
            when(col("avg_sell_in_30d") > 0, col("quantity_sell_in") / col("avg_sell_in_30d"))
            .otherwise(0)
        )
    )
    
    # Availability Rate (% of days in 30-day window with stock > 0)
    kpi_with_availability = (
        kpi_with_ratio
        .withColumn(
            "days_with_stock_30d",
            spark_sum(when(col("stock_days_available") > 0, 1).otherwise(0)).over(window_30d)
        )
        .withColumn(
            "availability_rate_pct",
            spark_round((col("days_with_stock_30d") / 30) * 100, 2)
        )
    )
    
    # Lost Sales Estimation
    kpi_with_lost_sales = (
        kpi_with_availability
        .withColumn(
            "lost_sales_estimated_units",
            when(col("stock_out_flag") == True, col("avg_sell_in_30d"))
            .otherwise(0)
        )
    )
    
    # Efficiency Score (pre-computed)
    kpi_final = (
        kpi_with_lost_sales
        .withColumn(
            "efficiency_score",
            when(
                (col("stock_days_available").between(5, 30)) & 
                (col("price_competitiveness_index").between(90, 110)),
                100
            )
            .when(
                (col("stock_days_available").between(3, 40)) & 
                (col("price_competitiveness_index").between(80, 120)),
                75
            )
            .otherwise(
                spark_round(
                    spark_max(
                        lit(0),
                        50 - (abs(col("stock_days_available") - 15) / 15 * 25) - 
                             (abs(col("price_competitiveness_index") - 100) / 100 * 25)
                    ),
                    0
                )
            )
        )
        .withColumn("_metadata_created_timestamp", current_timestamp())
        .select(
            "date_sk", "product_sk", "pdv_sk",
            "quantity_sell_in", "value_sell_in", "unit_price_sell_in",
            "observed_price", "avg_market_price", "price_competitiveness_index",
            "closing_stock_units", "stock_days_available", "stock_out_flag",
            "sell_in_sell_out_ratio",
            "availability_rate_pct",
            "lost_sales_estimated_units",
            "efficiency_score",
            "year",
            "_metadata_created_timestamp"
        )
    )
    
    return kpi_final

print(f"📝 Preparing gold_kpi_market_visibility_daily...")
gold_kpi_market_visibility_new = prepare_market_visibility_kpi(
    gold_fact_sell_in, gold_fact_price_audit, gold_fact_stock, BATCH_ID
)

# Dynamic Partition Overwrite by year
try:
    (
        gold_kpi_market_visibility_new
        .write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("year")
        .option("mergeSchema", "false")
        .saveAsTable(f"{GOLD_SCHEMA}.gold_kpi_market_visibility_daily", path=f"/user/hive/warehouse/gold_kpi_market_visibility_daily")
    )
    print(f"✅ gold_kpi_market_visibility_daily created/updated ({gold_kpi_market_visibility_new.count():,} rows)")
    
except Exception as e:
    print(f"⚠️  gold_kpi_market_visibility_daily write failed: {e}")
    raise

gold_kpi_market_visibility = spark.read.table(f"{GOLD_SCHEMA}.gold_kpi_market_visibility_daily")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Gold KPI Table 2: gold_kpi_market_share
# MAGIC 
# MAGIC **Purpose:** Market share and penetration analysis  
# MAGIC **Grain:** (date, product, region, segment, brand)  
# MAGIC **Partitioning:** By `year`  
# MAGIC **Refresh:** Incremental per date

# COMMAND ----------

def prepare_market_share_kpi(
    gold_fact_sell_in_df: "DataFrame",
    gold_dim_product_df: "DataFrame",
    gold_dim_pdv_df: "DataFrame",
    batch_id: str
) -> "DataFrame":
    """
    Materialize market share KPIs aggregated by geography and product attributes.
    """
    
    # Join facts with dimensions
    kpi_base = (
        gold_fact_sell_in_df
        .join(
            gold_dim_product_df.filter(col("is_current") == True).select(
                "product_sk", "product_sku", "brand", "segment"
            ),
            on="product_sk",
            how="inner"
        )
        .join(
            gold_dim_pdv_df.filter(col("is_current") == True).select(
                "pdv_sk", "pdv_code", "region"
            ),
            on="pdv_sk",
            how="inner"
        )
    )
    
    # Aggregate by (date_sk, product_sk, region, segment, brand)
    kpi_agg = (
        kpi_base
        .groupBy("date_sk", "product_sk", "region", "segment", "brand", "year")
        .agg(
            spark_sum("quantity_sell_in").alias("sell_in_units"),
            spark_sum("value_sell_in").alias("sell_in_value"),
            count(col("pdv_sk")).alias("market_share_pdv_count")
        )
    )
    
    # Window: total by (date, region)
    window_region = Window.partitionBy("date_sk", "region")
    
    # Calculate market share percentages
    kpi_with_share = (
        kpi_agg
        .withColumn(
            "market_share_units_pct",
            spark_round(
                (col("sell_in_units") / spark_sum(col("sell_in_units")).over(window_region)) * 100,
                2
            )
        )
        .withColumn(
            "market_share_value_pct",
            spark_round(
                (col("sell_in_value") / spark_sum(col("sell_in_value")).over(window_region)) * 100,
                2
            )
        )
    )
    
    # Market penetration (% PDVs with sales)
    window_total_pdvs = Window.partitionBy("date_sk", "region")
    kpi_with_penetration = (
        kpi_with_share
        .withColumn(
            "market_penetration_pct",
            spark_round(
                (col("market_share_pdv_count") / spark_max(col("market_share_pdv_count")).over(window_total_pdvs)) * 100,
                2
            )
        )
    )
    
    # Trend (day-over-day % change)
    window_trend = Window.partitionBy("product_sk", "region").orderBy("date_sk")
    kpi_final = (
        kpi_with_penetration
        .withColumn(
            "market_share_trend_pct",
            spark_round(
                col("market_share_units_pct") - lag(col("market_share_units_pct")).over(window_trend),
                2
            )
        )
        .withColumn("_metadata_created_timestamp", current_timestamp())
        .select(
            "date_sk", "product_sk", "region", "segment", "brand",
            "sell_in_units", "sell_in_value",
            "market_share_units_pct", "market_share_value_pct",
            "market_share_pdv_count", "market_penetration_pct",
            "market_share_trend_pct",
            "year",
            "_metadata_created_timestamp"
        )
    )
    
    return kpi_final

print(f"📝 Preparing gold_kpi_market_share...")
gold_kpi_market_share_new = prepare_market_share_kpi(
    gold_fact_sell_in, gold_dim_product, gold_dim_pdv, BATCH_ID
)

# Dynamic Partition Overwrite by year
try:
    (
        gold_kpi_market_share_new
        .write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("year")
        .option("mergeSchema", "false")
        .saveAsTable(f"{GOLD_SCHEMA}.gold_kpi_market_share", path=f"/user/hive/warehouse/gold_kpi_market_share")
    )
    print(f"✅ gold_kpi_market_share created/updated ({gold_kpi_market_share_new.count():,} rows)")
    
except Exception as e:
    print(f"⚠️  gold_kpi_market_share write failed: {e}")
    raise

gold_kpi_market_share = spark.read.table(f"{GOLD_SCHEMA}.gold_kpi_market_share")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Comprehensive Data Quality Validation

# COMMAND ----------

print("=" * 80)
print("🔍 GOLD LAYER DATA QUALITY VALIDATION")
print("=" * 80)

# Test 1: Surrogate Key Uniqueness
print("\n📊 Test 1: Surrogate Key Uniqueness")
print("-" * 40)
validate_surrogate_key_uniqueness(gold_dim_date, "date_sk", "gold_dim_date")
validate_surrogate_key_uniqueness(
    gold_dim_product.filter(col("is_current") == True),
    "product_sk",
    "gold_dim_product (current versions)"
)
validate_surrogate_key_uniqueness(
    gold_dim_pdv.filter(col("is_current") == True),
    "pdv_sk",
    "gold_dim_pdv (current versions)"
)

# Test 2: SCD2 Validity
print("\n📊 Test 2: SCD Type 2 Validity (Non-Overlapping Intervals)")
print("-" * 40)
validate_scd2_current_only(gold_dim_product, "product_sku", "gold_dim_product")
validate_scd2_current_only(gold_dim_pdv, "pdv_code", "gold_dim_pdv")

# Test 3: Referential Integrity
print("\n📊 Test 3: Referential Integrity (Fact → Dimension)")
print("-" * 40)
validate_referential_integrity(
    gold_fact_sell_in, gold_dim_date, "date_sk", "date_sk",
    "gold_fact_sell_in → gold_dim_date"
)
validate_referential_integrity(
    gold_fact_sell_in, gold_dim_product.filter(col("is_current") == True), "product_sk", "product_sk",
    "gold_fact_sell_in → gold_dim_product"
)
validate_referential_integrity(
    gold_fact_sell_in, gold_dim_pdv.filter(col("is_current") == True), "pdv_sk", "pdv_sk",
    "gold_fact_sell_in → gold_dim_pdv"
)

validate_referential_integrity(
    gold_fact_price_audit, gold_dim_date, "date_sk", "date_sk",
    "gold_fact_price_audit → gold_dim_date"
)

validate_referential_integrity(
    gold_fact_stock, gold_dim_date, "date_sk", "date_sk",
    "gold_fact_stock → gold_dim_date"
)

# Test 4: Data Volume Checks
print("\n📊 Test 4: Data Volume Summary")
print("-" * 40)
print(f"gold_dim_date: {gold_dim_date.count():,} rows")
print(f"gold_dim_product (all versions): {gold_dim_product.count():,} rows")
print(f"gold_dim_product (current): {gold_dim_product.filter(col('is_current')==True).count():,} rows")
print(f"gold_dim_pdv (all versions): {gold_dim_pdv.count():,} rows")
print(f"gold_dim_pdv (current): {gold_dim_pdv.filter(col('is_current')==True).count():,} rows")
print(f"gold_fact_sell_in: {gold_fact_sell_in.count():,} rows")
print(f"gold_fact_price_audit: {gold_fact_price_audit.count():,} rows")
print(f"gold_fact_stock: {gold_fact_stock.count():,} rows")
print(f"gold_kpi_market_visibility_daily: {gold_kpi_market_visibility.count():,} rows")
print(f"gold_kpi_market_share: {gold_kpi_market_share.count():,} rows")

print("\n" + "=" * 80)
print("✅ GOLD LAYER IMPLEMENTATION COMPLETE")
print("=" * 80)
print(f"\n📅 Execution Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📦 Batch ID: {BATCH_ID}")
print(f"📍 Schema: {GOLD_SCHEMA}")

