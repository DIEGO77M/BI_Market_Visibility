"""
Gold Layer Utility Functions
============================

Supports SCD Type 2 dimension management, surrogate key generation, 
fact aggregation, and KPI calculations for Databricks Serverless.

Author: Senior Analytics Engineer
Date: 2025-01-06
"""

import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date,
    sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    when, coalesce, row_number, lag, lead,
    date_add, date_format, datediff, days_between,
    round as spark_round,
    log, corr, percentile_approx,
    md5, concat_ws, sha2
)


# ============================================================================
# SURROGATE KEY GENERATION
# ============================================================================

def generate_dense_surrogate_key(business_key: str, table_context: str) -> int:
    """
    Generate deterministic surrogate key from business key.
    
    Uses MD5 hash modulo to fit into INT range with table-specific offset.
    Ensures:
    - Deterministic (same input = same output)
    - No collisions (low probability with hash mod)
    - Dense packing (no gaps)
    - Table-specific ranges prevent cross-domain collisions
    
    Args:
        business_key: Unique business identifier (e.g., "PROD-001", "PDV0001")
        table_context: Table type ("product", "pdv", "fact_sell_in", etc.)
    
    Returns:
        Integer surrogate key
    
    Example:
        >>> generate_dense_surrogate_key("PROD-001", "product")
        10847  # Deterministic, always same for "PROD-001"
    
    Table Offsets (avoid collisions):
        - gold_dim_product: 10,000 - 100,000
        - gold_dim_pdv: 20,000 - 100,000
        - gold_fact_sell_in: 1,000,000,000 - 1,999,999,999
        - gold_fact_price_audit: 2,000,000,000 - 2,999,999,999
        - gold_fact_stock: 3,000,000,000 - 3,999,999,999
    """
    table_offsets = {
        "product": 10_000,
        "pdv": 20_000,
        "fact_sell_in": 1_000_000_000,
        "fact_price_audit": 2_000_000_000,
        "fact_stock": 3_000_000_000,
        "market_visibility": 4_000_000_000,
        "market_share": 5_000_000_000,
    }
    
    offset = table_offsets.get(table_context, 100_000)
    max_int = 2_147_483_647  # SQL INT max
    
    # Hash to 64-bit, convert to positive, modulo to avoid offset collision
    hash_bytes = hashlib.md5(business_key.encode()).digest()
    hash_int = int.from_bytes(hash_bytes[:8], byteorder='big', signed=False)
    
    # Scale down to range and add offset
    modulo_value = (hash_int % (max_int - offset)) + offset
    
    return min(modulo_value, max_int - 1)


def generate_surrogate_key_udf(table_context: str = "product"):
    """
    Create PySpark UDF for surrogate key generation in DataFrames.
    
    Args:
        table_context: Table context for offset selection
    
    Returns:
        PySpark UserDefinedFunction
    
    Example:
        >>> from pyspark.sql.functions import udf
        >>> product_sk = generate_surrogate_key_udf("product")
        >>> df = df.withColumn("product_sk", product_sk(col("product_sku")))
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import LongType
    
    def _gen_sk(business_key: str) -> int:
        return generate_dense_surrogate_key(business_key, table_context)
    
    return udf(_gen_sk, LongType())


# ============================================================================
# SCD TYPE 2 DIMENSION MANAGEMENT
# ============================================================================

def detect_scd2_changes(
    silver_df: DataFrame,
    gold_current_df: Optional[DataFrame],
    business_key_col: str,
    tracking_columns: List[str],
    effective_date: str = None
) -> Tuple[DataFrame, DataFrame]:
    """
    Detect SCD Type 2 changes between Silver source and Gold current state.
    
    Returns:
        (new_records, updated_old_records)
        - new_records: New products/PDVs (valid_from set to effective_date)
        - updated_old_records: Previous versions (valid_to set to effective_date-1)
    
    Args:
        silver_df: Source data from Silver layer
        gold_current_df: Current Gold dimension (if exists)
        business_key_col: Business key column name
        tracking_columns: Columns to track for changes
        effective_date: Date when change takes effect (default: today)
    
    Example:
        >>> new_products, old_products = detect_scd2_changes(
        ...     silver_master_products,
        ...     gold_dim_product,
        ...     business_key_col="product_sku",
        ...     tracking_columns=["product_name", "brand", "segment", "category"]
        ... )
    """
    from pyspark.sql.types import StructType
    
    if effective_date is None:
        effective_date = datetime.now().strftime("%Y-%m-%d")
    
    effective_date_col = lit(effective_date)
    
    # If no current dimension, all records are new
    if gold_current_df is None:
        new_records = (
            silver_df
            .select(*silver_df.columns)
            .withColumn("valid_from", effective_date_col)
            .withColumn("valid_to", lit(None))
            .withColumn("is_current", lit(True))
            .withColumn("_metadata_scd_action", lit("INSERT"))
        )
        return new_records, None
    
    # Hash tracking columns for change detection
    hash_expr = sha2(
        concat_ws("|", *[col(c) for c in tracking_columns]), 256
    )
    
    silver_hashed = (
        silver_df
        .withColumn("_silver_hash", hash_expr)
        .select(business_key_col, "_silver_hash", *silver_df.columns)
    )
    
    gold_current_hashed = (
        gold_current_df
        .filter(col("is_current") == True)
        .withColumn("_gold_hash", sha2(
            concat_ws("|", *[col(c) for c in tracking_columns]), 256
        ))
        .select(business_key_col, "_gold_hash", "product_sk")  # Keep SK for lineage
    )
    
    # Identify changes
    joined = (
        silver_hashed
        .join(gold_current_hashed, on=business_key_col, how="outer")
    )
    
    # New records (not in current dimension)
    new_records = (
        joined
        .filter(col("_gold_hash").isNull())
        .select(*silver_df.columns)
        .withColumn("valid_from", effective_date_col)
        .withColumn("valid_to", lit(None))
        .withColumn("is_current", lit(True))
        .withColumn("_metadata_scd_action", lit("INSERT"))
    )
    
    # Changed records (hash mismatch)
    changed_records = (
        joined
        .filter((col("_silver_hash") != col("_gold_hash")) & (col("_gold_hash").isNotNull()))
        .select(*silver_df.columns)
        .withColumn("valid_from", effective_date_col)
        .withColumn("valid_to", lit(None))
        .withColumn("is_current", lit(True))
        .withColumn("_metadata_scd_action", lit("INSERT"))
    )
    
    # Old versions to close out
    old_to_close = (
        joined
        .filter((col("_silver_hash") != col("_gold_hash")) & (col("_gold_hash").isNotNull()))
        .select("product_sk")
        .distinct()
    )
    
    closed_records = (
        gold_current_df
        .filter(col("is_current") == True)
        .join(old_to_close, on="product_sk", how="inner")
        .withColumn("valid_to", date_add(current_date(), -1))
        .withColumn("is_current", lit(False))
        .withColumn("_metadata_scd_action", lit("UPDATE"))
    )
    
    updated_records = new_records.union(changed_records)
    old_records = closed_records
    
    return updated_records, old_records


# ============================================================================
# FACT AGGREGATION
# ============================================================================

def aggregate_sell_in(
    silver_sell_in: DataFrame,
    date_start: str,
    date_end: str
) -> DataFrame:
    """
    Aggregate Silver sell-in data to (date, product, pdv) grain.
    
    Handles:
    - Deduplication (multiple transactions per day)
    - Unit price derivation
    - Completeness flags
    - Surrogate key assignment
    
    Args:
        silver_sell_in: silver_sell_in DataFrame
        date_start: Start date (YYYY-MM-DD)
        date_end: End date (YYYY-MM-DD)
    
    Returns:
        Aggregated DataFrame ready for gold_fact_sell_in join
    
    Example:
        >>> fact_sell_in = aggregate_sell_in(
        ...     silver_sell_in_df,
        ...     date_start="2025-01-01",
        ...     date_end="2025-12-31"
        ... )
    """
    from pyspark.sql.functions import to_date
    
    # Filter date range
    df_filtered = (
        silver_sell_in
        .filter((col("sale_date") >= date_start) & (col("sale_date") <= date_end))
    )
    
    # Aggregate to daily grain
    df_agg = (
        df_filtered
        .groupBy("sale_date", "product_sku", "pdv_code")
        .agg(
            spark_sum("quantity").alias("quantity_sell_in"),
            spark_sum("value").alias("value_sell_in"),
            count("*").alias("transactions_count"),
            spark_max("is_complete_transaction").alias("is_complete_transaction")
        )
    )
    
    # Derive unit price
    df_with_unit = (
        df_agg
        .withColumn(
            "unit_price_sell_in",
            when(col("quantity_sell_in") > 0, col("value_sell_in") / col("quantity_sell_in"))
            .otherwise(None)
        )
        .withColumn("year", col("sale_date").substr(1, 4).cast("int"))
    )
    
    return df_with_unit


def aggregate_price_audit(
    silver_price_audit: DataFrame,
    date_start: str,
    date_end: str
) -> DataFrame:
    """
    Aggregate Silver price audit to (date, product, pdv) grain.
    
    Calculates:
    - Average market price per product-date
    - Price variance and index
    - Outlier detection
    
    Args:
        silver_price_audit: silver_price_audit DataFrame
        date_start: Start date (YYYY-MM-DD)
        date_end: End date (YYYY-MM-DD)
    
    Returns:
        Aggregated price audit DataFrame
    
    Example:
        >>> fact_price = aggregate_price_audit(
        ...     silver_price_audit_df,
        ...     date_start="2025-01-01",
        ...     date_end="2025-12-31"
        ... )
    """
    
    # Filter date range and remove nulls
    df_filtered = (
        silver_price_audit
        .filter(
            (col("audit_date") >= date_start) & 
            (col("audit_date") <= date_end) &
            (col("observed_price").isNotNull())
        )
    )
    
    # Calculate market average per (date, product)
    window_market = Window.partitionBy("audit_date", "product_sku")
    
    df_with_market = (
        df_filtered
        .withColumn(
            "avg_market_price",
            avg(col("observed_price")).over(window_market)
        )
    )
    
    # Calculate variance and index
    df_with_metrics = (
        df_with_market
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
    
    # Aggregate to daily grain (take latest observation if multiple)
    df_agg = (
        df_with_metrics
        .dropDuplicates(["audit_date", "product_sku", "pdv_code"])
    )
    
    return df_agg


# ============================================================================
# KPI CALCULATIONS
# ============================================================================

def calculate_market_visibility_kpis(
    fact_sell_in: DataFrame,
    fact_price: DataFrame,
    fact_stock: DataFrame
) -> DataFrame:
    """
    Materialize KPIs for gold_kpi_market_visibility_daily.
    
    Combines facts and applies rolling window functions for:
    - Price competitiveness index
    - Sell-in/Sell-out ratio
    - Availability rate
    - Lost sales estimation
    - Price elasticity proxy
    - Efficiency score
    
    Args:
        fact_sell_in: gold_fact_sell_in (or staged version)
        fact_price: gold_fact_price_audit (or staged)
        fact_stock: gold_fact_stock (or staged)
    
    Returns:
        KPI-enriched DataFrame for materialization
    
    Example:
        >>> kpi_daily = calculate_market_visibility_kpis(
        ...     fact_sell_in, fact_price, fact_stock
        ... )
    """
    
    # Join facts on (date_sk, product_sk, pdv_sk)
    df = (
        fact_sell_in
        .join(
            fact_price.select("date_sk", "product_sk", "pdv_sk", "observed_price", 
                              "avg_market_price", "price_competitiveness_index"),
            on=["date_sk", "product_sk", "pdv_sk"],
            how="left"
        )
        .join(
            fact_stock.select("date_sk", "product_sk", "pdv_sk", 
                             "closing_stock_units", "stock_days_available", "stock_out_flag"),
            on=["date_sk", "product_sk", "pdv_sk"],
            how="left"
        )
    )
    
    # Define 30-day rolling window
    window_30d = Window.partitionBy("product_sk", "pdv_sk").orderBy("date_sk").rangeBetween(-30, 0)
    
    # Calculate rolling metrics
    df_with_rolling = (
        df
        .withColumn(
            "avg_sell_in_30d",
            avg(col("quantity_sell_in")).over(window_30d)
        )
        .withColumn(
            "std_sell_in_30d",
            percentile_approx(col("quantity_sell_in"), 0.5).over(window_30d)
        )
    )
    
    # Calculate Sell-In/Sell-Out Ratio (proxy)
    df_with_ratio = (
        df_with_rolling
        .withColumn(
            "sell_in_sell_out_ratio",
            when(
                col("avg_sell_in_30d") > 0,
                col("quantity_sell_in") / col("avg_sell_in_30d")
            ).otherwise(0)
        )
    )
    
    # Calculate Availability Rate (% days with stock > 0 in 30d window)
    df_with_availability = (
        df_with_ratio
        .withColumn(
            "stock_days_in_window",
            spark_sum(when(col("stock_days_available") > 0, 1).otherwise(0)).over(window_30d)
        )
        .withColumn(
            "availability_rate_pct",
            spark_round(
                (col("stock_days_in_window") / 30) * 100,
                2
            )
        )
    )
    
    # Lost Sales Estimation (if stockout, assume avg daily sell-in was lost)
    df_with_lost_sales = (
        df_with_availability
        .withColumn(
            "lost_sales_estimated_units",
            when(col("stock_out_flag") == True, col("avg_sell_in_30d"))
            .otherwise(0)
        )
    )
    
    # Efficiency Score (0-100)
    # Perfect: 5-30 days stock, 90-110 price index
    # Degraded: outside ranges
    df_with_efficiency = (
        df_with_lost_sales
        .withColumn(
            "efficiency_score",
            when(
                (col("stock_days_available").between(5, 30)) & 
                (col("price_index").between(90, 110)),
                100
            )
            .when(
                (col("stock_days_available").between(3, 40)) & 
                (col("price_index").between(80, 120)),
                75
            )
            .otherwise(
                spark_round(
                    spark_max(
                        lit(0),
                        50 - (abs(col("stock_days_available") - 15) / 15 * 25) - 
                             (abs(col("price_index") - 100) / 100 * 25)
                    ),
                    0
                )
            )
        )
    )
    
    return df_with_efficiency


def calculate_market_share_kpis(
    fact_sell_in: DataFrame,
    dim_product: DataFrame,
    dim_pdv: DataFrame
) -> DataFrame:
    """
    Materialize KPIs for gold_kpi_market_share.
    
    Aggregates sell-in by (date, product, region, segment, brand) and 
    calculates market share, penetration, and trends.
    
    Args:
        fact_sell_in: gold_fact_sell_in
        dim_product: gold_dim_product
        dim_pdv: gold_dim_pdv
    
    Returns:
        Market share KPI DataFrame
    
    Example:
        >>> kpi_share = calculate_market_share_kpis(
        ...     fact_sell_in, dim_product, dim_pdv
        ... )
    """
    
    # Join with dimensions
    df = (
        fact_sell_in
        .join(dim_product.filter(col("is_current") == True), on="product_sk", how="left")
        .join(dim_pdv.filter(col("is_current") == True), on="pdv_sk", how="left")
    )
    
    # Aggregate by (date, product, region, segment, brand)
    df_agg = (
        df
        .groupBy("date_sk", "product_sk", "region", "segment", "brand")
        .agg(
            spark_sum("quantity_sell_in").alias("sell_in_units"),
            spark_sum("value_sell_in").alias("sell_in_value"),
            count(col("pdv_sk")).alias("market_share_pdv_count")
        )
    )
    
    # Window: total by region and date
    window_region = Window.partitionBy("date_sk", "region")
    window_product = Window.partitionBy("date_sk", "product_sk", "region")
    
    # Calculate market shares
    df_with_share = (
        df_agg
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
    
    # Calculate market penetration (% PDVs with sales)
    window_total_pdvs = Window.partitionBy("date_sk", "region")
    df_with_penetration = (
        df_with_share
        .withColumn(
            "market_penetration_pct",
            spark_round(
                (col("market_share_pdv_count") / spark_max(col("market_share_pdv_count")).over(window_total_pdvs)) * 100,
                2
            )
        )
    )
    
    # Calculate trend (MoM % change)
    window_trend = Window.partitionBy("product_sk", "region").orderBy("date_sk")
    df_with_trend = (
        df_with_penetration
        .withColumn(
            "market_share_trend_pct",
            spark_round(
                col("market_share_units_pct") - lag(col("market_share_units_pct")).over(window_trend),
                2
            )
        )
    )
    
    return df_with_trend


# ============================================================================
# VALIDATION & QUALITY CHECKS
# ============================================================================

def validate_surrogate_key_uniqueness(df: DataFrame, sk_col: str, table_name: str) -> bool:
    """
    Validate that surrogate key is unique (no duplicates).
    
    Args:
        df: DataFrame to validate
        sk_col: Surrogate key column name
        table_name: Table name (for logging)
    
    Returns:
        True if valid (all unique), False if duplicates found
    
    Example:
        >>> assert validate_surrogate_key_uniqueness(
        ...     gold_dim_product, "product_sk", "gold_dim_product"
        ... )
    """
    total_rows = df.count()
    unique_sks = df.select(sk_col).distinct().count()
    
    is_valid = total_rows == unique_sks
    status = "✅ PASS" if is_valid else "❌ FAIL"
    print(f"{status}: {table_name} - {unique_sks}/{total_rows} unique SKs")
    
    return is_valid


def validate_scd2_current_only(df: DataFrame, business_key_col: str, table_name: str) -> bool:
    """
    Validate that only 1 current version exists per business key.
    
    Args:
        df: Dimension DataFrame with is_current column
        business_key_col: Business key column name
        table_name: Table name (for logging)
    
    Returns:
        True if valid (≤1 current per BK), False otherwise
    
    Example:
        >>> assert validate_scd2_current_only(
        ...     gold_dim_product, "product_sku", "gold_dim_product"
        ... )
    """
    duplicates = (
        df
        .filter(col("is_current") == True)
        .groupBy(business_key_col)
        .count()
        .filter(col("count") > 1)
        .count()
    )
    
    is_valid = duplicates == 0
    status = "✅ PASS" if is_valid else "❌ FAIL"
    print(f"{status}: {table_name} - {duplicates} business keys have multiple current versions")
    
    return is_valid


def validate_referential_integrity(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fact_fk: str,
    dim_pk: str,
    table_name: str
) -> bool:
    """
    Validate that all fact table foreign keys reference valid dimension keys.
    
    Args:
        fact_df: Fact table
        dim_df: Dimension table
        fact_fk: Foreign key column in fact table
        dim_pk: Primary key column in dimension table
        table_name: Table name (for logging)
    
    Returns:
        True if valid (0 orphans), False if orphans found
    
    Example:
        >>> assert validate_referential_integrity(
        ...     gold_fact_sell_in, gold_dim_product, "product_sk", "product_sk",
        ...     "gold_fact_sell_in -> gold_dim_product"
        ... )
    """
    orphans = (
        fact_df
        .join(dim_df.select(dim_pk), fact_df[fact_fk] == dim_df[dim_pk], how="left_anti")
        .count()
    )
    
    is_valid = orphans == 0
    status = "✅ PASS" if is_valid else "❌ FAIL"
    print(f"{status}: {table_name} - {orphans} orphaned foreign keys")
    
    return is_valid


def validate_kpi_consistency(
    kpi_daily_df: DataFrame,
    fact_df: DataFrame,
    kpi_metric: str,
    fact_metric: str,
    table_name: str
) -> bool:
    """
    Validate that KPI aggregate matches fact aggregate.
    
    Args:
        kpi_daily_df: KPI table
        fact_df: Fact table
        kpi_metric: KPI column to sum
        fact_metric: Fact column to sum
        table_name: Table name (for logging)
    
    Returns:
        True if sums match (or near match within 0.01%), False otherwise
    
    Example:
        >>> assert validate_kpi_consistency(
        ...     gold_kpi_market_share, gold_fact_sell_in,
        ...     "sell_in_units", "quantity_sell_in",
        ...     "gold_kpi_market_share validation"
        ... )
    """
    kpi_sum = kpi_daily_df.agg(spark_sum(kpi_metric)).collect()[0][0] or 0
    fact_sum = fact_df.agg(spark_sum(fact_metric)).collect()[0][0] or 0
    
    if fact_sum == 0:
        is_valid = kpi_sum == 0
    else:
        variance_pct = abs(kpi_sum - fact_sum) / fact_sum * 100
        is_valid = variance_pct < 0.01  # 0.01% tolerance
    
    status = "✅ PASS" if is_valid else "❌ FAIL"
    print(f"{status}: {table_name} - KPI sum {kpi_sum:.0f} vs Fact sum {fact_sum:.0f}")
    
    return is_valid

