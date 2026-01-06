"""
Gold Layer Quality Monitoring & Drift Detection

Purpose: Comprehensive validation and monitoring for Gold Layer tables.
Includes:
- Data Quality Checks (DQ)
- Data Drift Detection (DD)
- Schema Drift Detection (SD)
- Freshness Monitoring (FM)
- Business Rule Validation (BR)
- Cross-Layer Reconciliation (XL)

Author: Senior Analytics Engineer
Date: 2025-01-06
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, count, sum as spark_sum, avg, stddev, min as spark_min, max as spark_max,
    when, abs as spark_abs, current_timestamp, current_date, datediff,
    percentile_approx, skewness, kurtosis
)
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json


# =============================================================================
# 1. DATA QUALITY CHECKS (DQ)
# =============================================================================

def dq_check_null_rates(df: DataFrame, critical_columns: List[str], table_name: str, threshold_pct: float = 1.0) -> Dict:
    """
    Check null rates in critical columns.
    
    Args:
        df: DataFrame to validate
        critical_columns: Columns that should have minimal nulls
        table_name: Name for reporting
        threshold_pct: Maximum allowed null percentage (default 1%)
    
    Returns:
        Dict with validation results
    """
    results = {
        "check_name": "null_rate_validation",
        "table": table_name,
        "timestamp": datetime.now().isoformat(),
        "status": "PASS",
        "details": []
    }
    
    total_rows = df.count()
    if total_rows == 0:
        results["status"] = "FAIL"
        results["error"] = "Table is empty"
        return results
    
    for col_name in critical_columns:
        if col_name not in df.columns:
            results["details"].append({
                "column": col_name,
                "status": "SKIP",
                "reason": "Column not found"
            })
            continue
            
        null_count = df.filter(col(col_name).isNull()).count()
        null_pct = (null_count / total_rows) * 100
        
        status = "PASS" if null_pct <= threshold_pct else "FAIL"
        if status == "FAIL":
            results["status"] = "FAIL"
        
        results["details"].append({
            "column": col_name,
            "null_count": null_count,
            "null_pct": round(null_pct, 4),
            "threshold_pct": threshold_pct,
            "status": status
        })
    
    return results


def dq_check_value_ranges(df: DataFrame, range_rules: Dict[str, Tuple], table_name: str) -> Dict:
    """
    Validate that numeric columns fall within expected ranges.
    
    Args:
        df: DataFrame to validate
        range_rules: Dict of {column: (min_val, max_val)}
        table_name: Name for reporting
    
    Returns:
        Dict with validation results
    """
    results = {
        "check_name": "value_range_validation",
        "table": table_name,
        "timestamp": datetime.now().isoformat(),
        "status": "PASS",
        "details": []
    }
    
    for col_name, (min_val, max_val) in range_rules.items():
        if col_name not in df.columns:
            continue
        
        stats = df.select(
            spark_min(col(col_name)).alias("actual_min"),
            spark_max(col(col_name)).alias("actual_max"),
            count(when(col(col_name) < min_val, 1)).alias("below_min_count"),
            count(when(col(col_name) > max_val, 1)).alias("above_max_count")
        ).collect()[0]
        
        violations = stats["below_min_count"] + stats["above_max_count"]
        status = "PASS" if violations == 0 else "WARN" if violations < 100 else "FAIL"
        
        if status == "FAIL":
            results["status"] = "FAIL"
        elif status == "WARN" and results["status"] != "FAIL":
            results["status"] = "WARN"
        
        results["details"].append({
            "column": col_name,
            "expected_range": [min_val, max_val],
            "actual_range": [stats["actual_min"], stats["actual_max"]],
            "violations": violations,
            "status": status
        })
    
    return results


def dq_check_uniqueness(df: DataFrame, unique_columns: List[str], table_name: str) -> Dict:
    """
    Validate uniqueness of key columns.
    
    Args:
        df: DataFrame to validate
        unique_columns: Columns that should be unique (composite key)
        table_name: Name for reporting
    
    Returns:
        Dict with validation results
    """
    total_rows = df.count()
    distinct_rows = df.select(unique_columns).distinct().count()
    duplicates = total_rows - distinct_rows
    
    return {
        "check_name": "uniqueness_validation",
        "table": table_name,
        "timestamp": datetime.now().isoformat(),
        "unique_columns": unique_columns,
        "total_rows": total_rows,
        "distinct_rows": distinct_rows,
        "duplicate_rows": duplicates,
        "status": "PASS" if duplicates == 0 else "FAIL"
    }


# =============================================================================
# 2. DATA DRIFT DETECTION (DD)
# =============================================================================

def dd_calculate_distribution_stats(df: DataFrame, numeric_columns: List[str]) -> Dict:
    """
    Calculate distribution statistics for drift detection baseline.
    
    Args:
        df: DataFrame to analyze
        numeric_columns: Columns to profile
    
    Returns:
        Dict with distribution statistics
    """
    stats = {}
    
    for col_name in numeric_columns:
        if col_name not in df.columns:
            continue
        
        col_stats = df.select(
            avg(col(col_name)).alias("mean"),
            stddev(col(col_name)).alias("stddev"),
            spark_min(col(col_name)).alias("min"),
            spark_max(col(col_name)).alias("max"),
            percentile_approx(col(col_name), 0.25).alias("p25"),
            percentile_approx(col(col_name), 0.50).alias("p50"),
            percentile_approx(col(col_name), 0.75).alias("p75"),
            skewness(col(col_name)).alias("skewness"),
            kurtosis(col(col_name)).alias("kurtosis")
        ).collect()[0]
        
        stats[col_name] = {
            "mean": float(col_stats["mean"]) if col_stats["mean"] else None,
            "stddev": float(col_stats["stddev"]) if col_stats["stddev"] else None,
            "min": float(col_stats["min"]) if col_stats["min"] else None,
            "max": float(col_stats["max"]) if col_stats["max"] else None,
            "p25": float(col_stats["p25"]) if col_stats["p25"] else None,
            "p50": float(col_stats["p50"]) if col_stats["p50"] else None,
            "p75": float(col_stats["p75"]) if col_stats["p75"] else None,
            "skewness": float(col_stats["skewness"]) if col_stats["skewness"] else None,
            "kurtosis": float(col_stats["kurtosis"]) if col_stats["kurtosis"] else None
        }
    
    return stats


def dd_detect_distribution_drift(
    current_stats: Dict,
    baseline_stats: Dict,
    drift_threshold_pct: float = 20.0
) -> Dict:
    """
    Detect distribution drift by comparing current vs baseline statistics.
    
    Args:
        current_stats: Current distribution statistics
        baseline_stats: Baseline (expected) statistics
        drift_threshold_pct: Percentage change threshold for drift alert
    
    Returns:
        Dict with drift detection results
    """
    results = {
        "check_name": "distribution_drift_detection",
        "timestamp": datetime.now().isoformat(),
        "drift_threshold_pct": drift_threshold_pct,
        "status": "PASS",
        "drifts": []
    }
    
    for col_name in current_stats:
        if col_name not in baseline_stats:
            continue
        
        current = current_stats[col_name]
        baseline = baseline_stats[col_name]
        
        # Check mean drift
        if baseline["mean"] and baseline["mean"] != 0:
            mean_drift_pct = abs((current["mean"] - baseline["mean"]) / baseline["mean"]) * 100
            
            if mean_drift_pct > drift_threshold_pct:
                results["status"] = "DRIFT_DETECTED"
                results["drifts"].append({
                    "column": col_name,
                    "metric": "mean",
                    "baseline": baseline["mean"],
                    "current": current["mean"],
                    "drift_pct": round(mean_drift_pct, 2)
                })
        
        # Check stddev drift (volatility change)
        if baseline["stddev"] and baseline["stddev"] != 0:
            stddev_drift_pct = abs((current["stddev"] - baseline["stddev"]) / baseline["stddev"]) * 100
            
            if stddev_drift_pct > drift_threshold_pct * 1.5:  # Higher threshold for variance
                results["status"] = "DRIFT_DETECTED"
                results["drifts"].append({
                    "column": col_name,
                    "metric": "stddev",
                    "baseline": baseline["stddev"],
                    "current": current["stddev"],
                    "drift_pct": round(stddev_drift_pct, 2)
                })
    
    return results


def dd_detect_kpi_anomalies(
    df: DataFrame,
    kpi_column: str,
    expected_range: Tuple[float, float],
    table_name: str
) -> Dict:
    """
    Detect anomalies in KPI values.
    
    Args:
        df: DataFrame with KPIs
        kpi_column: Column to check
        expected_range: (min, max) expected values
        table_name: Name for reporting
    
    Returns:
        Dict with anomaly detection results
    """
    min_expected, max_expected = expected_range
    
    anomaly_stats = df.select(
        count("*").alias("total_rows"),
        count(when(col(kpi_column) < min_expected, 1)).alias("below_min"),
        count(when(col(kpi_column) > max_expected, 1)).alias("above_max"),
        avg(col(kpi_column)).alias("actual_avg")
    ).collect()[0]
    
    total = anomaly_stats["total_rows"]
    anomalies = anomaly_stats["below_min"] + anomaly_stats["above_max"]
    anomaly_rate = (anomalies / total * 100) if total > 0 else 0
    
    return {
        "check_name": "kpi_anomaly_detection",
        "table": table_name,
        "kpi_column": kpi_column,
        "timestamp": datetime.now().isoformat(),
        "expected_range": expected_range,
        "actual_avg": float(anomaly_stats["actual_avg"]) if anomaly_stats["actual_avg"] else None,
        "anomaly_count": anomalies,
        "anomaly_rate_pct": round(anomaly_rate, 2),
        "status": "PASS" if anomaly_rate < 5 else "WARN" if anomaly_rate < 10 else "FAIL"
    }


# =============================================================================
# 3. SCHEMA DRIFT DETECTION (SD)
# =============================================================================

def sd_get_schema_signature(df: DataFrame) -> Dict:
    """
    Get schema signature for drift detection.
    
    Args:
        df: DataFrame to analyze
    
    Returns:
        Dict with schema signature
    """
    return {
        "columns": {field.name: str(field.dataType) for field in df.schema.fields},
        "column_count": len(df.columns),
        "column_order": df.columns
    }


def sd_detect_schema_drift(current_schema: Dict, baseline_schema: Dict, table_name: str) -> Dict:
    """
    Detect schema drift between current and baseline.
    
    Args:
        current_schema: Current schema signature
        baseline_schema: Baseline schema signature
        table_name: Name for reporting
    
    Returns:
        Dict with schema drift results
    """
    results = {
        "check_name": "schema_drift_detection",
        "table": table_name,
        "timestamp": datetime.now().isoformat(),
        "status": "PASS",
        "changes": []
    }
    
    current_cols = set(current_schema["columns"].keys())
    baseline_cols = set(baseline_schema["columns"].keys())
    
    # New columns
    new_cols = current_cols - baseline_cols
    if new_cols:
        results["status"] = "DRIFT_DETECTED"
        results["changes"].append({
            "type": "COLUMNS_ADDED",
            "columns": list(new_cols)
        })
    
    # Removed columns
    removed_cols = baseline_cols - current_cols
    if removed_cols:
        results["status"] = "DRIFT_DETECTED"
        results["changes"].append({
            "type": "COLUMNS_REMOVED",
            "columns": list(removed_cols)
        })
    
    # Type changes
    for col_name in current_cols & baseline_cols:
        if current_schema["columns"][col_name] != baseline_schema["columns"][col_name]:
            results["status"] = "DRIFT_DETECTED"
            results["changes"].append({
                "type": "TYPE_CHANGED",
                "column": col_name,
                "from": baseline_schema["columns"][col_name],
                "to": current_schema["columns"][col_name]
            })
    
    return results


# =============================================================================
# 4. FRESHNESS MONITORING (FM)
# =============================================================================

def fm_check_data_freshness(
    df: DataFrame,
    date_column: str,
    max_staleness_days: int,
    table_name: str
) -> Dict:
    """
    Check data freshness based on max date in table.
    
    Args:
        df: DataFrame to check
        date_column: Column containing dates
        max_staleness_days: Maximum allowed days since last data
        table_name: Name for reporting
    
    Returns:
        Dict with freshness check results
    """
    max_date_row = df.select(spark_max(col(date_column)).alias("max_date")).collect()[0]
    max_date = max_date_row["max_date"]
    
    if max_date is None:
        return {
            "check_name": "data_freshness",
            "table": table_name,
            "timestamp": datetime.now().isoformat(),
            "status": "FAIL",
            "error": "No dates found in table"
        }
    
    # Calculate staleness
    from datetime import date
    today = date.today()
    
    if hasattr(max_date, 'date'):
        max_date = max_date.date()
    
    staleness_days = (today - max_date).days
    
    return {
        "check_name": "data_freshness",
        "table": table_name,
        "timestamp": datetime.now().isoformat(),
        "date_column": date_column,
        "max_date": str(max_date),
        "staleness_days": staleness_days,
        "max_staleness_days": max_staleness_days,
        "status": "PASS" if staleness_days <= max_staleness_days else "STALE"
    }


# =============================================================================
# 5. BUSINESS RULE VALIDATION (BR)
# =============================================================================

def br_validate_market_share_sums(df: DataFrame, group_columns: List[str], share_column: str) -> Dict:
    """
    Validate that market share sums to ~100% per group.
    
    Args:
        df: DataFrame with market share data
        group_columns: Columns to group by (e.g., date, region)
        share_column: Column with market share percentage
    
    Returns:
        Dict with validation results
    """
    share_sums = (
        df.groupBy(group_columns)
        .agg(spark_sum(col(share_column)).alias("total_share"))
    )
    
    invalid_groups = share_sums.filter(
        (col("total_share") < 99.0) | (col("total_share") > 101.0)
    ).count()
    
    total_groups = share_sums.count()
    
    return {
        "check_name": "market_share_sum_validation",
        "timestamp": datetime.now().isoformat(),
        "group_columns": group_columns,
        "share_column": share_column,
        "total_groups": total_groups,
        "invalid_groups": invalid_groups,
        "status": "PASS" if invalid_groups == 0 else "WARN" if invalid_groups < total_groups * 0.05 else "FAIL"
    }


def br_validate_price_competitiveness(df: DataFrame, price_index_column: str, table_name: str) -> Dict:
    """
    Validate price competitiveness index is within business expectations.
    
    Business Rule: Price index should be between 50 and 200 (50% to 200% of market avg)
    
    Args:
        df: DataFrame with price data
        price_index_column: Column with price index
        table_name: Name for reporting
    
    Returns:
        Dict with validation results
    """
    stats = df.select(
        count("*").alias("total"),
        count(when((col(price_index_column) >= 50) & (col(price_index_column) <= 200), 1)).alias("valid"),
        avg(col(price_index_column)).alias("avg_index")
    ).collect()[0]
    
    valid_pct = (stats["valid"] / stats["total"] * 100) if stats["total"] > 0 else 0
    
    return {
        "check_name": "price_competitiveness_validation",
        "table": table_name,
        "timestamp": datetime.now().isoformat(),
        "expected_range": [50, 200],
        "avg_price_index": round(float(stats["avg_index"]), 2) if stats["avg_index"] else None,
        "valid_records_pct": round(valid_pct, 2),
        "status": "PASS" if valid_pct >= 95 else "WARN" if valid_pct >= 90 else "FAIL"
    }


def br_validate_stock_health(df: DataFrame, stock_days_column: str, table_name: str) -> Dict:
    """
    Validate stock health distribution.
    
    Business Rule: 
    - Stockout (0 days): Should be < 10%
    - Healthy (5-30 days): Should be > 70%
    - Overstock (>30 days): Should be < 20%
    
    Args:
        df: DataFrame with stock data
        stock_days_column: Column with days of stock
        table_name: Name for reporting
    
    Returns:
        Dict with validation results
    """
    total = df.count()
    
    if total == 0:
        return {
            "check_name": "stock_health_validation",
            "table": table_name,
            "status": "FAIL",
            "error": "No data"
        }
    
    distribution = df.select(
        count(when(col(stock_days_column) == 0, 1)).alias("stockout"),
        count(when((col(stock_days_column) > 0) & (col(stock_days_column) < 5), 1)).alias("low_stock"),
        count(when((col(stock_days_column) >= 5) & (col(stock_days_column) <= 30), 1)).alias("healthy"),
        count(when(col(stock_days_column) > 30, 1)).alias("overstock")
    ).collect()[0]
    
    stockout_pct = distribution["stockout"] / total * 100
    healthy_pct = distribution["healthy"] / total * 100
    overstock_pct = distribution["overstock"] / total * 100
    
    status = "PASS"
    issues = []
    
    if stockout_pct > 10:
        status = "WARN" if stockout_pct < 20 else "FAIL"
        issues.append(f"High stockout rate: {stockout_pct:.1f}%")
    
    if healthy_pct < 70:
        status = "WARN" if healthy_pct > 50 else "FAIL"
        issues.append(f"Low healthy stock rate: {healthy_pct:.1f}%")
    
    if overstock_pct > 20:
        status = "WARN" if overstock_pct < 30 else "FAIL"
        issues.append(f"High overstock rate: {overstock_pct:.1f}%")
    
    return {
        "check_name": "stock_health_validation",
        "table": table_name,
        "timestamp": datetime.now().isoformat(),
        "distribution": {
            "stockout_pct": round(stockout_pct, 2),
            "low_stock_pct": round(distribution["low_stock"] / total * 100, 2),
            "healthy_pct": round(healthy_pct, 2),
            "overstock_pct": round(overstock_pct, 2)
        },
        "issues": issues,
        "status": status
    }


# =============================================================================
# 6. CROSS-LAYER RECONCILIATION (XL)
# =============================================================================

def xl_reconcile_row_counts(
    silver_df: DataFrame,
    gold_df: DataFrame,
    silver_name: str,
    gold_name: str,
    tolerance_pct: float = 5.0
) -> Dict:
    """
    Reconcile row counts between Silver and Gold layers.
    
    Note: Gold may have fewer rows due to aggregation or more due to SCD2.
    
    Args:
        silver_df: Silver layer DataFrame
        gold_df: Gold layer DataFrame
        silver_name: Silver table name
        gold_name: Gold table name
        tolerance_pct: Acceptable variance percentage
    
    Returns:
        Dict with reconciliation results
    """
    silver_count = silver_df.count()
    gold_count = gold_df.count()
    
    variance_pct = abs((gold_count - silver_count) / silver_count * 100) if silver_count > 0 else 100
    
    return {
        "check_name": "cross_layer_row_reconciliation",
        "timestamp": datetime.now().isoformat(),
        "silver_table": silver_name,
        "gold_table": gold_name,
        "silver_row_count": silver_count,
        "gold_row_count": gold_count,
        "variance_pct": round(variance_pct, 2),
        "tolerance_pct": tolerance_pct,
        "status": "PASS" if variance_pct <= tolerance_pct else "WARN"
    }


def xl_reconcile_aggregates(
    silver_df: DataFrame,
    gold_df: DataFrame,
    silver_agg_column: str,
    gold_agg_column: str,
    silver_name: str,
    gold_name: str,
    tolerance_pct: float = 1.0
) -> Dict:
    """
    Reconcile aggregate sums between Silver and Gold layers.
    
    Args:
        silver_df: Silver layer DataFrame
        gold_df: Gold layer DataFrame
        silver_agg_column: Column to sum in Silver
        gold_agg_column: Column to sum in Gold
        silver_name: Silver table name
        gold_name: Gold table name
        tolerance_pct: Acceptable variance percentage
    
    Returns:
        Dict with reconciliation results
    """
    silver_sum = silver_df.select(spark_sum(col(silver_agg_column))).collect()[0][0] or 0
    gold_sum = gold_df.select(spark_sum(col(gold_agg_column))).collect()[0][0] or 0
    
    variance_pct = abs((gold_sum - silver_sum) / silver_sum * 100) if silver_sum > 0 else 100
    
    return {
        "check_name": "cross_layer_aggregate_reconciliation",
        "timestamp": datetime.now().isoformat(),
        "silver_table": silver_name,
        "gold_table": gold_name,
        "silver_sum": float(silver_sum),
        "gold_sum": float(gold_sum),
        "variance_pct": round(variance_pct, 4),
        "tolerance_pct": tolerance_pct,
        "status": "PASS" if variance_pct <= tolerance_pct else "FAIL"
    }


# =============================================================================
# 7. COMPREHENSIVE GOLD LAYER MONITORING SUITE
# =============================================================================

def run_gold_layer_monitoring(spark: SparkSession, schema: str = "workspace.default") -> Dict:
    """
    Run comprehensive monitoring suite for Gold Layer.
    
    Args:
        spark: SparkSession
        schema: Database schema
    
    Returns:
        Dict with all monitoring results
    """
    results = {
        "monitoring_run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "timestamp": datetime.now().isoformat(),
        "schema": schema,
        "checks": []
    }
    
    # Load tables
    try:
        gold_dim_date = spark.read.table(f"{schema}.gold_dim_date")
        gold_dim_product = spark.read.table(f"{schema}.gold_dim_product")
        gold_dim_pdv = spark.read.table(f"{schema}.gold_dim_pdv")
        gold_fact_sell_in = spark.read.table(f"{schema}.gold_fact_sell_in")
        gold_fact_price_audit = spark.read.table(f"{schema}.gold_fact_price_audit")
        gold_fact_stock = spark.read.table(f"{schema}.gold_fact_stock")
        gold_kpi_market_visibility = spark.read.table(f"{schema}.gold_kpi_market_visibility_daily")
        gold_kpi_market_share = spark.read.table(f"{schema}.gold_kpi_market_share")
        
        silver_sell_in = spark.read.table(f"{schema}.silver_sell_in")
        silver_price_audit = spark.read.table(f"{schema}.silver_price_audit")
    except Exception as e:
        results["status"] = "ERROR"
        results["error"] = str(e)
        return results
    
    # 1. Data Quality Checks
    results["checks"].append(dq_check_null_rates(
        gold_fact_sell_in,
        ["date_sk", "product_sk", "pdv_sk", "quantity_sell_in"],
        "gold_fact_sell_in"
    ))
    
    results["checks"].append(dq_check_value_ranges(
        gold_fact_sell_in,
        {
            "quantity_sell_in": (0, 100000),
            "value_sell_in": (0, 10000000),
            "unit_price_sell_in": (0.01, 10000)
        },
        "gold_fact_sell_in"
    ))
    
    results["checks"].append(dq_check_uniqueness(
        gold_fact_sell_in,
        ["date_sk", "product_sk", "pdv_sk"],
        "gold_fact_sell_in"
    ))
    
    # 2. KPI Anomaly Detection
    results["checks"].append(dd_detect_kpi_anomalies(
        gold_kpi_market_visibility,
        "efficiency_score",
        (0, 100),
        "gold_kpi_market_visibility_daily"
    ))
    
    results["checks"].append(dd_detect_kpi_anomalies(
        gold_kpi_market_visibility,
        "availability_rate_pct",
        (0, 100),
        "gold_kpi_market_visibility_daily"
    ))
    
    # 3. Freshness Monitoring
    results["checks"].append(fm_check_data_freshness(
        gold_dim_date,
        "date",
        1,  # Date dimension should always be current
        "gold_dim_date"
    ))
    
    # 4. Business Rule Validation
    results["checks"].append(br_validate_market_share_sums(
        gold_kpi_market_share,
        ["date_sk", "region"],
        "market_share_units_pct"
    ))
    
    results["checks"].append(br_validate_price_competitiveness(
        gold_fact_price_audit,
        "price_index",
        "gold_fact_price_audit"
    ))
    
    results["checks"].append(br_validate_stock_health(
        gold_fact_stock,
        "stock_days_available",
        "gold_fact_stock"
    ))
    
    # 5. Cross-Layer Reconciliation
    results["checks"].append(xl_reconcile_aggregates(
        silver_sell_in,
        gold_fact_sell_in,
        "quantity",
        "quantity_sell_in",
        "silver_sell_in",
        "gold_fact_sell_in"
    ))
    
    # Calculate overall status
    statuses = [c.get("status", "UNKNOWN") for c in results["checks"]]
    if "FAIL" in statuses:
        results["overall_status"] = "FAIL"
    elif "WARN" in statuses or "DRIFT_DETECTED" in statuses or "STALE" in statuses:
        results["overall_status"] = "WARN"
    else:
        results["overall_status"] = "PASS"
    
    results["summary"] = {
        "total_checks": len(results["checks"]),
        "passed": statuses.count("PASS"),
        "warnings": statuses.count("WARN") + statuses.count("DRIFT_DETECTED") + statuses.count("STALE"),
        "failed": statuses.count("FAIL")
    }
    
    return results


def print_monitoring_report(results: Dict) -> None:
    """Print formatted monitoring report."""
    print("=" * 80)
    print(f"GOLD LAYER MONITORING REPORT")
    print(f"   Run ID: {results['monitoring_run_id']}")
    print(f"   Timestamp: {results['timestamp']}")
    print("=" * 80)
    
    # Summary
    summary = results.get("summary", {})
    overall = results.get("overall_status", "UNKNOWN")
    
    status_emoji = {"PASS": "PASS", "WARN": "WARN", "FAIL": "FAIL"}.get(overall, "UNKNOWN")
    print(f"\nOverall Status: {status_emoji}")
    print(f"   Total Checks: {summary.get('total_checks', 0)}")
    print(f"   Passed: {summary.get('passed', 0)}")
    print(f"   Warnings: {summary.get('warnings', 0)}")
    print(f"   Failed: {summary.get('failed', 0)}")
    
    # Details
    print("\n" + "-" * 80)
    print("Check Details:")
    print("-" * 80)
    
    for check in results.get("checks", []):
        status = check.get("status", "UNKNOWN")
        emoji = {"PASS": "[OK]", "WARN": "[WARN]", "FAIL": "[FAIL]", "DRIFT_DETECTED": "[DRIFT]", "STALE": "[STALE]"}.get(status, "[?]")
        name = check.get("check_name", "unknown")
        table = check.get("table", "")
        
        print(f"\n{emoji} {name}")
        if table:
            print(f"   Table: {table}")
        
        # Print relevant details based on check type
        if "details" in check:
            for detail in check["details"][:3]:  # Limit to first 3
                print(f"   - {detail}")
        
        if "issues" in check and check["issues"]:
            for issue in check["issues"]:
                print(f"   [!] {issue}")
    
    print("\n" + "=" * 80)
