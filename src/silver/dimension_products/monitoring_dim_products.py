"""
SilverProductsMonitor: Enterprise-Grade Monitoring Layer

Implements comprehensive observability for Silver Products Dimension
following Medallion Architecture monitoring principles.

CRITICAL DISTINCTION FROM VALIDATION:
- Validation: Fail-fast, blocks pipeline on critical violations
- Monitoring: Observability, never blocks, tracks trends and health

Platform: Databricks Serverless (Unity Catalog)
Strategy: Observe, measure, warn (never fail)
Constraints: No transformations, no corrections, no mandatory persistence
"""

from typing import Dict, Any, List
from datetime import datetime, date, timedelta
import logging
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverProductsMonitor")


class SilverProductsMonitor:
    """
    Enterprise-grade monitoring for workspace.silver.dim_products.
    
    Purpose:
    - Observe dataset health over time
    - Detect drift, anomalies, and progressive degradation
    - Provide operational and quality metrics
    - Generate structured warnings (never exceptions)
    
    NOT responsible for:
    - Stopping pipelines (that's validation)
    - Data correction (that's transformation)
    - Hard rule enforcement (that's validation)
    
    Monitors:
    - Volume trends
    - Data freshness
    - Distribution patterns
    - Uniqueness rates
    - Domain coverage
    - Audit field consistency
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any] = None):
        self.spark = spark
        self.config = config or {}
        self.table_name = self.config.get("table_name", "workspace.silver.dim_products")
        
        # Thresholds for warnings (not failures)
        self.thresholds = {
            "duplicate_rate_warning": self.config.get("duplicate_rate_warning", 0.01),  # 1%
            "domain_violation_rate_warning": self.config.get("domain_violation_rate_warning", 0.05),  # 5%
            "freshness_days_warning": self.config.get("freshness_days_warning", 7),  # 7 days
            "min_records_warning": self.config.get("min_records_warning", 100),
            "audit_violation_rate_warning": self.config.get("audit_violation_rate_warning", 0.02)  # 2%
        }
        
        # Business domain values for coverage analysis
        self.allowed_segments = self.config.get("allowed_segments", 
            ["culinary", "beverages", "dairy"])
        
        self.allowed_categories = self.config.get("allowed_categories",
            ["ambient_culinary", "beverages", "dairy"])

    def _read_table(self) -> DataFrame:
        """Reads the Silver table for monitoring."""
        logger.info(f"Reading table for monitoring: {self.table_name}")
        return self.spark.table(self.table_name)

    def _compute_volume_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Computes volume metrics to track data growth and patterns.
        
        Metrics:
        - Total record count
        - Records per load_date
        - Distribution across time
        """
        logger.info("Computing volume metrics...")
        
        # Single aggregation for volume metrics
        volume_agg = df.agg(
            F.count("*").alias("total_records"),
            F.countDistinct("load_date").alias("distinct_load_dates"),
            F.countDistinct("product_code").alias("distinct_products")
        ).collect()[0]
        
        # Records per load_date (top 10 most recent)
        records_per_date = df.groupBy("load_date") \
            .agg(F.count("*").alias("record_count")) \
            .orderBy(F.desc("load_date")) \
            .limit(10) \
            .collect()
        
        return {
            "total_records": volume_agg["total_records"],
            "distinct_load_dates": volume_agg["distinct_load_dates"],
            "distinct_products": volume_agg["distinct_products"],
            "avg_records_per_date": round(
                volume_agg["total_records"] / volume_agg["distinct_load_dates"], 2
            ) if volume_agg["distinct_load_dates"] > 0 else 0,
            "recent_loads": [
                {"load_date": str(row["load_date"]), "records": row["record_count"]}
                for row in records_per_date
            ]
        }

    def _compute_freshness_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Computes data freshness metrics to detect staleness.
        
        Metrics:
        - Latest load_date
        - Days since last load
        - Latest processing timestamps
        """
        logger.info("Computing freshness metrics...")
        
        freshness_agg = df.agg(
            F.max("load_date").alias("latest_load_date"),
            F.max("ingestion_timestamp").alias("latest_ingestion"),
            F.max("silver_processed_at").alias("latest_processing")
        ).collect()[0]
        
        latest_load_date = freshness_agg["latest_load_date"]
        days_since_last_load = (date.today() - latest_load_date).days if latest_load_date else None
        
        return {
            "latest_load_date": str(latest_load_date) if latest_load_date else None,
            "days_since_last_load": days_since_last_load,
            "latest_ingestion_timestamp": freshness_agg["latest_ingestion"].isoformat() if freshness_agg["latest_ingestion"] else None,
            "latest_processing_timestamp": freshness_agg["latest_processing"].isoformat() if freshness_agg["latest_processing"] else None
        }

    def _compute_distribution_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Computes distribution metrics to detect skew and patterns.
        
        Metrics:
        - Records by segment
        - Records by category
        - Top subcategories
        - Brand distribution
        """
        logger.info("Computing distribution metrics...")
        
        # Segment distribution
        segment_dist = df.groupBy("segment") \
            .agg(F.count("*").alias("count")) \
            .orderBy(F.desc("count")) \
            .collect()
        
        # Category distribution
        category_dist = df.groupBy("category") \
            .agg(F.count("*").alias("count")) \
            .orderBy(F.desc("count")) \
            .collect()
        
        # Top 10 subcategories
        subcategory_dist = df.groupBy("subcategory") \
            .agg(F.count("*").alias("count")) \
            .orderBy(F.desc("count")) \
            .limit(10) \
            .collect()
        
        # Brand distribution
        brand_dist = df.groupBy("brand") \
            .agg(F.count("*").alias("count")) \
            .orderBy(F.desc("count")) \
            .collect()
        
        return {
            "segments": [
                {"segment": row["segment"], "count": row["count"]}
                for row in segment_dist
            ],
            "categories": [
                {"category": row["category"], "count": row["count"]}
                for row in category_dist
            ],
            "top_subcategories": [
                {"subcategory": row["subcategory"], "count": row["count"]}
                for row in subcategory_dist
            ],
            "brands": [
                {"brand": row["brand"], "count": row["count"]}
                for row in brand_dist
            ]
        }

    def _compute_uniqueness_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Computes uniqueness metrics to detect duplicate patterns.
        
        Note: This is observational, not enforcement.
        Duplicates are counted but don't fail the pipeline.
        """
        logger.info("Computing uniqueness metrics...")
        
        # Count duplicates on PK (product_code + load_date)
        total_count = df.count()
        
        window_pk = Window.partitionBy("product_code", "load_date")
        df_with_dup_flag = df.withColumn(
            "_is_duplicate",
            F.count("*").over(window_pk) > 1
        )
        
        duplicate_metrics = df_with_dup_flag.agg(
            F.sum(F.when(F.col("_is_duplicate"), 1).otherwise(0)).alias("duplicate_records")
        ).collect()[0]
        
        duplicate_count = duplicate_metrics["duplicate_records"] or 0
        duplicate_rate = round(duplicate_count / total_count, 4) if total_count > 0 else 0.0
        
        return {
            "total_records": total_count,
            "duplicate_records": int(duplicate_count),
            "duplicate_rate": duplicate_rate,
            "unique_records": total_count - int(duplicate_count)
        }

    def _compute_domain_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Computes domain coverage metrics to detect unknown values.
        
        Tracks how many records fall outside expected domain values.
        This is observational - doesn't enforce or block.
        """
        logger.info("Computing domain metrics...")
        
        total_count = df.count()
        
        domain_metrics = df.agg(
            F.sum(F.when(~F.col("segment").isin(self.allowed_segments), 1).otherwise(0)).alias("segment_violations"),
            F.sum(F.when(~F.col("category").isin(self.allowed_categories), 1).otherwise(0)).alias("category_violations")
        ).collect()[0]
        
        segment_violations = domain_metrics["segment_violations"] or 0
        category_violations = domain_metrics["category_violations"] or 0
        
        segment_coverage = round((total_count - segment_violations) / total_count, 4) if total_count > 0 else 0.0
        category_coverage = round((total_count - category_violations) / total_count, 4) if total_count > 0 else 0.0
        
        return {
            "segment_violations": int(segment_violations),
            "segment_coverage_rate": segment_coverage,
            "category_violations": int(category_violations),
            "category_coverage_rate": category_coverage
        }

    def _compute_audit_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Computes audit field consistency metrics.
        
        Tracks temporal consistency and field integrity.
        Observational only - doesn't enforce.
        """
        logger.info("Computing audit metrics...")
        
        total_count = df.count()
        
        audit_metrics = df.agg(
            F.sum(F.when(F.col("silver_processed_at") < F.col("ingestion_timestamp"), 1).otherwise(0)).alias("processing_before_ingestion"),
            F.sum(F.when(F.col("load_date") > F.current_date(), 1).otherwise(0)).alias("future_load_dates"),
            F.sum(F.when(F.length(F.col("silver_batch_id")) != 36, 1).otherwise(0)).alias("invalid_batch_ids")
        ).collect()[0]
        
        processing_violations = audit_metrics["processing_before_ingestion"] or 0
        future_dates = audit_metrics["future_load_dates"] or 0
        invalid_uuids = audit_metrics["invalid_batch_ids"] or 0
        
        total_audit_violations = processing_violations + future_dates + invalid_uuids
        audit_violation_rate = round(total_audit_violations / total_count, 4) if total_count > 0 else 0.0
        
        return {
            "processing_before_ingestion": int(processing_violations),
            "future_load_dates": int(future_dates),
            "invalid_batch_ids": int(invalid_uuids),
            "total_audit_violations": int(total_audit_violations),
            "audit_violation_rate": audit_violation_rate
        }

    def _generate_warnings(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generates structured warnings based on thresholds.
        
        Warnings are informational signals, not failures.
        They help identify trends that need attention.
        """
        warnings = []
        
        # Volume warnings
        if metrics["volume"]["total_records"] < self.thresholds["min_records_warning"]:
            warnings.append({
                "warning_code": "LOW_VOLUME",
                "metric_value": metrics["volume"]["total_records"],
                "threshold": self.thresholds["min_records_warning"],
                "message": f"Record count ({metrics['volume']['total_records']}) below minimum threshold"
            })
        
        # Freshness warnings
        if metrics["freshness"]["days_since_last_load"] and \
           metrics["freshness"]["days_since_last_load"] > self.thresholds["freshness_days_warning"]:
            warnings.append({
                "warning_code": "STALE_DATA",
                "metric_value": metrics["freshness"]["days_since_last_load"],
                "threshold": self.thresholds["freshness_days_warning"],
                "message": f"No data loaded in {metrics['freshness']['days_since_last_load']} days"
            })
        
        # Duplicate warnings
        if metrics["uniqueness"]["duplicate_rate"] > self.thresholds["duplicate_rate_warning"]:
            warnings.append({
                "warning_code": "DUPLICATE_RATE_HIGH",
                "metric_value": metrics["uniqueness"]["duplicate_rate"],
                "threshold": self.thresholds["duplicate_rate_warning"],
                "message": f"Duplicate rate ({metrics['uniqueness']['duplicate_rate']:.2%}) exceeds threshold"
            })
        
        # Domain warnings
        segment_violation_rate = 1 - metrics["domain"]["segment_coverage_rate"]
        if segment_violation_rate > self.thresholds["domain_violation_rate_warning"]:
            warnings.append({
                "warning_code": "DOMAIN_VIOLATION_SEGMENT",
                "metric_value": segment_violation_rate,
                "threshold": self.thresholds["domain_violation_rate_warning"],
                "message": f"Segment domain violation rate ({segment_violation_rate:.2%}) exceeds threshold"
            })
        
        # Audit warnings
        if metrics["audit"]["audit_violation_rate"] > self.thresholds["audit_violation_rate_warning"]:
            warnings.append({
                "warning_code": "AUDIT_INCONSISTENCY",
                "metric_value": metrics["audit"]["audit_violation_rate"],
                "threshold": self.thresholds["audit_violation_rate_warning"],
                "message": f"Audit field violation rate ({metrics['audit']['audit_violation_rate']:.2%}) exceeds threshold"
            })
        
        return warnings

    def run(self) -> Dict[str, Any]:
        """
        Executes the complete monitoring pipeline.
        
        Returns comprehensive metrics and warnings.
        NEVER raises exceptions for metric thresholds.
        """
        logger.info("Starting Enterprise Monitoring Pipeline...")
        start_time = datetime.now()
        
        try:
            # Read table
            df = self._read_table()
            
            # Compute all metrics (lazy evaluation until actions inside each method)
            volume_metrics = self._compute_volume_metrics(df)
            freshness_metrics = self._compute_freshness_metrics(df)
            distribution_metrics = self._compute_distribution_metrics(df)
            uniqueness_metrics = self._compute_uniqueness_metrics(df)
            domain_metrics = self._compute_domain_metrics(df)
            audit_metrics = self._compute_audit_metrics(df)
            
            # Consolidate metrics
            all_metrics = {
                "volume": volume_metrics,
                "freshness": freshness_metrics,
                "distribution": distribution_metrics,
                "uniqueness": uniqueness_metrics,
                "domain": domain_metrics,
                "audit": audit_metrics
            }
            
            # Generate warnings (observational, not blocking)
            warnings = self._generate_warnings(all_metrics)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            # Log warnings if any
            if warnings:
                logger.warning(f"Generated {len(warnings)} warnings (non-blocking)")
                for warning in warnings:
                    logger.warning(
                        f"[{warning['warning_code']}] {warning['message']} "
                        f"(value={warning['metric_value']}, threshold={warning['threshold']})"
                    )
            else:
                logger.info("No warnings generated - all metrics within thresholds")
            
            logger.info(
                f"Monitoring completed: {volume_metrics['total_records']} records observed, "
                f"{len(warnings)} warnings, {duration:.2f}s"
            )
            
            return {
                "status": "OK",
                "table": self.table_name,
                "monitoring_timestamp": datetime.utcnow().isoformat(),
                "duration_seconds": round(duration, 2),
                "records_observed": volume_metrics["total_records"],
                "metrics": all_metrics,
                "warnings": warnings,
                "error_message": None
            }
        
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Monitoring pipeline encountered error: {str(e)}")
            
            # Return error state but don't raise (monitoring never blocks)
            return {
                "status": "ERROR",
                "table": self.table_name,
                "monitoring_timestamp": datetime.utcnow().isoformat(),
                "duration_seconds": round(duration, 2),
                "records_observed": None,
                "metrics": None,
                "warnings": [],
                "error_message": str(e)
            }


# ------------------------------------------------------------------------------
# Public Interface
# ------------------------------------------------------------------------------
def run_monitoring_dim_products(
    spark: SparkSession = None,
    config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Public entrypoint for orchestrators and notebooks.
    
    Executes comprehensive monitoring of Silver Products dimension.
    
    IMPORTANT: This is MONITORING, not VALIDATION.
    - Never blocks the pipeline
    - Generates warnings, not exceptions
    - Observes trends, doesn't enforce rules
    
    Args:
        spark: SparkSession instance
        config: Optional configuration dict with keys:
            - table_name: Override default table
            - duplicate_rate_warning: Threshold for duplicate warning (default 0.01)
            - domain_violation_rate_warning: Threshold for domain warning (default 0.05)
            - freshness_days_warning: Days threshold for staleness (default 7)
            - min_records_warning: Minimum record count threshold (default 100)
            - audit_violation_rate_warning: Audit inconsistency threshold (default 0.02)
            - allowed_segments: List of valid segment values
            - allowed_categories: List of valid category values
    
    Returns:
        Dict with monitoring status, metrics, and warnings
        
    Never raises exceptions for threshold violations (only for infrastructure errors).
    """
    if spark is None:
        spark = (
            SparkSession.getActiveSession() 
            or SparkSession.builder.appName("silver_products_monitor").getOrCreate()
        )
    
    monitor = SilverProductsMonitor(spark, config)
    return monitor.run()


if __name__ == "__main__":
    result = run_monitoring_dim_products()
    
    # Pretty print results
    import json
    print(json.dumps(result, indent=2, default=str))