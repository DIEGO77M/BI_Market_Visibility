"""
MonitorDimPDV: Data Quality Monitoring for Silver PDV Dimension

This script implements a production-grade, serverless-compatible monitoring layer for the Silver PDV dimension.
It consumes only persistent validation metrics, evaluates dataset health, detects trends, and produces actionable monitoring outputs.

Key Features:
- 100% Serverless-compatible (no .collect() operations)
- Single-pass aggregations for optimal performance
- Comprehensive metric coverage (overall + detailed violations)
- Trend detection with moving averages
- Partitioned writes for efficient querying

Author: Senior Data Engineer (Portfolio)
"""

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from pyspark.sql.window import Window
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MonitoringDimPDV")

class MonitoringDimPDV:
    def __init__(self, spark: SparkSession, metrics_table: str, monitoring_table: str):
        self.spark = spark
        self.metrics_table = metrics_table
        self.monitoring_table = monitoring_table
        self.current_date = datetime.now().date()

    def _read_metrics(self) -> DataFrame:
        logger.info(f"Reading metrics table: {self.metrics_table}")
        return self.spark.table(self.metrics_table)

    def _evaluate_health_status(self, df: DataFrame) -> DataFrame:
        logger.info("Evaluating dataset health status")
        CRITICAL_THRESHOLD = 1
        VALID_PCT_SLA = 95.0
        MAJOR_THRESHOLD = 10
        df = df.withColumn(
            "has_critical_alert",
            (F.col("critical_violations") >= CRITICAL_THRESHOLD) | (F.col("valid_percentage") < VALID_PCT_SLA)
        )
        df = df.withColumn(
            "has_degraded_alert",
            (F.col("major_violations") > MAJOR_THRESHOLD) & (~F.col("has_critical_alert"))
        )
        df = df.withColumn(
            "dataset_health_status",
            F.when(F.col("has_critical_alert"), F.lit("CRITICAL"))
             .when(F.col("has_degraded_alert"), F.lit("DEGRADED"))
             .otherwise(F.lit("HEALTHY"))
        )
        return df

    def _evaluate_detailed_metrics(self, df: DataFrame) -> DataFrame:
        logger.info("Evaluating detailed violation metrics")
        BUSINESS_KEY_THRESHOLD = 5
        COORDINATES_THRESHOLD = 10
        DOMAIN_THRESHOLD = 15
        LOGIC_THRESHOLD = 8
        TEMPORAL_THRESHOLD = 5
        df = df.withColumn(
            "has_business_key_alert",
            F.col("business_key_violations") > BUSINESS_KEY_THRESHOLD
        )
        df = df.withColumn(
            "has_coordinates_alert",
            F.col("coordinates_violations") > COORDINATES_THRESHOLD
        )
        df = df.withColumn(
            "has_domain_alert",
            F.col("domain_violations") > DOMAIN_THRESHOLD
        )
        df = df.withColumn(
            "has_logic_alert",
            F.col("logic_violations") > LOGIC_THRESHOLD
        )
        df = df.withColumn(
            "has_temporal_alert",
            F.col("temporal_violations") > TEMPORAL_THRESHOLD
        )
        df = df.withColumn(
            "detailed_alerts_count",
            (F.col("has_business_key_alert").cast("int") +
             F.col("has_coordinates_alert").cast("int") +
             F.col("has_domain_alert").cast("int") +
             F.col("has_logic_alert").cast("int") +
             F.col("has_temporal_alert").cast("int"))
        )
        return df

    def _detect_trends(self, df: DataFrame) -> DataFrame:
        logger.info("Detecting trends in validation metrics (serverless-compatible)")
        # --- TRADE-OFF EXPLANATION ---
        # This moving window assumes one row per day (or execution), no missing days, and a relatively small dataset.
        # If there are gaps in the time series (missing days/executions), the moving average will still use the last 7 rows,
        # not necessarily the last 7 calendar days. This is a pragmatic choice for serverless performance and simplicity.
        # In high-frequency or large-scale environments, consider a time-based window or explicit date gap handling.
        window_spec = (
            Window
            .partitionBy()
            .orderBy(F.col("execution_date").desc())
            .rowsBetween(0, 6)
        )
        df = df.withColumn(
            "valid_pct_ma7",
            F.avg(F.col("valid_percentage")).over(window_spec)
        )
        df = df.withColumn(
            "critical_ma7",
            F.avg(F.col("critical_violations")).over(window_spec)
        )
        df = df.withColumn(
            "has_trend_warning",
            (
                (F.col("valid_percentage") < F.col("valid_pct_ma7")) |
                (F.col("critical_violations") > F.col("critical_ma7"))
            )
        )
        return df

    def _build_monitoring_output(self, df: DataFrame) -> DataFrame:
        logger.info("Building monitoring output DataFrame (serverless-compatible)")
        window_latest = Window.orderBy(F.col("execution_date").desc())
        df_latest = (
            df
            .withColumn("_rank", F.row_number().over(window_latest))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )
        df_latest = df_latest.withColumn(
            "monitoring_summary",
            F.when(
                F.col("dataset_health_status") == "CRITICAL",
                F.concat(
                    F.lit("CRITICAL: Data quality risk detected. "),
                    F.lit("Valid %: "), F.round(F.col("valid_percentage"), 2).cast("string"), F.lit("%. "),
                    F.lit("Critical violations: "), F.col("critical_violations").cast("string"), F.lit(". "),
                    F.lit("Immediate action required.")
                )
            ).when(
                F.col("dataset_health_status") == "DEGRADED",
                F.concat(
                    F.lit("DEGRADED: Quality issues detected. "),
                    F.lit("Major violations: "), F.col("major_violations").cast("string"), F.lit(". "),
                    F.lit("Monitor and address violations.")
                )
            ).otherwise(
                F.concat(
                    F.lit("HEALTHY: Dataset quality is good. "),
                    F.lit("Valid %: "), F.round(F.col("valid_percentage"), 2).cast("string"), F.lit("%.")
                )
            )
        )
        return df_latest.select(
            "execution_date",
            "dataset_health_status",
            "valid_percentage",
            "total_records",
            "valid_records",
            "critical_violations",
            "major_violations",
            "minor_violations",
            "business_key_violations",
            "coordinates_violations",
            "domain_violations",
            "logic_violations",
            "temporal_violations",
            "has_critical_alert",
            "has_degraded_alert",
            "has_business_key_alert",
            "has_coordinates_alert",
            "has_domain_alert",
            "has_logic_alert",
            "has_temporal_alert",
            "detailed_alerts_count",
            "has_trend_warning",
            "valid_pct_ma7",
            "critical_ma7",
            "monitoring_summary"
        )

    def _write_monitoring_table(self, df: DataFrame):
        logger.info(f"Writing monitoring output to {self.monitoring_table}")
        df.write.format("delta").mode("append").partitionBy("execution_date").option("mergeSchema", "true").saveAsTable(self.monitoring_table)

    def run(self) -> DataFrame:
        logger.info("Starting MonitoringDimPDV run (serverless-optimized)")
        try:
            df_metrics = self._read_metrics()
            df_health = self._evaluate_health_status(df_metrics)
            df_detailed = self._evaluate_detailed_metrics(df_health)
            df_trends = self._detect_trends(df_detailed)
            df_monitoring = self._build_monitoring_output(df_trends)
            self._write_monitoring_table(df_monitoring)
            logger.info("MonitoringDimPDV run complete")
            return df_monitoring
        except Exception as e:
            logger.error(f"Critical error in MonitoringDimPDV: {e}", exc_info=True)
            raise

def run_monitoring_dim_pdv(
    spark: SparkSession = None,
    metrics_table: str = "workspace.silver.validation_dim_pdv_metrics",
    monitoring_table: str = "workspace.silver.monitoring_dim_pdv"
) -> DataFrame:
    """
    Runs the MonitoringDimPDV for orchestration or local testing.
    
    Args:
        spark: SparkSession instance. If None, creates a new one (for local testing).
        metrics_table: Fully qualified name of the validation metrics table.
        monitoring_table: Fully qualified name of the monitoring output table.
    
    Returns:
        DataFrame: Monitoring results with health status and alerts.
    
    Example:
        # In Databricks notebook
        monitoring_df = run_monitoring_dim_pdv(spark)
        
        # For local testing
        monitoring_df = run_monitoring_dim_pdv()
    """
    local_spark = False
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
        local_spark = True
    monitor = MonitoringDimPDV(
        spark=spark,
        metrics_table=metrics_table,
        monitoring_table=monitoring_table
    )
    monitoring_df = monitor.run()
    if local_spark:
        logger.info("Monitoring finished in local mode.")
    return monitoring_df

if __name__ == "__main__":
    # Local test/demo run
    logger.info("Running MonitoringDimPDV in standalone mode")
    df = run_monitoring_dim_pdv()
    df.show(truncate=False)