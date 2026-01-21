"""
ValidatorDimPDV: Data Quality Validation for Silver PDV Dimension

This script evaluates the data quality of the Silver PDV dimension (workspace.silver.dim_pdv),
applying explicit, business-driven validation rules.

IMPORTANT:
- This layer DOES NOT transform or correct data.
- It produces persistent, auditable validation results and aggregated metrics.
- Designed to be orchestrated as an independent step in a Silver pipeline.

Author: Senior Data Engineer (Portfolio)
"""

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window
from datetime import datetime
import logging

# ------------------------------------------------------------------------------
# Logging configuration
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ValidatorDimPDV")


class ValidatorDimPDV:
    """
    Production-grade validation class for the Silver PDV dimension.

    Responsibilities:
    - Apply business-driven validation rules
    - Persist row-level validation results
    - Persist aggregated validation metrics
    - Avoid data mutation or correction
    """

    def __init__(
        self,
        spark: SparkSession,
        silver_table: str,
        results_table: str,
        metrics_table: str
    ):
        self.spark = spark
        self.silver_table = silver_table
        self.results_table = results_table
        self.metrics_table = metrics_table
        self.current_date = datetime.now().date()

    # --------------------------------------------------------------------------
    # Data access
    # --------------------------------------------------------------------------
    def _read_silver(self) -> DataFrame:
        logger.info(f"Reading Silver table: {self.silver_table}")
        return self.spark.table(self.silver_table)

    # --------------------------------------------------------------------------
    # Validation rules
    # --------------------------------------------------------------------------
    def _validate_business_key(self, df: DataFrame) -> DataFrame:
        """
        BUSINESS KEY validation:
        - pdv_code must not be null or empty
        - must be unique per (pdv_code, load_date)
        """
        logger.info("Validating business key: pdv_code")

        df = df.withColumn(
            "is_valid_pdv_code",
            (~F.col("pdv_code").isNull()) & (F.length(F.trim(F.col("pdv_code"))) > 0)
        )

        df = df.withColumn(
            "_dup_count",
            F.count("pdv_code").over(Window.partitionBy("pdv_code", "load_date"))
        )

        df = df.withColumn("is_unique_pdv_code", F.col("_dup_count") == 1)

        df = df.withColumn(
            "is_valid_pdv_code",
            F.col("is_valid_pdv_code") & F.col("is_unique_pdv_code")
        )

        df = df.withColumn(
            "pdv_code_violation_reason",
            F.when(
                F.col("pdv_code").isNull() | (F.length(F.trim(F.col("pdv_code"))) == 0),
                "NULL_OR_EMPTY"
            ).when(
                ~F.col("is_unique_pdv_code"),
                "DUPLICATE"
            ).otherwise(F.lit(None))
        )

        return df.drop("_dup_count", "is_unique_pdv_code")

    def _validate_coordinates(self, df: DataFrame) -> DataFrame:
        """
        COORDINATES validation (WARNING-level for this domain):
        - latitude and longitude must exist
        - must be in valid geographic ranges
        - (0,0) explicitly flagged as suspicious
        """
        logger.info("Validating coordinates")

        df = df.withColumn(
            "is_valid_coordinates",
            (~F.col("latitude").isNull()) &
            (~F.col("longitude").isNull()) &
            (F.col("latitude").between(-90, 90)) &
            (F.col("longitude").between(-180, 180)) &
            (~((F.col("latitude") == 0) & (F.col("longitude") == 0)))
        )

        df = df.withColumn(
            "coordinates_violation_reason",
            F.when(F.col("latitude").isNull() | F.col("longitude").isNull(), "NULL_COORDINATE")
             .when(~F.col("latitude").between(-90, 90), "LATITUDE_OUT_OF_RANGE")
             .when(~F.col("longitude").between(-180, 180), "LONGITUDE_OUT_OF_RANGE")
             .when((F.col("latitude") == 0) & (F.col("longitude") == 0), "SUSPICIOUS_ZERO_ZERO")
             .otherwise(F.lit(None))
        )

        return df

    def _validate_domains(self, df: DataFrame) -> DataFrame:
        """
        DOMAIN validation:
        Hardcoded allowed values for transparency and auditability.
        """
        logger.info("Validating domain fields")

        allowed_channel = ["modern", "traditional", "wholesale", "other"]
        allowed_status = ["active", "inactive", "closed"]
        allowed_type_of_service = ["delivery", "in_store", "pickup"]
        allowed_country = ["MX", "US", "CA"]

        def domain_check(col, allowed):
            return (
                (~F.col(col).isNull()) &
                (F.length(F.trim(F.col(col))) > 0) &
                (F.col(col).isin(allowed))
            )

        df = df.withColumn(
            "is_valid_domain",
            domain_check("channel", allowed_channel) &
            domain_check("status", allowed_status) &
            domain_check("type_of_service", allowed_type_of_service) &
            domain_check("country", allowed_country)
        )

        df = df.withColumn(
            "domain_violation_reason",
            F.when(F.col("channel").isNull(), "CHANNEL_NULL_OR_EMPTY")
             .when(~F.col("channel").isin(allowed_channel), "CHANNEL_OUT_OF_DOMAIN")
             .when(F.col("status").isNull(), "STATUS_NULL_OR_EMPTY")
             .when(~F.col("status").isin(allowed_status), "STATUS_OUT_OF_DOMAIN")
             .when(F.col("type_of_service").isNull(), "TYPE_OF_SERVICE_NULL_OR_EMPTY")
             .when(~F.col("type_of_service").isin(allowed_type_of_service), "TYPE_OF_SERVICE_OUT_OF_DOMAIN")
             .when(F.col("country").isNull(), "COUNTRY_NULL_OR_EMPTY")
             .when(~F.col("country").isin(allowed_country), "COUNTRY_OUT_OF_DOMAIN")
             .otherwise(F.lit(None))
        )

        return df

    def _validate_logical_consistency(self, df: DataFrame) -> DataFrame:
        """
        LOGIC validation:
        - Assignment completeness
        - Active status consistency
        """
        logger.info("Validating logical consistency")

        df = df.withColumn(
            "is_logically_consistent",
            F.when(
                (F.col("assignment_complete") == True) &
                (F.col("supervisor_code").isNull() | F.col("merchandiser_code").isNull()),
                False
            ).when(
                (F.col("status") == "active") & (F.col("is_active") != True),
                False
            ).otherwise(True)
        )

        df = df.withColumn(
            "logic_violation_reason",
            F.when(
                (F.col("assignment_complete") == True) &
                (F.col("supervisor_code").isNull() | F.col("merchandiser_code").isNull()),
                "ASSIGNMENT_INCOMPLETE"
            ).when(
                (F.col("status") == "active") & (F.col("is_active") != True),
                "STATUS_ACTIVE_BUT_NOT_ACTIVE_FLAG"
            ).otherwise(F.lit(None))
        )

        return df

    def _validate_temporal_fields(self, df: DataFrame) -> DataFrame:
        """
        TEMPORAL validation:
        - load_date must not be in the future
        - record_age_days must be >= 0
        """
        logger.info("Validating temporal fields")

        return df.withColumn(
            "is_valid_temporal",
            (~F.col("load_date").isNull()) &
            (F.col("load_date") <= F.lit(self.current_date)) &
            (F.col("record_age_days") >= 0)
        )

    # --------------------------------------------------------------------------
    # Aggregation & persistence
    # --------------------------------------------------------------------------
    def _aggregate_validation_results(self, df: DataFrame):
        """
        Aggregates row-level validation into:
        - Detailed results table
        - Metrics table

        IMPORTANT:
        Explicit NULL-handling is required to avoid zero-metrics paradox
        caused by Spark NULL semantics on arrays and boolean casts.
        """
        logger.info("Aggregating validation results and metrics")

        df = df.withColumn(
            "failed_rules",
            F.array_remove(F.array(
                F.when(~F.col("is_valid_pdv_code"), "BUSINESS_KEY"),
                F.when(~F.col("is_valid_coordinates"), "COORDINATES"),
                F.when(~F.col("is_valid_domain"), "DOMAIN"),
                F.when(~F.col("is_logically_consistent"), "LOGIC"),
                F.when(~F.col("is_valid_temporal"), "TEMPORAL")
            ), None)
        )

        df = df.withColumn(
            "critical_failed_rules",
            F.array_remove(F.array(
                F.when(~F.col("is_valid_pdv_code"), "BUSINESS_KEY"),
                F.when(~F.col("is_valid_domain"), "DOMAIN"),
                F.when(~F.col("is_logically_consistent"), "LOGIC"),
                F.when(~F.col("is_valid_temporal"), "TEMPORAL")
            ), None)
        )

        # ---- CRITICAL FIX: normalize NULL arrays to empty arrays ----
        df = df.withColumn(
            "critical_failed_rules",
            F.when(
                F.col("critical_failed_rules").isNull(),
                F.array().cast("array<string>")
            ).otherwise(F.col("critical_failed_rules"))
        )

        df = df.withColumn(
            "overall_is_valid",
            F.size(F.col("critical_failed_rules")) == 0
        )

        df = df.withColumn("validation_timestamp", F.current_timestamp())

        results_df = df.select(
            "pdv_code",
            "load_date",
            "is_valid_pdv_code",
            "pdv_code_violation_reason",
            "is_valid_coordinates",
            "coordinates_violation_reason",
            "is_valid_domain",
            "domain_violation_reason",
            "is_logically_consistent",
            "logic_violation_reason",
            "is_valid_temporal",
            "overall_is_valid",
            "failed_rules",
            "critical_failed_rules",
            "validation_timestamp",
            "silver_batch_id"
        )

        # ---- Metrics aggregation (unchanged, now correct) ----
        agg_df = df.withColumn(
            "overall_is_valid_int",
            F.when(F.col("overall_is_valid") == True, F.lit(1)).otherwise(F.lit(0))
        )

        metrics_df = agg_df.agg(
            F.count("*").alias("total_records"),
            F.sum("overall_is_valid_int").alias("valid_records"),
            F.sum(F.array_contains(F.col("failed_rules"), "BUSINESS_KEY").cast("int")).alias("business_key_violations"),
            F.sum(F.array_contains(F.col("failed_rules"), "COORDINATES").cast("int")).alias("coordinates_violations"),
            F.sum(F.array_contains(F.col("failed_rules"), "DOMAIN").cast("int")).alias("domain_violations"),
            F.sum(F.array_contains(F.col("failed_rules"), "LOGIC").cast("int")).alias("logic_violations"),
            F.sum(F.array_contains(F.col("failed_rules"), "TEMPORAL").cast("int")).alias("temporal_violations")
        ).withColumn("execution_date", F.current_timestamp())

        metrics_df = metrics_df.withColumn(
            "valid_percentage",
            F.when(
                F.col("total_records") > 0,
                (F.col("valid_records") / F.col("total_records")) * 100
            ).otherwise(F.lit(0.0))
        )

        return results_df, metrics_df

    def _write_validation_tables(self, results_df: DataFrame, metrics_df: DataFrame):
        logger.info(f"Writing validation results to {self.results_table}")
        results_df.write.format("delta") \
            .mode("append") \
            .partitionBy("load_date") \
            .option("mergeSchema", "true") \
            .saveAsTable(self.results_table)

        logger.info(f"Writing validation metrics to {self.metrics_table}")
        metrics_df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(self.metrics_table)

    # --------------------------------------------------------------------------
    # Public entrypoint
    # --------------------------------------------------------------------------
    def run(self) -> DataFrame:
        logger.info("Starting ValidatorDimPDV run")
        df = self._read_silver()
        df = self._validate_business_key(df)
        df = self._validate_coordinates(df)
        df = self._validate_domains(df)
        df = self._validate_logical_consistency(df)
        df = self._validate_temporal_fields(df)
        results_df, metrics_df = self._aggregate_validation_results(df)
        self._write_validation_tables(results_df, metrics_df)
        logger.info("ValidatorDimPDV run complete")
        return metrics_df


def run_validation_dim_pdv(
    spark: SparkSession = None,
    silver_table: str = "workspace.silver.dim_pdv",
    results_table: str = "workspace.silver.validation_dim_pdv_results",
    metrics_table: str = "workspace.silver.validation_dim_pdv_metrics"
) -> DataFrame:
    """
    Orchestrator-friendly entrypoint.
    Can be executed from a Databricks notebook or locally.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    validator = ValidatorDimPDV(
        spark=spark,
        silver_table=silver_table,
        results_table=results_table,
        metrics_table=metrics_table
    )
    return validator.run()


if __name__ == "__main__":
    df_metrics = run_validation_dim_pdv()
    df_metrics.show(truncate=False)
