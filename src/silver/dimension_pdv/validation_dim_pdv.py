"""
DimPDVValidator: Data Quality Validation for Silver PDV Dimension

This script evaluates the data quality of the Silver PDV dimension (workspace.silver.dim_pdv),
applying explicit, business-driven validation rules. It produces persistent, auditable validation results
and aggregated metrics, without transforming or correcting data.

Author: Senior Data Engineer (Portfolio)
"""

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from pyspark.sql.window import Window
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DimPDVValidator")

class DimPDVValidator:
    def __init__(self, spark: SparkSession, silver_table: str, results_table: str, metrics_table: str):
        self.spark = spark
        self.silver_table = silver_table
        self.results_table = results_table
        self.metrics_table = metrics_table
        self.current_date = datetime.now().date()

    def _read_silver(self) -> DataFrame:
        logger.info(f"Reading Silver table: {self.silver_table}")
        return self.spark.table(self.silver_table)

    def _validate_business_key(self, df: DataFrame) -> DataFrame:
        logger.info("Validating business key: pdv_code")
        df = df.withColumn(
            "is_valid_pdv_code",
            (~F.col("pdv_code").isNull()) & (F.length(F.trim(F.col("pdv_code"))) > 0)
        )
        # Uniqueness by (pdv_code, load_date)
        df = df.withColumn(
            "_dup_count",
            F.count("pdv_code").over(
                Window.partitionBy("pdv_code", "load_date")
            )
        )
        df = df.withColumn(
            "is_unique_pdv_code",
            F.col("_dup_count") == 1
        )
        df = df.withColumn(
            "is_valid_pdv_code",
            F.col("is_valid_pdv_code") & F.col("is_unique_pdv_code")
        )
        df = df.withColumn(
            "pdv_code_violation_reason",
            F.when(F.col("pdv_code").isNull() | (F.length(F.trim(F.col("pdv_code"))) == 0), "NULL_OR_EMPTY")
             .when(~F.col("is_unique_pdv_code"), "DUPLICATE")
             .otherwise(F.lit(None))
        )
        df = df.drop("_dup_count", "is_unique_pdv_code")
        return df

    def _validate_coordinates(self, df: DataFrame) -> DataFrame:
        logger.info("Validating coordinates: latitude, longitude")
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
        logger.info("Validating domain fields: channel, status, type_of_service, country")
        # --- BUSINESS TRADE-OFF ---
        # Allowed domains are hardcoded for transparency and auditability.
        # In a production-grade solution, these should be parameterized or loaded from a config table or data contract.
        # This approach is defensible in interviews to show explicit business logic and control.
        allowed_channel = ["modern", "traditional", "wholesale", "other"]
        allowed_status = ["active", "inactive", "closed"]
        allowed_type_of_service = ["delivery", "in_store", "pickup"]
        allowed_country = ["MX", "US", "CA"]

        def domain_check(col, allowed):
            return (~F.col(col).isNull()) & (F.length(F.trim(F.col(col))) > 0) & (F.col(col).isin(allowed))

        df = df.withColumn(
            "is_valid_domain",
            domain_check("channel", allowed_channel) &
            domain_check("status", allowed_status) &
            domain_check("type_of_service", allowed_type_of_service) &
            domain_check("country", allowed_country)
        )
        df = df.withColumn(
            "domain_violation_reason",
            F.when(F.col("channel").isNull() | (F.length(F.trim(F.col("channel"))) == 0), "CHANNEL_NULL_OR_EMPTY")
             .when(~F.col("channel").isin(allowed_channel), "CHANNEL_OUT_OF_DOMAIN")
             .when(F.col("status").isNull() | (F.length(F.trim(F.col("status"))) == 0), "STATUS_NULL_OR_EMPTY")
             .when(~F.col("status").isin(allowed_status), "STATUS_OUT_OF_DOMAIN")
             .when(F.col("type_of_service").isNull() | (F.length(F.trim(F.col("type_of_service"))) == 0), "TYPE_OF_SERVICE_NULL_OR_EMPTY")
             .when(~F.col("type_of_service").isin(allowed_type_of_service), "TYPE_OF_SERVICE_OUT_OF_DOMAIN")
             .when(F.col("country").isNull() | (F.length(F.trim(F.col("country"))) == 0), "COUNTRY_NULL_OR_EMPTY")
             .when(~F.col("country").isin(allowed_country), "COUNTRY_OUT_OF_DOMAIN")
             .otherwise(F.lit(None))
        )
        return df

    def _validate_logical_consistency(self, df: DataFrame) -> DataFrame:
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
            F.when((F.col("assignment_complete") == True) & (F.col("supervisor_code").isNull() | F.col("merchandiser_code").isNull()), "ASSIGNMENT_INCOMPLETE")
             .when((F.col("status") == "active") & (F.col("is_active") != True), "STATUS_ACTIVE_BUT_NOT_ACTIVE_FLAG")
             .otherwise(F.lit(None))
        )
        return df

    def _validate_temporal_fields(self, df: DataFrame) -> DataFrame:
        logger.info("Validating temporal fields: load_date")
        df = df.withColumn(
            "is_valid_temporal",
            (~F.col("load_date").isNull()) &
            (F.col("load_date") <= F.lit(self.current_date)) &
            (F.col("record_age_days") >= 0)
        )
        return df

    def _aggregate_validation_results(self, df: DataFrame) -> (DataFrame, DataFrame):
        logger.info("Aggregating validation results and metrics (serverless compatible)")
        # 1. Define validation columns (unchanged)
        failed_rules_expr = F.array_remove(F.array(
            F.when(~F.col("is_valid_pdv_code"), F.lit("BUSINESS_KEY")).otherwise(F.lit(None)),
            F.when(~F.col("is_valid_coordinates"), F.lit("COORDINATES")).otherwise(F.lit(None)),
            F.when(~F.col("is_valid_domain"), F.lit("DOMAIN")).otherwise(F.lit(None)),
            F.when(~F.col("is_logically_consistent"), F.lit("LOGIC")).otherwise(F.lit(None)),
            F.when(~F.col("is_valid_temporal"), F.lit("TEMPORAL")).otherwise(F.lit(None))
        ), None)

        df = df.withColumn("failed_rules", failed_rules_expr)
        df = df.withColumn("overall_is_valid", F.size(F.col("failed_rules")) == 0)
        df = df.withColumn("validation_timestamp", F.current_timestamp())

        results_df = df.select(
            "pdv_code", "load_date", "is_valid_pdv_code", "pdv_code_violation_reason",
            "is_valid_coordinates", "coordinates_violation_reason",
            "is_valid_domain", "domain_violation_reason",
            "is_logically_consistent", "logic_violation_reason",
            "is_valid_temporal", "overall_is_valid", "failed_rules",
            "validation_timestamp", "silver_batch_id"
        )

        # 2. SERVERLESS FIX: Use a list of expressions instead of a dictionary
        rules = ["BUSINESS_KEY", "COORDINATES", "DOMAIN", "LOGIC", "TEMPORAL"]
        severity_map = {
            "BUSINESS_KEY": "CRITICAL",
            "COORDINATES": "MAJOR",
            "DOMAIN": "MINOR",
            "LOGIC": "MAJOR",
            "TEMPORAL": "MAJOR"
        }

        # Create a list of aggregation columns with alias
        agg_list = [
            F.count(F.lit(1)).alias("total_records"),
            F.sum(F.col("overall_is_valid").cast("int")).alias("valid_records")
        ]
        # Add metrics per rule
        for rule in rules:
            agg_list.append(
                F.sum(F.array_contains(F.col("failed_rules"), rule).cast("int")).alias(f"{rule.lower()}_violations")
            )

        # Execute aggregation by unpacking the list
        metrics_row = df.agg(*agg_list).withColumn("execution_date", F.current_timestamp())

        # 3. Severity and percentage calculation (remains the same but on metrics_row)
        for sev in ["CRITICAL", "MAJOR", "MINOR"]:
            rules_for_sev = [rule for rule, s in severity_map.items() if s == sev]
            sum_expr = F.lit(0)
            for rule in rules_for_sev:
                sum_expr = sum_expr + F.col(f"{rule.lower()}_violations")
            metrics_row = metrics_row.withColumn(f"{sev.lower()}_violations", sum_expr)

        metrics_row = metrics_row.withColumn(
            "valid_percentage",
            F.when(F.col("total_records") > 0, (F.col("valid_records") / F.col("total_records")) * 100).otherwise(F.lit(0))
        )

        metrics_cols = [
            "total_records", "valid_records", "valid_percentage"
        ] + [f"{rule.lower()}_violations" for rule in rules] + [f"{sev.lower()}_violations" for sev in ["CRITICAL", "MAJOR", "MINOR"]] + ["execution_date"]
        metrics_df = metrics_row.select(*metrics_cols)
        return results_df, metrics_df

    def _write_validation_tables(self, results_df: DataFrame, metrics_df: DataFrame):
        logger.info(f"Writing validation results to {self.results_table}")
        results_df.write.format("delta").mode("append").partitionBy("load_date").option("overwriteSchema", "true").saveAsTable(self.results_table)
        logger.info(f"Writing validation metrics to {self.metrics_table}")
        metrics_df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable(self.metrics_table)

    def run(self):
        logger.info("Starting DimPDVValidator run")
        try:
            df = self._read_silver()
            df = self._validate_business_key(df)
            df = self._validate_coordinates(df)
            df = self._validate_domains(df)
            df = self._validate_logical_consistency(df)
            df = self._validate_temporal_fields(df)
            results_df, metrics_df = self._aggregate_validation_results(df)
            self._write_validation_tables(results_df, metrics_df)
            logger.info("DimPDVValidator run complete")
            return metrics_df
        except Exception as e:
            logger.error(f"Critical error in DimPDVValidator: {e}", exc_info=True)
            raise

def run_validation_dim_pdv(
    spark: SparkSession = None,
    silver_table: str = "workspace.silver.dim_pdv",
    results_table: str = "workspace.silver.validation_dim_pdv_results",
    metrics_table: str = "workspace.silver.validation_dim_pdv_metrics"
) -> 'DataFrame':
    """
    Runs the DimPDVValidator for orchestration or local testing.
    If no SparkSession is provided, creates a new one (for local test).
    Returns the metrics DataFrame.
    """
    local_spark = False
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
        local_spark = True
    validator = DimPDVValidator(
        spark=spark,
        silver_table=silver_table,
        results_table=results_table,
        metrics_table=metrics_table
    )
    metrics_df = validator.run()
    if local_spark:
        logger.info("Validation finished in local mode.")
    return metrics_df


if __name__ == "__main__":
    # Local test/demo run
    metrics_df = run_validation_dim_pdv()
    metrics_df.show(truncate=False)
