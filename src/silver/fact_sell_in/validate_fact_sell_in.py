"""
Silver Fact Table Validation Script: Sell-In
Author: Diego Mayorga

Purpose:
    Validates the integrity, schema, grain, and lineage of the silver.fact_sell_in table after transformation from bronze.sell_in.
    Ensures auditability, idempotence, and business contract compliance for downstream analytics and governance.

Technical Context:
    - Databricks Serverless (Unity Catalog)
    - Delta Lake, Medallion Architecture
    - Table: silver.fact_sell_in (partitioned by year_month)
    - Read-only, deterministic, re-executable

Validation Scope:
    - No data modification, no enrichment, no schema changes
    - Explicitly fails on any critical contract violation
    - Returns structured metrics for orchestration and audit
"""


import time
import datetime
import logging
from typing import Any, Dict
from pyspark.sql import SparkSession, functions as F, types as T

EXPECTED_SCHEMA = [
    ("pdv_code", "string"),
    ("product_code", "string"),
    ("year", "int"),
    ("month", "int"),
    ("year_month", "int"),
    ("opening_stock_units", "int"),
    ("sell_in_units", "int"),
    ("returns_units", "int"),
    ("closing_stock_units", "int"),
    ("days_of_inventory", "int"),
    ("inventory_turnover", "decimal(10,2)"),
    ("replenishment_flag", "string"),
    ("stock_risk_level", "string"),
    ("ingestion_timestamp", "timestamp"),
    ("load_date", "date"),
    ("source_file", "string"),
    ("bronze_batch_id", "string"),
    ("silver_processed_at", "timestamp"),
    ("silver_batch_id", "string")
]


FACT_TABLE = "silver.fact_sell_in"


# Custom exception for validation errors
class ValidationError(Exception):
    """Raised when data quality validation fails."""
    pass

class SilverFactSellInValidator:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        # Ensure logging is configured only once (prevents duplicate handlers)
        if not self.logger.hasHandlers():
            logging.basicConfig(level=logging.INFO)

    def validate(self) -> Dict[str, Any]:
        start = time.time()
        failed_validations = 0
        validation_results = {}
        status = "success"

        # 1. Existence and schema validation
        if not self.spark.catalog.tableExists(FACT_TABLE):
            self.logger.error(f"Table {FACT_TABLE} does not exist")
            raise ValidationError(f"Table {FACT_TABLE} does not exist")
        df = self.spark.table(FACT_TABLE)
        actual_schema = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
        expected_schema = EXPECTED_SCHEMA

        schema_check = "PASS" if actual_schema == expected_schema else "FAIL"
        if schema_check == "FAIL":
            failed_validations += 1
            status = "failed"
        validation_results["schema_check"] = {
            "status": schema_check,
            "expected_schema": expected_schema,
            "actual_schema": actual_schema
        }

        # 2. Dimensional key validation (not null, uppercase)
        key_cols = ["pdv_code", "product_code", "year_month"]
        key_nulls = df.filter(
            F.col("pdv_code").isNull() | F.col("product_code").isNull()
        ).count()
        key_format = df.filter(
            (F.col("pdv_code") != F.upper(F.col("pdv_code"))) |
            (F.col("product_code") != F.upper(F.col("product_code")))
        ).count()
        key_check = "PASS" if key_nulls == 0 and key_format == 0 else "FAIL"
        if key_check == "FAIL":
            failed_validations += 1
            status = "failed"
        validation_results["dimensional_key_check"] = {
            "status": key_check,
            "null_keys": key_nulls,
            "format_issues": key_format
        }

        # 3. Grain & uniqueness validation (groupBy required, kept as a separate scan)
        dup_count = df.groupBy(key_cols).count().filter(F.col("count") > 1).count()
        grain_check = "PASS" if dup_count == 0 else "FAIL"
        if grain_check == "FAIL":
            failed_validations += 1
            status = "failed"
        validation_results["grain_check"] = {
            "status": grain_check,
            "duplicate_key_count": dup_count
        }

        # 4-6. All other validations in a single scan (temporal, metrics, flags, lineage)
        current_year = datetime.datetime.now().year
        temporal_expr = (
            (F.col("year_month") == F.col("year") * 100 + F.col("month")) &
            (F.col("month").between(1, 12)) &
            (F.col("year").between(2020, current_year + 1)) &
            (F.col("year_month").isNotNull()) &
            (F.col("load_date").isNotNull()) &
            (F.col("silver_processed_at") >= F.col("ingestion_timestamp"))
        )
        metric_cols = [
            "opening_stock_units", "sell_in_units", "returns_units",
            "closing_stock_units", "days_of_inventory", "inventory_turnover"
        ]
        metrics_expr = F.lit(True)
        for col in metric_cols:
            metrics_expr = metrics_expr & (F.col(col).isNotNull()) & (F.col(col) >= 0)
        flag_expr = (
            F.col("replenishment_flag").isNotNull() &
            F.col("stock_risk_level").isNotNull()
        )
        lineage_expr = (
            F.col("bronze_batch_id").isNotNull() &
            F.col("silver_batch_id").isNotNull()
        )

        validation_agg = df.agg(
            F.count("*").alias("total_records"),
            F.sum(F.when(~temporal_expr, 1).otherwise(0)).alias("temporal_failures"),
            F.sum(F.when(~metrics_expr, 1).otherwise(0)).alias("metrics_failures"),
            F.sum(F.when(~flag_expr, 1).otherwise(0)).alias("flag_failures"),
            F.sum(F.when(~lineage_expr, 1).otherwise(0)).alias("lineage_failures")
        ).collect()[0]

        total_records = validation_agg["total_records"]
        temporal_failures = validation_agg["temporal_failures"]
        metrics_failures = validation_agg["metrics_failures"]
        flag_failures = validation_agg["flag_failures"]
        lineage_failures = validation_agg["lineage_failures"]


        temporal_check = "PASS" if temporal_failures == 0 else "FAIL"
        if temporal_check == "FAIL":
            failed_validations += 1
            status = "failed"
        validation_results["temporal_check"] = {
            "status": temporal_check,
            "failures": temporal_failures
        }

        metrics_check = "PASS" if metrics_failures == 0 else "FAIL"
        if metrics_check == "FAIL":
            failed_validations += 1
            status = "failed"
        validation_results["metrics_check"] = {
            "status": metrics_check,
            "failures": metrics_failures
        }

        flag_check = "PASS" if flag_failures == 0 else "FAIL"
        lineage_check = "PASS" if lineage_failures == 0 else "FAIL"
        if flag_check == "FAIL" or lineage_check == "FAIL":
            failed_validations += 1
            status = "failed"
        validation_results["flags_check"] = {
            "status": flag_check,
            "failures": flag_failures
        }
        validation_results["lineage_check"] = {
            "status": lineage_check,
            "failures": lineage_failures
        }

        result = {
            "table": FACT_TABLE,
            "total_records": total_records,
            "failed_validations": failed_validations,
            "validation_results": validation_results,
            "status": status,
            "duration_seconds": round(time.time() - start, 2)
        }
        
        # Always log validation summary for audit and monitoring
        self.logger.info(
            f"Validation completed: status={status}, total_records={total_records}, "
            f"failed_validations={failed_validations}, duration={round(time.time() - start, 2)}s"
        )
        if status == "failed":
            self.logger.error(f"Validation failed with {failed_validations} issues: {validation_results}")
            raise ValidationError(f"Validation failed: {validation_results}")
        return result


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    validator = SilverFactSellInValidator(spark)
    result = validator.validate()
    print(result)
