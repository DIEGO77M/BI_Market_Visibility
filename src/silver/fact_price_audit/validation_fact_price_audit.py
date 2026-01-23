"""
Validation module for Price Audit (Silver Layer)

This module implements enterprise-grade, deterministic data quality validation
for the Price Audit Silver fact table, fully aligned with the Transformation
layer design and Medallion Architecture best practices.

Design principles:
    - Validation-only responsibility (no data mutation)
    - Declarative, extensible validation rules
    - Explicit severity governance and pipeline decisioning
    - Serverless-optimized Spark execution (minimal actions)
    - Unity Catalog compliant Delta alert persistence
    - Orchestrator-ready with local execution support

Author: Diego Mayorga
"""

import logging
import uuid
import time
from datetime import datetime
from typing import Optional, Dict, Any, List

from pyspark.sql import SparkSession, DataFrame, functions as F

# ------------------------------------------------------------------------------
# Logging configuration
# ------------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ------------------------------------------------------------------------------
# Declarative validation rule definition
# ------------------------------------------------------------------------------
class ValidationRule:
    """
    Represents a single declarative validation rule.

    A rule defines:
        - name: unique identifier
        - severity: P0 / P1 / P2
        - condition: Spark SQL boolean expression identifying invalid records
        - description: human-readable explanation
        - affected_column: column(s) involved (for alerting)
        - blocking: whether this rule can fail the pipeline
    """

    def __init__(
        self,
        name: str,
        severity: str,
        condition,
        description: str,
        affected_column: str,
        blocking: bool,
    ):
        self.name = name
        self.severity = severity
        self.condition = condition
        self.description = description
        self.affected_column = affected_column
        self.blocking = blocking


# ------------------------------------------------------------------------------
# Main validator class
# ------------------------------------------------------------------------------
class SilverPriceAuditValidator:
    """
    Validates the Silver Price Audit fact table for data quality issues
    and emits structured alerts for external consumption.

    No transformations or corrections are applied to the data.
    """

    def __init__(self, spark: SparkSession, config: Optional[dict] = None):
        self.spark = spark
        self.config = config or {}

        # Tables (do not modify explicit names)
        self.source_table = self.config.get(
            "source_table", "workspace.silver.fact_price_audit"
        )
        self.alerts_table = self.config.get(
            "alerts_table", "workspace.silver.validation_alerts_price_audit"
        )

        # Execution control
        self.write_alerts = self.config.get("write_alerts", True)

        # Validation metadata
        self.validation_timestamp = datetime.utcnow().isoformat()

        # Contract with Transformation layer
        self.required_columns = [
            "date",
            "pdv_code",
            "product_code",
            "price",
            "has_promotion",
            "promotional_price",
            "load_date",
            "silver_batch_id",
            "silver_processed_at",
        ]

        # Severity governance
        # P0 => always blocking
        # P1 => configurable
        # P2 => never blocking
        self.fail_on_severities = self.config.get(
            "fail_on_severities", ["P0"]
        )

        # Declarative validation rules
        self.rules = self._define_rules()

    # --------------------------------------------------------------------------
    # Data access
    # --------------------------------------------------------------------------
    def _read_silver(self) -> DataFrame:
        """
        Reads the Silver fact table and validates the schema contract.
        """
        logger.info(f"Reading Silver table: {self.source_table}")
        df = self.spark.table(self.source_table)

        missing = set(self.required_columns) - set(df.columns)
        if missing:
            raise ValueError(
                f"Missing required columns in Silver table: {missing}"
            )

        return df

    # --------------------------------------------------------------------------
    # Rule definition
    # --------------------------------------------------------------------------
    def _define_rules(self) -> List[ValidationRule]:
        """
        Defines all validation rules in a declarative, extensible manner.
        """
        return [
            # ------------------------------------------------------------------
            # Null checks (P0)
            # ------------------------------------------------------------------
            ValidationRule(
                name="null_pdv_code",
                severity="P0",
                condition=F.col("pdv_code").isNull(),
                description="Null values found in pdv_code",
                affected_column="pdv_code",
                blocking=True,
            ),
            ValidationRule(
                name="null_product_code",
                severity="P0",
                condition=F.col("product_code").isNull(),
                description="Null values found in product_code",
                affected_column="product_code",
                blocking=True,
            ),
            ValidationRule(
                name="null_date",
                severity="P0",
                condition=F.col("date").isNull(),
                description="Null values found in date",
                affected_column="date",
                blocking=True,
            ),
            ValidationRule(
                name="null_price",
                severity="P0",
                condition=F.col("price").isNull(),
                description="Null values found in price",
                affected_column="price",
                blocking=True,
            ),
            # ------------------------------------------------------------------
            # Business rules
            # ------------------------------------------------------------------
            ValidationRule(
                name="price_must_be_positive",
                severity="P0",
                condition=F.col("price") <= 0,
                description="Price must be greater than 0",
                affected_column="price",
                blocking=True,
            ),
            ValidationRule(
                name="promotional_price_non_negative",
                severity="P1",
                condition=(
                    F.col("promotional_price").isNotNull()
                    & (F.col("promotional_price") < 0)
                ),
                description="Promotional price must be non-negative",
                affected_column="promotional_price",
                blocking=False,
            ),
            ValidationRule(
                name="promotional_price_less_than_price",
                severity="P0",
                condition=(
                    F.col("promotional_price").isNotNull()
                    & (F.col("promotional_price") >= F.col("price"))
                ),
                description="Promotional price must be less than price",
                affected_column="promotional_price",
                blocking=True,
            ),
            # ------------------------------------------------------------------
            # Consistency rules
            # ------------------------------------------------------------------
            ValidationRule(
                name="promo_price_without_flag",
                severity="P1",
                condition=(
                    (F.col("has_promotion") == False)
                    & F.col("promotional_price").isNotNull()
                ),
                description="promotional_price present but has_promotion is False",
                affected_column="promotional_price",
                blocking=False,
            ),
            ValidationRule(
                name="flag_without_promo_price",
                severity="P1",
                condition=(
                    (F.col("has_promotion") == True)
                    & F.col("promotional_price").isNull()
                ),
                description="has_promotion is True but promotional_price is NULL",
                affected_column="promotional_price",
                blocking=False,
            ),
        ]

    # --------------------------------------------------------------------------
    # Rule execution
    # --------------------------------------------------------------------------
    def _evaluate_rules(self, df: DataFrame) -> DataFrame:
        """
        Evaluates all validation rules in a single Spark pass where possible.

        Returns a DataFrame with aggregated alert metrics per rule.
        """
        logger.info("Evaluating validation rules")

        aggregations = []
        for rule in self.rules:
            aggregations.append(
                F.sum(
                    F.when(rule.condition, F.lit(1)).otherwise(F.lit(0))
                ).alias(rule.name)
            )

        metrics_row = df.agg(*aggregations).collect()[0].asDict()

        alert_rows = []
        for rule in self.rules:
            count = metrics_row.get(rule.name, 0)
            if count > 0:
                alert_rows.append({
                    "alert_id": str(uuid.uuid4()),
                    "alert_type": rule.name,
                    "alert_severity": rule.severity,
                    "alert_description": rule.description,
                    "affected_column": rule.affected_column,
                    "affected_records_count": int(count),
                    "validation_timestamp": self.validation_timestamp,
                    "source_table": self.source_table,
                    "is_blocking": rule.blocking,
                })

        return self.spark.createDataFrame(alert_rows) if alert_rows else None

    # --------------------------------------------------------------------------
    # Duplicate check (explicit post-MERGE contract)
    # --------------------------------------------------------------------------
    def _check_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Detects violations of Silver uniqueness guarantees after MERGE.
        """
        dup_df = (
            df.groupBy("date", "pdv_code", "product_code")
            .count()
            .filter(F.col("count") > 1)
        )

        if dup_df.count() == 0:
            return None

        return dup_df.select(
            F.lit(str(uuid.uuid4())).alias("alert_id"),
            F.lit("duplicate_business_key").alias("alert_type"),
            F.lit("P2").alias("alert_severity"),
            F.lit(
                "Duplicate records found by (date, pdv_code, product_code)"
            ).alias("alert_description"),
            F.lit("date,pdv_code,product_code").alias("affected_column"),
            F.col("count").alias("affected_records_count"),
            F.lit(self.validation_timestamp).alias("validation_timestamp"),
            F.lit(self.source_table).alias("source_table"),
            F.lit(False).alias("is_blocking"),
        )

    # --------------------------------------------------------------------------
    # Alert persistence
    # --------------------------------------------------------------------------
    def _write_alerts(self, alert_df: DataFrame) -> None:
        """
        Writes validation alerts as append-only Delta rows.
        """
        if not self.write_alerts:
            logger.info("Alert writing disabled (local testing mode)")
            return

        if not self.spark.catalog.tableExists(self.alerts_table):
            logger.info("Creating alerts Delta table")
            (
                alert_df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(self.alerts_table)
            )
        else:
            alert_df.write.format("delta").mode("append").saveAsTable(
                self.alerts_table
            )

    # --------------------------------------------------------------------------
    # Orchestration
    # --------------------------------------------------------------------------
    def run(self) -> Dict[str, Any]:
        """
        Orchestrates the full validation lifecycle and returns a structured
        result for pipeline decisioning.
        """
        start = time.time()
        status = "SUCCESS"
        error_message = None

        try:
            df = self._read_silver()

            rule_alerts = self._evaluate_rules(df)
            dup_alerts = self._check_duplicates(df)

            all_alerts = None
            if rule_alerts and dup_alerts:
                all_alerts = rule_alerts.unionByName(dup_alerts)
            elif rule_alerts:
                all_alerts = rule_alerts
            elif dup_alerts:
                all_alerts = dup_alerts

            total_alerts = 0
            max_severity = None
            should_fail_pipeline = False

            if all_alerts:
                total_alerts = all_alerts.count()
                max_severity = (
                    all_alerts
                    .select(F.max("alert_severity"))
                    .first()[0]
                )

                if (
                    all_alerts
                    .filter(
                        (F.col("is_blocking") == True)
                        & (F.col("alert_severity").isin(self.fail_on_severities))
                    )
                    .count()
                    > 0
                ):
                    should_fail_pipeline = True
                    status = "FAILURE"

                self._write_alerts(all_alerts)

        except Exception as e:
            logger.error(f"Validation error: {e}")
            status = "FAILURE"
            error_message = str(e)

        duration = round(time.time() - start, 2)

        return {
            "status": status,
            "source_table": self.source_table,
            "alerts_table": self.alerts_table,
            "total_alerts": total_alerts,
            "max_severity": max_severity,
            "should_fail_pipeline": should_fail_pipeline,
            "execution_timestamp": self.validation_timestamp,
            "duration_seconds": duration,
            "error_message": error_message,
        }


# ------------------------------------------------------------------------------
# Public orchestrator entrypoint
# ------------------------------------------------------------------------------
def run_price_audit_validation(
    spark: SparkSession, config: Optional[dict] = None
) -> dict:
    """
    Public entrypoint for orchestration (Databricks Jobs, Workflows, notebooks).
    """
    validator = SilverPriceAuditValidator(spark, config)
    return validator.run()


# ------------------------------------------------------------------------------
# Local testing entrypoint
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("LOCAL TESTING MODE - Price Audit Silver Validation")
    print("=" * 70 + "\n")

    spark = SparkSession.builder.appName(
        "PriceAuditValidationLocal"
    ).getOrCreate()

    local_config = {
        "write_alerts": False,
    }

    result = run_price_audit_validation(spark, local_config)

    # Extra: Mostrar detalle de alertas generadas en consola (solo local)
    print("\n" + "-"*70)
    print("ALERT DETAILS (local run)")
    print("-"*70)
    try:
        # Re-ejecutar el validador para obtener el DataFrame de alertas (sin escribir)
        validator = SilverPriceAuditValidator(spark, local_config)
        df = validator._read_silver()
        rule_alerts = validator._evaluate_rules(df)
        dup_alerts = validator._check_duplicates(df)
        all_alerts = None
        if rule_alerts and dup_alerts:
            all_alerts = rule_alerts.unionByName(dup_alerts)
        elif rule_alerts:
            all_alerts = rule_alerts
        elif dup_alerts:
            all_alerts = dup_alerts
        if all_alerts:
            alerts = all_alerts.collect()
            for alert in alerts:
                print(f"Type: {alert['alert_type']}")
                print(f"Severity: {alert['alert_severity']}")
                print(f"Column: {alert['affected_column']}")
                print(f"Description: {alert['alert_description']}")
                print(f"Affected Records: {alert['affected_records_count']}")
                print("-"*40)
        else:
            print("No alerts generated.")
    except Exception as e:
        print(f"[WARN] Could not print alert details: {e}")

    print("\n" + "="*70)
    for k, v in result.items():
        print(f"{k:.<30} {v}")
