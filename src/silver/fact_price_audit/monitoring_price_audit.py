"""
Monitoring module for Price Audit (Silver Layer)

This module implements enterprise-grade pipeline monitoring and observability for the Price Audit Silver pipeline, fully aligned with Medallion Architecture and Databricks Serverless best practices.

Design principles:
    - Monitoring-only responsibility (no data transformation or validation)
    - Deterministic, idempotent, and orchestrator-ready
    - Graceful handling of missing audit logs in local/testing
    - Unity Catalog compliant Delta log persistence
    - Clear separation of control plane and business logic

Author: Diego Mayorga
"""

import logging
import uuid
from datetime import datetime
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame, functions as F

# ------------------------------------------------------------------------------
# Logging configuration
# ------------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ------------------------------------------------------------------------------
# Main monitoring class
# ------------------------------------------------------------------------------
class SilverPriceAuditMonitoring:
        SEVERITY_ORDER = {"P0": 3, "P1": 2, "P2": 1}

        def _normalize_severity(self, severity: Optional[str]) -> Optional[str]:
            return severity if severity in self.SEVERITY_ORDER else None
        """
        Consolidates transformation, validation, and execution metadata
        into a unified monitoring log. Monitoring-only, no data transformation.

        Each execution emits a new monitoring record; retries are intentionally recorded as independent events.
        """
        def __init__(self, spark: SparkSession, validation_result: dict, config: Optional[dict] = None):
            self.spark = spark
            self.validation_result = validation_result or {}
            # Minimal validation of required fields in validation_result
            required_fields = ["status", "total_alerts", "max_severity"]
            missing = [f for f in required_fields if f not in self.validation_result]
            if missing:
                logger.warning(f"validation_result missing fields: {missing}")
            self.config = config or {}
            self.transformation_audit_log = self.config.get(
                "transformation_audit_log", "workspace.silver.transformation_audit_log"
            )
            self.monitoring_table = self.config.get(
                "monitoring_table", "workspace.silver.pipeline_monitoring_log"
            )
            self.write_monitoring_log = self.config.get("write_monitoring_log", True)
            self.environment = self.config.get("environment", "prod")
            self.pipeline_name = self.config.get("pipeline_name", "price_audit_silver_pipeline")
            self.created_at = datetime.utcnow().isoformat()

        def _read_transformation_audit_log(self) -> Optional[dict]:
            """
            Reads the latest transformation audit log entry.
            Returns None if the table does not exist or is empty.
            Optimized for serverless: avoids full table scan.
            """
            logger.info(f"Reading transformation audit log: {self.transformation_audit_log}")
            try:
                if not self.spark.catalog.tableExists(self.transformation_audit_log):
                    logger.warning("Transformation audit log table does not exist.")
                    return None
                df = self.spark.table(self.transformation_audit_log)
                # Efficiently check if table is empty
                if df.limit(1).count() == 0:
                    logger.warning("Transformation audit log table is empty.")
                    return None
                latest = df.orderBy(F.col("execution_timestamp").desc()).limit(1).collect()[0].asDict()
                return latest
            except Exception as e:
                logger.error(f"Error reading transformation audit log: {e}")
                return None

        def _build_monitoring_payload(self, transformation_log: Optional[dict]) -> dict:
            """
            Consolidates transformation, validation, and execution metadata into a monitoring payload.
            Uses explicit columns for each relevant source table for better external consumability.
            """
            logger.info("Building monitoring payload")
            monitoring_id = str(uuid.uuid4())
            transformation_status = transformation_log["status"] if transformation_log else "UNKNOWN"
            validation_status = self.validation_result.get("status", "UNKNOWN")
            total_alerts = self.validation_result.get("total_alerts", 0)
            max_severity = self._normalize_severity(self.validation_result.get("max_severity", None))
            should_fail_pipeline = self.validation_result.get("should_fail_pipeline", False)
            execution_timestamp = transformation_log["execution_timestamp"] if transformation_log else self.validation_result.get("execution_timestamp", self.created_at)
            # Compute total_duration_seconds only if values exist
            total_duration_seconds = None
            if transformation_log and transformation_log.get("duration_seconds") is not None:
                total_duration_seconds = float(transformation_log["duration_seconds"])
            if self.validation_result.get("duration_seconds") is not None:
                if total_duration_seconds is not None:
                    total_duration_seconds += float(self.validation_result["duration_seconds"])
                else:
                    total_duration_seconds = float(self.validation_result["duration_seconds"])
            # Explicit source tables for external consumption
            source_table_bronze = transformation_log["source_table"] if transformation_log and transformation_log.get("source_table") else None
            source_table_silver_fact = transformation_log["target_table"] if transformation_log and transformation_log.get("target_table") else None
            source_table_validation_alerts = self.validation_result.get("alerts_table", None)
            payload = {
                "monitoring_id": monitoring_id,
                "pipeline_name": self.pipeline_name,
                "environment": self.environment,
                "transformation_status": transformation_status,
                "validation_status": validation_status,
                "total_alerts": total_alerts,
                "max_severity": max_severity,
                "should_fail_pipeline": should_fail_pipeline,
                "execution_timestamp": execution_timestamp,
                "total_duration_seconds": total_duration_seconds,
                "source_table_bronze": source_table_bronze,
                "source_table_silver_fact": source_table_silver_fact,
                "source_table_validation_alerts": source_table_validation_alerts,
                "created_at": self.created_at,
            }
            return payload

        def _ensure_monitoring_table(self, monitoring_df: DataFrame) -> None:
            """
            Ensures the monitoring Delta table exists and is schema-compatible.
            """
            logger.info(f"Ensuring monitoring table exists: {self.monitoring_table}")
            if not self.spark.catalog.tableExists(self.monitoring_table):
                logger.info("Monitoring table does not exist. Creating Delta table.")
                (
                    monitoring_df.write.format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(self.monitoring_table)
                )
            else:
                logger.info(f"Monitoring table exists: {self.monitoring_table}")

        def _write_monitoring_log(self, monitoring_payload: dict) -> None:
            """
            Persists the monitoring payload as an append-only Delta row.
            """
            if not self.write_monitoring_log:
                logger.info("Monitoring log writing disabled (local/testing mode)")
                return
            from pyspark.sql import Row
            monitoring_df = self.spark.createDataFrame([Row(**monitoring_payload)])
            self._ensure_monitoring_table(monitoring_df)
            monitoring_df.write.format("delta").mode("append").saveAsTable(self.monitoring_table)
            logger.info(f"Monitoring log written to {self.monitoring_table}")

        def run(self) -> Dict[str, Any]:
            """
            Orchestrates the full monitoring pipeline and returns a structured result for pipeline control.
            The monitor only consolidates; pipeline_status is determined solely by validation_result['should_fail_pipeline'].
            """
            status = "SUCCESS"
            error_message = None
            try:
                transformation_log = self._read_transformation_audit_log()
                monitoring_payload = self._build_monitoring_payload(transformation_log)
                if self.write_monitoring_log:
                    self._write_monitoring_log(monitoring_payload)
                # Pipeline status comes strictly from validation_result
                pipeline_status = "FAILURE" if monitoring_payload.get("should_fail_pipeline", False) else "SUCCESS"
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
                status = "FAILURE"
                error_message = str(e)
                monitoring_payload = {}
                pipeline_status = "FAILURE"
            logger.info(f"Pipeline status resolved as: {pipeline_status}")
            result = {
                "pipeline_status": pipeline_status,
                "transformation_status": monitoring_payload.get("transformation_status", None),
                "validation_status": monitoring_payload.get("validation_status", None),
                "total_alerts": monitoring_payload.get("total_alerts", 0),
                "max_severity": monitoring_payload.get("max_severity", None),
                "should_fail_pipeline": monitoring_payload.get("should_fail_pipeline", False),
                "execution_timestamp": monitoring_payload.get("execution_timestamp", None),
                "total_duration_seconds": monitoring_payload.get("total_duration_seconds", 0.0),
                "environment": monitoring_payload.get("environment", self.environment),
                "error_message": error_message,
            }
            return result

# ------------------------------------------------------------------------------
# Public orchestrator entrypoint
# ------------------------------------------------------------------------------
def run_price_audit_monitoring(
    spark: SparkSession,
    validation_result: dict,
    config: Optional[dict] = None
) -> dict:
    """
    Public entrypoint for orchestration (Databricks Jobs, Workflows, notebooks).
    """
    monitor = SilverPriceAuditMonitoring(spark, validation_result, config)
    return monitor.run()

# ------------------------------------------------------------------------------
# Local testing entrypoint
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import pprint
    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.appName("PriceAuditMonitoringLocal").getOrCreate()
    # Simulación de resultado de validación local
    validation_result = {
        "status": "SUCCESS",
        "total_alerts": 2,
        "max_severity": "P1",
        "should_fail_pipeline": False,
        "execution_timestamp": datetime.utcnow().isoformat(),
        "duration_seconds": 3.2,
        "source_table": "workspace.silver.fact_price_audit",
        "alerts_table": "workspace.silver.validation_alerts_price_audit",
    }
    local_config = {
        "write_monitoring_log": False,
        "environment": "local",
        "pipeline_name": "price_audit_silver_pipeline_local"
    }
    result = run_price_audit_monitoring(spark, validation_result, local_config)
    print("\nMONITORING RESULT (local run)")
    pprint.pprint(result)
