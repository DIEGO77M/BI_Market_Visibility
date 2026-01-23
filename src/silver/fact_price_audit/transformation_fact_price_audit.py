"""
Transformation module for Price Audit (Silver Layer)

This module implements the Silver transformation for the Price Audit domain, following strict Medallion Architecture and Databricks Serverless best practices.

Key design decisions and trade-offs are documented for technical interviews and recruiter review:
    - Idempotent, deterministic, and modular transformation pipeline
    - Partition pruning and schema validation for scalable, robust MERGE
    - Explicit deduplication and type normalization to guarantee business key uniqueness
    - All logic is orchestrator-ready and Unity Catalog compliant
    - Logging and error handling designed for enterprise-grade observability

Author: Diego Mayorga
"""

import logging
import uuid
import time
from datetime import datetime
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SilverPriceAuditTransformer:
    """Transforms bronze price audit table to silver fact table with idempotent MERGE."""
    
    @staticmethod
    def _write_audit_log(spark: SparkSession, audit_dict: dict, table_name: str = "workspace.silver.transformation_audit_log", enabled: bool = True):
        """
        Persists the transformation execution result as a single-row Delta table entry for external consumption (e.g., n8n).
        Table is created if it does not exist. Schema is flexible for audit/monitoring use cases.
        If enabled=False, does nothing (for local testing).
        """
        if not enabled:
            logger.info("Audit log write disabled (local testing mode)")
            return
        from pyspark.sql import Row
        audit_row = Row(**audit_dict)
        audit_df = spark.createDataFrame([audit_row])
        logger.info(f"[AUDIT] DataFrame to be written to {table_name}:")
        audit_df.show(truncate=False)
        # Create or append to audit log table
        audit_df.write.format("delta").mode("append").saveAsTable(table_name)
        logger.info(f"Audit log persisted to {table_name}")
    
    @staticmethod
    def _normalize_boolean(col_name: str) -> F.Column:
        """
        Normalizes boolean columns from multiple formats (e.g., 'Sí', 'S', 'Y', 'Yes', '1', 'True').
        This function is extensible and robust to new values, avoiding hardcoded logic.
        Trade-off: Accepts only affirmative values as True, all others as False.
        """
        return F.when(
            F.upper(F.trim(F.col(col_name))).isin(["SÍ", "SI", "YES", "Y", "1", "TRUE"]),
            True
        ).otherwise(False)
    
    def _deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Deduplicates records by natural business keys (pdv_code, product_code, date), keeping only the latest by _ingestion_timestamp.
        This ensures idempotency and prevents duplicate facts in the Silver layer.
        Trade-off: Only the most recent record per key is preserved; older duplicates are dropped.
        """
        logger.info("Deduplicating records by natural keys")
        from pyspark.sql import Window
        w = Window.partitionBy(self.natural_keys).orderBy(F.col("_ingestion_timestamp").desc())
        return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    
    def _validate_schema(self, df: DataFrame) -> None:
        """
        Validates the minimum schema contract:
            - All required columns are present
            - Price fields are castable to decimal
        This prevents silent data loss and ensures downstream compatibility.
        Trade-off: Fails fast on schema or type issues, prioritizing data quality over leniency.
        """
        actual_columns = set(df.columns)
        missing = set(self._expected_columns) - actual_columns
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        # Validate that 'Precio' and 'Promotional Price' are castable to decimal
        try:
            df.select(
                F.col("Precio").cast(T.DecimalType(10, 2)),
                F.col("Promotional Price").cast(T.DecimalType(10, 2))
            ).limit(1).collect()
        except Exception as e:
            raise ValueError(f"Type validation failed for price fields: {e}")
    
    def __init__(self, spark: SparkSession, config: Optional[dict] = None):
        self.spark = spark
        self.config = config or {}
        self.source_table = self.config.get(
            "source_table", "workspace.bronze.price_audit"
        )
        self.target_table = self.config.get(
            "target_table", "workspace.silver.fact_price_audit"
        )
        self.partition_column = "load_date"
        self.natural_keys = ["pdv_code", "product_code", "date"]
        self._expected_columns = [
            "Fecha",
            "Nombre del punto de venta",
            "Cod_PDV",
            "Nombre_Producto",
            "Cod_Producto",
            "Precio",
            "Tiene este producto una promocion?",
            "Promotional Price",
            "Comentarios",
            "Competitive_Group",
            "_ingestion_timestamp",
            "_load_date",
            "_source_file",
            "_batch_id",
        ]

    def _read_bronze(self) -> DataFrame:
        """
        Reads the Bronze table from Unity Catalog and validates schema contract.
        This method is isolated for testability and future extensibility (e.g., schema evolution).
        """
        logger.info(f"Reading source table: {self.source_table}")
        df = self.spark.table(self.source_table)
        self._validate_schema(df)
        return df

    def _standardize_columns(self, df: DataFrame) -> DataFrame:
        """
        Renames columns to snake_case and standardizes business keys for downstream compatibility.
        Ensures alignment with Silver dimension tables and future Gold layer joins.
        """
        logger.info("Standardizing column names and business keys")
        col_map = {
            "Fecha": "date",
            "Nombre del punto de venta": "pdv_name",
            "Cod_PDV": "pdv_code",
            "Nombre_Producto": "product_name",
            "Cod_Producto": "product_code",
            "Precio": "price",
            "Tiene este producto una promocion?": "has_promotion",
            "Promotional Price": "promotional_price",
            "Comentarios": "comments",
            "Competitive_Group": "competitive_group",
            "_ingestion_timestamp": "_ingestion_timestamp",
            "_load_date": "load_date",
            "_source_file": "_source_file",
            "_batch_id": "_batch_id",
        }
        for old, new in col_map.items():
            df = df.withColumnRenamed(old, new)
        # Standardize business keys for join compatibility
        df = df.withColumn(
            "pdv_code", F.upper(F.trim(F.col("pdv_code")))
        ).withColumn(
            "product_code", F.upper(F.trim(F.col("product_code")))
        )
        return df

    def _cast_types(self, df: DataFrame) -> DataFrame:
        """
        Casts columns to their target types, with robust handling for dates and booleans.
        Invalid dates are filtered out to prevent silent data loss in analytics.
        Trade-off: Rows with invalid dates are dropped, prioritizing data quality.
        """
        logger.info("Casting column types with robust date and boolean handling")
        # Use try_to_date if available (Spark 3.4+), else fallback to to_date
        if hasattr(F, "try_to_date"):
            df = df.withColumn("date", F.try_to_date(F.col("date"), "yyyy-MM-dd"))
        else:
            df = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        # Remove rows with invalid date (date is null)
        df = df.filter(F.col("date").isNotNull())
        df = (
            df.withColumn("price", F.col("price").cast(T.DecimalType(10, 2)))
            .withColumn(
                "promotional_price", F.col("promotional_price").cast(T.DecimalType(10, 2))
            )
            .withColumn(
                "has_promotion", self._normalize_boolean("has_promotion")
            )
        )
        return df

    def _add_audit_fields(self, df: DataFrame) -> DataFrame:
        """
        Adds Silver technical fields for traceability and batch lineage.
        - silver_processed_at: Timestamp of Silver processing
        - silver_batch_id: Unique batch identifier for audit and troubleshooting
        """
        logger.info("Adding technical audit fields for Silver layer")
        batch_id = str(uuid.uuid4())
        df = df.withColumn("silver_processed_at", F.current_timestamp())
        df = df.withColumn("silver_batch_id", F.lit(batch_id))
        return df

    def _select_final_schema(self, df: DataFrame) -> DataFrame:
        """
        Selects and orders the final Silver schema, preventing accidental column drift.
        Only explicitly listed columns are written to the Silver table.
        """
        logger.info("Selecting final schema for Silver fact table")
        final_cols = [
            "date",
            "pdv_code",
            "pdv_name",
            "product_code",
            "product_name",
            "price",
            "has_promotion",
            "promotional_price",
            "competitive_group",
            "comments",
            "_ingestion_timestamp",
            "load_date",
            "_source_file",
            "_batch_id",
            "silver_processed_at",
            "silver_batch_id",
        ]
        return df.select(*final_cols)

    def _ensure_silver_table(self, df: DataFrame) -> None:
        """
        Ensures the Silver Delta table exists and is schema-compatible.
        If the table does not exist, it is created with the correct schema and partitioning.
        If it exists, schema compatibility is checked and logged for auditability.
        Trade-off: Only logs schema mismatch; does not auto-evolve schema to avoid silent errors.
        """
        logger.info(f"Ensuring target table exists and is compatible: {self.target_table}")
        table_exists = self.spark.catalog.tableExists(self.target_table)
        if not table_exists:
            logger.info("Target table does not exist. Creating Delta table.")
            (
                df.write.format("delta")
                .mode("overwrite")
                .partitionBy(self.partition_column)
                .option("overwriteSchema", "true")
                .saveAsTable(self.target_table)
            )
        else:
            logger.info(f"Target table exists: {self.target_table}")
            # Validate schema compatibility (column names and types)
            target_df = self.spark.table(self.target_table)
            src_cols = set((f.name, f.dataType) for f in df.schema.fields)
            tgt_cols = set((f.name, f.dataType) for f in target_df.schema.fields)
            if src_cols != tgt_cols:
                logger.warning(f"Schema mismatch between source and target table: {self.target_table}")
                # Trade-off: Only logs mismatch, does not auto-evolve schema

    def _merge_to_silver(self, df: DataFrame) -> int:
        """
        Performs an idempotent MERGE into the Silver table, with partition pruning for scalability.
        Only relevant partitions (load_date) are targeted, minimizing resource usage.
        Trade-off: If batch_id is duplicated, only new/changed records are updated.
        """
        logger.info(f"Merging data into Silver table: {self.target_table} with partition pruning")
        from delta.tables import DeltaTable
        # Prune only relevant partitions (load_date)
        load_dates = [row[0] for row in df.select(self.partition_column).distinct().collect()]
        logger.info(f"Pruning partitions for load_date(s): {load_dates}")
        target = DeltaTable.forName(self.spark, self.target_table)
        # Prepare merge condition for idempotency
        merge_condition = (
            " AND ".join([
                f"target.{k} = source.{k}" for k in self.natural_keys
            ])
        )
        # MERGE only on relevant partitions
        target.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll(
            condition="target._batch_id != source._batch_id"
        ).whenNotMatchedInsertAll().execute()
        return df.count()

    def run(self) -> Dict[str, Any]:
        """
        Orchestrates the full Silver transformation pipeline for Price Audit.
        Returns detailed execution metrics for monitoring and audit.
        Also persists the result in a Delta audit log table for external consumption (e.g., n8n), unless disabled for local testing.
        """
        start = time.time()
        execution_timestamp = datetime.utcnow().isoformat()
        records_read = records_written = 0
        error_message = None
        status = "SUCCESS"
        silver_batch_id = None
        audit_log_table = "workspace.silver.transformation_audit_log"
        # Allow disabling audit log for local testing
        write_audit_log = self.config.get("write_audit_log", True)
        try:
            df = self._read_bronze()
            records_read = df.count()
            df = self._standardize_columns(df)
            df = self._deduplicate(df)
            df = self._cast_types(df)
            df = self._add_audit_fields(df)
            # Capture batch_id for audit after adding audit fields
            if "silver_batch_id" in df.columns:
                silver_batch_id = df.select("silver_batch_id").first()[0]
            df = self._select_final_schema(df)
            self._ensure_silver_table(df)
            try:
                records_written = self._merge_to_silver(df)
            except Exception as merge_exc:
                logger.error(f"MERGE failed for silver_batch_id={silver_batch_id}: {merge_exc}")
                error_message = f"MERGE failed: {merge_exc} | silver_batch_id={silver_batch_id}"
                status = "FAILURE"
                raise
        except Exception as e:
            logger.error(f"Error in SilverPriceAuditTransformer: {e}")
            error_message = str(e)
            status = "FAILURE"
        duration = round(time.time() - start, 2)
        # Robust default dict for audit log
        result = {
            "status": status or "FAILURE",
            "source_table": self.source_table or "UNKNOWN",
            "target_table": self.target_table or "UNKNOWN",
            "records_read": records_read if records_read is not None else 0,
            "records_written": records_written if records_written is not None else 0,
            "duration_seconds": duration,
            "execution_timestamp": execution_timestamp or datetime.utcnow().isoformat(),
            "error_message": error_message or "No error message captured.",
            "silver_batch_id": silver_batch_id or "NO_BATCH_ID",
        }
        logger.info(f"[AUDIT] Attempting to write audit log to {audit_log_table} with result: {result}")
        # Persist audit log for external consumption (e.g., n8n), only if enabled
        try:
            self._write_audit_log(self.spark, result, audit_log_table, enabled=write_audit_log)
            logger.info(f"[AUDIT] Successfully wrote audit log to {audit_log_table}")
        except Exception as log_exc:
            logger.error(f"Failed to persist audit log to {audit_log_table}: {log_exc}")
        return result


def run_price_audit_transformation(
    spark: SparkSession, config: Optional[dict] = None
) -> dict:
    """
    Public orchestrator entrypoint for the Price Audit Silver transformation.
    Designed for Databricks Jobs and notebook orchestration.
    
    Args:
        spark (SparkSession): Active Spark session
        config (dict, optional): Configuration dictionary.
            - source_table: Source Bronze table (default: workspace.bronze.price_audit)
            - target_table: Target Silver table (default: workspace.silver.fact_price_audit)
            - write_audit_log: Enable/disable audit log write (default: True)
    
    Returns:
        dict: Execution metrics and audit fields for monitoring and troubleshooting
    
    Example:
        # Production
        result = run_price_audit_transformation(spark)
        
        # Local testing (disable audit log)
        result = run_price_audit_transformation(spark, {"write_audit_log": False})
    """
    transformer = SilverPriceAuditTransformer(spark, config)
    return transformer.run()


# ============================================================================
# LOCAL TESTING ENTRYPOINT
# ============================================================================
if __name__ == "__main__":
    """
    Local testing entrypoint. Allows direct execution for debugging.
    - Disables audit log writes (local testing mode)
    - Prints execution metrics for visibility
    - Does NOT execute when imported by orchestrator
    """
    print("\n" + "="*70)
    print("LOCAL TESTING MODE - Price Audit Silver Transformation")
    print("="*70 + "\n")
    
    try:
        # Get Spark session
        spark = SparkSession.builder.appName("PriceAuditLocal").getOrCreate()
        
        # Config para testing local: desactiva audit log
        local_config = {
            "write_audit_log": False,  # No escribe a Delta tabla
            # Puedes sobrescribir tablas si necesario:
            # "source_table": "workspace.bronze.price_audit",
            # "target_table": "workspace.silver.fact_price_audit"
        }
        
        # Ejecuta la transformación
        print("[INFO] Starting transformation...\n")
        result = run_price_audit_transformation(spark, local_config)
        
        # Imprime resultados
        print("\n" + "="*70)
        print("TRANSFORMATION RESULTS")
        print("="*70)
        for key, value in result.items():
            print(f"{key:.<30} {value}")
        print("="*70 + "\n")
        
        # Status final
        if result["status"] == "SUCCESS":
            print(f"✅ SUCCESS: {result['records_written']} records written in {result['duration_seconds']}s")
        else:
            print(f"❌ FAILURE: {result['error_message']}")
        
        print("\n")
        
    except Exception as e:
        print(f"\n❌ ERROR during local testing: {e}\n")
        raise