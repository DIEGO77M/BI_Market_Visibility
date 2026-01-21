"""
Silver Transformation Script for Products Dimension (Bronze → Silver)

Author: Diego Mayorga
Role: Senior Data Engineer / Analytics Engineer
Environment: Databricks Serverless + Unity Catalog
Architecture: Medallion (Bronze → Silver)
Paradigm: OOP + Functional Programming (chained transformations)

Purpose:
--------
This module transforms the Products master data from the Bronze layer
into a curated Silver dimension table.

Responsibilities:
-----------------
- Enforce Bronze data contract (schema + types)
- Standardize and normalize product attributes
- Derive analytical and hierarchical fields
- Preserve full lineage and auditability
- Perform idempotent upserts using Delta Lake MERGE
- Return execution metrics for orchestration

Non-Responsibilities:
---------------------
- No aggregations
- No data quality rules (handled in validation layer)
- No monitoring logic (handled in monitoring layer)
"""

from typing import Dict, Any, Set
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
import logging
import uuid
import time
from datetime import datetime


class SilverProductsTransformer:
    """
    Transforms Products data from Bronze to Silver layer.
    Designed for orchestration and enterprise-grade execution.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any] = None):
        self.spark = spark
        self.config = config or {}

        # Logger setup (safe for Databricks notebooks and jobs)
        self.logger = logging.getLogger("silver_dim_products_transformer")
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s: %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

        # Table configuration (Unity Catalog compliant)
        self.source_table = self.config.get(
            "source_table", "workspace.bronze.master_products"
        )
        self.target_table = self.config.get(
            "target_table", "workspace.silver.dim_products"
        )

        # Partition strategy (query + maintenance friendly)
        self.partition_cols = ["load_date", "category"]

        # Business-approved default values (explicit and auditable)
        self.defaults = self.config.get(
            "defaults",
            {
                "product_code": "UNKNOWN",
                "product_name": "Unknown Product",
                "brand": "Unknown Brand",
                "segment": "unknown",
                "subsegment": "unknown",
                "category": "unknown",
                "subcategory": "unknown",
            },
        )

        # Expected Bronze contract (columns + types)
        self.expected_schema = {
            "Product_Code": T.StringType(),
            "Product_Name": T.StringType(),
            "Brand": T.StringType(),
            "Segment": T.StringType(),
            "Subsegment": T.StringType(),
            "Category": T.StringType(),
            "Subcategory": T.StringType(),
            "_ingestion_timestamp": T.TimestampType(),
            "_load_date": T.DateType(),
            "_source_file": T.StringType(),
            "_batch_id": T.StringType(),
        }

    # ------------------------------------------------------------------
    # Contract validation
    # ------------------------------------------------------------------
    def _validate_bronze_schema(self, df: DataFrame) -> None:
        """
        Enforces the Bronze data contract.
        Fails fast if columns or data types do not match expectations.
        """
        actual_fields = {f.name: f.dataType for f in df.schema.fields}

        missing_cols = set(self.expected_schema) - set(actual_fields)
        if missing_cols:
            raise ValueError(
                f"Missing required Bronze columns: {sorted(missing_cols)}"
            )

        invalid_types = {
            col: str(actual_fields[col])
            for col, expected_type in self.expected_schema.items()
            if not isinstance(actual_fields[col], type(expected_type))
        }

        if invalid_types:
            raise TypeError(
                f"Invalid data types in Bronze table: {invalid_types}"
            )

        self.logger.info("Bronze schema contract validation passed")

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------
    def _read_bronze(self) -> DataFrame:
        self.logger.info(f"Reading Bronze table: {self.source_table}")
        df = self.spark.table(self.source_table)
        self._validate_bronze_schema(df)
        return df

    # ------------------------------------------------------------------
    # Transformations
    # ------------------------------------------------------------------
    @staticmethod
    def _snake_case(col):
        """Normalizes text to snake_case (lowercase, underscores)."""
        return F.regexp_replace(F.lower(F.trim(col)), r"\s+", "_")

    def _standardize_fields(self, df: DataFrame) -> DataFrame:
        """
        Standardizes raw attributes into analytics-ready dimensions.
        """
        self.logger.info("Standardizing product attributes")

        return (
            df
            .withColumn(
                "product_code",
                F.coalesce(
                    F.upper(F.trim(F.col("Product_Code"))),
                    F.lit(self.defaults["product_code"]),
                ),
            )
            .withColumn(
                "product_name",
                F.coalesce(
                    F.initcap(F.trim(F.col("Product_Name"))),
                    F.lit(self.defaults["product_name"]),
                ),
            )
            .withColumn(
                "brand",
                F.coalesce(
                    F.initcap(F.trim(F.col("Brand"))),
                    F.lit(self.defaults["brand"]),
                ),
            )
            .withColumn(
                "segment",
                F.coalesce(
                    self._snake_case(F.col("Segment")),
                    F.lit(self.defaults["segment"]),
                ),
            )
            .withColumn(
                "subsegment",
                F.coalesce(
                    self._snake_case(F.col("Subsegment")),
                    F.lit(self.defaults["subsegment"]),
                ),
            )
            .withColumn(
                "category",
                F.coalesce(
                    self._snake_case(F.col("Category")),
                    F.lit(self.defaults["category"]),
                ),
            )
            .withColumn(
                "subcategory",
                F.coalesce(
                    self._snake_case(F.col("Subcategory")),
                    F.lit(self.defaults["subcategory"]),
                ),
            )
        )

    def _derive_fields(self, df: DataFrame) -> DataFrame:
        """
        Creates analytical and audit-ready derived fields.
        """
        self.logger.info("Deriving analytical and audit fields")

        return (
            df
            .withColumn(
                "product_full_name",
                F.concat_ws(" ", F.col("brand"), F.col("product_name")),
            )
            .withColumn(
                "product_hierarchy",
                F.struct(
                    F.col("segment"),
                    F.col("category"),
                    F.col("subcategory"),
                ),
            )
            # Preserve Bronze lineage
            .withColumnRenamed("_ingestion_timestamp", "ingestion_timestamp")
            .withColumnRenamed("_load_date", "load_date")
            .withColumnRenamed("_source_file", "source_file")
            .withColumnRenamed("_batch_id", "bronze_batch_id")
            # Silver audit fields
            .withColumn("silver_processed_at", F.current_timestamp())
            .withColumn("silver_batch_id", F.lit(str(uuid.uuid4())))
        )

    def _select_final_schema(self, df: DataFrame) -> DataFrame:
        """
        Explicit final schema for the Silver dimension.
        Prevents accidental column leaks.
        """
        return df.select(
            "product_code",
            "product_name",
            "brand",
            "segment",
            "subsegment",
            "category",
            "subcategory",
            "product_full_name",
            "product_hierarchy",
            "ingestion_timestamp",
            "load_date",
            "source_file",
            "bronze_batch_id",
            "silver_processed_at",
            "silver_batch_id",
        )

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------
    def _ensure_silver_table(self) -> None:
        """
        Creates the Silver table if it does not exist.
        Safe to call multiple times.
        """
        self.logger.info(f"Ensuring Silver table exists: {self.target_table}")

        partition_clause = ", ".join(self.partition_cols)

        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.target_table} (
                product_code STRING,
                product_name STRING,
                brand STRING,
                segment STRING,
                subsegment STRING,
                category STRING,
                subcategory STRING,
                product_full_name STRING,
                product_hierarchy STRUCT<
                    segment: STRING,
                    category: STRING,
                    subcategory: STRING
                >,
                ingestion_timestamp TIMESTAMP,
                load_date DATE,
                source_file STRING,
                bronze_batch_id STRING,
                silver_processed_at TIMESTAMP,
                silver_batch_id STRING
            )
            USING DELTA
            PARTITIONED BY ({partition_clause})
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            """
        )

    def _merge_to_silver(self, df: DataFrame) -> int:
        """
        Performs idempotent upsert using Delta Lake MERGE.
        """
        self._ensure_silver_table()

        temp_view = "silver_products_src"
        df.createOrReplaceTempView(temp_view)

        self.spark.sql(
            f"""
            MERGE INTO {self.target_table} AS target
            USING {temp_view} AS source
            ON target.product_code = source.product_code
               AND target.load_date = source.load_date
            WHEN MATCHED THEN UPDATE SET
                target.product_name = source.product_name,
                target.brand = source.brand,
                target.segment = source.segment,
                target.subsegment = source.subsegment,
                target.category = source.category,
                target.subcategory = source.subcategory,
                target.product_full_name = source.product_full_name,
                target.product_hierarchy = source.product_hierarchy,
                target.ingestion_timestamp = source.ingestion_timestamp,
                target.source_file = source.source_file,
                target.bronze_batch_id = source.bronze_batch_id,
                target.silver_processed_at = source.silver_processed_at,
                target.silver_batch_id = source.silver_batch_id
            WHEN NOT MATCHED THEN INSERT *
            """
        )

        count = df.count()
        self.spark.catalog.dropTempView(temp_view)
        return count

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------
    def run(self) -> Dict[str, Any]:
        """
        Executes the full Bronze → Silver transformation.
        """
        start_time = time.time()
        execution_ts = datetime.utcnow()

        try:
            bronze_df = self._read_bronze()

            silver_df = (
                bronze_df
                .transform(self._standardize_fields)
                .transform(self._derive_fields)
                .transform(self._select_final_schema)
            )

            records_read = silver_df.count()
            records_written = self._merge_to_silver(silver_df)


            duration = round(time.time() - start_time, 2)

            return {
                "status": "SUCCESS",
                "source_table": self.source_table,
                "target_table": self.target_table,
                "records_read": records_read,
                "records_written": records_written,
                "duration_seconds": duration,
                "execution_timestamp": execution_ts.isoformat(),
                "error_message": None,
            }

        except Exception as e:
            return {
                "status": "FAILED",
                "source_table": self.source_table,
                "target_table": self.target_table,
                "records_read": None,
                "records_written": None,
                "duration_seconds": round(time.time() - start_time, 2),
                "execution_timestamp": execution_ts.isoformat(),
                "error_message": str(e),
            }


# ----------------------------------------------------------------------
# Orchestrator entrypoint
# ----------------------------------------------------------------------
def run_transformation_dim_products(
    spark: SparkSession = None, config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Public entrypoint for Databricks notebooks and orchestrators.
    """
    if spark is None:
        spark = (
            SparkSession.getActiveSession()
            or SparkSession.builder.appName("silver_dim_products").getOrCreate()
        )

    transformer = SilverProductsTransformer(spark, config)
    return transformer.run()


if __name__ == "__main__":
    result = run_transformation_dim_products()
    print(result)
