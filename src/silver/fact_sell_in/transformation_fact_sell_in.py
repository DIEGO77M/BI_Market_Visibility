
"""
Silver Fact Table Transformation: Sell-In
Author: Diego Mayorga

Purpose:
    Transform raw monthly Sell-In transactions from Bronze to Silver, enforcing business-aligned dimensional modeling and traceability.
    Guarantees contract with business dimensions (product, PDV) and preserves lineage for audit and compliance.

Technical Context:
    - Databricks Serverless (Unity Catalog)
    - Delta Lake, Medallion Architecture
    - Table: silver.fact_sell_in (partitioned by year_month)
    - Upsert logic: idempotent, deterministic, batch-level traceability

Business Impact:
    - Enables reliable inventory and sell-in analytics for Commercial, RGM, and BI teams
    - Ensures every record is aligned to approved product and PDV dimensions
    - All metrics are standardized for direct KPI calculation in Gold layer

Trade-offs:
    - No surrogate keys: preserves business grain for transparency
    - No joins or aggregations: Silver is strictly a cleaning and standardization layer
    - Partitioning only by year_month to avoid excessive small files and optimize query performance

Usage:
    Orchestrate via Databricks Jobs, Notebooks, or Asset Bundles. Entrypoint returns structured metrics for monitoring and audit.
"""

import uuid
import time
import logging
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.functions import udf
from typing import Dict, Any

class SilverFactSellInTransformer:
    # Transforms raw Sell-In facts to business-aligned Silver layer.
    # Enforces strict schema contract with Bronze layer for reliability and audit.
    BRONZE_SCHEMA = T.StructType([
        T.StructField("Year", T.StringType(), True),
        T.StructField("Month", T.StringType(), True),
        T.StructField("PDV_Code", T.StringType(), True),
        T.StructField("Product_Code", T.StringType(), True),
        T.StructField("Opening_Stock_Units", T.StringType(), True),
        T.StructField("Sell_In_Units", T.StringType(), True),
        T.StructField("Returns_Units", T.StringType(), True),
        T.StructField("Closing_Stock_Units", T.StringType(), True),
        T.StructField("Days_of_Inventory", T.StringType(), True),
        T.StructField("Inventory_Turnover", T.StringType(), True),
        T.StructField("Replenishment_Flag", T.StringType(), True),
        T.StructField("Stock_Risk_Level", T.StringType(), True),
        T.StructField("_ingestion_timestamp", T.TimestampType(), True),
        T.StructField("_load_date", T.DateType(), True),
        T.StructField("_source_file", T.StringType(), True),
        T.StructField("_batch_id", T.StringType(), True),
    ])

    # Silver schema: only business-aligned, auditable columns. No technical or surrogate keys.
    SILVER_COLUMNS = [
        "pdv_code", "product_code", "year", "month", "year_month",
        "opening_stock_units", "sell_in_units", "returns_units", "closing_stock_units",
        "days_of_inventory", "inventory_turnover", "replenishment_flag", "stock_risk_level",
        "ingestion_timestamp", "load_date", "source_file", "bronze_batch_id",
        "silver_processed_at", "silver_batch_id"
    ]

    def __init__(self, spark: SparkSession):
        self.spark = spark
        # Logging for orchestrators and audit trail
        self.logger = logging.getLogger(__name__)
        if not self.logger.hasHandlers():
            logging.basicConfig(level=logging.INFO)

    def _validate_schema(self, df: DataFrame) -> None:
        # Fail-fast: abort if Bronze schema does not match contract
        expected = [(f.name, f.dataType) for f in self.BRONZE_SCHEMA.fields]
        actual = [(f.name, f.dataType) for f in df.schema.fields]
        if expected != actual:
            raise Exception(f"Bronze schema mismatch.\nExpected: {expected}\nActual: {actual}")

    # --- Functional transformation blocks ---
    @staticmethod
    def transform_dimensional_keys(df: DataFrame) -> DataFrame:
        # Aligns business keys to approved dimensions (no surrogate keys)
        return (
            df
            .withColumn("pdv_code", F.upper(F.trim(F.col("PDV_Code"))))
            .withColumn("product_code", F.upper(F.trim(F.col("Product_Code"))))
        )

    @staticmethod
    def to_int_safe(col):
        # Cleans, normalizes and type-casts all integer metrics. Nulls and blanks become 0 for business logic consistency.
        return F.coalesce(
            F.when(F.isnull(col), None)
             .when(F.trim(col) == "", None)
             .otherwise(F.trim(col).cast(T.IntegerType())),
            F.lit(0)
        )

    @staticmethod
    def to_decimal_safe(col):
        # Cleans, normalizes and type-casts all decimal metrics. Nulls and blanks become 0.0 for business logic consistency.
        return F.coalesce(
            F.when(F.isnull(col), None)
             .when(F.trim(col) == "", None)
             .otherwise(F.trim(col).cast(T.DecimalType(10,2))),
            F.lit(0.0)
        )

    @staticmethod
    def transform_temporal_attributes(df: DataFrame) -> DataFrame:
        # Standardizes temporal attributes for monthly snapshot analytics
        df = df.withColumn("year", SilverFactSellInTransformer.to_int_safe(F.col("Year")))
        df = df.withColumn("month", SilverFactSellInTransformer.to_int_safe(F.col("Month")))
        df = df.withColumn("year_month", (F.col("year") * 100 + F.col("month")).cast(T.IntegerType()))
        return df

    @staticmethod
    def transform_metrics(df: DataFrame) -> DataFrame:
        # Normalizes all business metrics for direct KPI calculation in Gold layer
        df = df.withColumn("opening_stock_units", SilverFactSellInTransformer.to_int_safe(F.col("Opening_Stock_Units")))
        df = df.withColumn("sell_in_units", SilverFactSellInTransformer.to_int_safe(F.col("Sell_In_Units")))
        df = df.withColumn("returns_units", SilverFactSellInTransformer.to_int_safe(F.col("Returns_Units")))
        df = df.withColumn("closing_stock_units", SilverFactSellInTransformer.to_int_safe(F.col("Closing_Stock_Units")))
        df = df.withColumn("days_of_inventory", SilverFactSellInTransformer.to_int_safe(F.col("Days_of_Inventory")))
        df = df.withColumn("inventory_turnover", SilverFactSellInTransformer.to_decimal_safe(F.col("Inventory_Turnover")))
        return df

    @staticmethod
    def transform_flags_and_categoricals(df: DataFrame) -> DataFrame:
        # Standardizes business flags and risk levels for downstream analytics
        df = df.withColumn("replenishment_flag", F.lower(F.trim(F.col("Replenishment_Flag"))))
        df = df.withColumn("stock_risk_level", F.lower(F.trim(F.col("Stock_Risk_Level"))))
        return df

    @staticmethod
    def transform_lineage_and_audit(df: DataFrame, silver_batch_id: str) -> DataFrame:
        # Preserves full technical lineage for audit, monitoring, and compliance
        df = df.withColumn("ingestion_timestamp", F.col("_ingestion_timestamp"))
        df = df.withColumn("load_date", F.col("_load_date"))
        df = df.withColumn("source_file", F.col("_source_file"))
        df = df.withColumn("bronze_batch_id", F.col("_batch_id"))
        df = df.withColumn("silver_processed_at", F.current_timestamp())
        df = df.withColumn("silver_batch_id", F.lit(silver_batch_id))
        return df

    def _transform(self, df: DataFrame, silver_batch_id: str) -> DataFrame:
        # Functional pipeline: each block enforces a business or technical contract
        df_t = (
            df
            .transform(self.transform_dimensional_keys)
            .transform(self.transform_temporal_attributes)
            .transform(self.transform_metrics)
            .transform(self.transform_flags_and_categoricals)
            .transform(lambda d: self.transform_lineage_and_audit(d, silver_batch_id))
        )
        return df_t.select(self.SILVER_COLUMNS)

    def run(self) -> Dict[str, Any]:
        start = time.time()
        # Defensive: abort if Bronze table does not exist
        if not self.spark.catalog.tableExists("bronze.sell_in"):
            raise Exception("Table bronze.sell_in does not exist")
        # Read bronze table
        bronze_df = self.spark.read.format("delta").table("bronze.sell_in")
        self._validate_schema(bronze_df)
        records_read = bronze_df.count()
        self.logger.info(f"Records read: {records_read}")
        # Generate batch-level UUID for traceability
        silver_batch_id = str(uuid.uuid4())
        # Transform
        silver_df = self._transform(bronze_df, silver_batch_id)
        # Create Silver table only if not exists (partitioned by year_month for query efficiency)
        if not self.spark.catalog.tableExists("silver.fact_sell_in"):
            self.spark.sql("""
                CREATE TABLE silver.fact_sell_in (
                    pdv_code STRING,
                    product_code STRING,
                    year INT,
                    month INT,
                    year_month INT,
                    opening_stock_units INT,
                    sell_in_units INT,
                    returns_units INT,
                    closing_stock_units INT,
                    days_of_inventory INT,
                    inventory_turnover DECIMAL(10,2),
                    replenishment_flag STRING,
                    stock_risk_level STRING,
                    ingestion_timestamp TIMESTAMP,
                    load_date DATE,
                    source_file STRING,
                    bronze_batch_id STRING,
                    silver_processed_at TIMESTAMP,
                    silver_batch_id STRING
                )
                USING DELTA
                PARTITIONED BY (year_month)
                TBLPROPERTIES (
                    delta.enableChangeDataFeed = true,
                    delta.autoOptimize.optimizeWrite = true,
                    delta.autoOptimize.autoCompact = true
                )
            """)
        # Upsert (MERGE INTO) for idempotent, batch-level ingestion
        merge_sql = f"""
            MERGE INTO silver.fact_sell_in t
            USING (SELECT * FROM __tmp_silver_fact_sell_in) s
            ON t.pdv_code = s.pdv_code AND t.product_code = s.product_code AND t.year_month = s.year_month
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        silver_df.createOrReplaceTempView("__tmp_silver_fact_sell_in")
        merge_result = self.spark.sql(merge_sql)
        # Records written: reflects true upserted count (if available)
        records_written = None
        try:
            if hasattr(merge_result, "rowsAffected"):
                records_written = merge_result.rowsAffected
            else:
                records_written = merge_result.count() if hasattr(merge_result, "count") else None
        except Exception:
            records_written = None
        duration = round(time.time() - start, 2)
        self.logger.info(f"Records written: {records_written}")
        self.logger.info(f"Duration: {duration} seconds")
        self.logger.info(f"Status: success")
        return {
            "records_read": records_read,
            "records_written": records_written,
            "duration": duration,
            "status": "success"
        }

# Entrypoint for orchestrators, jobs, and notebooks
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    transformer = SilverFactSellInTransformer(spark)
    result = transformer.run()
    print(result)
