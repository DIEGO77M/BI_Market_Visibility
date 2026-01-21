"""
Silver Layer Transformation Script: Master PDV Dimension
Author: Diego Mayorga

This script performs all required cleaning, standardization, enrichment, and writing to Silver for the master_pdv entity.
It is modular, idempotent, and ready for orchestration in Databricks Serverless with Unity Catalog.

Responsibilities:
- Read from workspace.bronze.master_pdv
- Apply all required transformations (see docstring in class)
- Write to workspace.silver.master_pdv using MERGE (Upsert)
- Return execution metrics for orchestration

No data quality validation or monitoring is performed here (see validation.py, monitoring.py).
"""

import logging
import uuid
import time
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T

# Constants
BRONZE_TABLE = "workspace.bronze.master_pdv"
SILVER_TABLE = "workspace.silver.dim_pdv"
PARTITION_COLS = ["load_date", "country"]
CLUSTER_COLS = ["country", "status", "channel"]
TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}

class TransformationDimPDV:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger("TransformationDimPDV")

    def run(self) -> Dict[str, Any]:
        start_time = time.time()
        execution_ts = datetime.utcnow().isoformat()
        silver_batch_id = str(uuid.uuid4())
        df_transformed = None
        try:
            self.logger.info("Starting Silver transformation for master_pdv.")
            df_bronze = self._read_bronze()
            records_read = df_bronze.count()
            self.logger.info(f"Read {records_read} records from Bronze.")

            df_transformed = (
                df_bronze
                .transform(self._fix_coordinates)
                .transform(self._standardize_text_fields)
                .transform(self._convert_boolean_fields)
                .transform(self._create_derived_fields)
                .transform(lambda df: self._add_audit_fields(df, silver_batch_id))
                .transform(self._select_final_schema)
            )

            records_written = df_transformed.count()
            self.logger.info(f"Transformed {records_written} records. Writing to Silver...")
            self._write_to_silver(df_transformed)
            duration = round(time.time() - start_time, 2)
            self.logger.info(f"Silver transformation completed in {duration} seconds.")
            return {
                "status": "SUCCESS",
                "source_table": BRONZE_TABLE,
                "target_table": SILVER_TABLE,
                "records_read": records_read,
                "records_written": records_written,
                "duration_seconds": duration,
                "execution_timestamp": execution_ts
            }
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}", exc_info=True)
            return {
                "status": "FAILED",
                "source_table": BRONZE_TABLE,
                "target_table": SILVER_TABLE,
                "records_read": 0,
                "records_written": 0,
                "duration_seconds": round(time.time() - start_time, 2),
                "execution_timestamp": execution_ts,
                "error_message": str(e)
            }
        finally:
            pass  # No unpersist needed on Serverless

    def _read_bronze(self) -> DataFrame:
        self.logger.info(f"Reading Bronze table: {BRONZE_TABLE}")
        return self.spark.table(BRONZE_TABLE)

    def _fix_coordinates(self, df: DataFrame) -> DataFrame:
        self.logger.info("Fixing coordinates format.")
        def fix_coord(col):
            return (
                F.when(F.col(col).rlike(r"^-?\d+\.\d+\.\d+$"),
                    F.regexp_replace(F.col(col), r"\.([0-9]{3})\.([0-9]+)", r".\1\2").cast(T.DecimalType(10,7))
                )
                .when(F.col(col).rlike(r"^-?\d+\.\d+$"), F.col(col).cast(T.DecimalType(10,7)))
                .otherwise(F.col(col).cast(T.DecimalType(10,7)))
            )
        df = df.withColumn("latitude", fix_coord("Latitude"))
        df = df.withColumn("longitude", fix_coord("Longitude"))
        df = df.withColumn(
            "coordinate_point",
            F.struct(F.col("latitude").alias("lat"), F.col("longitude").alias("lon"))
        )
        return df

    def _standardize_text_fields(self, df: DataFrame) -> DataFrame:
        self.logger.info("Standardizing text fields.")
        def upper_trim(col):
            return F.upper(F.trim(F.col(col)))
        def proper_trim(col):
            return F.initcap(F.trim(F.col(col)))
        def trim(col):
            return F.trim(F.col(col))
        def snake_case_lower(col):
            return F.lower(F.regexp_replace(F.trim(F.col(col)), r'\s+', '_'))
        return (
            df
            .withColumnRenamed("Code (eLeader)", "pdv_code")
            .withColumnRenamed("Store Name", "store_name")
            .withColumnRenamed("Supervisor Code", "supervisor_code")
            .withColumnRenamed("Supervisor Name", "supervisor_name")
            .withColumnRenamed("Merchandiser Code", "merchandiser_code")
            .withColumnRenamed("Merchandiser Name", "merchandiser_name")
            .withColumnRenamed("CODE PO", "code_po")
            .withColumnRenamed("Store SAP Code", "store_sap_code")
            .withColumnRenamed("Sales Rep", "sales_rep")
            .withColumnRenamed("Neighborhood", "neighborhood")
            .withColumnRenamed("City", "city")
            .withColumnRenamed("Parish", "parish")
            .withColumnRenamed("Country", "country")
            .withColumnRenamed("Channel", "channel")
            .withColumnRenamed("Sub Channel", "sub_channel")
            .withColumnRenamed("Chain", "chain")
            .withColumnRenamed("Type of Service", "type_of_service")
            .withColumnRenamed("Status", "status")
            .withColumnRenamed("Aditional_Exhibitions", "has_additional_exhibitions_raw")
            .withColumnRenamed("Commercial Activities", "has_commercial_activities_raw")
            .withColumnRenamed("Planograms", "has_planograms_raw")
            # Standardize codes
            .withColumn("pdv_code", upper_trim("pdv_code"))
            .withColumn("supervisor_code", upper_trim("supervisor_code"))
            .withColumn("merchandiser_code", upper_trim("merchandiser_code"))
            .withColumn("code_po", upper_trim("code_po"))
            .withColumn("store_sap_code", upper_trim("store_sap_code"))
            # Standardize names
            .withColumn("store_name", proper_trim("store_name"))
            .withColumn("supervisor_name", proper_trim("supervisor_name"))
            .withColumn("merchandiser_name", proper_trim("merchandiser_name"))
            .withColumn("sales_rep", proper_trim("sales_rep"))
            .withColumn("city", proper_trim("city"))
            .withColumn("parish", proper_trim("parish"))
            .withColumn("neighborhood", proper_trim("neighborhood"))
            # Standardize country (UPPERCASE for partitioning)
            .withColumn("country", upper_trim("country"))
            # Standardize categories (snake_case lowercase)
            .withColumn("channel", snake_case_lower("channel"))
            .withColumn("sub_channel", snake_case_lower("sub_channel"))
            .withColumn("chain", snake_case_lower("chain"))
            .withColumn("type_of_service", snake_case_lower("type_of_service"))
            .withColumn("status", snake_case_lower("status"))
        )

    def _convert_boolean_fields(self, df: DataFrame) -> DataFrame:
        self.logger.info("Converting boolean fields.")
        def yesno_to_bool(col):
            return (
                F.when(F.upper(F.trim(F.col(col))) == "YES", F.lit(True))
                 .when(F.upper(F.trim(F.col(col))) == "NO", F.lit(False))
                 .otherwise(F.lit(None))
            )
        df = df.withColumn("has_additional_exhibitions", yesno_to_bool("has_additional_exhibitions_raw"))
        df = df.withColumn("has_commercial_activities", yesno_to_bool("has_commercial_activities_raw"))
        df = df.withColumn("has_planograms", yesno_to_bool("has_planograms_raw"))
        # status ya está en snake_case_lower, así que comparar con 'active' en minúsculas
        df = df.withColumn("is_active", (F.col("status") == F.lit("active")))
        return df

    def _create_derived_fields(self, df: DataFrame) -> DataFrame:
        self.logger.info("Creating derived fields.")
        df = df.withColumn("full_location", F.concat_ws(", ", F.col("city"), F.col("parish"), F.col("country")))
        df = df.withColumn("assignment_complete", (F.col("supervisor_code").isNotNull()) & (F.col("merchandiser_code").isNotNull()))
        df = df.withColumn("record_age_days", F.datediff(F.current_date(), F.col("_load_date")))
        return df

    def _add_audit_fields(self, df: DataFrame, silver_batch_id: str) -> DataFrame:
        self.logger.info("Adding audit fields.")
        return (
            df
            .withColumnRenamed("_ingestion_timestamp", "ingestion_timestamp")
            .withColumnRenamed("_load_date", "load_date")
            .withColumnRenamed("_source_file", "source_file")
            .withColumnRenamed("_batch_id", "bronze_batch_id")
            .withColumn("silver_processed_at", F.current_timestamp())
            .withColumn("silver_batch_id", F.lit(silver_batch_id))
        )

    def _select_final_schema(self, df: DataFrame) -> DataFrame:
        self.logger.info("Selecting final schema and ordering columns.")
        return df.select(
            "pdv_code", "store_name", "channel", "sub_channel", "chain", "store_sap_code",
            "neighborhood", "city", "parish", "country", "full_location",
            "latitude", "longitude", "coordinate_point",
            "type_of_service", "status", "is_active",
            "supervisor_code", "supervisor_name", "merchandiser_code", "merchandiser_name", "sales_rep", "assignment_complete",
            "code_po", "has_additional_exhibitions", "has_commercial_activities", "has_planograms",
            "ingestion_timestamp", "load_date", "source_file", "bronze_batch_id",
            "silver_processed_at", "silver_batch_id", "record_age_days"
        )

    def _write_to_silver(self, df: DataFrame) -> None:
        self.logger.info(f"Writing to Silver table: {SILVER_TABLE} (MERGE Upsert)")
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            pdv_code STRING NOT NULL,
            store_name STRING,
            channel STRING,
            sub_channel STRING,
            chain STRING,
            store_sap_code STRING,
            neighborhood STRING,
            city STRING,
            parish STRING,
            country STRING,
            full_location STRING,
            latitude DECIMAL(10,7),
            longitude DECIMAL(10,7),
            coordinate_point STRUCT<lat:DECIMAL(10,7), lon:DECIMAL(10,7)>,
            type_of_service STRING,
            status STRING,
            is_active BOOLEAN,
            supervisor_code STRING,
            supervisor_name STRING,
            merchandiser_code STRING,
            merchandiser_name STRING,
            sales_rep STRING,
            assignment_complete BOOLEAN,
            code_po STRING,
            has_additional_exhibitions BOOLEAN,
            has_commercial_activities BOOLEAN,
            has_planograms BOOLEAN,
            ingestion_timestamp TIMESTAMP,
            load_date DATE,
            source_file STRING,
            bronze_batch_id STRING,
            silver_processed_at TIMESTAMP,
            silver_batch_id STRING,
            record_age_days INT
        )
        USING DELTA
        PARTITIONED BY (load_date, country)
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """)
        df.createOrReplaceTempView("transformed_data")
        self.spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING transformed_data AS source
        ON target.pdv_code = source.pdv_code AND target.load_date = source.load_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)
        self.logger.info("Silver write (MERGE) completed.")
    """
    Main transformation class for Silver master_pdv dimension.
    Encapsulates all transformation logic and Silver write.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger("TransformationDimPDV")



# Orchestrable public function for Silver transformation
def run_transformation_dim_pdv(
    spark: SparkSession = None
) -> Dict[str, Any]:
    """
    Runs the Silver transformation for master_pdv dimension.
    If no SparkSession is provided, creates a new one (for local test).
    Returns execution metrics as a dictionary.
    
    Args:
        spark: SparkSession instance. If None, creates a new one.
    
    Returns:
        Dict[str, Any]: Transformation execution metrics including:
            - status: SUCCESS or FAILED
            - source_table: Bronze table name
            - target_table: Silver table name
            - records_read: Number of records read
            - records_written: Number of records written
            - duration_seconds: Execution time
            - execution_timestamp: ISO timestamp
    
    Example:
        # In Databricks notebook
        result = run_transformation_dim_pdv(spark)
        
        # For local testing
        result = run_transformation_dim_pdv()
    """
    local_spark = False
    if spark is None:
        spark = SparkSession.builder.appName("Silver_dim_pdv_transformation").getOrCreate()
        local_spark = True
    
    transformer = TransformationDimPDV(spark)
    result = transformer.run()
    
    if local_spark:
        spark.stop()
    
    return result

# Standalone execution for local/Databricks testing
if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession

    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.appName("Silver_dim_pdv_local_test").getOrCreate()
    try:
        transformer = TransformationDimPDV(spark)
        result = transformer.run()
        print("\n=== Silver dim_pdv Transformation Result ===")
        for k, v in result.items():
            print(f"{k}: {v}")

    finally:
        spark.stop()
