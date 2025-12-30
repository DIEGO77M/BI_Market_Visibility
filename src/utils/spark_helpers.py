"""
Spark Helper Functions

This module contains helper functions for common Spark operations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from typing import Optional, Dict


def get_spark_session(app_name: str = "BI_Market_Visibility") -> SparkSession:
    """
    Create or get existing Spark session.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession object
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    return spark


def read_delta_table(spark: SparkSession, path: str) -> DataFrame:
    """
    Read Delta table from specified path.
    
    Args:
        spark: SparkSession object
        path: Path to Delta table
        
    Returns:
        DataFrame
    """
    return spark.read.format("delta").load(path)


def write_delta_table(
    df: DataFrame, 
    path: str, 
    mode: str = "overwrite",
    partition_by: Optional[list] = None
) -> None:
    """
    Write DataFrame to Delta table.
    
    Args:
        df: DataFrame to write
        path: Destination path
        mode: Write mode (overwrite, append, etc.)
        partition_by: List of columns to partition by
    """
    writer = df.write.format("delta").mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    writer.save(path)


def add_audit_columns(df: DataFrame) -> DataFrame:
    """
    Add standard audit columns to DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with audit columns added
    """
    return df \
        .withColumn("created_timestamp", F.current_timestamp()) \
        .withColumn("created_date", F.current_date())


def optimize_delta_table(spark: SparkSession, path: str) -> None:
    """
    Optimize Delta table (compaction and Z-ordering).
    
    Args:
        spark: SparkSession object
        path: Path to Delta table
    """
    spark.sql(f"OPTIMIZE delta.`{path}`")


def vacuum_delta_table(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
    """
    Vacuum old versions of Delta table.
    
    Args:
        spark: SparkSession object
        path: Path to Delta table
        retention_hours: Retention period in hours (default 7 days)
    """
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")


def create_or_replace_temp_view(df: DataFrame, view_name: str) -> None:
    """
    Create or replace temporary view from DataFrame.
    
    Args:
        df: Input DataFrame
        view_name: Name of the temporary view
    """
    df.createOrReplaceTempView(view_name)


def cast_columns(df: DataFrame, column_types: Dict[str, str]) -> DataFrame:
    """
    Cast multiple columns to specified types.
    
    Args:
        df: Input DataFrame
        column_types: Dictionary mapping column names to target types
        
    Returns:
        DataFrame with casted columns
    """
    for col_name, col_type in column_types.items():
        df = df.withColumn(col_name, F.col(col_name).cast(col_type))
    
    return df
