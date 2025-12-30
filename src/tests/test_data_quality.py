"""
Unit tests for data quality functions
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date
import sys
sys.path.append('..')

from utils.data_quality import (
    check_null_values,
    check_duplicates,
    check_data_completeness,
    validate_date_range,
    generate_quality_report
)


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("test") \
        .master("local[*]") \
        .getOrCreate()


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame for testing."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True),
        StructField("date", DateType(), True)
    ])
    
    data = [
        (1, "Alice", 100, date(2024, 1, 1)),
        (2, "Bob", None, date(2024, 1, 2)),
        (3, None, 300, date(2024, 1, 3)),
        (4, "David", 400, date(2024, 1, 4)),
        (5, "Alice", 100, date(2024, 1, 1))  # Duplicate
    ]
    
    return spark.createDataFrame(data, schema)


def test_check_null_values(sample_df):
    """Test null value detection."""
    result = check_null_values(sample_df, ["name", "value"])
    
    assert result["name"] == 1
    assert result["value"] == 1


def test_check_duplicates(sample_df):
    """Test duplicate detection."""
    duplicate_count = check_duplicates(sample_df)
    
    assert duplicate_count == 1


def test_check_data_completeness(sample_df):
    """Test data completeness calculation."""
    result = check_data_completeness(sample_df)
    
    assert result["id"] == 100.0
    assert result["name"] == 80.0
    assert result["value"] == 80.0


def test_validate_date_range(sample_df):
    """Test date range validation."""
    is_valid, invalid_count = validate_date_range(
        sample_df,
        "date",
        min_date="2024-01-01",
        max_date="2024-01-31"
    )
    
    assert is_valid is True
    assert invalid_count == 0


def test_generate_quality_report(sample_df):
    """Test quality report generation."""
    report = generate_quality_report(sample_df, ["name", "value"])
    
    assert report["total_records"] == 5
    assert report["total_columns"] == 4
    assert "null_counts" in report
    assert "completeness" in report
