"""
Pytest configuration and shared fixtures
"""

import pytest
from pyspark.sql import SparkSession


def pytest_configure(config):
    """Configure pytest."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for the entire test session."""
    spark = SparkSession.builder \
        .appName("test_session") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()
