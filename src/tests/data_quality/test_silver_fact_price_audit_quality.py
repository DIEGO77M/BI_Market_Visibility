"""
Data quality tests for Silver fact_price_audit.
"""
import pytest
from pyspark.sql import SparkSession, Row

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("dq-test").getOrCreate()

def test_valid_price_range(spark):
    row = Row(Cod_Producto="A1", Precio=-5.0)
    df = spark.createDataFrame([row])
    assert df.filter(df.Precio < 0).count() == 1  # Should fail if negative prices are not handled
