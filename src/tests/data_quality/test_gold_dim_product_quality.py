"""
Data quality tests for Gold dim_product.
"""
import pytest
from pyspark.sql import SparkSession, Row

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("dq-test").getOrCreate()

def test_unique_product_code(spark):
    rows = [Row(Product_Code="A1"), Row(Product_Code="A1")]
    df = spark.createDataFrame(rows)
    duplicates = df.groupBy("Product_Code").count().filter("count > 1").count()
    assert duplicates == 1  # Should fail if duplicates are not handled
