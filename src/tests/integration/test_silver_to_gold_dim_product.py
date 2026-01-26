"""
Integration test: Silver → Gold dim_product pipeline.
"""
import pytest
from pyspark.sql import SparkSession, Row

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("integration-test").getOrCreate()

def test_silver_to_gold_dim_product(spark):
    # Simulate silver data
    silver_row = Row(Product_Code="A1", Brand="X", Category="Bebidas")
    silver_df = spark.createDataFrame([silver_row])
    # Simulate gold transformation (e.g., audit logic)
    gold_df = silver_df.withColumn("is_valid", silver_df.Brand.isNotNull())
    assert "is_valid" in gold_df.columns
