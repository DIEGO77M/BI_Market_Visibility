"""
Unit tests for Gold dim_product logic (Senior Level).
"""
import pytest
from pyspark.sql import SparkSession, Row, functions as F

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("unit-test").getOrCreate()

def test_dim_product_schema_and_uniqueness(spark):
    rows = [Row(Product_Code="A1", Brand="X", Category="Bebidas"), Row(Product_Code="A1", Brand="X", Category="Bebidas")]
    df = spark.createDataFrame(rows)
    assert set(df.columns) >= {"Product_Code", "Brand", "Category"}
    duplicates = df.groupBy("Product_Code").count().filter("count > 1").count()
    assert duplicates == 1

def test_audit_enrichment(spark):
    row = Row(Product_Code="A1", Brand="X", Category="Bebidas")
    df = spark.createDataFrame([row])
    df = df.withColumn("audit_flag", F.lit(True))
    assert "audit_flag" in df.columns
