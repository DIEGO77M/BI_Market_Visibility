"""
Unit tests for Silver fact_price_audit transformation logic.
"""
import pytest
from pyspark.sql import SparkSession, Row, functions as F

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("unit-test").getOrCreate()

def test_join_with_master_products_and_integrity(spark):
    price_row = Row(Cod_Producto="A1", Precio=10.5, Fecha="2026-01-01")
    prod_row = Row(Product_Code="A1", Brand="X")
    price_df = spark.createDataFrame([price_row])
    prod_df = spark.createDataFrame([prod_row])
    joined = price_df.join(prod_df, price_df.Cod_Producto == prod_df.Product_Code)
    assert joined.count() == 1
    assert "Brand" in joined.columns

def test_date_normalization(spark):
    row = Row(Year=2026, Month=1)
    df = spark.createDataFrame([row])
    df = df.withColumn("date", F.expr("make_date(Year, Month, 1)"))
    assert "date" in df.columns

def test_deduplication_logic(spark):
    rows = [Row(Cod_Producto="A1", Cod_PDV="001", Fecha="2026-01-01"), Row(Cod_Producto="A1", Cod_PDV="001", Fecha="2026-01-01")]
    df = spark.createDataFrame(rows)
    deduped = df.dropDuplicates(["Cod_Producto", "Cod_PDV", "Fecha"])
    assert deduped.count() == 1
