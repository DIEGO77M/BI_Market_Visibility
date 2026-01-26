"""
Data quality tests for Bronze Price Audit.
"""
import pytest
from pyspark.sql import SparkSession, Row

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("dq-test").getOrCreate()

def test_no_null_prices(spark):
    row = Row(Fecha="2026-01-01", Cod_PDV="001", Cod_Producto="A1", Precio=None)
    df = spark.createDataFrame([row])
    null_count = df.filter(df.Precio.isNull()).count()
    assert null_count == 1

def test_no_duplicate_keys(spark):
    rows = [Row(Fecha="2026-01-01", Cod_PDV="001", Cod_Producto="A1", Precio=10.5),
            Row(Fecha="2026-01-01", Cod_PDV="001", Cod_Producto="A1", Precio=11.0)]
    df = spark.createDataFrame(rows)
    duplicates = df.groupBy("Fecha", "Cod_PDV", "Cod_Producto").count().filter("count > 1").count()
    assert duplicates == 1

def test_price_range(spark):
    row = Row(Fecha="2026-01-01", Cod_PDV="001", Cod_Producto="A1", Precio=-5.0)
    df = spark.createDataFrame([row])
    assert df.filter(df.Precio < 0).count() == 1
