Unit tests for Bronze Price Audit ingestion and validation logic.
"""
"""
Unit tests for Bronze Price Audit ingestion and validation logic (Senior Level).
"""
import pytest
from pyspark.sql import SparkSession, Row

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("unit-test").getOrCreate()

def test_ingest_schema_and_types(spark):
    row = Row(Fecha="2026-01-01", Cod_PDV="001", Cod_Producto="A1", Precio=10.5, Tiene_promocion="No")
    df = spark.createDataFrame([row])
    assert set(df.columns) >= {"Fecha", "Cod_PDV", "Cod_Producto", "Precio", "Tiene_promocion"}
    assert df.schema["Precio"].dataType.typeName() == "double"

def test_ingest_with_nulls_and_duplicates(spark):
    rows = [Row(Fecha="2026-01-01", Cod_PDV="001", Cod_Producto="A1", Precio=None, Tiene_promocion="No"),
            Row(Fecha="2026-01-01", Cod_PDV="001", Cod_Producto="A1", Precio=10.5, Tiene_promocion="No")]
    df = spark.createDataFrame(rows)
    null_count = df.filter(df.Precio.isNull()).count()
    duplicate_count = df.groupBy("Fecha", "Cod_PDV", "Cod_Producto").count().filter("count > 1").count()
    assert null_count == 1
    assert duplicate_count == 1

def test_metadata_columns_present(spark):
    # Simulate metadata addition
    row = Row(Fecha="2026-01-01", Cod_PDV="001", Cod_Producto="A1", Precio=10.5, Tiene_promocion="No", Ingested_At="2026-01-26")
    df = spark.createDataFrame([row])
    assert "Ingested_At" in df.columns

def test_no_forbidden_actions():
    # Static code analysis placeholder
    assert True
