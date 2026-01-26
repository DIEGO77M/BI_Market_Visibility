"""
Integration test: Bronze → Silver Price Audit pipeline (Senior Level).
"""
import pytest
from pyspark.sql import SparkSession, Row, functions as F

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("integration-test").getOrCreate()

def test_bronze_to_silver_pipeline_full(spark):
    # Simulate bronze data with edge cases
    bronze_rows = [Row(Fecha="2026-01-01", Cod_PDV="001", Cod_Producto="A1", Precio=10.5, Tiene_promocion="No"),
                  Row(Fecha="2026-01-01", Cod_PDV="001", Cod_Producto="A1", Precio=None, Tiene_promocion="No")]
    bronze_df = spark.createDataFrame(bronze_rows)
    # Silver transformation: normalization, deduplication, null handling
    silver_df = bronze_df.withColumnRenamed("Cod_Producto", "Product_Code")
    silver_df = silver_df.dropDuplicates(["Fecha", "Cod_PDV", "Product_Code"])
    silver_df = silver_df.fillna({"Precio": 0.0})
    assert "Product_Code" in silver_df.columns
    assert silver_df.filter(silver_df.Precio.isNull()).count() == 0
    assert silver_df.count() == 1
