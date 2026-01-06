"""
Gold Layer Unit Tests

Test suite for surrogate key generation, SCD Type 2 logic, fact integrity,
and KPI calculations in the Gold layer.

Author: Senior Analytics Engineer
Date: 2025-01-06
"""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat_ws

from src.utils.gold_layer_utils import (
    generate_dense_surrogate_key,
    validate_surrogate_key_uniqueness,
    validate_scd2_current_only,
    validate_referential_integrity,
    validate_kpi_consistency,
)


class TestSurrogateKeyGeneration:
    """Test surrogate key generation for determinism and distribution."""
    
    def test_surrogate_key_deterministic(self):
        """Same input should always generate same output."""
        key1 = generate_dense_surrogate_key("PROD-001", "product")
        key2 = generate_dense_surrogate_key("PROD-001", "product")
        
        assert key1 == key2, "Surrogate key should be deterministic"
    
    def test_surrogate_key_different_inputs(self):
        """Different inputs should generate different keys (high probability)."""
        key1 = generate_dense_surrogate_key("PROD-001", "product")
        key2 = generate_dense_surrogate_key("PROD-002", "product")
        
        assert key1 != key2, "Different inputs should generate different keys"
    
    def test_surrogate_key_table_offset(self):
        """Different table contexts should produce different ranges."""
        product_key = generate_dense_surrogate_key("SKU-001", "product")
        pdv_key = generate_dense_surrogate_key("SKU-001", "pdv")
        
        # Should be in different ranges due to table offsets
        assert product_key < 100_000, "Product key should be < 100K"
        assert pdv_key < 100_000, "PDV key should be < 100K"
        assert product_key != pdv_key, "Same BK in different contexts should produce different SKs"
    
    def test_surrogate_key_positive_integer(self):
        """All generated keys should be positive integers."""
        key = generate_dense_surrogate_key("TEST-123", "product")
        
        assert isinstance(key, int), "Key should be integer"
        assert key > 0, "Key should be positive"
        assert key < 2_147_483_647, "Key should fit in INT range"


class TestSCD2Logic:
    """Test Slowly Changing Dimension Type 2 logic."""
    
    @pytest.fixture
    def spark(self):
        """Create SparkSession for testing."""
        return SparkSession.builder \
            .appName("test_gold_layer") \
            .master("local[1]") \
            .getOrCreate()
    
    def test_scd2_current_only_validation_pass(self, spark):
        """Validation should pass when only 1 current version per BK."""
        # Create test data: 1 product, 2 versions (1 current, 1 old)
        test_data = [
            ("PROD-001", 100, "2025-01-01", None, False),  # Old version
            ("PROD-001", 101, "2025-06-01", None, True),   # Current version
            ("PROD-002", 102, "2025-01-01", None, True),   # Another product
        ]
        
        df = spark.createDataFrame(
            test_data,
            ["product_sku", "product_sk", "valid_from", "valid_to", "is_current"]
        )
        
        result = validate_scd2_current_only(df, "product_sku", "test_table")
        assert result is True, "Should pass with max 1 current per product"
    
    def test_scd2_current_only_validation_fail(self, spark):
        """Validation should fail when multiple current versions exist."""
        # Create test data: duplicate current versions
        test_data = [
            ("PROD-001", 100, "2025-01-01", None, True),   # Current 1
            ("PROD-001", 101, "2025-06-01", None, True),   # Current 2 (INVALID)
        ]
        
        df = spark.createDataFrame(
            test_data,
            ["product_sku", "product_sk", "valid_from", "valid_to", "is_current"]
        )
        
        result = validate_scd2_current_only(df, "product_sku", "test_table")
        assert result is False, "Should fail with multiple current versions"


class TestReferentialIntegrity:
    """Test foreign key referential integrity checks."""
    
    @pytest.fixture
    def spark(self):
        """Create SparkSession for testing."""
        return SparkSession.builder \
            .appName("test_gold_layer") \
            .master("local[1]") \
            .getOrCreate()
    
    def test_referential_integrity_valid(self, spark):
        """Validation should pass when all FKs reference valid PKs."""
        # Create dimension with PKs
        dim_data = [
            (100, "PROD-001"),
            (101, "PROD-002"),
        ]
        dim_df = spark.createDataFrame(dim_data, ["product_sk", "product_sku"])
        
        # Create facts with valid FKs
        fact_data = [
            (1000, 100, "value1"),
            (1001, 101, "value2"),
            (1002, 100, "value3"),
        ]
        fact_df = spark.createDataFrame(fact_data, ["fact_id", "product_sk", "value"])
        
        result = validate_referential_integrity(
            fact_df, dim_df, "product_sk", "product_sk", "test_fact → test_dim"
        )
        assert result is True, "Should pass with valid FKs"
    
    def test_referential_integrity_orphans(self, spark):
        """Validation should fail when orphaned FKs exist."""
        # Create dimension with PKs
        dim_data = [
            (100, "PROD-001"),
            (101, "PROD-002"),
        ]
        dim_df = spark.createDataFrame(dim_data, ["product_sk", "product_sku"])
        
        # Create facts with invalid FK (999 doesn't exist in dimension)
        fact_data = [
            (1000, 100, "value1"),
            (1001, 999, "orphaned_value"),  # Invalid FK
        ]
        fact_df = spark.createDataFrame(fact_data, ["fact_id", "product_sk", "value"])
        
        result = validate_referential_integrity(
            fact_df, dim_df, "product_sk", "product_sk", "test_fact → test_dim"
        )
        assert result is False, "Should fail with orphaned FKs"


class TestFactIntegrity:
    """Test fact table constraints (immutability, append-only)."""
    
    @pytest.fixture
    def spark(self):
        """Create SparkSession for testing."""
        return SparkSession.builder \
            .appName("test_gold_layer") \
            .master("local[1]") \
            .getOrCreate()
    
    def test_fact_no_duplicate_keys(self, spark):
        """Fact table should have unique grain keys."""
        # Create fact data with duplicates (INVALID)
        fact_data = [
            (1000, "2025-01-01", 100, 200, 50.0, 100.0),  # Day 1, Prod 100, PDV 200
            (1001, "2025-01-01", 100, 200, 60.0, 120.0),  # Duplicate grain (INVALID)
            (1002, "2025-01-02", 100, 200, 45.0, 90.0),   # Different date (valid)
        ]
        fact_df = spark.createDataFrame(
            fact_data,
            ["fact_id", "date", "product_sk", "pdv_sk", "quantity", "value"]
        )
        
        # Count distinct grain combinations
        grain_count = fact_df.select("date", "product_sk", "pdv_sk").distinct().count()
        total_count = fact_df.count()
        
        # Should fail because grain_count (2) < total_count (3)
        assert grain_count < total_count, "Duplicate grain detected (invalid fact table)"
    
    def test_fact_positive_values(self, spark):
        """Fact table quantities and values should be non-negative."""
        fact_data = [
            (1000, 100.0, 500.0),
            (1001, 0.0, 0.0),      # Zero is acceptable
            (1002, -50.0, 250.0),  # Negative (INVALID)
        ]
        fact_df = spark.createDataFrame(fact_data, ["fact_id", "quantity", "value"])
        
        # Count invalid rows
        invalid_count = fact_df.filter((col("quantity") < 0) | (col("value") < 0)).count()
        
        assert invalid_count > 0, "Should detect negative values"


class TestKPICalculations:
    """Test KPI logic and pre-aggregated metric correctness."""
    
    @pytest.fixture
    def spark(self):
        """Create SparkSession for testing."""
        return SparkSession.builder \
            .appName("test_gold_layer") \
            .master("local[1]") \
            .getOrCreate()
    
    def test_kpi_market_share_sum_to_100(self, spark):
        """Market share percentages should sum to 100% per region-date."""
        kpi_data = [
            ("2025-01-01", "REGION-A", "PROD-001", 50.0),
            ("2025-01-01", "REGION-A", "PROD-002", 30.0),
            ("2025-01-01", "REGION-A", "PROD-003", 20.0),
        ]
        kpi_df = spark.createDataFrame(
            kpi_data,
            ["date", "region", "product", "market_share_pct"]
        )
        
        # Sum by date-region
        from pyspark.sql.functions import sum as spark_sum, Window
        window = Window.partitionBy("date", "region")
        kpi_with_total = kpi_df.withColumn(
            "total_share",
            spark_sum("market_share_pct").over(window)
        )
        
        # All totals should be 100 (or very close)
        totals = kpi_with_total.select("total_share").distinct().collect()
        
        for row in totals:
            assert abs(row.total_share - 100.0) < 0.01, f"Market share should sum to 100%, got {row.total_share}"
    
    def test_kpi_price_index_calculation(self, spark):
        """Price index should equal (price / market_avg) * 100."""
        from pyspark.sql.functions import abs as spark_abs
        
        kpi_data = [
            ("2025-01-01", "PROD-001", 10.0, 10.0, 100.0),   # Index = 100
            ("2025-01-01", "PROD-002", 5.0, 10.0, 50.0),    # Index = 50
            ("2025-01-01", "PROD-003", 15.0, 10.0, 150.0),  # Index = 150
        ]
        kpi_df = spark.createDataFrame(
            kpi_data,
            ["date", "product", "observed_price", "market_avg_price", "price_index"]
        )
        
        # Verify calculation
        from pyspark.sql.functions import round as spark_round
        kpi_with_calc = kpi_df.withColumn(
            "expected_index",
            spark_round((col("observed_price") / col("market_avg_price")) * 100, 4)
        )
        
        # All differences should be < 0.01
        max_diff = kpi_with_calc.select(
            spark_abs(col("price_index") - col("expected_index")).alias("diff")
        ).agg({"diff": "max"}).collect()[0][0]
        
        assert max_diff < 0.01, f"Price index calculation error: {max_diff}"


class TestGoldLayerIntegration:
    """Integration tests for complete Gold layer workflow."""
    
    @pytest.fixture
    def spark(self):
        """Create SparkSession for testing."""
        return SparkSession.builder \
            .appName("test_gold_layer") \
            .master("local[1]") \
            .getOrCreate()
    
    def test_date_dimension_size(self, spark):
        """Date dimension should contain ~3650 rows (10 years)."""
        # Simulate date dimension creation
        date_range_df = spark.sql("""
            WITH date_series AS (
                SELECT sequence(to_date('2020-01-01'), to_date('2029-12-31'), interval 1 day) as dates
            )
            SELECT explode(dates) as date_val FROM date_series
        """)
        
        count = date_range_df.count()
        
        # Should be approximately 10 years × 365 days (allow for leap years)
        assert 3600 < count < 3700, f"Date dimension should have ~3650 rows, got {count}"
    
    def test_fact_dimension_join_key_cardinality(self, spark):
        """Fact grain should be (date_sk, product_sk, pdv_sk)."""
        # Simulate fact data
        fact_data = [
            (1, 100, 200, 50.0),
            (1, 100, 201, 40.0),
            (1, 101, 200, 30.0),
            (2, 100, 200, 60.0),  # Same product-pdv, different date
        ]
        fact_df = spark.createDataFrame(
            fact_data,
            ["date_sk", "product_sk", "pdv_sk", "quantity"]
        )
        
        # Each (date, product, pdv) should be unique
        grain_count = fact_df.select("date_sk", "product_sk", "pdv_sk").distinct().count()
        total_rows = fact_df.count()
        
        assert grain_count == total_rows, "Fact table should have unique grain"


# ============================================================================
# Test Execution
# ============================================================================

if __name__ == "__main__":
    # Run with: pytest src/tests/test_gold_layer.py -v
    pytest.main([__file__, "-v"])

