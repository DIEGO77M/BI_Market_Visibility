# Silver Layer - Architecture Decision Records (ADR)

**Purpose:** Document the architectural decisions, rationale, and trade-offs for the Silver Layer implementation.

**Author:** Diego Mayorga  
**Date:** 2026-01-06  
**Project:** BI Market Visibility Analysis

---

## Overview

The Silver Layer implements **curated standardization** - transforming raw Bronze snapshots into analysis-ready datasets with consistent schema, validated domains, and deterministic quality controls. This layer bridges raw ingestion (Bronze) and business logic (Gold) by establishing data contracts.

### Design Philosophy

```
Bronze (Raw)              Silver (Clean)              Gold (Analytics)
━━━━━━━━━━━━              ━━━━━━━━━━━━               ━━━━━━━━━━━━
Snapshot ingestion   →    Schema contracts      →    Business KPIs
Schema inference     →    Type enforcement      →    Star schema
Append-only          →    Deduplication         →    SCD Type 2
Raw nulls            →    Validated domains     →    Aggregations
```

---

## Architectural Decision Records

### ADR-001: Snapshot-Based Processing (No Incremental)

**Decision:** Read complete Bronze Delta tables for each Silver refresh  
**Problem Solved:** Complexity of incremental processing with deduplication  
**Why This Matters:**
- Simplifies lineage and enables deterministic replay
- Deduplication requires full dataset visibility (cannot dedupe incrementally)
- Bronze tables are small enough (<1M rows) that snapshot processing is efficient

**Trade-offs:**
| Approach | Performance | Complexity | Data Correctness |
|----------|------------|------------|------------------|
| Snapshot (selected) | ⚠️ Moderate | ✅ Simple | ✅ Guaranteed |
| Incremental | ✅ Fast | ❌ Complex | ⚠️ Dedup risk |

**Rationale:** For portfolio-scale data (<1M rows), snapshot processing provides deterministic results with minimal complexity. Incremental patterns add value only at scale (>10M rows).

---

### ADR-002: Serverless-First Design (No Cache/Persist)

**Decision:** No `cache()` or `persist()` operations in Silver transformations  
**Problem Solved:** Databricks Serverless incompatibility with caching  
**Why This Matters:**
- Serverless compute doesn't support persistent caching
- Forces single-pass transformation design (cleaner code)
- Reduces memory pressure and cost

**Implementation:**
```python
# ❌ Don't: Caching breaks Serverless
df.cache()
df.count()  # Triggers cache materialization
transform(df)

# ✅ Do: Single-pass transformation chain
df = spark.read.table("bronze_table")
df = apply_transformations(df)
df.write.saveAsTable("silver_table")  # One action
```

**Benefits:**
- ✅ Serverless compatible (no cluster configuration)
- ✅ Memory efficient (no cached DataFrames)
- ✅ Predictable cost (no cache eviction surprises)

---

### ADR-003: Deterministic Deduplication by Temporal Ordering

**Decision:** Deduplicate by business key using `bronze_ingestion_timestamp` ordering  
**Problem Solved:** Non-deterministic results when duplicates exist  
**Why This Matters:**
- Multiple Bronze loads may create duplicate business keys
- Random deduplication produces inconsistent Silver outputs
- Temporal ordering ensures most recent record wins

**Implementation:**
```python
window = Window.partitionBy("business_key") \
               .orderBy(col("bronze_ingestion_timestamp").desc_nulls_last())
df = df.withColumn("_rn", row_number().over(window)) \
       .filter(col("_rn") == 1) \
       .drop("_rn")
```

**Selection Criteria:**
- **Primary:** Most recent `bronze_ingestion_timestamp`
- **Fallback:** Deterministic column-based ordering (if timestamp absent)

**Benefits:**
- ✅ Reproducible results across pipeline runs
- ✅ Audit trail preserved (Bronze timestamp retained)
- ✅ Predictable behavior for debugging

---

### ADR-004: Schema Stability with Explicit Type Casting

**Decision:** Explicit type casting for business-critical columns  
**Problem Solved:** Schema drift from type inference inconsistencies  
**Why This Matters:**
- Bronze uses schema inference (types may vary between loads)
- Gold Layer joins require consistent types
- Prevents silent type coercion errors

**Implementation:**
```python
# Business keys → StringType (prevents numeric confusion)
df = df.withColumn("product_code", col("product_code").cast(StringType()))
df = df.withColumn("pdv_code", col("pdv_code").cast(StringType()))

# Numeric fields → Explicit precision
df = df.withColumn("latitude", col("latitude").cast(DoubleType()))
df = df.withColumn("price", spark_round(col("price").cast(DoubleType()), 2))
```

**Type Enforcement Rules:**
| Column Type | Target Type | Rationale |
|------------|-------------|-----------|
| Business keys | StringType | Prevent leading-zero truncation |
| Coordinates | DoubleType | Geographic precision |
| Prices/Values | DoubleType, 2 decimals | Monetary standard |
| Quantities | IntegerType | Whole units |
| Dates | DateType | Standard format |

---

### ADR-005: Intentional Null Preservation (No Default Imputation)

**Decision:** Preserve nulls as-is, add completeness flags instead  
**Problem Solved:** Data quality obfuscation through silent imputation  
**Why This Matters:**
- Imputed values can mislead downstream analysis
- Nulls explicitly signal missing data
- Completeness flags enable transparent filtering decisions

**Implementation:**
```python
# ❌ Don't: Silent imputation hides data quality issues
df = df.withColumn("quantity", coalesce(col("quantity"), lit(0)))

# ✅ Do: Preserve null + add quality flag
df = df.withColumn("is_complete_transaction", 
                   col("quantity").isNotNull() & col("value").isNotNull())
```

**Quality Flags Added:**
| Flag | Logic | Purpose |
|------|-------|---------|
| `is_complete_transaction` | quantity AND value non-null | Transaction completeness |
| `is_active_sale` | quantity > 0 | Filter actual vs cancelled |

**Benefits:**
- ✅ Transparent data quality (nulls visible in reports)
- ✅ Downstream flexibility (analyst chooses imputation strategy)
- ✅ Audit compliance (no hidden transformations)

---

### ADR-006: Unified Price Validation Criteria (Strictly Positive)

**Decision:** All price/value fields must be > 0, otherwise nullified  
**Problem Solved:** Inconsistent validation rules across tables  
**Why This Matters:**
- Zero prices are semantically ambiguous (free vs missing?)
- Negative prices indicate data errors
- Unified rule simplifies Gold Layer aggregation logic

**Implementation:**
```python
# Applied to ALL price columns across all Silver tables
df = df.withColumn(
    "price",
    when(col("price").isNull(), lit(None))
    .when(col("price") <= 0, lit(None))  # Strictly positive
    .otherwise(spark_round(col("price").cast(DoubleType()), 2))
)
```

**Validation Rules:**
| Field Type | Rule | Invalid Handling |
|------------|------|------------------|
| Prices | > 0 | Nullify |
| Values | > 0 | Nullify |
| Quantities | >= 0 | Nullify if negative |
| Dates | <= current_date | Nullify future dates |

---

### ADR-007: Dynamic Partition Overwrite for Fact Tables

**Decision:** Use `partitionOverwriteMode=dynamic` for transactional tables  
**Problem Solved:** Inefficient full-table rewrites for partitioned data  
**Why This Matters:**
- Price Audit: 24+ monthly partitions (year_month)
- Sell-In: Annual partitions (year)
- Dynamic overwrite rewrites only incoming partitions

**Implementation:**
```python
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("year_month") \
    .saveAsTable(SILVER_PRICE_AUDIT)
```

**Performance Comparison:**
| Write Mode | Behavior | Performance | Use Case |
|------------|----------|-------------|----------|
| Full Overwrite | Drops all partitions | ❌ Slow | Dimension tables |
| Dynamic Overwrite | Overwrites only touched partitions | ✅ Fast | Fact tables |

**Constraint:** Schema must be stable (no `overwriteSchema` with dynamic partition overwrite)

---

### ADR-008: Derived Technical Columns in Silver (Not Gold)

**Decision:** Add `unit_price`, `transaction_month`, quality flags in Silver  
**Problem Solved:** Redundant calculations in Gold queries  
**Why This Matters:**
- `unit_price = value / quantity` is row-level (not aggregation)
- Enables Silver-level filtering without Gold joins
- Reduces Gold Layer complexity

**Columns Added:**
```python
# Unit price calculation (row-level, not aggregation)
df = df.withColumn(
    "unit_price",
    when((col("quantity") > 0) & (col("value") > 0),
         spark_round(col("value") / col("quantity"), 2))
    .otherwise(lit(None))
)

# Temporal extraction
df = df.withColumn("transaction_month", month(col("date")))

# Quality flags
df = df.withColumn("is_active_sale", col("quantity") > 0)
```

**Silver vs Gold Scope:**
| Silver (Technical) | Gold (Business) |
|-------------------|-----------------|
| unit_price | avg_unit_price (aggregated) |
| transaction_month | monthly_sales_kpi |
| is_complete_transaction | data_quality_score |

---

### ADR-009: snake_case Column Standardization

**Decision:** Convert all column names to snake_case  
**Problem Solved:** Cross-platform compatibility and SQL ergonomics  
**Why This Matters:**
- Bronze preserves original names (spaces, parentheses)
- Power BI and SQL prefer snake_case
- Consistent naming reduces downstream errors

**Implementation:**
```python
def standardize_column_names(df, exclude_cols=None):
    exclude_cols = exclude_cols or []
    for old_name in df.columns:
        if old_name in exclude_cols:
            continue
        new_name = old_name.lower().strip()
        new_name = new_name.replace(" ", "_").replace("-", "_")
        new_name = new_name.replace("(", "").replace(")", "")
        df = df.withColumnRenamed(old_name, new_name)
    return df
```

**Examples:**
| Bronze Name | Silver Name |
|-------------|-------------|
| `Code (eLeader)` | `code_eleader` |
| `Product Name` | `product_name` |
| `Sales Value` | `sales_value` |

---

## Silver Layer Tables Summary

| Table | Source | Dedup Key | Partition | Write Mode |
|-------|--------|-----------|-----------|------------|
| `silver_master_pdv` | bronze_master_pdv | code_eleader | None | Overwrite |
| `silver_master_products` | bronze_master_products | product_code | None | Overwrite |
| `silver_price_audit` | bronze_price_audit | None | year_month | Dynamic Overwrite |
| `silver_sell_in` | bronze_sell_in | None | year | Dynamic Overwrite |

---

## Quality Metrics (Zero-Compute)

All Silver tables validated using Delta History metadata:

```python
history = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
metrics = history["operationMetrics"]
rows_written = metrics["numOutputRows"]
files_written = metrics["numFiles"]
```

**No `count()` operations** - maintains Serverless cost efficiency.

---

## Interview Talking Points

**Questions These ADRs Address:**
1. "How do you handle schema drift between layers?"
2. "Why preserve nulls instead of imputing default values?"
3. "When would you use dynamic partition overwrite vs full overwrite?"
4. "How do you ensure deduplication is deterministic?"
5. "What's the difference between Silver technical columns and Gold KPIs?"

**Key Differentiators:**
- ✅ Serverless-first design (no cache/persist)
- ✅ Deterministic deduplication (temporal ordering)
- ✅ Explicit type enforcement (schema contracts)
- ✅ Null preservation with quality flags (transparent data quality)
- ✅ Zero-compute validation (Delta History metrics)

---

## Related Documentation

- [Bronze Architecture Decisions](BRONZE_ARCHITECTURE_DECISIONS.md)
- [Gold Architecture Design](GOLD_ARCHITECTURE_DESIGN.md)
- [Data Dictionary](data_dictionary.md)
- [Power BI Integration Guide](POWERBI_INTEGRATION_GUIDE.md)
