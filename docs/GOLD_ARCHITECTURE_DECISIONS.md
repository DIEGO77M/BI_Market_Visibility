# Gold Layer - Architecture Decision Records (ADR)

**Purpose:** Document the architectural decisions, rationale, and trade-offs for the Gold Layer implementation.

**Author:** Diego Mayorga  
**Date:** 2026-01-06  
**Project:** BI Market Visibility Analysis

---

## Overview

The Gold Layer implements **analytics-ready datasets** optimized for Market Visibility decision-making. This layer transforms curated Silver data into dimensional models and pre-calculated KPIs designed for direct consumption by Power BI and Streamlit.

### Architectural Philosophy

```
Silver (Clean)                 Gold (Analytics)                   Consumers
━━━━━━━━━━━━                  ━━━━━━━━━━━━━━                     ━━━━━━━━━
Standardized data       →     Dimensions (SCD Type 2)      →    Power BI
Validated domains       →     Facts (Star Schema)          →    Streamlit
Deduplication          →     KPIs (Denormalized)          →    Alerts
Quality flags          →     Pre-aggregated metrics       →    Reports
```

---

## Anti-Saturation Architecture (Critical)

### ADR-001: Decoupled Job Architecture

**Decision:** Each Gold table is processed by an independent notebook/job  
**Problem Solved:** Monolithic notebooks saturating Serverless clusters  
**Why This Matters:**
- Single notebook failures don't cascade
- Each table can be reprocesed independently
- Resource utilization is predictable
- Parallelization is possible for dimensions

**Implementation Structure:**
```
notebooks/gold/
├── 03_gold_orchestrator.py       # Configuration + Automated Pipeline Execution
├── dimensions/
│   ├── gold_dim_date.py          # Independent job
│   ├── gold_dim_product.py       # Independent job
│   └── gold_dim_pdv.py           # Independent job
├── facts/
│   ├── gold_fact_sell_in.py      # Independent job
│   ├── gold_fact_price_audit.py  # Independent job
│   └── gold_fact_stock.py        # Independent job
├── kpis/
│   ├── gold_kpi_market_visibility.py  # Independent job
│   └── gold_kpi_market_share.py       # Independent job
└── validation/
    └── gold_validation.py        # Read-only, post-execution
```

**Orchestrator Execution Options:**
1. **Widget Mode:** Select `run_pipeline = yes` in Databricks UI
2. **Code Mode:** Set `RUN_PIPELINE = True` in orchestrator
3. **CLI Mode:** Run notebook with parameters via `dbutils.notebook.run()`

**Trade-offs:**

| Approach | Saturation Risk | Recovery Time | Complexity |
|----------|-----------------|---------------|------------|
| Monolithic (rejected) | ❌ High | Slow | Low |
| Decoupled (selected) | ✅ Low | Fast | Medium |
| Microservices | ✅ Low | Fast | High |

**Rationale:** Decoupled jobs provide optimal balance between operational stability and implementation complexity for a portfolio-scale project.

---

### ADR-002: No Cache/Persist Operations

**Decision:** Eliminate all `cache()` and `persist()` calls  
**Problem Solved:** Databricks Serverless incompatibility and memory pressure  
**Why This Matters:**
- Serverless doesn't support persistent caching
- Cache eviction is unpredictable
- Single-pass transformations are cleaner

**Implementation Pattern:**
```python
# ❌ Don't: Caching breaks Serverless
df = spark.read.table("silver_table")
df.cache()
df_transformed = transform(df)
df_transformed.write.saveAsTable("gold_table")

# ✅ Do: Single-pass transformation
df = spark.read.table("silver_table")
df_transformed = transform(df)  # All transformations chained
df_transformed.write.saveAsTable("gold_table")  # One action
```

**Benefits:**
- ✅ Serverless compatible
- ✅ Predictable memory usage
- ✅ Simpler debugging

---

### ADR-003: Single Write Operation Per Notebook

**Decision:** Each notebook performs exactly one write action  
**Problem Solved:** Multiple write operations causing resource contention  
**Why This Matters:**
- Multiple writes = multiple Spark jobs = unpredictable scheduling
- Single write ensures atomic completion
- Easier monitoring and alerting

**Implementation:**
```python
# Entire notebook transformation chain
df = read_silver()
df = join_dimensions(df)
df = calculate_metrics(df)
df = apply_business_rules(df)

# SINGLE WRITE at the end
df.write.saveAsTable("gold_table")
```

---

### ADR-004: Partitioning Strategy by Date

**Decision:** Partition fact tables and KPIs by `date_sk` or `year_month`  
**Problem Solved:** Full table scans on time-series queries  
**Why This Matters:**
- 95% of queries filter by date
- Dynamic partition overwrite enables incremental updates
- Query performance improves dramatically

**Partitioning Rules:**

| Table Type | Partition Column | Rationale |
|------------|------------------|-----------|
| Facts | `date_sk` | Daily grain, time-series queries |
| KPI Daily | `date_sk` | Dashboard date filters |
| KPI Monthly | `year_month` | Trend analysis |
| Dimensions | None | Small tables, full scan is fast |

---

## Dimensional Model Design

### ADR-005: SCD Type 2 for Master Dimensions

**Decision:** Implement Slowly Changing Dimension Type 2 for Product and PDV  
**Problem Solved:** Loss of historical context when attributes change  
**Why This Matters:**
- Product brand/category may change (acquisitions, rebranding)
- Store channel/region may change (reorganizations)
- Historical analysis requires point-in-time accuracy

**Schema Pattern:**
```sql
CREATE TABLE gold_dim_product (
    product_sk STRING,       -- Surrogate key (hash-based)
    product_code STRING,     -- Business key
    product_name STRING,     -- Tracked attribute
    brand STRING,            -- Tracked attribute
    segment STRING,          -- Tracked attribute
    category STRING,         -- Tracked attribute
    valid_from DATE,         -- Version start
    valid_to DATE,           -- Version end (9999-12-31 for current)
    is_current BOOLEAN       -- Active version flag
)
```

**Query Patterns:**
```sql
-- Current attributes
SELECT * FROM gold_dim_product WHERE is_current = true

-- Attributes as of a specific date
SELECT * FROM gold_dim_product 
WHERE '2025-06-15' BETWEEN valid_from AND valid_to
```

**Trade-offs:**

| SCD Type | History | Complexity | Storage |
|----------|---------|------------|---------|
| Type 1 | ❌ No | Low | Low |
| Type 2 (selected) | ✅ Full | Medium | Medium |
| Type 3 | ⚠️ Limited | Low | Low |

---

### ADR-006: Hash-Based Surrogate Keys

**Decision:** Use SHA-256 hash (truncated to 16 chars) for surrogate keys  
**Problem Solved:** Integer sequences don't scale across distributed systems  
**Why This Matters:**
- Deterministic: same input always produces same key
- No sequence management needed
- Safe for incremental loads

**Implementation:**
```python
from pyspark.sql.functions import sha2, concat_ws, current_timestamp

df = df.withColumn(
    "product_sk",
    sha2(concat_ws("||", 
        col("product_code"),
        col("product_name"),
        col("brand"),
        current_timestamp().cast(StringType())
    ), 256).substr(1, 16)
)
```

**Special Case - Date Dimension:**
```python
# Integer key in YYYYMMDD format (human-readable)
date_sk = date_format(col("date"), "yyyyMMdd").cast(IntegerType())
# Example: 20260106
```

---

### ADR-007: Star Schema for Facts, Denormalized for KPIs

**Decision:** Use star schema for facts, intentionally denormalize KPI tables  
**Problem Solved:** Performance vs flexibility trade-off  
**Why This Matters:**
- Facts need flexibility for ad-hoc analysis (star schema)
- KPIs need speed for dashboards (denormalized)
- Different use cases = different designs

**Design Pattern:**

```
┌─────────────────┐
│  STAR SCHEMA    │  ← For Ad-Hoc Analysis
│  (Facts + Dims) │
├─────────────────┤
│ gold_fact_*     │  Granular data
│ gold_dim_*      │  Joins required
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  DENORMALIZED   │  ← For Dashboards
│  (KPI Tables)   │
├─────────────────┤
│ gold_kpi_*      │  Pre-joined
│                 │  Pre-aggregated
│                 │  No DAX needed
└─────────────────┘
```

---

## KPI Design Decisions

### ADR-008: Pre-Calculated KPIs (No Runtime Computation)

**Decision:** Materialize KPIs in Gold tables, not in BI tools  
**Problem Solved:** Slow dashboards, inconsistent metrics, DAX complexity  
**Why This Matters:**
- Power BI calculates at query time (slow for complex metrics)
- Multiple dashboards may calculate same KPI differently
- Pre-calculation ensures consistency and speed

**Metrics Pre-Calculated:**

| KPI | Table | Formula |
|-----|-------|---------|
| `availability_rate` | kpi_market_visibility | (PDVs with stock / Total PDVs) × 100 |
| `price_competitiveness_index` | kpi_market_visibility | (Observed / Market Avg) × 100 |
| `sell_in_sell_out_ratio` | kpi_market_visibility | Sell-In / Estimated Sell-Out |
| `lost_sales_value_estimated` | kpi_market_visibility | Stock-Out Days × Avg Demand × Avg Price |
| `stock_health_score` | kpi_market_visibility | Weighted score (0-100) |
| `portfolio_share_qty_pct` | kpi_market_share | (Brand Qty / Total Qty) × 100 |

**Benefits:**
- ✅ Sub-second dashboard response
- ✅ Consistent KPIs across all consumers
- ✅ Reduced Power BI Pro license compute

---

### ADR-009: Stock Estimation Model (Documented Assumptions)

**Decision:** Estimate stock from sell-in patterns (no actual inventory data)  
**Problem Solved:** Lack of inventory system integration  
**Why This Matters:**
- Real-world constraint: inventory data not available
- Estimation enables proactive stock management
- Must document assumptions for transparency

**Estimation Model:**
```
estimated_sell_out = sell_in × SELL_OUT_RATE (0.85)
estimated_closing_stock = cumulative_sell_in - estimated_cumulative_sell_out
stock_days = estimated_closing_stock / avg_daily_sell_out
```

**Documented Assumptions:**

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| SELL_OUT_RATE | 0.85 | Industry standard FMCG sell-through |
| STOCK_OUT_THRESHOLD | 0 days | Zero inventory = stock-out |
| OVERSTOCK_THRESHOLD | 45 days | >6 weeks = excess inventory |
| LOOKBACK_PERIOD | 30 days | Monthly average for stability |

**Trade-off Acknowledgment:**
- ⚠️ Estimation is not actual inventory
- ⚠️ Accuracy depends on sell-out rate assumption
- ✅ Better than no stock visibility
- ✅ Enables proactive alerts

---

### ADR-010: Market Share Limitation (Portfolio Share Only)

**Decision:** Calculate portfolio share, not true market share  
**Problem Solved:** No competitor data available  
**Why This Matters:**
- True market share requires competitor sales data
- Portfolio share still valuable for internal optimization
- Must be transparent about limitation

**What We Can Measure:**

| Metric | Available | Use Case |
|--------|-----------|----------|
| Portfolio share by brand | ✅ Yes | Brand performance tracking |
| Portfolio share by channel | ✅ Yes | Channel strategy |
| Portfolio share by region | ✅ Yes | Regional focus |
| True market share vs competitors | ❌ No | Requires competitor data |

**Naming Convention:**
```python
# Explicit naming to avoid confusion
col("portfolio_share_qty_pct")   # Not "market_share"
col("portfolio_share_value_pct")
```

---

## Operational Decisions

### ADR-011: Zero-Compute Validation via Delta History

**Decision:** Use Delta Lake metadata for validation instead of DataFrame scans  
**Problem Solved:** Validation consuming significant compute resources  
**Why This Matters:**
- `df.count()` triggers full table scan
- Delta History provides row counts from metadata
- Zero compute cost for basic validation

**Implementation:**
```python
# ❌ Don't: Full table scan
row_count = df.count()  # Expensive

# ✅ Do: Delta metadata
history = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
metrics = history["operationMetrics"]
row_count = metrics.get("numOutputRows", "N/A")  # Free
```

---

### ADR-012: Broadcast Joins for Dimension Lookups

**Decision:** Broadcast dimension tables when joining to facts  
**Problem Solved:** Shuffle-heavy joins for small dimension tables  
**Why This Matters:**
- Dimensions are small (< 10MB typically)
- Facts are large (millions of rows potentially)
- Broadcast eliminates shuffle

**Implementation:**
```python
from pyspark.sql.functions import broadcast

df_dim = broadcast(spark.read.table(GOLD_DIM_PRODUCT)
    .filter(col("is_current") == True))

df_fact_enriched = df_fact.join(df_dim, "product_sk", "left")
```

**Guidelines:**
- Broadcast if dimension < 10MB
- Filter to `is_current = true` before broadcast (SCD2)
- Monitor broadcast size in Spark UI

---

## Power BI Integration

### ADR-013: Direct Query Optimization

**Decision:** Design KPI tables for Power BI Direct Query mode  
**Problem Solved:** Large imports and stale data  
**Why This Matters:**
- Import mode: stale data, large PBIX files
- Direct Query: always fresh, but needs optimization
- Gold KPI tables optimized for Direct Query

**Optimization Techniques:**

| Technique | Implementation |
|-----------|----------------|
| Pre-aggregation | KPIs already aggregated |
| Denormalization | No joins needed in DAX |
| Partitioning | date_sk enables pruning |
| Minimal columns | Only needed metrics |

**Recommended Power BI Model:**
```
gold_kpi_market_visibility_daily  ← Primary table (Direct Query)
    │
    └── Slice by date, product, channel (pre-denormalized)
```

---

## Summary: Key Differentiators

**Questions These ADRs Address:**

1. "How do you prevent Serverless cluster saturation?"
   → Decoupled jobs, single write, no cache

2. "Why denormalize KPI tables?"
   → Dashboard performance, consistent metrics, reduced DAX

3. "How do you handle lack of inventory data?"
   → Documented estimation model with explicit assumptions

4. "How do you validate without expensive scans?"
   → Delta History metadata (zero compute)

5. "Why SCD Type 2 for dimensions?"
   → Historical accuracy for point-in-time analysis

**Architectural Guarantees:**
- ✅ Each job can fail independently
- ✅ Each job can be reprocessed alone
- ✅ No cache/persist operations
- ✅ Single write per job
- ✅ Partitioned for query efficiency
- ✅ Pre-calculated KPIs for speed

---

## Related Documentation

- [Bronze Architecture Decisions](BRONZE_ARCHITECTURE_DECISIONS.md)
- [Silver Architecture Decisions](SILVER_ARCHITECTURE_DECISIONS.md)
- [Data Dictionary](data_dictionary.md)
- [Power BI Integration Guide](POWERBI_INTEGRATION_GUIDE.md)

---

## ADR-014: Scalability & Evolution Path

**Decision:** Design for current volume with documented path to scale  
**Problem Solved:** Balance between immediate delivery and future-proofing  

### Current Architecture (Portfolio/Demo)

The current implementation uses `dbutils.notebook.run()` for orchestration:

```python
# 03_gold_orchestrator.py - Sequential execution
dbutils.notebook.run("./dimensions/gold_dim_date", timeout)
dbutils.notebook.run("./dimensions/gold_dim_product", timeout)
# ... etc
```

**Pros:**
- ✅ Simple to understand and demo
- ✅ No external dependencies
- ✅ Works in any Databricks environment
- ✅ Easy debugging

**Cons:**
- ⚠️ Sequential execution (no parallelism)
- ⚠️ Full re-run on failure
- ⚠️ No native monitoring/alerting

### Production Architecture (Enterprise Scale)

For production workloads, use Databricks Workflows:

```
.databricks/workflows/gold_pipeline.yml
```

**Execution Flow (Parallel DAG):**

```
   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
   │ dim_date    │   │ dim_product │   │  dim_pdv    │  ← PARALLEL
   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
          │                 │                 │
          └─────────────────┼─────────────────┘
                            ▼
   ┌─────────────────────────────────────────────────┐
   │   fact_sell_in ║ fact_price_audit               │  ← PARALLEL
   └────────┬───────╨────────────┬───────────────────┘
            │                    │
            ▼                    │
      ┌───────────┐              │
      │fact_stock │──────────────┘                      ← SEQUENTIAL
      └─────┬─────┘
            ▼
   ┌─────────────────────────────────────────────────┐
   │   kpi_market_visibility → kpi_market_share      │  ← SEQUENTIAL
   └──────────────────────┬──────────────────────────┘
                          ▼
                  ┌──────────────┐
                  │  validation  │                      ← FINAL
                  └──────────────┘
```

**Benefits at Scale:**
- ✅ ~60% time reduction (parallel phases)
- ✅ Individual task retry on failure
- ✅ Native Databricks monitoring
- ✅ Job clusters (cost-efficient vs Serverless)
- ✅ Service principal for security

### Scalability Matrix

| Volume | Rows per Fact | Orchestration | SCD2 Strategy | Broadcast |
|--------|---------------|---------------|---------------|-----------|
| **Current** | 500K - 2M | `notebook.run()` | Full table merge | 10MB |
| **10x Growth** | 5M - 20M | Databricks Workflows | Partition pruning | 100MB |
| **100x Growth** | 50M - 200M | DLT + Workflows | Incremental merge | Adaptive |

### Migration Checklist (When Volume Justifies)

- [ ] Deploy `gold_pipeline.yml` to Databricks Workflows
- [ ] Add partition columns to SCD2 dimensions
- [ ] Increase broadcast threshold to 100MB
- [ ] Enable Adaptive Query Execution (AQE)
- [ ] Switch from Serverless to Job Clusters for scheduled runs
- [ ] Implement checkpoint table for recovery
- [ ] Add data quality expectations in DLT

### Trade-off Documentation

**Why Not DLT From Day 1?**

| Factor | DLT | Current Approach |
|--------|-----|------------------|
| Learning curve | High | Low |
| Debugging complexity | High | Low |
| Portfolio demonstration | Overkill | Right-sized |
| Interview defensibility | Complex to explain | Clear architecture |
| Time to implement | 2-3x longer | Immediate |

**Rationale:** The current architecture prioritizes demonstrability and clarity for portfolio/interview contexts. The documented evolution path shows enterprise awareness without over-engineering.

---

**Author:** Diego Mayorga | diego.mayorgacapera@gmail.com  
**Date:** 2026-01-06  
**Repository:** [github.com/DIEGO77M/BI_Market_Visibility](https://github.com/DIEGO77M/BI_Market_Visibility)
