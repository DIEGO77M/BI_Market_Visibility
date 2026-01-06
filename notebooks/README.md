# 📓 Databricks Notebooks - Medallion Architecture

End-to-end data pipeline implementing Bronze-Silver-Gold architecture with production-grade practices.

---


## 🎯 Execution Order

| # | Notebook | Status | Runtime | Purpose |
|---|----------|--------|---------|---------|
| 1 | **01_bronze_ingestion** | ✅ | ~5 min | Raw data ingestion (CSV, Excel) |
| 2 | **02_silver_standardization** | ✅ | ~3 min | Standardization, deduplication, validation |
| 3 | **03_gold_analytics** | ✅ | ~5 min | Star schema: dims, facts, KPIs |

---

## 📊 Bronze Layer: Raw Data Ingestion

**[01_bronze_ingestion.py](01_bronze_ingestion.py)**

### What It Does
Ingests raw data from multiple sources into Delta Lake with minimal transformation, establishing an immutable historical record.

### Data Sources & Outputs

| Source | Format | Output Table | Records | Strategy |
|--------|--------|--------------|---------|----------|
| Master PDV | CSV | `bronze_master_pdv` | 51 | Full overwrite |
| Master Products | CSV | `bronze_master_products` | 201 | Full overwrite |
| Price Audit | 24 Excel files | `bronze_price_audit` | 1,200+ | Incremental append |
| Sell-In | 2 Excel files | `bronze_sell_in` | 400+ | Dynamic partition overwrite |

### Architectural Highlights

**ADR-001: Delta History for Zero-Compute Validation**
- Uses `DESCRIBE HISTORY` instead of `df.count()` for metrics
- Saves ~$36/year in Serverless compute costs
- Millisecond latency vs seconds for traditional validation

**ADR-002: Dynamic Partition Overwrite**
- 10-20x faster than MERGE for complete replacements
- ACID-compliant atomic updates per partition
- Idempotent re-runs (safe to execute multiple times)

**ADR-003: Metadata Prefix Pattern**
- All audit columns use `_metadata_` prefix
- Prevents collision with source business columns
- Easy filtering in downstream Silver layer

**ADR-004: Deterministic Batch IDs**
- Format: `YYYYMMDD_HHMMSS_notebook`
- Enables surgical rollbacks via Delta time travel
- Example: `20251231_030015_bronze_ingestion`

### Technical Features
- ✅ Unity Catalog managed tables with governance
- ✅ File-by-file Excel processing (memory-efficient)
- ✅ TBLPROPERTIES for metadata and ownership
- ✅ Serverless optimized (no cache/persist)

---


## 🥈 Silver Layer: Standardization & Drift Monitoring

**[02_silver_standardization.py](02_silver_standardization.py)**

**[silver_drift_monitoring.ipynb](silver_drift_monitoring.ipynb)**

### What It Does
Applies business rules, data quality checks, and standardization to Bronze data, creating analytics-ready datasets. After each Silver write, the drift monitoring notebook detects schema, quality, and volume drift, logging results in the audit table for observability and executive action.

### How to Run Drift Monitoring
1. Run the Silver pipeline notebook.
2. Run `silver_drift_monitoring.ipynb` in Databricks (manual or as a post-write job).
3. Review recent drift events in the `silver_drift_history` table.

**Audit Table Example:**
```sql
SELECT * FROM workspace.default.silver_drift_history ORDER BY timestamp DESC;
```

**[02_silver_standardization.py](02_silver_standardization.py)**

### What It Does
Applies business rules, data quality checks, and standardization to Bronze data, creating analytics-ready datasets.

### Transformations Applied

| Transformation | Purpose | Example |
|----------------|---------|---------|
| **Schema Standardization** | snake_case, explicit types | `ProductName` → `product_name STRING` |
| **Text Normalization** | Consistent formatting | `trim()`, `upper()` on codes |
| **Deduplication** | Single source of truth | Latest record by PDV/Product code |
| **Domain Validation** | Business rule enforcement | prices > 0, no future dates |
| **Derived Columns** | Enrich with calculations | `unit_price`, `is_active_sale` |

### Architecture Principles

**"Read ONLY from Bronze Delta Tables"**
- No raw file ingestion in Silver
- Bronze is single source of truth
- Clear layer separation

**"One Write Action Per Dataset"**
- Single `.write()` operation per table
- Avoids data loss and partial writes
- Serverless cost optimization

**"No Over-Engineering"**
- No streaming (batch is sufficient)
- No CDC (not required)
- No unnecessary complexity

### Output Tables

| Table | Partitioning | Business Logic |
|-------|--------------|----------------|
| `silver_master_pdv` | None | Deduplicated by PDV_Code, standardized names |
| `silver_master_products` | None | Price validated, brand hierarchy |
| `silver_price_audit` | `year_month` | Filtered invalid prices, enriched with month |
| `silver_sell_in` | `year` | Unit price calculated, active sales flagged |

---

## 🥇 Gold Layer: Dimensional Modeling & Analytics

**[03_gold_analytics.py](03_gold_analytics.py)** ✅

### What It Does
Transforms Silver curated data into enterprise analytics through **star schema** dimensional modeling optimized for Power BI consumption and real-time decision-making.

### Architecture: Star Schema

```
Dimensions (Small, Broadcast):
├── gold_dim_date          [3,650 rows - immutable, 10-year calendar]
├── gold_dim_product       [250+ rows - SCD Type 2, tracks brand/segment changes]
└── gold_dim_pdv           [75+ rows - SCD Type 2, tracks location/channel changes]

Facts (Large, Partitioned):
├── gold_fact_sell_in      [500K-2M rows - daily aggregation, partitioned by year]
├── gold_fact_price_audit  [500K-2M rows - price observations, partitioned by year_month]
└── gold_fact_stock        [500K-2M rows - estimated inventory, partitioned by year]

KPI Tables (Pre-Aggregated):
├── gold_kpi_market_visibility_daily  [Ready for Power BI, no DAX needed]
└── gold_kpi_market_share             [Market penetration analysis]
```

### Design Principles

**1. Surrogate Key Strategy**
- Deterministic hash-based integer keys (no strings in joins)
- Prevents collisions via table offset (products: 10K, pdvs: 20K, facts: 1B+)
- Reproducible, testable, incremental-friendly

**2. SCD Type 2 Dimensions**
- Tracks historical product/PDV attributes (brand, segment, location changes)
- Preserves historical accuracy (facts classify correctly at transaction date)
- One current version per business key (enforced by MERGE logic)

**3. Append-Only Facts**
- Insert-only (no deletes or updates)
- Idempotent via Dynamic Partition Overwrite (DPO)
- Enables incremental refresh (10-20x faster than MERGE)

**4. Pre-Computed KPIs**
- All business logic in Gold (immutable, versioned)
- Minimal DAX in Power BI (just SUM/AVG aggregates)
- Testable, auditable metric calculations

### Output Tables & KPIs

| Table | Grain | Rows | Partition | Business Value |
|-------|-------|------|-----------|-----------------|
| `gold_dim_date` | 1 per day | 3.6K | None | Calendar, fiscal periods |
| `gold_dim_product` | 1 per version | 250-350 | None | Product hierarchy with history |
| `gold_dim_pdv` | 1 per version | 75-100 | None | Store attributes with history |
| `gold_fact_sell_in` | (date, product, pdv) | 500K-2M | `year` | Daily sales by product/store |
| `gold_fact_price_audit` | (date, product, pdv) | 500K-2M | `year_month` | Price observations + market avg |
| `gold_fact_stock` | (date, product, pdv) | 500K-2M | `year` | Inventory levels (estimated) |
| `gold_kpi_market_visibility_daily` | (date, product, pdv) | 500K-2M | `year` | Efficiency score, availability, lost sales |
| `gold_kpi_market_share` | (date, product, region) | 20K-100K | `year` | Market share %, penetration % |

### Key Metrics (Now Available in Power BI)

```
💰 Sell-In Visibility
   └─ Daily quantities & values by product × PDV
   
💲 Price Competitiveness
   └─ Price index (observed vs market avg)
   └─ Variance detection & outlier flags
   
🌍 Market Penetration
   └─ Market share % (units & value)
   └─ PDV coverage by region/segment
   
📦 Stock Availability
   └─ Days of supply (estimated from sell-in)
   └─ Stockout detection
   └─ Overstock alerts
   
⚙️ Operational Efficiency
   └─ Efficiency score (0-100)
   └─ Lost sales estimation
   └─ Availability rate %
```

### Incremental Refresh Strategy

| Table | Partition | Cadence | Mode | Idempotency |
|-------|-----------|---------|------|------------|
| Dimensions | None | Daily (SCD2 check) | MERGE | Upsert by business key |
| Facts | Year/Month | Incremental | DPO | Upsert by partition |
| KPIs | Year | Daily | DPO | Upsert by partition |

**DPO (Dynamic Partition Overwrite):**
- Only rewrites partitions with new data
- 10-20x faster than traditional MERGE
- Guarantees idempotency (safe to re-run)

### Data Quality Validation

✅ **Automated Checks:**
- Surrogate key uniqueness (0 duplicates)
- Referential integrity (all FKs valid)
- SCD2 validity (≤1 current version per business key)
- KPI consistency (pre-agg sums = fact sums)
- Partition completeness (expected sparse matrix)

### How It Powers BI

**Power BI Connection:**
1. Connect to Databricks (get all 8 Gold tables)
2. Configure relationships (date, product, pdv)
3. Filter dimensions to `is_current = TRUE` (SCD2 handling)
4. Create simple measures (just SUM/AVG, no complex DAX)
5. Build dashboards (pre-calculated KPIs ready to use)

**Expected Performance:**
- Dashboard load: <2s
- Drill-through query: <1s
- Refresh (incremental): <30 min

See [POWERBI_INTEGRATION_GUIDE.md](../docs/POWERBI_INTEGRATION_GUIDE.md) for detailed setup.

---

## 📚 Documentation

| Document | Purpose |
|----------|---------|
| [GOLD_ARCHITECTURE_DESIGN.md](../docs/GOLD_ARCHITECTURE_DESIGN.md) | Complete technical design (DDL, ADRs, trade-offs) |
| [GOLD_IMPLEMENTATION_SUMMARY.md](../docs/GOLD_IMPLEMENTATION_SUMMARY.md) | Executive summary (KPIs, assumptions, rollout) |
| [POWERBI_INTEGRATION_GUIDE.md](../docs/POWERBI_INTEGRATION_GUIDE.md) | BI connection guide (relationships, DAX examples) |
| [data_dictionary.md](../docs/data_dictionary.md) | Column definitions (all layers) |

---

## 📈 Production Deployment

**Databricks Job Configuration:**

```yaml
Job: BI_Pipeline_Daily
Schedule: 08:00 UTC daily
Tasks:
  1. bronze_ingestion (5 min timeout)
  2. silver_standardization (3 min, depends on task 1)
  3. gold_analytics (5 min, depends on task 2)
  4. drift_monitoring (monitoring, depends on task 3)
```

**Monitoring:** 
- Bronze schema drift via `monitoring/drift_monitoring_bronze.py`
- Silver schema drift via `monitoring/silver_drift_monitoring.py`
- Gold quality checks via `src/tests/test_gold_layer.py`

---

## 💡 Key Architectural Insights (For Interviews)

1. **Why Star Schema?** → Simple joins, broadcast dimensions, minimal DAX
2. **Why SCD Type 2?** → Historical accuracy (facts classify correctly at transaction date)
3. **Why Pre-Aggregated KPIs?** → Performance (sub-second BI queries) + testability (versioned logic)
4. **Why DPO Refresh?** → 10-20x faster than MERGE for incremental updates
5. **Why Surrogate Keys?** → Deterministic, collision-free, incremental-friendly

---

**Author:** Senior Analytics Engineer  
**Date:** 2025-01-06  
**Status:** ✅ Production Ready
