# 📓 Databricks Notebooks - Medallion Architecture

End-to-end data pipeline implementing Bronze-Silver-Gold architecture with production-grade practices.

---


## 🎯 Execution Order

| # | Notebook | Status | Runtime | Purpose |
|---|----------|--------|---------|---------|
| 1 | **01_bronze_ingestion** | ✅ | ~3 min | Raw data ingestion with ACID guarantees |
| 2 | **02_silver_standardization** | ✅ | ~2 min | Data cleaning and business rules |
| 3 | **silver_drift_monitoring** | ✅ | ~1 min | Silver schema, quality & volume drift monitoring |
| 4 | **03_gold_analytics** | 🚧 | TBD | Dimensional modeling for BI |


| # | Notebook | Status | Runtime | Purpose |
|---|----------|--------|---------|---------|
| 1 | **01_bronze_ingestion** | ✅ | ~3 min | Raw data ingestion with ACID guarantees |
| 2 | **02_silver_standardization** | ✅ | ~2 min | Data cleaning and business rules |
| 3 | **03_gold_analytics** | 🚧 | TBD | Dimensional modeling for BI |

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

## 🥇 Gold Layer: Analytics (Planned)

**03_gold_analytics.py** 🚧

### What It Will Do
- Dimensional modeling (star schema)
- Pre-aggregated fact tables
- KPI calculations
- BI-optimized denormalization

### Planned Outputs
- `gold_fact_sales` - Transaction-level fact table
- `gold_dim_pdv` - Point of sale dimension
- `gold_dim_product` - Product hierarchy dimension
- `gold_dim_date` - Calendar dimension with fiscal periods

---

## 📈 Production Deployment

**Databricks Job Configuration:**

```yaml
Job: Bronze_Pipeline_Daily
Schedule: 3:00 AM daily
Tasks:
  1. bronze_ingestion (60 min timeout)
  2. drift_monitoring_bronze (depends on task 1)
  3. silver_standardization (depends on task 1)
  4. gold_analytics (depends on task 3)
```

**Monitoring:** Schema drift detection via [`monitoring/drift_monitoring_bronze.py`](../monitoring/drift_monitoring_bronze.py)

---

## 💡 Key Learnings for Interviews

1. **Cost Optimization:** Zero-compute validation saves Serverless costs
2. **Architectural Decisions:** Documented WHY (not just HOW) via ADRs
3. **Production Readiness:** Deterministic batch IDs, idempotent runs, monitoring
4. **Trade-off Analysis:** When to use MERGE vs Overwrite, pandas vs spark-excel
5. **Best Practices:** Single Responsibility, no over-engineering, clear layer boundaries

---

**Author:** Diego Mayorga | [GitHub](https://github.com/DIEGO77M/BI_Market_Visibility)  
**Last Updated:** 2025-12-31
