# 📊 BI Market Visibility Analytics

[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=flat-square&logo=python)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C?style=flat-square&logo=apache-spark)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Serverless-FF3621?style=flat-square&logo=databricks)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID-004687?style=flat-square)](https://delta.io/)
[![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-F2C811?style=flat-square&logo=powerbi)](https://powerbi.microsoft.com/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

**End-to-end BI solution** implementing **Medallion Architecture** (Bronze→Silver→Gold) using **Databricks Serverless**, **PySpark**, **Delta Lake**, and **Power BI** for FMCG market analytics.

---

## 🎯 Business Value

| Metric | Result |
|--------|--------|
| **Pipeline Speed** | 2-4 min (optimized for Serverless) |
| **Market KPIs** | 15+ automated metrics |
| **Data Quality** | 99%+ accuracy with validation |
| **BI Integration** | Direct Power BI → Gold layer |
| **Query Performance** | <2 sec dashboard, <15 min full refresh |

## 🏗️ Architecture

**Medallion Pattern** with complete star schema:

```
BRONZE (Raw)          SILVER (Clean)           GOLD (Analytics)
━━━━━━━━━━━━          ━━━━━━━━━━━━             ━━━━━━━━━━━━
PDV Master      →     PDV Validated     →      gold_dim_pdv (SCD2)
Products Master →     Products Cleaned  →      gold_dim_product (SCD2)
Price Audit     →     Price Validated   →      gold_fact_price_audit
Sell-In         →     Sell-In Validated →      gold_fact_sell_in
                                               gold_fact_stock_estimated
                                               gold_dim_date (10-year)
                                               gold_kpi_market_visibility_daily
                                               gold_kpi_market_share
```

**8 Gold tables** optimized for Power BI:
- **3 Conformed Dimensions:** Date, Product (SCD Type 2), PDV (SCD Type 2)
- **3 Fact Tables:** Sell-In, Price Audit, Stock (append-only, incremental)
- **2 KPI Tables:** Market Visibility, Market Share (pre-aggregated)

## 🛠️ Stack

- **Data:** Databricks Serverless, PySpark, Delta Lake, Unity Catalog
- **BI:** Power BI (star schema optimized)
- **Languages:** Python 3.8+, Spark SQL
- **Testing:** pytest (40+ unit tests)
- **Version Control:** GitHub

## 📦 Project Structure

```
BI_Market_Visibility/
├── .databricks/
│   └── workflows/
│       └── gold_pipeline.yml        # ⭐ Databricks Workflow (Production)
├── notebooks/                 
│   ├── bronze/
│   │   └── 01_bronze_ingestion.py
│   ├── silver/
│   │   └── 02_silver_standardization.py
│   └── gold/                        # ✨ Modular Gold Layer
│       ├── 03_gold_orchestrator.py  # Configuration & pipeline execution
│       ├── dimensions/
│       │   ├── gold_dim_date.py     # Static calendar dimension
│       │   ├── gold_dim_product.py  # SCD Type 2
│       │   └── gold_dim_pdv.py      # SCD Type 2
│       ├── facts/
│       │   ├── gold_fact_sell_in.py      # Append-only, partitioned
│       │   ├── gold_fact_price_audit.py  # Price visibility
│       │   └── gold_fact_stock.py        # Estimated stock
│       ├── kpis/
│       │   ├── gold_kpi_market_visibility.py  # Daily operations
│       │   └── gold_kpi_market_share.py       # Monthly trends
│       └── validation/
│           └── gold_validation.py   # Read-only post-checks
├── src/
│   ├── utils/                 
│   │   ├── data_quality.py
│   │   └── spark_helpers.py
│   └── tests/                 
│       └── test_data_quality.py
├── monitoring/
│   ├── drift_monitoring_bronze.py
│   └── silver_drift_monitoring.py
├── docs/                      
│   ├── BRONZE_ARCHITECTURE_DECISIONS.md
│   ├── SILVER_ARCHITECTURE_DECISIONS.md
│   ├── GOLD_ARCHITECTURE_DECISIONS.md  # ✨ NEW: 13 ADRs
│   ├── POWERBI_INTEGRATION_GUIDE.md
│   └── data_dictionary.md
├── dashboards/                
├── data/                      
├── requirements.txt
├── databricks.yml
└── LICENSE
```

## 🚀 Quick Start

### Prerequisites
```bash
python >= 3.8
databricks-connect >= 14.0
pyspark >= 3.5
```

### Execution in Databricks

#### Option A: Orchestrator (Development/Demo)
```python
# In Databricks notebook 03_gold_orchestrator.py:
# 1. Set widget "Execute Pipeline" = "yes"
# 2. Run All Cells
# OR
# Set RUN_PIPELINE = True and run
```

#### Option B: Databricks Workflows (Production)
```bash
# Deploy the workflow
databricks bundle deploy --target prod

# Run manually
databricks jobs run-now --job-id <gold_layer_pipeline_job_id>

# Workflow definition: .databricks/workflows/gold_pipeline.yml
```

#### Option C: Manual Execution (Individual Notebooks)
```
# Phase 1: Ingestion
notebooks/bronze/01_bronze_ingestion.py          # ~2-4 min

# Phase 2: Standardization
notebooks/silver/02_silver_standardization.py    # ~3 min

# Phase 3: Gold Layer (Order matters for dependencies)
notebooks/gold/dimensions/gold_dim_date.py       # ~30 sec
notebooks/gold/dimensions/gold_dim_product.py    # ~30 sec
notebooks/gold/dimensions/gold_dim_pdv.py        # ~30 sec
notebooks/gold/facts/gold_fact_sell_in.py        # ~1 min
notebooks/gold/facts/gold_fact_price_audit.py    # ~1 min
notebooks/gold/facts/gold_fact_stock.py          # ~1 min
notebooks/gold/kpis/gold_kpi_market_visibility.py # ~2 min
notebooks/gold/kpis/gold_kpi_market_share.py     # ~1 min
notebooks/gold/validation/gold_validation.py     # ~30 sec (read-only)
```

### Validate
```bash
pytest src/tests/test_gold_layer.py -v
```

### Connect Power BI
→ [Integration Guide](docs/POWERBI_INTEGRATION_GUIDE.md)

## 📚 Documentation

| Document | Content |
|----------|---------|
| [BRONZE_ARCHITECTURE_DECISIONS.md](docs/BRONZE_ARCHITECTURE_DECISIONS.md) | Bronze layer ADRs (5 decisions) |
| [SILVER_ARCHITECTURE_DECISIONS.md](docs/SILVER_ARCHITECTURE_DECISIONS.md) | Silver layer ADRs (9 decisions) |
| [GOLD_ARCHITECTURE_DECISIONS.md](docs/GOLD_ARCHITECTURE_DECISIONS.md) | Gold layer ADRs (14 decisions) |
| [POWERBI_INTEGRATION_GUIDE.md](docs/POWERBI_INTEGRATION_GUIDE.md) | BI connection setup + DAX measures |
| [data_dictionary.md](docs/data_dictionary.md) | Schema definitions for all 16 tables |

## 🧪 Testing

```bash
pytest src/tests/ -v              # All tests
pytest src/tests/test_gold_layer.py -v  # Gold layer only (40+ assertions)
```

## ✨ Gold Layer Features (Anti-Saturation Architecture)

✅ **Decoupled Jobs** - Each table = independent notebook (no cascading failures)  
✅ **Surrogate Keys** - Deterministic SHA-256 hash generation  
✅ **SCD Type 2** - Full historical tracking (Product, PDV dimensions)  
✅ **Dynamic Partition Overwrite** - Incremental refresh by date  
✅ **Pre-Aggregated KPIs** - Zero DAX complexity in Power BI  
✅ **Databricks Workflows** - Production-ready parallel DAG execution  
✅ **Zero-Compute Validation** - Delta History metadata checks  
✅ **Serverless Optimized** - No cache/persist, single write per job  
✅ **Stock Estimation** - Derived from sell-in patterns (documented assumptions)  

### Gold Layer Tables (8 Total)

| Table | Type | Grain | Purpose |
|-------|------|-------|---------|
| `gold_dim_date` | Dimension | date | Calendar attributes |
| `gold_dim_product` | Dimension (SCD2) | product × version | Product hierarchy |
| `gold_dim_pdv` | Dimension (SCD2) | pdv × version | Store attributes |
| `gold_fact_sell_in` | Fact | date × product × pdv | Commercial push |
| `gold_fact_price_audit` | Fact | date × product × pdv | Price visibility |
| `gold_fact_stock` | Fact | date × product × pdv | Inventory risk |
| `gold_kpi_market_visibility_daily` | KPI | date × product × channel | Daily operations |
| `gold_kpi_market_share` | KPI | month × brand × channel | Trend analysis |

## 📊 Key Metrics Available

```
✅ Sell-In Analysis
   → Daily quantities & values by product × PDV
   → Unit economics & transaction frequency

✅ Price Competitiveness
   → Price index (observed vs market average)
   → Price variance detection
   → Market outlier flagging

✅ Market Penetration
   → Market share % (units & value)
   → PDV coverage by region/segment
   → Brand performance trends

✅ Stock Availability
   → Days of supply (calculated proxy)
   → Stockout detection
   → Overstock alerts

✅ Operational Efficiency
   → Efficiency score (0-100)
   → Availability rate %
   → Sell-in/Sell-out ratio proxy
```

---

## 📧 Contact

- **GitHub:** [@DIEGO77M](https://github.com/DIEGO77M/BI_Market_Visibility)
- **Email:** diego.mayorgacapera@gmail.com
- **License:** MIT

<div align="center">

**Built with ❤️ for Data Engineering & Business Intelligence**

</div>
