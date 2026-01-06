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
├── notebooks/
│   ├── 01_bronze_ingestion.py         # Raw → Delta (2-4 min)
│   ├── 02_silver_standardization.py   # Clean & validate (3 min)
│   └── 03_gold_analytics.py           # Star schema (5 min) ✨ NEW
├── src/
│   ├── utils/
│   │   ├── data_quality.py
│   │   ├── spark_helpers.py
│   │   └── gold_layer_utils.py        # SCD2, surrogate keys ✨ NEW
│   └── tests/
│       └── test_gold_layer.py         # 40+ assertions ✨ NEW
├── docs/
│   ├── GOLD_ARCHITECTURE_DESIGN.md    # Design + 9 ADRs ✨ NEW
│   ├── POWERBI_INTEGRATION_GUIDE.md   # Connection guide ✨ NEW
│   ├── GOLD_IMPLEMENTATION_SUMMARY.md # Executive summary ✨ NEW
│   ├── BRONZE_ARCHITECTURE_DECISIONS.md
│   ├── data_dictionary.md
│   └── architecture/
├── dashboards/
│   └── screenshots/
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

1. **Setup:**
   ```bash
   git clone https://github.com/DIEGO77M/BI_Market_Visibility.git
   pip install -r requirements.txt
   ```

2. **Run Notebooks (in order):**
   ```
   01_bronze_ingestion.py       # ~2-4 min
   02_silver_standardization.py # ~3 min
   03_gold_analytics.py         # ~5 min ✨ NEW
   ```

3. **Validate:**
   ```bash
   pytest src/tests/test_gold_layer.py -v
   ```

4. **Connect Power BI** → [Integration Guide](docs/POWERBI_INTEGRATION_GUIDE.md)

## 📚 Documentation

| Document | Content |
|----------|---------|
| [GOLD_ARCHITECTURE_DESIGN.md](docs/GOLD_ARCHITECTURE_DESIGN.md) | Technical design + 9 ADRs |
| [POWERBI_INTEGRATION_GUIDE.md](docs/POWERBI_INTEGRATION_GUIDE.md) | BI connection + DAX |
| [GOLD_IMPLEMENTATION_SUMMARY.md](docs/GOLD_IMPLEMENTATION_SUMMARY.md) | Executive summary |
| [BRONZE_ARCHITECTURE_DECISIONS.md](docs/BRONZE_ARCHITECTURE_DECISIONS.md) | Bronze design rationale |
| [data_dictionary.md](docs/data_dictionary.md) | Schema definitions |

## 🧪 Testing

```bash
pytest src/tests/ -v              # All tests
pytest src/tests/test_gold_layer.py -v  # Gold layer only (40+ assertions)
```

## ✨ Gold Layer Features

✅ **Surrogate Keys** - Deterministic hash-based generation  
✅ **SCD Type 2** - Historical change tracking (valid_from/valid_to)  
✅ **Dynamic Partition Overwrite** - Incremental refresh optimization  
✅ **Data Quality** - 8 validation types (uniqueness, referential integrity, consistency)  
✅ **Pre-Aggregated KPIs** - Ready for instant Power BI queries  
✅ **Serverless Optimized** - No cache/persist, zero-compute metrics  

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
