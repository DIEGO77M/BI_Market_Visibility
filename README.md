# 📊 BI Market Visibility Analytics

[![Databricks](https://img.shields.io/badge/Databricks-Serverless-FF3621?style=flat-square&logo=databricks)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.x-004687?style=flat-square)](https://delta.io/)
[![Power BI](https://img.shields.io/badge/Power_BI-DirectQuery-F2C811?style=flat-square&logo=powerbi)](https://powerbi.microsoft.com/)

**Enterprise BI platform for Market Visibility in FMCG/Retail**

> End-to-end Medallion Architecture → Star Schema → Power BI  
> Built for Commercial Directors, RGM, and Sales Leadership

---

## 🎯 Business Impact

| Question | Solution | Table |
|----------|----------|-------|
| Where is price competitiveness lost? | Price Index vs market average | `gold_fact_price_audit` |
| Which products erode margin? | Competitiveness scoring | `gold_kpi_market_visibility` |
| How consistent is pricing across channels? | Channel variance analysis | `gold_kpi_market_share` |
| Sell-in vs actual execution? | Sell-in/Sell-out ratio proxy | `gold_kpi_market_visibility` |

---

## 📐 Architecture

```
Sources (CSV/Excel)
       │
       ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────────────────────┐
│   BRONZE    │───▶│   SILVER    │───▶│           GOLD              │
│  4 tables   │    │  4 tables   │    │  8 tables (Star Schema)     │
│  Raw vault  │    │ Standardized│    │  • 3 Dimensions (SCD2)      │
│             │    │ Deduplicated│    │  • 3 Facts                  │
│             │    │             │    │  • 2 Pre-aggregated KPIs    │
└─────────────┘    └─────────────┘    └──────────────┬──────────────┘
                                                     │
                                                     ▼
                                              ┌─────────────┐
                                              │  POWER BI   │
                                              │ DirectQuery │
                                              └─────────────┘
```

---

## ⚡ Key Metrics

| Metric | Value |
|--------|-------|
| End-to-end runtime | **15-20 min** |
| Gold layer tables | **8** (dimensional model) |
| Pre-aggregated KPIs | **15+** business metrics |
| Query performance | **<2 sec** dashboard refresh |

---

## 🏗️ Technical Decisions

### Why Serverless?

| Factor | Classic Clusters | Serverless ✅ |
|--------|------------------|---------------|
| Startup time | 2-5 min | Instant |
| Idle cost | $$$  | $0 |
| Maintenance | Manual scaling | Auto |

**Trade-off:** No `cache()`/`persist()` → Solved with single-write pattern

### Why Star Schema over Snowflake?

| Factor | Snowflake Schema | Star Schema ✅ |
|--------|------------------|----------------|
| Power BI joins | Multiple | Minimal |
| Query complexity | High | Low |
| Maintenance | Complex | Simple |

### Why Pre-aggregated KPIs?

| Factor | DAX-only | Pre-aggregated ✅ |
|--------|----------|-------------------|
| Dashboard speed | Slow at scale | Fast always |
| DAX complexity | High | Trivial (SUM/AVG) |
| Single source of truth | No | Yes |

**Trade-off:** +5-10% storage → Acceptable for 10x query speed

---

## 📁 Project Structure

```
├── notebooks/
│   ├── bronze/01_bronze_ingestion.py
│   ├── silver/02_silver_standardization.py
│   └── gold/
│       ├── 03_gold_orchestrator.py      # Central pipeline control
│       ├── dimensions/                   # SCD Type 2
│       ├── facts/                        # Transactional
│       ├── kpis/                         # Pre-aggregated
│       └── validation/                   # Integrity checks
├── monitoring/                           # Zero-coupling drift detection
├── docs/                                 # Architecture Decision Records
└── .databricks/workflows/                # Production DAGs
```

---

## 🚀 Quick Start

```bash
# Deploy and run full pipeline
databricks bundle deploy --target dev
databricks bundle run full_pipeline
```

Pipeline executes **3 tasks** (optimized for Serverless):
```
bronze_ingestion → silver_standardization → gold_orchestrator
    (~4 min)            (~3 min)              (~10 min)
```

---

## 📚 Deep Dive Documentation

| Document | Content |
|----------|---------|
| [Gold Architecture](docs/GOLD_ARCHITECTURE_DECISIONS.md) | 14 ADRs: SCD2, surrogate keys, KPI design |
| [Silver Architecture](docs/SILVER_ARCHITECTURE_DECISIONS.md) | 9 ADRs: Deduplication, quality flags |
| [Bronze Architecture](docs/BRONZE_ARCHITECTURE_DECISIONS.md) | 5 ADRs: Ingestion patterns, Excel handling |
| [Power BI Guide](docs/POWERBI_INTEGRATION_GUIDE.md) | Connection setup, DAX measures |
| [Data Dictionary](docs/data_dictionary.md) | All 16 tables with schemas |

---

## 🔍 Code Highlights

### Deterministic Surrogate Keys (SCD2)
```python
# Reproducible across environments, no sequence coordination
surrogate_key = sha2(concat(business_key, valid_from), 256)
```

### Quality Philosophy
```python
# Flags, not imputation - transparency over magic
df.withColumn("is_price_valid", col("price") > 0)
  .withColumn("is_complete", col("product_id").isNotNull())
# Flagged records visible in BI, never silently dropped
```

### Zero-Coupling Monitoring
```python
# Reads Delta History metadata only - zero compute cost
spark.sql("DESCRIBE HISTORY table").select("operation", "operationMetrics")
# Pipeline never fails due to monitoring issues
```

---

## 👤 Author

**Diego Mayorga** — Analytics Engineer  
[GitHub](https://github.com/DIEGO77M) · [Email](mailto:diego.mayorgacapera@gmail.com)

---

<div align="center">

*Demonstrating: Medallion Architecture • Dimensional Modeling • Databricks Serverless • Enterprise BI*

</div>
