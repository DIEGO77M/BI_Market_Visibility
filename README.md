# 📊 BI Market Visibility Analytics

[![Databricks](https://img.shields.io/badge/Databricks-Serverless-FF3621?style=flat-square&logo=databricks)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.x-004687?style=flat-square)](https://delta.io/)
[![Power BI](https://img.shields.io/badge/Power_BI-DirectQuery-F2C811?style=flat-square&logo=powerbi)](https://powerbi.microsoft.com/)

**Enterprise Analytics Platform for Market Visibility in FMCG/Retail**

---

## The Problem: Decision Latency

This is not a data availability problem. **This is a time-to-decision problem.**

Commercial teams have data. What they lack is the ability to act on it before the opportunity is gone.

| What Happens Today | Business Cost |
|-------------------|---------------|
| Stock-outs detected after the fact | Lost sales, damaged retailer trust |
| Pricing inconsistencies found post-mortem | Margin erosion, channel conflict |
| Execution gaps discovered in monthly reviews | Wasted trade spend, missed targets |
| Metrics calculated differently across teams | Debates over numbers, not actions |

**The result:** Reactive decisions. Firefighting instead of optimizing.

---

## Why Architecture Matters

Dashboards alone don't solve this. The root causes are structural:

| Root Cause | Consequence |
|------------|-------------|
| Fragmented data sources | No single version of truth |
| Ad-hoc transformations | Different numbers in every report |
| No standardized business logic | Endless reconciliation meetings |
| Quality issues discovered late | Low trust in metrics |

**The solution requires a platform**, not more reports.

A platform that:
- Ingests data reliably and traceably
- Applies business rules consistently
- Delivers metrics that stakeholders trust
- Enables decisions in hours, not weeks

---

## What Was Built

An end-to-end analytics platform using **Medallion Architecture**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│   Sources          Bronze           Silver              Gold                │
│   (Raw)            (Vault)          (Standardized)      (Decision-Ready)    │
│                                                                             │
│   CSV/Excel   →    4 tables    →    4 tables      →    8 tables            │
│                    Traceability      Business rules     Star Schema         │
│                    Audit trail       Quality flags      Pre-aggregated KPIs │
│                                                                             │
│                                                         ↓                   │
│                                                    Power BI                 │
│                                                    DirectQuery              │
│                                                    <2 sec refresh           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

| Component | Technology | Purpose |
|-----------|------------|---------|
| Compute | Databricks Serverless | Zero idle cost, instant start |
| Storage | Delta Lake | ACID transactions, time travel |
| Governance | Unity Catalog | Lineage, access control |
| Consumption | Power BI DirectQuery | Real-time dashboards |

---

## Architecture by Layer

### Bronze: Trust Starts Here

**Role:** Capture everything, lose nothing.

| Responsibility | Why It Matters |
|----------------|----------------|
| Raw ingestion with audit metadata | Know exactly when and what was loaded |
| Schema drift detection | Catch source changes before they break reports |
| Full traceability | Answer "where did this number come from?" |

**What it prevents:** Untraceable data, silent failures, audit gaps.

---

### Silver: Business Rules Live Here

**Role:** One version of truth, consistently applied.

| Responsibility | Why It Matters |
|----------------|----------------|
| Deduplication | Same record counted once, everywhere |
| Type standardization | Consistent joins, no casting errors |
| Quality flags | Transparent issues, not hidden surprises |
| Business key enforcement | Reliable entity identification |

**What it prevents:** Metric discrepancies, reconciliation debates, broken joins.

---

### Gold: Decisions Happen Here

**Role:** Metrics ready for action, not interpretation.

| Component | Purpose |
|-----------|---------|
| **Dimensions (SCD2)** | Track changes over time: products, PDVs, dates |
| **Facts** | Daily grain: sell-in, price audit, stock |
| **Pre-aggregated KPIs** | Business metrics calculated once, used everywhere |

**What it prevents:** Complex DAX, slow dashboards, inconsistent calculations.

**Star Schema Design:**
```
                    ┌─────────────┐
                    │  dim_date   │
                    └──────┬──────┘
                           │
┌─────────────┐    ┌───────┴───────┐    ┌─────────────┐
│ dim_product │────│  fact_tables  │────│   dim_pdv   │
└─────────────┘    └───────┬───────┘    └─────────────┘
                           │
                    ┌──────┴──────┐
                    │  kpi_tables │
                    └─────────────┘
```

---

## Orchestration & Operations

Reliability is not optional. The platform runs predictably, every time.

| Principle | Implementation |
|-----------|----------------|
| Phase-based execution | Bronze → Silver → Gold (strict order) |
| Single orchestrator | Gold layer controlled by one entry point |
| Deterministic runs | Same input = same output, always |
| Failure isolation | One table fails, others continue |

**Pipeline execution:**
```
bronze_ingestion → silver_standardization → gold_orchestrator
    (~4 min)            (~3 min)              (~10 min)
```

**Total runtime:** 15-20 minutes, end-to-end.

---

## Data Quality Philosophy

Quality is a business concern, not a technical checkbox.

| Approach | Rationale |
|----------|-----------|
| **Flags, not imputation** | Stakeholders see issues, decide how to handle |
| **Business-relevant checks only** | No generic validations without purpose |
| **Validation without blocking** | Pipeline completes, issues are visible |
| **Observability decoupled** | Monitoring doesn't break processing |

**The goal:** Trust in metrics. When a number appears in a dashboard, it means something.

---

## Analytics Use Cases

The architecture enables specific business decisions:

### Operational (Daily)

| Insight | Decision Enabled |
|---------|------------------|
| **Perfect Store Score** | Which PDVs need intervention today? |
| **Stock-Out Risk** | What products need replenishment before breaking? |
| **Price Compliance** | Where is pricing outside policy? |

### Tactical (Weekly)

| Insight | Decision Enabled |
|---------|------------------|
| **Lost Sales Analysis** | Where is revenue leaking: stock, price, or rotation? |
| **Channel Performance** | Which channels deliver value vs. volume? |
| **Price Elasticity** | Where can promotions be more aggressive? |

### Strategic (Monthly)

| Insight | Decision Enabled |
|---------|------------------|
| **Market Share Trends** | Are we gaining or losing ground? |
| **Geographic Opportunities** | Where is untapped growth potential? |
| **Product Lifecycle** | Which products need investment vs. harvest? |

---

## Why This Design

Every architectural choice is a trade-off. These were made deliberately:

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| **Medallion Architecture** | Direct source-to-BI | Traceability, quality control, reusability |
| **Pre-aggregated KPIs** | DAX-only calculations | Consistency, performance, single source of truth |
| **Star Schema** | Snowflake/3NF | Power BI optimization, simpler DAX |
| **Serverless** | Classic clusters | Zero idle cost, instant availability |
| **SCD Type 2** | Overwrite history | Answer "what was true when this happened?" |
| **Daily grain** | Transaction-level | Balance between detail and performance |

**What was sacrificed:**
- Serverless: No `cache()`/`persist()` → Solved with single-write pattern
- Pre-aggregation: +5-10% storage → Acceptable for 10x query speed
- Daily grain: Less detail → Sufficient for 90% of business questions

---

## Who This Is For

| Audience | What They Get |
|----------|---------------|
| **Commercial Leadership** | Trusted metrics, faster decisions |
| **BI Teams** | Clean data model, minimal DAX complexity |
| **Analytics Engineers** | Reference architecture, documented trade-offs |
| **Technical Reviewers** | Evidence of senior-level thinking |

---

## Project Structure

```
├── notebooks/
│   ├── bronze/                      # Raw ingestion
│   ├── silver/                      # Standardization
│   └── gold/
│       ├── 03_gold_orchestrator.py  # Pipeline control
│       ├── dimensions/              # SCD Type 2
│       ├── facts/                   # Daily grain
│       ├── kpis/                    # Pre-aggregated
│       └── validation/              # Integrity checks
├── monitoring/                      # Drift detection
├── docs/                            # Architecture decisions
└── .databricks/workflows/           # Production pipelines
```

---

## Quick Start

```bash
databricks bundle deploy --target dev
databricks bundle run full_pipeline
```

---

## Documentation

| Document | Purpose |
|----------|---------|
| [Gold Architecture](docs/GOLD_ARCHITECTURE_DECISIONS.md) | Dimensional model decisions |
| [Silver Architecture](docs/SILVER_ARCHITECTURE_DECISIONS.md) | Standardization rules |
| [Bronze Architecture](docs/BRONZE_ARCHITECTURE_DECISIONS.md) | Ingestion patterns |
| [Power BI Guide](docs/POWERBI_INTEGRATION_GUIDE.md) | Connection and DAX |
| [Data Dictionary](docs/data_dictionary.md) | All table schemas |

---

## Author

**Diego Mayorga** — Analytics Engineer  
[GitHub](https://github.com/DIEGO77M) · [Email](mailto:diego.mayorgacapera@gmail.com)

---

<div align="center">

*This project demonstrates: Business-driven architecture • Medallion + Star Schema • Operational reliability • Senior analytical thinking*

</div>
