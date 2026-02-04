# Orchestration Jobs – Executive Overview

## Executive Summary
Automated orchestration jobs ensure that every data pipeline runs in strict business order, with full auditability and zero manual intervention. This guarantees that all KPIs and analytics are always up-to-date, trusted, and ready for executive decision-making.

**Business Impact**
- Eliminates manual errors and delays in data refresh
- Enables daily and monthly executive dashboards with validated metrics
- Ensures compliance, auditability, and operational rigor

---

## Architecture at a Glance

- **Upload to Volume**: Simulates automated ingestion from cloud sources (e.g., S3) to Databricks Unity Catalog Volumes. Demonstrates enterprise-grade data landing zone.
- **Silver Orchestrator**: Runs all Silver layer notebooks in business order (dim_pdv → dim_products → fact_sell_in → fact_price_audit). Daily schedule, idempotent, modular, and observable.
- **Gold Orchestrator**: Executes Gold layer notebooks (dimensions → facts & marts → KPIs). Monthly schedule, full audit trail, and executive-ready outputs.

---

## Key Design Decisions

| Decision                | Reason                        | Business Trade-off           |
|-------------------------|-------------------------------|------------------------------|
| Asset Bundle Orchestration | CI/CD, modularity, auditability | Requires strict notebook structure |
| Paused Schedules        | Controlled deployment, demo-ready | Manual resume for production |
| Owner Tagging           | Clear accountability           | Requires governance discipline |

---

## Core Outputs

- Automated data refresh for all layers (Bronze, Silver, Gold)
- Executive KPI dashboards with up-to-date metrics
- Finance-ready datasets and audit logs
- Full lineage and traceability for compliance

---

## Job Details

### 1. upload_to_volume.py
- **Purpose**: Simulates cloud-to-Databricks data upload for architecture demonstration.
- **Business Value**: Shows how raw data lands securely and predictably in the analytics platform.
- **Grain**: Folder-level simulation (no real files transferred).
- **Used By**: Data Engineering, Architecture Review.

### 2. orchestrate_silver_pipeline.yml
- **Purpose**: Orchestrates Silver layer transformations in strict business order.
- **Schedule**: Daily at 3 AM UTC (paused for demo).
- **Tasks**:
  - Silver Dimension PDV
  - Silver Dimension Products
  - Silver Fact Sell-In
  - Silver Fact Price Audit
- **Business Value**: Guarantees referential integrity, deduplication, and business rule enforcement before analytics.

### 3. orchestrate_gold_pipeline.yml
- **Purpose**: Orchestrates Gold layer for executive KPIs and business logic.
- **Schedule**: Monthly at 3 AM UTC (paused for demo).
- **Tasks**:
  - Gold Dimensions
  - Gold Facts & Mart
  - Gold KPIs
- **Business Value**: Delivers audit-first, executive-ready metrics for Commercial, RGM, and Finance teams.

---

## Documentation & Governance

- All jobs are tagged with owner, frequency, and business domain for clear accountability.
- Schedules are paused by default for safe demonstration and controlled deployment.
- Each job is modular, idempotent, and ready for CI/CD integration.

---

## Principle Highlights

| Principle              | Why It Matters                |
| ---------------------- | ----------------------------- |
| **Scannable**          | Recruiters skim, not read     |
| **Business-first**     | Value before technology       |
| **Architecture-aware** | Shows seniority               |
| **Decision-driven**    | Demonstrates judgment         |
| **Always current**     | Outdated jobs = low ownership |

---

> These jobs transform raw operational data into executive-ready analytics, with full traceability and business alignment. Designed for rapid recruiter review and technical leadership evaluation.
