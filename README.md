# BI Market Visibility – Executive Analytics Platform

<p align="center">
  <img src="https://databricks.com/wp-content/uploads/2021/10/databricks-logo.png" alt="Databricks" height="32"/>
  <img src="https://upload.wikimedia.org/wikipedia/commons/3/31/Delta_Lake_logo.png" alt="Delta Lake" height="32"/>
  <img src="https://upload.wikimedia.org/wikipedia/commons/0/05/Streamlit_logo.svg" alt="Streamlit" height="32"/>
  <img src="https://upload.wikimedia.org/wikipedia/commons/0/0a/PySpark_logo.png" alt="PySpark" height="32"/>
  <img src="https://upload.wikimedia.org/wikipedia/commons/c/c3/Python-logo-notext.svg" alt="Python" height="32"/>
  <img src="https://upload.wikimedia.org/wikipedia/commons/8/87/Sql_data_base_with_logo.png" alt="SQL" height="32"/>
</p>

---

## 1. Project Overview (Executive Summary)
A unified analytics platform for Retail/FMCG, designed to solve revenue leakage, pricing misalignment, and operational blind spots. Built for Commercial, RGM, BI, and Finance teams to enable confident, data-driven decisions at scale.

---

## 2. Business Problem & Real-World Context
- Fragmented data leads to missed opportunities, stockouts, and pricing errors
- Lack of trust in numbers undermines executive decisions
- Ad-hoc solutions fail to deliver auditability, scalability, and business impact
- Real risks: lost revenue, poor negotiation, slow response to market changes

---

## 3. Solution Approach
- Enterprise-grade Medallion Architecture (Bronze → Silver → Gold)
- Every record is validated, auditable, and never deleted—issues are flagged, not lost
- Full traceability from raw source to executive KPIs
- Design for trust, scalability, and business alignment

---

## 4. Data Architecture

| Layer   | Responsibility                        | Guarantees                  | Business Role                |
|---------|----------------------------------------|-----------------------------|------------------------------|
| 🟫 Bronze  | Raw ingestion, technical validation    | No business logic, full lineage | Data Engineering             |
| 🟪 Silver  | Cleaning, enrichment, business rules   | Deduplication, referential integrity | Analytics, BI                |
| 🟨 Gold    | KPI calculation, business logic        | Executive-ready, audit-first | Commercial, RGM, Finance     |

- Gold layer is the only source for business decisions and KPIs
- Architecture supports scale, governance, and rapid change

---

## 5. Key Data Assets
- **fact_pdv_monthly_health**: Monthly PDV–Product health, stock, compliance, risk signals
- **fact_pdv_price_audit**: Price execution, promo rate, competitive index
- **mart_revenue_leakage**: Revenue leakage factors, lost opportunity quantification
- Each table exposes actionable metrics for executive dashboards and operational alerts

---

## 6. Data Quality, Validation & Trust
- Multi-layer validation: structural, referential, business logic
- Automated quality checks and drift detection; results stored in Delta tables
- Audit-first: all anomalies flagged, nothing deleted
- Enables root-cause analysis and executive confidence in reported numbers

---

## 7. Technology Stack & Tools

| Tool / Platform   | Purpose / Role                       |
|-------------------|--------------------------------------|
| <img src="https://databricks.com/wp-content/uploads/2021/10/databricks-logo.png" height="20"/> Databricks | Serverless compute, orchestration, governance |
| <img src="https://upload.wikimedia.org/wikipedia/commons/3/31/Delta_Lake_logo.png" height="20"/> Delta Lake | ACID storage, time travel, schema evolution   |
| <img src="https://upload.wikimedia.org/wikipedia/commons/0/05/Streamlit_logo.svg" height="20"/> Streamlit | Executive dashboards, rapid prototyping      |
| <img src="https://upload.wikimedia.org/wikipedia/commons/0/0a/PySpark_logo.png" height="20"/> PySpark    | Distributed data processing                  |
| <img src="https://upload.wikimedia.org/wikipedia/commons/c/c3/Python-logo-notext.svg" height="20"/> Python | Data engineering, scripting                  |
| <img src="https://upload.wikimedia.org/wikipedia/commons/8/87/Sql_data_base_with_logo.png" height="20"/> SQL    | Data modeling, validation, business logic     |
| Unity Catalog     | Centralized governance, security     |
| Asset Bundles     | CI/CD, deployment automation         |
| Git               | Version control, collaboration       |

---

## 8. Business Impact
- Faster, more confident decisions on pricing, stock, and promotions
- Reduced revenue leakage and operational risk
- Commercial, RGM, and Finance teams work from a single source of truth
- Improved negotiation, forecasting, and market response

---

## 9. Why This Project Stands Out (For Recruiters)
- Demonstrates senior-level architecture, governance, and business alignment
- Ownership of end-to-end data lifecycle and executive KPIs
- Real-world trade-offs, not academic prototypes
- Clear separation of concerns, auditability, and operational rigor

---

## 10. How to Navigate This Repository
- **docs/**: Architecture, technical specs, data dictionary
- **src/bronze, src/silver, src/gold/**: Layered data pipelines and logic
- **src/tests/**: Data quality, integration, and unit tests
- **orchestration/**: Jobs, workflows, and automation scripts
- Start with docs/architecture/README.md for a high-level overview

---

> This project is designed to be understood and evaluated in minutes by technical and business leaders. Every decision is intentional, every metric is traceable, and every layer is built for enterprise impact.