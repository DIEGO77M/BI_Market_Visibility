# BI Market Visibility – Executive Analytics Platform

---

## 0. Project Authorship & AI Collaboration
This project was developed and architected by Diego Mayorga, leveraging AI tools such as GitHub Copilot exclusively for support in documentation, code review, and productivity. All engineering, architecture, and business logic decisions are original and reflect real-world experience in enterprise analytics.

- **AI Usage**: Copilot was used as a documentation and coding assistant, not as a decision-maker.
- **Documentation Skills**: Custom skills and templates were applied to ensure business-first, recruiter-ready documentation.
- **Development Environment**: All work was performed in Visual Studio Code, using Databricks Asset Bundles for file management, deployment, and orchestration.

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
| 🥉 Bronze  | Raw ingestion, technical validation    | No business logic, full lineage | Data Engineering             |
| 🥈 Silver  | Cleaning, enrichment, business rules   | Deduplication, referential integrity | Analytics, BI                |
| 🥇 Gold    | KPI calculation, business logic        | Executive-ready, audit-first | Commercial, RGM, Finance     |

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

## 📹 Featured Video: AI-Powered Business Intelligence Pipeline

[![Watch the video](https://img.youtube.com/vi/VTwLHWUq4xo/0.jpg)](https://youtu.be/VTwLHWUq4xo)

> This video showcases an automated analytics system that transforms kpi metrcis into prioritized daily insights—without manual intervention.

**Tech Stack:**
- n8n (Workflow Orchestration)
- Databricks (Data Warehouse & Compute)
- Anthropic Claude AI (Intelligence Layer)
- Python (Data Transformation)
- Streamlit (Dashboard Visualization)

**Architecture Overview:**
1. **Data Ingestion:** n8n orchestrates 7 KPI tables from Databricks via REST API
2. **Transformation Layer:** Custom Code nodes parse JSON, apply business logic, filter anomalies, and calculate metrics
3. **Unification Pipeline:** Merge node consolidates 7 parallel data streams into a unified analytics payload
4. **AI Intelligence:** Anthropic Claude acts as a Management BI Analyst, prioritizing insights and urgent actions
5. **Data Persistence:** AI-generated analysis writes back to Databricks, then renders in real-time Streamlit dashboard

**Result:**
- Automated daily business intelligence reports with AI-driven prioritization
- Converts AI from a reactive tool into a proactive strategic analyst

**Key Innovation:**
- AI ranks urgency and flags critical interventions for immediate action

**Business Impact:**
- Zero manual analysis required
- Same-day actionable insights
- Executive-level intelligence at scale
- Continuous monitoring and alerting

**Project Details:**
- Production-ready for retail operations forensics
- Processes $200K+ in financial metrics and generates comprehensive audit reports automatically

---

## 7. Technology Stack & Tools

| Tool / Platform   | Purpose / Role                       |
|-------------------|--------------------------------------|
| 🟧 Databricks      | Serverless compute, orchestration, governance |
| 💧 Delta Lake      | ACID storage, time travel, schema evolution   |
| 🚦 Streamlit       | Executive dashboards, rapid prototyping      |
| 🔥 PySpark         | Distributed data processing                  |
| 🐍 Python          | Data engineering, scripting                  |
| 🗄️ SQL             | Data modeling, validation, business logic     |
| 🛡️ Unity Catalog   | Centralized governance, security             |
| 📦 Asset Bundles   | CI/CD, deployment automation                 |
| 🗂️ Git             | Version control, collaboration               |
| 🤖 Anthropic Claude AI | AI-driven business intelligence           |
| 🔗 n8n             | Workflow orchestration, REST API integration |

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