# Gold Layer – fact_pdv_monthly_health

> **Purpose:** Executive-ready, monthly health metrics for each Point of Sale (PDV) and product, with full business validation and auditability.

---

## 🚀 Quick Facts

- ⏱️ Gold pipeline runs in: **~2–3 minutes**
- 🏗️ Strict Medallion Architecture: Bronze → Silver → Gold
- 📦 100% Delta Lake, Databricks Serverless compatible
- 🛡️ Full business, referential, and structural validation
- 📊 Results and validation metrics stored in Delta tables for monitoring and audit

---

## 📊 What is fact_pdv_monthly_health?

This Gold fact table provides a monthly snapshot of inventory, sales, and health signals for every PDV–product combination. It enables:

- Executive dashboards for stock, coverage, and compliance
- Early warning signals for operational risk (stockouts, low quality, orphan records)
- Root-cause analysis via full audit trail and validation metrics

---

## 🗂️ Structure

| Folder                | Purpose                                 |
|-----------------------|-----------------------------------------|
| ddl/                  | Table creation scripts (DDL)            |
| dml/                  | Data transformation and load (DML)      |
| validation/           | Business and technical validation logic |
| README.md             | Layer documentation                     |

---

## 🧩 Key Business Validations

- **Row Count & Primary Key Uniqueness:** Ensures no duplicates or missing records for each PDV–product–month
- **Null Business Keys:** Flags missing keys for traceability
- **Referential Integrity:** Validates all keys exist in dimensions (date, PDV, product, assortment)
- **Stock Logic:** Detects inconsistencies between stock levels and in-stock flags
- **Coverage Compliance:** Flags mismatches between expected assortment and actual coverage
- **Data Confidence Score:** Ensures scores are within [0.0–1.0], flags low-quality and orphan PDVs
- **Inventory Action Signals:** Summarizes operational signals (stockout, urgent replenishment, etc.)
- **Audit Fields:** Verifies batch and processing metadata for every record

---

## 🛡️ Validation & Monitoring

- All validation results are written to a dedicated Delta table (`validation_pdv_monthly_health`), partitioned by status (PASS, FAIL, WARNING, INFO)
- Each validation incluye: nombre, categoría (structural, referential, business_logic, data_quality, summary), status, severidad, filas fallidas, valores de métrica, acciones recomendadas y timestamp de ejecución
- No se elimina ningún dato: todos los issues se flaggean, no se borran, para total transparencia
- Permite monitoreo automatizado, alertas y reporting ejecutivo sobre la salud de los datos

---

## 🛠️ Orchestration Flow

1. **DDL:** Table is created with business-aligned schema and audit fields
2. **DML:** Data is loaded and transformed from Silver, applying business rules and calculations
3. **Validation:** Automated SQL validation runs, generating a full set of metrics and alerts
4. **Monitoring:** Results are stored in Delta for consumption by BI, RGM, and executive teams

---

## 📈 Example: Validation Metrics Table

| validation_name                | category         | status   | failed_rows | details                                 | recommended_action                  |
|-------------------------------|------------------|----------|-------------|-----------------------------------------|-------------------------------------|
| row_count_check                | structural       | PASS     | 0           | Total records loaded: 12000             | No action required                  |
| primary_key_uniqueness         | structural       | PASS     | 0           | Duplicate records found: 0              | No action required                  |
| referential_integrity_dim_pdv  | referential      | WARNING  | 5           | PDVs not found in dim_pdv: 5 (0.4%)     | Monitor orphan PDVs - acceptable    |
| stock_logic_validation         | business_logic   | FAIL     | 12          | Inconsistent in_stock vs stock units    | Review in_stock CASE logic in DML   |
| data_confidence_score_range    | data_quality     | PASS     | 0           | Scores out of valid range: 0            | No action required                  |
| low_data_quality_records       | data_quality     | INFO     | 8           | Records with score < 0.5: 8 (0.07%)     | Monitor data quality                |

---

## 📚 Documentation

- [Project Architecture](../../../docs/architecture/)
- [Data Dictionary](../../../docs/data_dictionary/)
- [Technical Specs](../../../docs/technical_specs/)

---

## 💡 Why This Matters (for Recruiters)

- **Executive-ready:** Delivers actionable, validated KPIs for business decision-makers
- **Full auditability:** Every metric and anomaly is traceable and explained
- **Portfolio-ready:** Demonstrates senior-level BI architecture, validation, and monitoring

---

## License

MIT
# Gold Layer: fact_pdv_monthly_health – Temporal Grain Change Rationale

## Context
This document explains the rationale and business impact behind the change in temporal grain for the `fact_pdv_monthly_health` table, specifically the migration from a surrogate `year_month_id` (INT) to a canonical `date` (DATE, first day of month) as the primary time key.

---

## Previous Design
- **Key:** `year_month_id` (INT)
- **Grain:** Month (e.g., 202501 for January 2025)
- **Limitation:** Artificial key, not directly relatable to the enterprise date dimension (`dim_date`).

## New Design
- **Key:** `date` (DATE, always first day of month)
- **Grain:** Month (e.g., 2025-01-01 for January 2025)
- **Advantage:** Direct logical FK to `dim_date.date`, enabling seamless integration with all time-based analytics and reporting.

---

## Why Use the First Day of the Month?
- **Industry Standard:** Using the first day of the month as the canonical key for monthly grain is a best practice in analytics engineering. It ensures that all monthly data can be joined to a single, unambiguous row in the date dimension.
- **Simplicity:** Avoids confusion with artificial keys or string concatenations. All monthly facts share the same date value, regardless of the actual day of the event within the month.
- **Flexibility:** Enables future drill-downs to daily grain if required, without breaking referential integrity.
- **Compatibility:** Facilitates joins with other tables (e.g., `dim_expected_assortment`) that may use date ranges or require point-in-time logic.

---

## Business Impact
- **Consistent Time Alignment:** All facts for a given month are grouped under a single, canonical date, simplifying aggregation and reporting.
- **Robust Point-in-Time Logic:** The association with `dim_expected_assortment` leverages date ranges (e.g., `valid_from_date`), ensuring that assortment compliance is always evaluated in the correct temporal context.
- **Data Quality:** Reduces risk of row multiplication or orphaned records due to mismatched keys.
- **Executive Transparency:** The model is easily explainable to both technical and business stakeholders, supporting auditability and trust.

---

## Senior-Level Trade-Offs & Decision Rationale
- **Alternatives Considered:**
  - Retain `year_month_id` (INT): Simpler, but less flexible and not directly relatable to enterprise date models.
  - Use full event date: Increases granularity, but breaks monthly snapshot logic and complicates reporting.
- **Decision:** Adopt `date` (DATE, first day of month) as the canonical key for monthly facts.
- **What We Gain:**
  - Enterprise-grade dimensional modeling
  - Future-proofing for time-based analytics
  - Simpler, more robust joins
- **What We Sacrifice:**
  - Slight increase in ETL complexity (date construction)
  - Need for clear documentation (addressed here)

---

## Recruiter/Stakeholder Takeaway
This design choice demonstrates:
- Mastery of dimensional modeling best practices
- Focus on business impact and data trust
- Ability to communicate and defend architectural decisions to both technical and executive audiences
- Readiness for senior analytics engineering and BI leadership roles

---

For further details, see the DDL and DML scripts in this module or contact the project owner.
