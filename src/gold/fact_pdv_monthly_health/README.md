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
