# Gold Dimension: dim_pdv (Single Source of Truth)

## Purpose
This table provides the single, business-approved reference for all Point of Sale (POS) entities, supporting analytics, BI, and operational reporting in the Gold layer. It is designed for direct consumption by business users and downstream data products.

## Rebuild Strategy
**Chosen:** FULL REBUILD (CREATE OR REPLACE TABLE, SQL-only)

## Why FULL REBUILD?
- Low data volume: No performance or cost risk.
- Operational simplicity: No incremental logic, no SCD2, no CDC.
- Deterministic: Gold always reflects the latest Silver state, ensuring trust and auditability.
- Safe reprocessing: Idempotent and reproducible, supporting batch refreshes and backfills.

## Business & Modeling Rules
- **Source of Truth:** Only workspace.silver.dim_pdv is used as input.
- **Store Size Derivation:**
	- 'independent_supermarket' → 'small'
	- 'convenience_store' → 'small'
	- 'supermarket_chain' → 'medium'
	- 'hypermarket' → 'large'
	- Any other value → 'unknown' (for data governance)
- **Coordinates:**
	- Only valid latitude/longitude (DECIMAL(10,7)) are included.
	- Coordinates are for drill-down only, not for analytics/modeling.
- **SCD Type 1:**
	- Only the latest state per pdv_code is kept (no history, no SCD2 logic).
- **Data Quality:**
	- Records with null pdv_code or invalid coordinates are excluded from Gold.
- **Audit:**
	- gold_processed_at records the ETL processing timestamp for traceability.

## Alternatives Considered (Rejected)
- Change Data Capture (CDC): Not needed due to batch nature and low frequency.
- Streaming ingestion: Unnecessary for current business requirements.
- SCD Type 2: Not required; Gold is a snapshot, not a history store.

## Rationale
- No business requirement for historical tracking or incremental logic.
- No technical need for streaming or CDC.
- SCD Type 2 not required: Gold is a snapshot, not a history store.

## Table Comment
`POS dimension with SCD Type 1. Represents current store configuration.`

**This decision and logic are explicitly documented for audit, governance, and future architecture reviews.**
