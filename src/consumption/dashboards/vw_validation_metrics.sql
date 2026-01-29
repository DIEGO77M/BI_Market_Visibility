CREATE OR REPLACE VIEW workspace.gold.vw_validation_metrics AS
SELECT
  'pdv_monthly_health'      AS source,
  validation_name,
  validation_category,
  status,
  severity,
  total_rows,
  failed_rows,
  failed_percentage,
  execution_timestamp,
  date                      AS period
FROM workspace.gold.validation_pdv_monthly_health

UNION ALL

SELECT
  'mart_revenue_leakage'    AS source,
  validation_name,
  validation_category,
  status,
  severity,
  total_rows,
  failed_rows,
  failed_percentage,
  execution_timestamp,
  audit_date_id             AS period
FROM workspace.gold.validation_mart_revenue_leakage;
