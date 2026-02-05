-- =====================================================================
-- KPI 3: Execution ROI - Revenue Recovered per Merchandiser Visit
-- =====================================================================
-- Business Purpose:
--   Measures real in-store execution effectiveness (merchandiser visits,
--   planograms, exhibitions) by correlating it with revenue leakage.
--
-- Decision Enabled:
--   - Optimize merchandiser routes
--   - Measure ROI of in-store execution
--   - Identify high/low performing merchandisers
--
-- Refresh: Monthly (aligned with fact_pdv_monthly_health grain)
-- Serverless Compatible: Yes
-- =====================================================================

CREATE OR REPLACE TABLE workspace.gold.kpi_execution_roi
USING DELTA
PARTITIONED BY (date)
AS

-- =====================================================================
-- CTE: Pre-aggregate revenue leakage by PDV and month
-- This prevents counting the same PDV multiple times (once per product)
-- =====================================================================
WITH revenue_by_pdv AS (
  SELECT
    YEAR(audit_date_id) AS year,
    MONTH(audit_date_id) AS month,
    pdv_code,
    
    -- Aggregate all products per PDV
    SUM(potential_revenue_lost_usd) AS total_revenue_lost_usd,
    AVG(revenue_leakage_pct) AS avg_leakage_pct,
    AVG(execution_visibility_factor) AS avg_exec_factor,
    
    -- Execution-driven loss (aggregate across products)
    SUM(potential_revenue_lost_usd * (1 - execution_visibility_factor)) AS exec_driven_loss_usd
    
  FROM workspace.gold.mart_revenue_leakage
  GROUP BY YEAR(audit_date_id), MONTH(audit_date_id), pdv_code
),

-- =====================================================================
-- CTE: Aggregate health metrics by PDV and month
-- Count execution elements once per PDV (not per product)
-- =====================================================================
health_by_pdv AS (
  SELECT
    date,
    pdv_code,
    
    -- Execution flags (take MAX to avoid double counting)
    MAX(CASE WHEN merchandiser_executed THEN 1 ELSE 0 END) AS was_visited,
    MAX(CASE WHEN has_planogram_active THEN 1 ELSE 0 END) AS has_planogram,
    MAX(CASE WHEN has_exhibition_active THEN 1 ELSE 0 END) AS has_exhibition
    
  FROM workspace.gold.fact_pdv_monthly_health
  GROUP BY date, pdv_code
)

-- =====================================================================
-- Main SELECT: Join pre-aggregated data
-- =====================================================================
SELECT
  h.date,
  pdv.merchandiser_code,
  pdv.merchandiser_name,
  pdv.chain,
  pdv.channel,
  UPPER(pdv.chain) || '|' || UPPER(pdv.channel) AS chain_channel_id,
  pdv.city,
  
  -- Coverage metrics
  COUNT(DISTINCT h.pdv_code) AS total_pdvs_assigned,
  SUM(h.was_visited) AS pdvs_visited,
  CAST(
    SUM(h.was_visited) * 100.0 / 
    NULLIF(COUNT(DISTINCT h.pdv_code), 0) 
    AS DECIMAL(7,2)
  ) AS visit_coverage_pct,
  
  -- Execution quality
  SUM(h.has_planogram) AS pdvs_with_planogram,
  SUM(h.has_exhibition) AS pdvs_with_exhibition,
  
  -- Revenue impact (already pre-aggregated by PDV in CTE)
  CAST(ROUND(COALESCE(SUM(rl.total_revenue_lost_usd), 0), 2) AS DECIMAL(15,2)) AS total_revenue_at_risk_usd,
  CAST(ROUND(COALESCE(AVG(rl.avg_leakage_pct), 0), 4) AS DECIMAL(10,4)) AS avg_leakage_rate,
  CAST(ROUND(COALESCE(AVG(rl.avg_exec_factor), 0), 4) AS DECIMAL(10,4)) AS avg_execution_factor,
  
  -- Execution-driven revenue loss (already pre-calculated in CTE)
  CAST(ROUND(COALESCE(SUM(rl.exec_driven_loss_usd), 0), 2) AS DECIMAL(15,2)) AS execution_driven_loss_usd,
  
  -- Revenue at risk with/without execution elements
  CAST(ROUND(
    COALESCE(SUM(CASE WHEN h.has_planogram = 1 THEN rl.total_revenue_lost_usd ELSE 0 END), 0), 
    2
  ) AS DECIMAL(15,2)) AS revenue_risk_with_planogram_usd,
  
  CAST(ROUND(
    COALESCE(SUM(CASE WHEN h.has_exhibition = 1 THEN rl.total_revenue_lost_usd ELSE 0 END), 0), 
    2
  ) AS DECIMAL(15,2)) AS revenue_risk_with_exhibition_usd,
  
  CAST(ROUND(
    COALESCE(SUM(CASE WHEN h.has_planogram = 0 AND h.has_exhibition = 0 THEN rl.total_revenue_lost_usd ELSE 0 END), 0), 
    2
  ) AS DECIMAL(15,2)) AS revenue_risk_no_execution_usd,
  
  -- ROI proxy: revenue at risk per PDV visited
  CAST(ROUND(
    CASE 
      WHEN SUM(h.was_visited) > 0
      THEN COALESCE(SUM(rl.total_revenue_lost_usd), 0) / NULLIF(SUM(h.was_visited), 0)
      ELSE 0
    END,
    2
  ) AS DECIMAL(15,2)) AS revenue_at_risk_per_visit_usd,
  
  CURRENT_TIMESTAMP() AS kpi_processed_at
  
FROM health_by_pdv h
INNER JOIN workspace.gold.dim_pdv pdv
  ON h.pdv_code = pdv.pdv_code
LEFT JOIN revenue_by_pdv rl
  ON YEAR(h.date) = rl.year
  AND MONTH(h.date) = rl.month
  AND h.pdv_code = rl.pdv_code
  
WHERE pdv.merchandiser_code IS NOT NULL

GROUP BY 
  h.date,
  pdv.merchandiser_code,
  pdv.merchandiser_name,
  pdv.chain,
  pdv.channel,
  pdv.city;