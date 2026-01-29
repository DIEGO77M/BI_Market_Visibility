-- =====================================================================
-- KPI 5: Product Portfolio Health - Winners vs Losers
-- =====================================================================
-- Business Purpose:
--   Identifies products that are losing share due to avoidable causes
--   (stock, price, execution) to redefine active portfolio by PDV.
--
-- Decision Enabled:
--   - Redefine active portfolio by PDV (discontinue/strengthen)
--   - Identify star products vs problem products
--   - Optimize assortment and shelf space
--
-- Refresh: Monthly (aligned with mart_revenue_leakage grain)
-- Serverless Compatible: Yes
-- =====================================================================

CREATE OR REPLACE TABLE workspace.gold.kpi_product_portfolio_health
USING DELTA
PARTITIONED BY (date)
AS
SELECT
  CONCAT(YEAR(audit_date_id), '-', LPAD(MONTH(audit_date_id), 2, '0'), '-01') AS date,
  product_code,
  brand,
  category,
  
  -- Coverage metrics
  COUNT(DISTINCT pdv_code) AS total_pdvs_exposed,
  COUNT(DISTINCT CASE WHEN in_stock THEN pdv_code END) AS pdvs_in_stock,
  COUNT(DISTINCT CASE WHEN coverage_compliant THEN pdv_code END) AS pdvs_coverage_compliant,
  CAST(
    ROUND(
      COUNT(DISTINCT CASE WHEN in_stock THEN pdv_code END) * 100.0 /
      NULLIF(COUNT(DISTINCT pdv_code), 0),
      2
    ) AS DECIMAL(5,2)
  ) AS stock_availability_pct,
  
  -- Revenue impact
  CAST(ROUND(COALESCE(SUM(potential_revenue_lost_usd), 0), 2) AS DECIMAL(15,2)) AS total_revenue_at_risk_usd,
  CAST(ROUND(COALESCE(AVG(revenue_leakage_pct), 0), 4) AS DECIMAL(10,4)) AS avg_leakage_pct,
  
  -- Performance factors (weighted by importance)
  CAST(ROUND(COALESCE(AVG(stock_availability_factor), 0), 4) AS DECIMAL(10,4)) AS avg_stock_factor,
  CAST(ROUND(COALESCE(AVG(price_competitiveness_factor), 0), 4) AS DECIMAL(10,4)) AS avg_price_factor,
  CAST(ROUND(COALESCE(AVG(execution_visibility_factor), 0), 4) AS DECIMAL(10,4)) AS avg_execution_factor,
  CAST(ROUND(COALESCE(AVG(assortment_alignment_factor), 0), 4) AS DECIMAL(10,4)) AS avg_assortment_factor,
  
  -- Product health classification
  CASE
    WHEN COALESCE(AVG(revenue_leakage_pct), 0) > 0.3 
     AND COALESCE(SUM(potential_revenue_lost_usd), 0) > 2000 
      THEN 'Critical - High Loss'
    
    WHEN COALESCE(AVG(revenue_leakage_pct), 0) > 0.2 
     AND COALESCE(SUM(potential_revenue_lost_usd), 0) > 1000 
      THEN 'At Risk'
    
    WHEN COALESCE(AVG(revenue_leakage_pct), 0) > 0.15 
      THEN 'Monitor Closely'
    
    WHEN COALESCE(AVG(revenue_leakage_pct), 0) < 0.1 
      THEN 'Healthy'
    
    ELSE 'Stable'
  END AS product_health_status,
  
  -- Recommended action based on root cause
  CASE
    WHEN COALESCE(AVG(stock_availability_factor), 0) < 0.6 
      THEN 'Improve Distribution'
    
    WHEN COALESCE(AVG(price_competitiveness_factor), 0) < 0.6 
      THEN 'Review Pricing'
    
    WHEN COALESCE(AVG(execution_visibility_factor), 0) < 0.6 
      THEN 'Strengthen Execution'
    
    WHEN COALESCE(AVG(assortment_alignment_factor), 0) < 0.7 
      THEN 'Reassess Assortment Fit'
    
    WHEN COALESCE(AVG(revenue_leakage_pct), 0) > 0.3 
      THEN 'Consider Portfolio Exit'
    
    ELSE 'Maintain & Grow'
  END AS recommended_action,
  
  -- Strategic value indicator
  CASE
    WHEN COALESCE(SUM(potential_revenue_lost_usd), 0) > 5000 
      THEN 'Strategic Product'
    
    WHEN COALESCE(SUM(potential_revenue_lost_usd), 0) > 1000 
      THEN 'Important Product'
    
    ELSE 'Standard Product'
  END AS strategic_value,
  
  CURRENT_TIMESTAMP() AS kpi_processed_at
  
FROM workspace.gold.mart_revenue_leakage

-- Data quality validations
WHERE potential_revenue_lost_usd >= 0
  AND revenue_leakage_pct >= 0
  AND revenue_leakage_pct <= 1

GROUP BY 
  YEAR(audit_date_id),
  MONTH(audit_date_id),
  product_code,
  brand,
  category;