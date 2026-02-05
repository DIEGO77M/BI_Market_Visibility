-- =====================================================================
-- KPI 4: Channel & Chain Performance Matrix
-- =====================================================================
-- Business Purpose:
--   Segments revenue leakage by channel and chain to identify
--   where to concentrate resources and differentiated strategies.
--
-- Decision Enabled:
--   - Differentiated commercial strategy by channel/chain
--   - Investment prioritization by performance
--   - Identify chains with the greatest opportunity for improvement
--
-- Refresh: Monthly (aligned with mart_revenue_leakage grain)
-- Serverless Compatible: Yes
-- =====================================================================

CREATE OR REPLACE TABLE workspace.gold.kpi_channel_chain_performance
USING DELTA
PARTITIONED BY (date)
AS
SELECT
  audit_date_id AS date,
  UPPER(chain) || '|' || UPPER(channel) AS chain_channel_id,
  channel,
  chain,
  
  -- Coverage
  COUNT(DISTINCT pdv_code) AS total_pdvs,
  COUNT(DISTINCT product_code) AS total_products,
  COUNT(DISTINCT CONCAT(pdv_code, '-', product_code)) AS total_pdv_product_combinations,
  
  -- Revenue metrics
  SUM(potential_revenue_lost_usd) AS total_revenue_lost_usd,
  AVG(revenue_leakage_pct) AS avg_leakage_rate,
  
  -- Driver decomposition
  SUM(CASE WHEN in_stock = FALSE THEN potential_revenue_lost_usd ELSE 0 END) AS oos_driven_loss_usd,
  SUM(CASE WHEN coverage_compliant = FALSE THEN potential_revenue_lost_usd ELSE 0 END) AS assortment_driven_loss_usd,
  SUM(potential_revenue_lost_usd * (1 - price_competitiveness_factor)) AS price_driven_loss_usd,
  SUM(potential_revenue_lost_usd * (1 - execution_visibility_factor)) AS execution_driven_loss_usd,
  
  -- Factor averages
  AVG(stock_availability_factor) AS avg_stock_availability,
  AVG(price_competitiveness_factor) AS avg_price_competitiveness,
  AVG(execution_visibility_factor) AS avg_execution_visibility,
  AVG(assortment_alignment_factor) AS avg_assortment_alignment,
  
  -- Performance classification
  CASE
    WHEN AVG(revenue_leakage_pct) > 0.3 THEN 'Critical Performance'
    WHEN AVG(revenue_leakage_pct) > 0.2 THEN 'Poor Performance'
    WHEN AVG(revenue_leakage_pct) > 0.1 THEN 'Average Performance'
    ELSE 'Good Performance'
  END AS performance_tier,
  
  -- Primary issue identification
  CASE
    WHEN SUM(CASE WHEN in_stock = FALSE THEN potential_revenue_lost_usd ELSE 0 END) = 
         GREATEST(
           SUM(CASE WHEN in_stock = FALSE THEN potential_revenue_lost_usd ELSE 0 END),
           SUM(potential_revenue_lost_usd * (1 - price_competitiveness_factor)),
           SUM(potential_revenue_lost_usd * (1 - execution_visibility_factor))
         ) THEN 'Stock Issue'
    WHEN SUM(potential_revenue_lost_usd * (1 - price_competitiveness_factor)) = 
         GREATEST(
           SUM(CASE WHEN in_stock = FALSE THEN potential_revenue_lost_usd ELSE 0 END),
           SUM(potential_revenue_lost_usd * (1 - price_competitiveness_factor)),
           SUM(potential_revenue_lost_usd * (1 - execution_visibility_factor))
         ) THEN 'Price Issue'
    WHEN SUM(potential_revenue_lost_usd * (1 - execution_visibility_factor)) = 
         GREATEST(
           SUM(CASE WHEN in_stock = FALSE THEN potential_revenue_lost_usd ELSE 0 END),
           SUM(potential_revenue_lost_usd * (1 - price_competitiveness_factor)),
           SUM(potential_revenue_lost_usd * (1 - execution_visibility_factor))
         ) THEN 'Execution Issue'
    ELSE 'Mixed Issues'
  END AS primary_issue,
  
  CURRENT_TIMESTAMP() AS kpi_processed_at
  
FROM workspace.gold.mart_revenue_leakage

GROUP BY 
  audit_date_id,
  UPPER(chain),
  channel,
  chain;
