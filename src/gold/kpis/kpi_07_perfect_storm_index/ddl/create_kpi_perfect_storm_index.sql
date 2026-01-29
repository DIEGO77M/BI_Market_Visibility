-- =====================================================================
-- KPI 7: Assortment-Stock-Price-Execution Perfect Storm Index
-- =====================================================================
-- Business Purpose:
--   Identifies PDV-products where ALL factors fail simultaneously
--   (maximum risk). Prioritizes interventions where multiple problems
--   amplify each other.
--
-- Decision Enabled:
--   - Ultra-prioritized list for immediate intervention
--   - Identify "perfect storms" that amplify losses
--   - Assign cross-functional resources (not just one area)
--
-- Refresh: Monthly (aligned with mart_revenue_leakage grain)
-- Serverless Compatible: Yes
-- =====================================================================

CREATE OR REPLACE TABLE workspace.gold.kpi_perfect_storm_index
USING DELTA
PARTITIONED BY (date)
AS
SELECT
  audit_date_id AS date,
  pdv_code,
  product_code,
  brand,
  category,
  chain,
  channel,
  city,
  
  -- Revenue impact
  potential_revenue_lost_usd,
  revenue_leakage_pct,
  
  -- Individual factors
  stock_availability_factor,
  price_competitiveness_factor,
  execution_visibility_factor,
  assortment_alignment_factor,
  
  -- Factor failure flags
  CASE WHEN stock_availability_factor < 0.5 THEN 1 ELSE 0 END AS stock_failure_flag,
  CASE WHEN price_competitiveness_factor < 0.6 THEN 1 ELSE 0 END AS price_failure_flag,
  CASE WHEN execution_visibility_factor < 0.5 THEN 1 ELSE 0 END AS execution_failure_flag,
  CASE WHEN assortment_alignment_factor < 0.7 THEN 1 ELSE 0 END AS assortment_failure_flag,
  
  -- Count of failing factors
  (
    CASE WHEN stock_availability_factor < 0.5 THEN 1 ELSE 0 END +
    CASE WHEN price_competitiveness_factor < 0.6 THEN 1 ELSE 0 END +
    CASE WHEN execution_visibility_factor < 0.5 THEN 1 ELSE 0 END +
    CASE WHEN assortment_alignment_factor < 0.7 THEN 1 ELSE 0 END
  ) AS failing_factors_count,
  
  -- Risk tier classification
  CASE
    WHEN stock_availability_factor < 0.5
      AND price_competitiveness_factor < 0.6
      AND execution_visibility_factor < 0.5
      AND assortment_alignment_factor < 0.7
    THEN 'Perfect Storm - All 4 Factors Failing'
    
    WHEN (
      (stock_availability_factor < 0.5 AND price_competitiveness_factor < 0.6 AND execution_visibility_factor < 0.5) OR
      (stock_availability_factor < 0.5 AND price_competitiveness_factor < 0.6 AND assortment_alignment_factor < 0.7) OR
      (stock_availability_factor < 0.5 AND execution_visibility_factor < 0.5 AND assortment_alignment_factor < 0.7) OR
      (price_competitiveness_factor < 0.6 AND execution_visibility_factor < 0.5 AND assortment_alignment_factor < 0.7)
    ) THEN 'Critical Storm - 3 Factors Failing'
    
    WHEN (
      (stock_availability_factor < 0.5 AND price_competitiveness_factor < 0.6) OR
      (stock_availability_factor < 0.5 AND execution_visibility_factor < 0.5) OR
      (stock_availability_factor < 0.5 AND assortment_alignment_factor < 0.7) OR
      (price_competitiveness_factor < 0.6 AND execution_visibility_factor < 0.5) OR
      (price_competitiveness_factor < 0.6 AND assortment_alignment_factor < 0.7) OR
      (execution_visibility_factor < 0.5 AND assortment_alignment_factor < 0.7)
    ) THEN 'High Risk - 2 Factors Failing'
    
    WHEN (
      stock_availability_factor < 0.5 OR
      price_competitiveness_factor < 0.6 OR
      execution_visibility_factor < 0.5 OR
      assortment_alignment_factor < 0.7
    ) THEN 'Medium Risk - 1 Factor Failing'
    
    ELSE 'Under Control'
  END AS risk_tier,
  
  -- Intervention complexity score (0-100)
  -- Higher = more complex (more teams needed)
  CAST(
    (
      CASE WHEN stock_availability_factor < 0.5 THEN 25 ELSE 0 END +
      CASE WHEN price_competitiveness_factor < 0.6 THEN 25 ELSE 0 END +
      CASE WHEN execution_visibility_factor < 0.5 THEN 25 ELSE 0 END +
      CASE WHEN assortment_alignment_factor < 0.7 THEN 25 ELSE 0 END
    ) AS INT
  ) AS intervention_complexity_score,
  
  -- Required teams for intervention
  CONCAT_WS(', ',
    CASE WHEN stock_availability_factor < 0.5 THEN 'Supply Chain' END,
    CASE WHEN price_competitiveness_factor < 0.6 THEN 'Pricing' END,
    CASE WHEN execution_visibility_factor < 0.5 THEN 'Trade Marketing' END,
    CASE WHEN assortment_alignment_factor < 0.7 THEN 'Category Management' END
  ) AS required_teams,
  
  -- Amplification factor (problems compound each other)
  CAST(
    POWER(2, (
      CASE WHEN stock_availability_factor < 0.5 THEN 1 ELSE 0 END +
      CASE WHEN price_competitiveness_factor < 0.6 THEN 1 ELSE 0 END +
      CASE WHEN execution_visibility_factor < 0.5 THEN 1 ELSE 0 END +
      CASE WHEN assortment_alignment_factor < 0.7 THEN 1 ELSE 0 END
    )) AS DECIMAL(5,2)
  ) AS problem_amplification_factor,
  
  in_stock,
  coverage_compliant,
  
  CURRENT_TIMESTAMP() AS kpi_processed_at
  
FROM workspace.gold.mart_revenue_leakage
WHERE potential_revenue_lost_usd > 0;
