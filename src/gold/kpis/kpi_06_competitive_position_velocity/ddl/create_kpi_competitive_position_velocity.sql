-- =====================================================================
-- KPI 6: Competitive Position Erosion Velocity
-- =====================================================================
-- Business Purpose:
--   Measures velocity of competitive deterioration (not just current state).
--   Detects trends before they become critical.
--
-- Decision Enabled:
--   - Early alerts of competitive deterioration
--   - Prioritize preventive vs corrective actions
--   - Identify changes in competitor strategy
--
-- Refresh: Monthly (aligned with fact_pdv_price_audit grain)
-- Serverless Compatible: Yes (Unity Catalog compatible)
-- =====================================================================

CREATE OR REPLACE TABLE workspace.gold.kpi_competitive_position_velocity
USING DELTA
PARTITIONED BY (date)
AS

-- =====================================================================
-- CTE 1: Aggregate to monthly level first (prevents LAG issues)
-- =====================================================================
WITH monthly_price_data AS (
  SELECT
    YEAR(audit_date) AS year,
    MONTH(audit_date) AS month,
    pdv_code,
    product_code,
    
    -- Normalize price gap to decimal format (handles both 7.5 and 0.075)
    AVG(
      CASE 
        WHEN ABS(price_vs_competitor_pct) > 1 
        THEN price_vs_competitor_pct / 100.0
        ELSE price_vs_competitor_pct
      END
    ) AS avg_price_gap_decimal,
    
    -- Competitive position - derive from avg price gap if inconsistent in month
    CASE
      WHEN AVG(
        CASE 
          WHEN ABS(price_vs_competitor_pct) > 1 
          THEN price_vs_competitor_pct / 100.0
          ELSE price_vs_competitor_pct
        END
      ) > 0.05 THEN 'Losing'
      WHEN AVG(
        CASE 
          WHEN ABS(price_vs_competitor_pct) > 1 
          THEN price_vs_competitor_pct / 100.0
          ELSE price_vs_competitor_pct
        END
      ) < -0.05 THEN 'Winning'
      ELSE 'At Par'
    END AS competitive_position,
    
    AVG(observed_price) AS avg_observed_price,
    AVG(avg_competitor_price) AS avg_competitor_price,
    
    COUNT(*) AS audit_count
    
  FROM workspace.gold.fact_pdv_price_audit
  
  -- Data quality filters
  WHERE price_vs_competitor_pct IS NOT NULL
    AND ABS(price_vs_competitor_pct) <= 100
    AND observed_price > 0
    AND avg_competitor_price > 0
  
  GROUP BY 
    YEAR(audit_date),
    MONTH(audit_date),
    pdv_code,
    product_code
),

-- =====================================================================
-- CTE 2: Calculate LAG once (avoid repetition)
-- =====================================================================
windowed_data AS (
  SELECT
    year,
    month,
    pdv_code,
    product_code,
    avg_price_gap_decimal,
    competitive_position,
    avg_observed_price,
    avg_competitor_price,
    audit_count,
    
    -- Previous month values (LAG calculated once)
    LAG(avg_price_gap_decimal, 1) OVER (
      PARTITION BY pdv_code, product_code
      ORDER BY year, month
    ) AS prev_price_gap_decimal,
    
    LAG(competitive_position, 1) OVER (
      PARTITION BY pdv_code, product_code
      ORDER BY year, month
    ) AS prev_position
    
  FROM monthly_price_data
),

-- =====================================================================
-- CTE 3: Calculate velocity metrics at PDV level
-- =====================================================================
pdv_level_velocity AS (
  SELECT
    w.year,
    w.month,
    CONCAT(w.year, '-', LPAD(w.month, 2, '0'), '-01') AS date,
    w.pdv_code,
    w.product_code,
    p.brand,
    p.category,
    pdv.chain,
    pdv.channel,
    pdv.city,
    
    -- Current state (convert back to percentage for readability)
    w.competitive_position AS current_position,
    CAST(ROUND(w.avg_price_gap_decimal * 100, 2) AS DECIMAL(10,2)) AS current_price_gap_pct,
    CAST(ROUND(w.avg_observed_price, 2) AS DECIMAL(12,2)) AS current_observed_price,
    CAST(ROUND(w.avg_competitor_price, 2) AS DECIMAL(12,2)) AS current_competitor_price,
    
    -- Previous state
    w.prev_position AS previous_position,
    CAST(ROUND(COALESCE(w.prev_price_gap_decimal, 0) * 100, 2) AS DECIMAL(10,2)) AS previous_price_gap_pct,
    
    -- Velocity calculation (in percentage points)
    CAST(
      ROUND((w.avg_price_gap_decimal - COALESCE(w.prev_price_gap_decimal, 0)) * 100, 2) 
      AS DECIMAL(10,2)
    ) AS price_gap_velocity_pct,
    
    -- Competitive trend classification
    CASE
      WHEN w.prev_position IS NULL THEN 'First Observation'
      
      WHEN w.competitive_position = 'Losing' 
       AND w.prev_position IN ('Winning', 'At Par') 
        THEN 'Deteriorating'
      
      WHEN w.competitive_position = 'Winning' 
       AND w.prev_position = 'Losing' 
        THEN 'Recovering'
      
      WHEN w.competitive_position = 'At Par'
       AND w.prev_position = 'Losing'
        THEN 'Recovering'
      
      WHEN w.competitive_position = 'Losing'
       AND w.prev_position = 'Losing'
       AND (w.avg_price_gap_decimal - w.prev_price_gap_decimal) > 0.02
        THEN 'Deteriorating'
        
      WHEN w.competitive_position = w.prev_position 
        THEN 'Stable'
      
      ELSE 'Position Changed'
    END AS competitive_trend,
    
    -- Velocity magnitude (absolute change)
    CASE
      WHEN w.prev_price_gap_decimal IS NULL THEN 'First Observation'
      
      WHEN ABS(w.avg_price_gap_decimal - w.prev_price_gap_decimal) * 100 > 5 
        THEN 'High Velocity'
      
      WHEN ABS(w.avg_price_gap_decimal - w.prev_price_gap_decimal) * 100 > 2 
        THEN 'Medium Velocity'
      
      ELSE 'Low Velocity'
    END AS velocity_magnitude,
    
    -- Critical alert flag
    CASE
      WHEN w.competitive_position = 'Losing' 
       AND w.prev_position IN ('Winning', 'At Par')
       AND w.avg_price_gap_decimal > 0.10
        THEN TRUE
      ELSE FALSE
    END AS critical_alert
    
  FROM windowed_data w
  INNER JOIN workspace.gold.dim_product p 
    ON w.product_code = p.product_code
  INNER JOIN workspace.gold.dim_pdv pdv 
    ON w.pdv_code = pdv.pdv_code
)

-- =====================================================================
-- Final SELECT: Aggregate to product level for strategic KPI
-- =====================================================================
SELECT
  date,
  product_code,
  brand,
  category,
  
  -- Coverage metrics
  COUNT(DISTINCT pdv_code) AS total_pdvs_tracked,
  
  -- PDV distribution by trend
  COUNT(DISTINCT CASE WHEN competitive_trend = 'Deteriorating' THEN pdv_code END) AS pdvs_deteriorating,
  COUNT(DISTINCT CASE WHEN competitive_trend = 'Recovering' THEN pdv_code END) AS pdvs_recovering,
  COUNT(DISTINCT CASE WHEN competitive_trend = 'Stable' THEN pdv_code END) AS pdvs_stable,
  COUNT(DISTINCT CASE WHEN competitive_trend = 'First Observation' THEN pdv_code END) AS pdvs_first_observation,
  
  -- Percentage distribution
  CAST(
    ROUND(
      COUNT(DISTINCT CASE WHEN competitive_trend = 'Deteriorating' THEN pdv_code END) * 100.0 /
      NULLIF(COUNT(DISTINCT pdv_code), 0),
      2
    ) AS DECIMAL(5,2)
  ) AS pct_pdvs_deteriorating,
  
  CAST(
    ROUND(
      COUNT(DISTINCT CASE WHEN competitive_trend = 'Recovering' THEN pdv_code END) * 100.0 /
      NULLIF(COUNT(DISTINCT pdv_code), 0),
      2
    ) AS DECIMAL(5,2)
  ) AS pct_pdvs_recovering,
  
  -- Current state aggregates
  CAST(ROUND(AVG(current_price_gap_pct), 2) AS DECIMAL(10,2)) AS avg_current_price_gap_pct,
  CAST(ROUND(AVG(previous_price_gap_pct), 2) AS DECIMAL(10,2)) AS avg_previous_price_gap_pct,
  
  -- Velocity metrics
  CAST(ROUND(AVG(price_gap_velocity_pct), 2) AS DECIMAL(10,2)) AS avg_velocity_pct,
  CAST(ROUND(MAX(price_gap_velocity_pct), 2) AS DECIMAL(10,2)) AS max_velocity_pct,
  CAST(ROUND(MIN(price_gap_velocity_pct), 2) AS DECIMAL(10,2)) AS min_velocity_pct,
  
  -- Velocity distribution
  COUNT(DISTINCT CASE WHEN velocity_magnitude = 'High Velocity' THEN pdv_code END) AS pdvs_high_velocity,
  COUNT(DISTINCT CASE WHEN velocity_magnitude = 'Medium Velocity' THEN pdv_code END) AS pdvs_medium_velocity,
  COUNT(DISTINCT CASE WHEN velocity_magnitude = 'Low Velocity' THEN pdv_code END) AS pdvs_low_velocity,
  
  -- Alert metrics
  COUNT(DISTINCT CASE WHEN critical_alert THEN pdv_code END) AS pdvs_with_critical_alert,
  MAX(CASE WHEN critical_alert THEN 1 ELSE 0 END) AS has_critical_alert,
  
  -- Overall trend classification (product level)
  CASE
    WHEN COUNT(DISTINCT CASE WHEN competitive_trend = 'Deteriorating' THEN pdv_code END) * 100.0 /
         NULLIF(COUNT(DISTINCT pdv_code), 0) > 50 
      THEN 'Widespread Deterioration'
    
    WHEN COUNT(DISTINCT CASE WHEN competitive_trend = 'Deteriorating' THEN pdv_code END) * 100.0 /
         NULLIF(COUNT(DISTINCT pdv_code), 0) > 25 
      THEN 'Selective Deterioration'
    
    WHEN COUNT(DISTINCT CASE WHEN competitive_trend = 'Recovering' THEN pdv_code END) * 100.0 /
         NULLIF(COUNT(DISTINCT pdv_code), 0) > 50 
      THEN 'Widespread Recovery'
    
    WHEN COUNT(DISTINCT CASE WHEN competitive_trend = 'Recovering' THEN pdv_code END) * 100.0 /
         NULLIF(COUNT(DISTINCT pdv_code), 0) > 25 
      THEN 'Selective Recovery'
    
    WHEN COUNT(DISTINCT CASE WHEN competitive_trend = 'Stable' THEN pdv_code END) * 100.0 /
         NULLIF(COUNT(DISTINCT pdv_code), 0) > 70 
      THEN 'Stable Position'
    
    ELSE 'Mixed Trends'
  END AS overall_trend,
  
  -- Recommended action
  CASE
    WHEN COUNT(DISTINCT CASE WHEN critical_alert THEN pdv_code END) > 0
      THEN 'Immediate Price Review Required'
    
    WHEN COUNT(DISTINCT CASE WHEN competitive_trend = 'Deteriorating' THEN pdv_code END) * 100.0 /
         NULLIF(COUNT(DISTINCT pdv_code), 0) > 50
      THEN 'Strategic Pricing Intervention'
    
    WHEN AVG(price_gap_velocity_pct) > 2
      THEN 'Monitor Closely - Negative Trend'
    
    WHEN COUNT(DISTINCT CASE WHEN competitive_trend = 'Recovering' THEN pdv_code END) * 100.0 /
         NULLIF(COUNT(DISTINCT pdv_code), 0) > 50
      THEN 'Continue Current Strategy'
    
    ELSE 'Maintain Vigilance'
  END AS recommended_action,
  
  CURRENT_TIMESTAMP() AS kpi_processed_at

FROM pdv_level_velocity

GROUP BY 
  date,
  product_code,
  brand,
  category;