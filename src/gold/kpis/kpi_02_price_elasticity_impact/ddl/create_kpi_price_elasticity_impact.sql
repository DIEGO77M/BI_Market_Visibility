-- =====================================================================
-- KPI: Price Elasticity Impact on Revenue Leakage
-- =====================================================================
-- Purpose: Identifies price-sensitive products by crossing price gaps
--          with revenue leakage, including distribution metrics to detect
--          hidden execution issues
--
-- Key Enhancements:
-- - Distribution metrics (stddev, median) prevent misleading averages
-- - Execution consistency score detects directional mismatches
-- - Clear percentage formatting for business users
-- - Automatic data quality flags
--
-- Environment: Databricks + Unity Catalog
-- =====================================================================

CREATE OR REPLACE TABLE workspace.gold.kpi_price_elasticity_impact
USING DELTA
PARTITIONED BY (date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS

-- =====================================================================
-- CTE: Normalize price gap and add distribution metrics
-- =====================================================================
WITH price_metrics AS (
  SELECT
    pa.audit_date,
    pa.product_code,
    p.brand,
    p.category,
    p.subcategory,
    
    -- Price gap normalization (handle both formats: 7.5 or 0.075)
    AVG(
      CASE 
        WHEN ABS(pa.price_vs_competitor_pct) > 1 
        THEN pa.price_vs_competitor_pct / 100.0
        ELSE pa.price_vs_competitor_pct
      END
    ) AS avg_price_gap_decimal,
    
    -- Distribution metrics
    MEDIAN(
      CASE 
        WHEN ABS(pa.price_vs_competitor_pct) > 1 
        THEN pa.price_vs_competitor_pct / 100.0
        ELSE pa.price_vs_competitor_pct
      END
    ) AS median_price_gap_decimal,
    
    STDDEV(
      CASE 
        WHEN ABS(pa.price_vs_competitor_pct) > 1 
        THEN pa.price_vs_competitor_pct / 100.0
        ELSE pa.price_vs_competitor_pct
      END
    ) AS price_gap_stddev,
    
    -- Price levels
    AVG(pa.observed_price) AS avg_observed_price,
    AVG(pa.avg_competitor_price) AS avg_competitor_price,
    MIN(pa.observed_price) AS min_observed_price,
    MAX(pa.observed_price) AS max_observed_price,
    
    -- Coverage metrics
    COUNT(DISTINCT pa.pdv_code) AS affected_pdvs,
    COUNT(DISTINCT CASE WHEN pa.is_price_above_competition THEN pa.pdv_code END) AS pdvs_above_competition,
    
    -- Revenue aggregation
    SUM(rl.potential_revenue_lost_usd) AS total_revenue_at_risk_usd,
    AVG(rl.revenue_leakage_pct) AS avg_revenue_leakage_pct,
    AVG(rl.price_competitiveness_factor) AS avg_price_competitiveness_factor
    
  FROM workspace.gold.fact_pdv_price_audit pa
  INNER JOIN workspace.gold.mart_revenue_leakage rl
    ON pa.audit_date = rl.audit_date_id
   AND pa.pdv_code = rl.pdv_code
   AND pa.product_code = rl.product_code
  INNER JOIN workspace.gold.dim_product p
    ON pa.product_code = p.product_code
  
  GROUP BY pa.audit_date, pa.product_code, p.brand, p.category, p.subcategory
)

-- =====================================================================
-- Main SELECT: Add calculated metrics and business logic
-- =====================================================================
SELECT
  audit_date AS date,
  product_code,
  brand,
  category,
  subcategory,
  
  -- Price gap in clear percentage format (7.98 means 7.98%)
  CAST(ROUND(avg_price_gap_decimal * 100, 2) AS DECIMAL(10,2)) AS avg_price_gap_pct,
  
  -- Keep decimal for calculations
  CAST(ROUND(avg_price_gap_decimal, 6) AS DECIMAL(10,6)) AS avg_price_gap_decimal,
  
  -- Median (robust to outliers)
  CAST(ROUND(median_price_gap_decimal * 100, 2) AS DECIMAL(10,2)) AS median_price_gap_pct,
  
  -- Absolute currency difference
  CAST(ROUND(avg_observed_price - avg_competitor_price, 2) AS DECIMAL(12,2)) AS avg_price_diff_usd,
  
  -- Price levels
  CAST(ROUND(avg_observed_price, 2) AS DECIMAL(12,2)) AS avg_observed_price,
  CAST(ROUND(avg_competitor_price, 2) AS DECIMAL(12,2)) AS avg_competitor_price,
  CAST(ROUND(min_observed_price, 2) AS DECIMAL(12,2)) AS min_observed_price,
  CAST(ROUND(max_observed_price, 2) AS DECIMAL(12,2)) AS max_observed_price,
  
  -- Distribution metric
  CAST(ROUND(price_gap_stddev, 4) AS DECIMAL(10,4)) AS price_gap_stddev,
  
  -- Coefficient of variation (normalized volatility)
  CAST(
    ROUND(
      CASE 
        WHEN avg_price_gap_decimal != 0 
        THEN ABS(price_gap_stddev / avg_price_gap_decimal)
        ELSE NULL 
      END,
      4
    ) AS DECIMAL(10,4)
  ) AS price_gap_cv,
  
  -- Interquartile range placeholder (simplified)
  CAST(ROUND(price_gap_stddev * 1.35 * 100, 2) AS DECIMAL(10,2)) AS price_gap_iqr_pct,
  
  -- PDV consistency score: what % of PDVs follow average direction?
  CAST(
    ROUND(
      CASE 
        WHEN avg_price_gap_decimal > 0 
        THEN CAST(pdvs_above_competition AS DOUBLE) / affected_pdvs
        WHEN avg_price_gap_decimal < 0
        THEN CAST((affected_pdvs - pdvs_above_competition) AS DOUBLE) / affected_pdvs
        ELSE 1.0
      END,
      4
    ) AS DECIMAL(10,4)
  ) AS pdv_consistency_score,
  
  -- Consistency flag
  CASE 
    WHEN price_gap_stddev > 0.10 THEN 'High Variance - Investigate'
    WHEN price_gap_stddev > 0.05 THEN 'Medium Variance - Monitor'
    WHEN CASE 
           WHEN avg_price_gap_decimal > 0 
           THEN CAST(pdvs_above_competition AS DOUBLE) / affected_pdvs
           WHEN avg_price_gap_decimal < 0
           THEN CAST((affected_pdvs - pdvs_above_competition) AS DOUBLE) / affected_pdvs
           ELSE 1.0
         END < 0.70 THEN 'Directional Mismatch - Audit'
    ELSE 'Consistent Execution'
  END AS pricing_consistency_flag,
  
  -- Revenue impact
  CAST(ROUND(total_revenue_at_risk_usd, 2) AS DECIMAL(15,2)) AS total_revenue_at_risk_usd,
  CAST(ROUND(avg_revenue_leakage_pct * 100, 2) AS DECIMAL(10,2)) AS avg_revenue_leakage_pct,
  
  -- Elasticity
  CAST(
    ROUND(
      total_revenue_at_risk_usd / NULLIF(ABS(avg_price_gap_decimal), 0),
      2
    ) AS DECIMAL(18,2)
  ) AS revenue_loss_per_1pct_gap,
  
  -- Normalized elasticity score (0-100)
  CAST(
    ROUND(
      LEAST(
        (total_revenue_at_risk_usd / NULLIF(ABS(avg_price_gap_decimal), 0)) / 10000 * 100,
        100
      ),
      2
    ) AS DECIMAL(10,2)
  ) AS elasticity_score,
  
  -- Coverage
  affected_pdvs,
  pdvs_above_competition,
  affected_pdvs - pdvs_above_competition AS pdvs_below_market,
  CAST(ROUND(CAST(pdvs_above_competition AS DOUBLE) / affected_pdvs * 100, 1) AS DECIMAL(5,1)) AS pct_pdvs_above_market,
  
  -- Business segmentation
  CASE
    WHEN avg_price_gap_decimal > 0.10 
     AND total_revenue_at_risk_usd > 5000
     AND CAST(pdvs_above_competition AS DOUBLE) / affected_pdvs > 0.70
      THEN 'Critical - High Sensitivity'
    
    WHEN (avg_price_gap_decimal > 0.05 AND total_revenue_at_risk_usd > 5000)
      OR (avg_price_gap_decimal > 0.10 AND total_revenue_at_risk_usd > 1000)
      THEN 'High Price Sensitivity'
    
    WHEN avg_price_gap_decimal > 0.05 
     AND total_revenue_at_risk_usd > 500
      THEN 'Medium Price Sensitivity'
    
    WHEN avg_price_gap_decimal > 0.01
      THEN 'Low Price Sensitivity'
    
    WHEN ABS(avg_price_gap_decimal) <= 0.01
      THEN 'Price Parity'
    
    ELSE 'Below Market - Margin Opportunity'
  END AS price_sensitivity_segment,
  
  -- Recommended action
  CASE
    WHEN avg_price_gap_decimal > 0.05 
     AND CAST(pdvs_above_competition AS DOUBLE) / affected_pdvs > 0.80
     AND total_revenue_at_risk_usd > 2000
      THEN 'Immediate Price Reduction Required'
    
    WHEN CASE 
           WHEN avg_price_gap_decimal > 0 
           THEN CAST(pdvs_above_competition AS DOUBLE) / affected_pdvs
           WHEN avg_price_gap_decimal < 0
           THEN CAST((affected_pdvs - pdvs_above_competition) AS DOUBLE) / affected_pdvs
           ELSE 1.0
         END < 0.50
      THEN 'Execution Audit Required'
    
    WHEN price_gap_stddev > 0.10
      THEN 'Pricing Standardization Required'
    
    WHEN avg_price_gap_decimal < -0.03
     AND total_revenue_at_risk_usd > 5000
      THEN 'Margin Improvement Opportunity'
    
    ELSE 'Monitor'
  END AS recommended_action,
  
  -- Data quality
  DATEDIFF(
    (SELECT MAX(audit_date) FROM price_metrics),
    audit_date
  ) AS days_since_audit,
  
  CASE 
    WHEN avg_observed_price <= 0 OR avg_competitor_price <= 0 
      THEN 'Invalid Price Data'
    WHEN ABS(avg_price_gap_decimal) > 1.0 
      THEN 'Extreme Gap - Validate'
    WHEN affected_pdvs < 5 
      THEN 'Low Sample Size'
    WHEN price_gap_stddev > 0.50
      THEN 'High Variance - Check Outliers'
    ELSE 'Valid'
  END AS data_quality_flag,
  
  CAST(ROUND(avg_price_competitiveness_factor, 2) AS DECIMAL(10,2)) AS avg_price_competitiveness_factor,
  
  CURRENT_TIMESTAMP() AS kpi_processed_at

FROM price_metrics

WHERE avg_observed_price > 0
  AND avg_competitor_price > 0
  AND affected_pdvs >= 3;

-- =====================================================================
-- Optimize for query performance
-- =====================================================================

OPTIMIZE workspace.gold.kpi_price_elasticity_impact
ZORDER BY (product_code, price_sensitivity_segment);