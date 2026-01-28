-- =====================================================================
-- KPI 1: Revenue Recovery Opportunity Score
-- =====================================================================
-- Business Purpose:
--   Prioritizes PDV-product recovery opportunities by combining
--   economic impact, leakage severity, and operational feasibility.
--
-- Why Recruiters Care:
--   - Demonstrates senior-level KPI design with financial consistency
--   - Shows understanding of Spark SQL numeric behavior in serverless
--   - Translates analytics into actionable commercial decisions
--
-- Decision Enabled:
--   - Prioritized recovery roadmap (pricing, stock, execution, assortment)
--   - Identification of quick wins vs structural issues
--   - Efficient allocation of commercial and trade marketing resources
--
-- Refresh Cadence: Monthly
-- Grain: PDV x Product x Month
-- Platform: Databricks Serverless + Unity Catalog
-- =====================================================================

CREATE OR REPLACE TABLE workspace.gold.kpi_recovery_opportunity_score
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

  potential_revenue_lost_usd,
  revenue_leakage_pct,

  stock_availability_factor,
  price_competitiveness_factor,
  execution_visibility_factor,
  assortment_alignment_factor,

  -- -------------------------------------------------------------
  -- Action type classification based on primary weakness
  -- -------------------------------------------------------------
  CASE
    WHEN stock_availability_factor < 0.5 AND in_stock = FALSE THEN 'Quick Win - Restock'
    WHEN price_competitiveness_factor < 0.6 THEN 'Price Adjustment Needed'
    WHEN execution_visibility_factor < 0.5 THEN 'Execution Gap'
    WHEN assortment_alignment_factor < 0.7 THEN 'Assortment Mismatch'
    ELSE 'Under Control'
  END AS recovery_action_type,

  -- -------------------------------------------------------------
  -- Revenue loss decomposition
  -- Corrected to:
  --   * Enforce explicit DECIMAL casting (financial accuracy)
  --   * Avoid numeric inflation due to implicit type promotion
  --   * Ensure driver losses are proportional to actual leakage
  -- -------------------------------------------------------------

  CAST(
    potential_revenue_lost_usd * revenue_leakage_pct *
    CASE
      WHEN (
        (1 - stock_availability_factor) +
        (1 - price_competitiveness_factor) +
        (1 - execution_visibility_factor) +
        (1 - assortment_alignment_factor)
      ) > 0
      THEN (1 - stock_availability_factor) /
           (
            (1 - stock_availability_factor) +
            (1 - price_competitiveness_factor) +
            (1 - execution_visibility_factor) +
            (1 - assortment_alignment_factor)
           )
      ELSE 0
    END
  AS DECIMAL(10,2)) AS stock_driven_loss_usd,

  CAST(
    potential_revenue_lost_usd * revenue_leakage_pct *
    CASE
      WHEN (
        (1 - stock_availability_factor) +
        (1 - price_competitiveness_factor) +
        (1 - execution_visibility_factor) +
        (1 - assortment_alignment_factor)
      ) > 0
      THEN (1 - price_competitiveness_factor) /
           (
            (1 - stock_availability_factor) +
            (1 - price_competitiveness_factor) +
            (1 - execution_visibility_factor) +
            (1 - assortment_alignment_factor)
           )
      ELSE 0
    END
  AS DECIMAL(10,2)) AS price_driven_loss_usd,

  CAST(
    potential_revenue_lost_usd * revenue_leakage_pct *
    CASE
      WHEN (
        (1 - stock_availability_factor) +
        (1 - price_competitiveness_factor) +
        (1 - execution_visibility_factor) +
        (1 - assortment_alignment_factor)
      ) > 0
      THEN (1 - execution_visibility_factor) /
           (
            (1 - stock_availability_factor) +
            (1 - price_competitiveness_factor) +
            (1 - execution_visibility_factor) +
            (1 - assortment_alignment_factor)
           )
      ELSE 0
    END
  AS DECIMAL(10,2)) AS execution_driven_loss_usd,

  CAST(
    potential_revenue_lost_usd * revenue_leakage_pct *
    CASE
      WHEN (
        (1 - stock_availability_factor) +
        (1 - price_competitiveness_factor) +
        (1 - execution_visibility_factor) +
        (1 - assortment_alignment_factor)
      ) > 0
      THEN (1 - assortment_alignment_factor) /
           (
            (1 - stock_availability_factor) +
            (1 - price_competitiveness_factor) +
            (1 - execution_visibility_factor) +
            (1 - assortment_alignment_factor)
           )
      ELSE 0
    END
  AS DECIMAL(10,2)) AS assortment_driven_loss_usd,

  -- -------------------------------------------------------------
  -- Recovery priority score (0–100)
  -- Designed for ranking and operational prioritization
  -- -------------------------------------------------------------
  LEAST(
    ROUND(
      (
        COALESCE(potential_revenue_lost_usd /
          NULLIF(MAX(potential_revenue_lost_usd) OVER (), 0), 0) * 0.4 +
        COALESCE(revenue_leakage_pct /
          NULLIF(MAX(revenue_leakage_pct) OVER (), 0), 0) * 0.3 +
        (1 - stock_availability_factor) * 0.15 +
        (1 - price_competitiveness_factor) * 0.15
      ) * 100, 2
    ),
    100.00
  ) AS recovery_priority_score,

  in_stock,
  coverage_compliant,

  CURRENT_TIMESTAMP() AS kpi_processed_at

FROM workspace.gold.mart_revenue_leakage
WHERE potential_revenue_lost_usd > 0;
