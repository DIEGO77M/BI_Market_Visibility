-- ============================================================================
-- CREATE dim_product (Final Gold)
-- ============================================================================
-- Enriched product dimension:
-- - Joins dim_product_base + metric_price_reference
-- - Populates price_list and other price metrics
-- - Tracks gold_processed_at
-- - Identifies orphaned products (without price data)
-- ============================================================================
CREATE OR REPLACE TABLE workspace.gold.dim_product
USING DELTA
COMMENT 'Final enriched product dimension. Ready for BI/KPIs/alerts.'
AS
SELECT
  base.product_code,
  base.product_name,
  base.brand,
  base.category,
  base.subcategory,
  metric.price_reference AS price_list,
  metric.price_observations,
  metric.price_stddev,
  metric.price_min,
  metric.price_max,
  metric.price_mean,
  CASE
    WHEN metric.price_reference IS NULL THEN 'ORPHANED'
    WHEN metric.price_reference > 0 THEN 'ACTIVE'
    ELSE 'INVALID'
  END AS price_status,
  GREATEST(
    base.gold_processed_at,
    COALESCE(metric.gold_processed_at, base.gold_processed_at)
  ) AS gold_processed_at
FROM workspace.gold.dim_product_base AS base
LEFT JOIN workspace.gold.metric_price_reference AS metric
  ON base.product_code = metric.product_code;
