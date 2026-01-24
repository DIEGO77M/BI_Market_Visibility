-- ============================================================================
-- CREATE metric_price_reference
-- ============================================================================
-- Analytical metric for price reference
-- - Excludes promotional pricing
-- - Median price (robust to outliers)
-- - Price observations, standard deviation, min, max, mean
-- ============================================================================
CREATE OR REPLACE TABLE workspace.gold.metric_price_reference
USING DELTA
COMMENT 'Price reference metric. Median price per product excluding promotions. Aggregated across all PDVs.'
AS
WITH price_non_promo AS (
  SELECT
    product_code,
    price
  FROM workspace.silver.fact_price_audit
  WHERE product_code IS NOT NULL
    AND price IS NOT NULL
    AND price > 0
    AND has_promotion = FALSE
),
price_stats AS (
  SELECT
    product_code,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS price_reference_calc,
    COUNT(*) AS price_observations,
    STDDEV_POP(price) AS price_stddev_calc,
    MIN(price) AS price_min,
    MAX(price) AS price_max,
    ROUND(AVG(price), 2) AS price_mean
  FROM price_non_promo
  GROUP BY product_code
  HAVING COUNT(*) >= 2
)
SELECT
  product_code, -- Product identifier (PK)
  ROUND(price_reference_calc, 2) AS price_reference, -- Median price (reference, robust to outliers)
  price_observations, -- Number of valid non-promotional price records used
  ROUND(COALESCE(price_stddev_calc,0),2) AS price_stddev, -- Price standard deviation (volatility)
  ROUND(price_min,2) AS price_min, -- Minimum observed price
  ROUND(price_max,2) AS price_max, -- Maximum observed price
  price_mean, -- Mean price (average)
  current_timestamp() AS gold_processed_at -- Gold table processing timestamp
FROM price_stats
WHERE price_reference_calc IS NOT NULL;
