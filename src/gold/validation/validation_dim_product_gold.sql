-- ============================================================================
-- VALIDATION: Compare source counts and reconcile
-- ============================================================================
-- Purpose: Ensure traceability from Silver → Gold
-- Detects: Missing products, duplicates, data loss


WITH counts AS (
  SELECT 'dim_products (Silver)' AS source,
         COUNT(DISTINCT product_code) AS distinct_count,
         COUNT(*) - COUNT(DISTINCT product_code) AS duplicates
  FROM workspace.silver.dim_products
  WHERE product_code IS NOT NULL
  UNION ALL
  SELECT 'dim_product_base (Gold)',
         COUNT(DISTINCT product_code),
         COUNT(*) - COUNT(DISTINCT product_code)
  FROM workspace.gold.dim_product_base
  UNION ALL
  SELECT 'metric_price_reference (Gold)',
         COUNT(DISTINCT product_code),
         COUNT(*) - COUNT(DISTINCT product_code)
  FROM workspace.gold.metric_price_reference
  UNION ALL
  SELECT 'dim_product (Final Gold)',
         COUNT(DISTINCT product_code),
         COUNT(*) - COUNT(DISTINCT product_code)
  FROM workspace.gold.dim_product
),
price_coverage AS (
  SELECT
    COUNT(*) AS total_products,
    COUNT(CASE WHEN price_list IS NOT NULL THEN 1 END) AS products_with_price,
    ROUND(100.0 * COUNT(CASE WHEN price_list IS NOT NULL THEN 1 END)/COUNT(*),2) AS price_coverage_pct
  FROM workspace.gold.dim_product
)
SELECT *
FROM counts
UNION ALL
SELECT 'Price Coverage', products_with_price, total_products - products_with_price
FROM price_coverage;
