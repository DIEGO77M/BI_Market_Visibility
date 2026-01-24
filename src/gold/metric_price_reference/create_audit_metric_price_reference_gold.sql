-- ============================================================================
-- AUDIT: metric_price_reference
-- ============================================================================
-- Checks:
-- 1. Total products with price metrics
-- 2. Products with non-zero variance
-- 3. Missing price references
-- ============================================================================
SELECT
  COUNT(*) AS total_metrics,
  COUNT(DISTINCT product_code) AS unique_products,
  SUM(price_observations) AS total_observations,
  COUNT(CASE WHEN price_stddev > 0 THEN 1 END) AS products_with_variance,
  COUNT(CASE WHEN price_reference IS NULL THEN 1 END) AS products_missing_reference,
  CASE 
    WHEN COUNT(CASE WHEN price_reference IS NULL THEN 1 END) = 0 
    THEN 'PASS' 
    ELSE 'FAIL' 
  END AS validation_check
FROM workspace.gold.metric_price_reference;
