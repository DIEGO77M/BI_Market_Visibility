-- ============================================================================
-- AUDIT: dim_product (Final Gold)
-- ============================================================================
-- Checks:
-- 1. Total products and uniqueness
-- 2. Products with valid prices
-- 3. Orphaned products (missing price_reference)
-- 4. Price coverage percentage
-- ============================================================================
SELECT
  COUNT(*) AS total_products,
  COUNT(DISTINCT product_code) AS unique_products,
  COUNT(CASE WHEN price_list IS NOT NULL THEN 1 END) AS products_with_price,
  COUNT(CASE WHEN price_status = 'ORPHANED' THEN 1 END) AS orphaned_products,
  COUNT(CASE WHEN price_status = 'ACTIVE' THEN 1 END) AS active_products,
  ROUND(100.0 * COUNT(CASE WHEN price_list IS NOT NULL THEN 1 END) / COUNT(*), 2) AS price_coverage_pct,
  CASE
    WHEN COUNT(*) = COUNT(DISTINCT product_code) THEN 'PASS: No duplicates'
    ELSE 'FAIL: Duplicates detected'
  END AS uniqueness_check,
  CASE
    WHEN ROUND(100.0 * COUNT(CASE WHEN price_list IS NOT NULL THEN 1 END) / COUNT(*), 2) >= 90 THEN 'PASS: Adequate price coverage'
    WHEN ROUND(100.0 * COUNT(CASE WHEN price_list IS NOT NULL THEN 1 END) / COUNT(*), 2) >= 70 THEN 'WARNING: Low price coverage'
    ELSE 'FAIL: Critical price coverage'
  END AS coverage_check
FROM workspace.gold.dim_product;
