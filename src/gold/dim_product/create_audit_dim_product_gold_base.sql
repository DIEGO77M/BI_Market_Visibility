-- ============================================================================
-- AUDIT: dim_product_base
-- ============================================================================
-- Checks:
-- 1. Total rows vs unique product_code
-- 2. Mandatory fields populated
-- ============================================================================
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT product_code) AS distinct_products,
  CASE 
    WHEN COUNT(*) = COUNT(DISTINCT product_code) THEN 'PASS' 
    ELSE 'FAIL' 
  END AS uniqueness_check,
  COUNT(CASE WHEN product_name IS NULL THEN 1 END) AS missing_name,
  COUNT(CASE WHEN brand IS NULL THEN 1 END) AS missing_brand,
  COUNT(CASE WHEN category IS NULL THEN 1 END) AS missing_category,
  CASE 
    WHEN SUM(CASE WHEN product_name IS NULL OR brand IS NULL OR category IS NULL THEN 1 ELSE 0 END) = 0 
    THEN 'PASS' 
    ELSE 'FAIL' 
  END AS business_rule_check
FROM workspace.gold.dim_product_base;
