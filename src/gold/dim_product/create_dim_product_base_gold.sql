-- ============================================================================
-- CREATE dim_product_base
-- ============================================================================
-- Single source of truth for product attributes
-- One row per product_code, deduplicated
-- No metrics, no price logic
-- ============================================================================
CREATE OR REPLACE TABLE workspace.gold.dim_product_base
USING DELTA
COMMENT 'Product base dimension. Single source of truth for product attributes. One row per product_code. No metrics, no price logic.'
AS
WITH deduplicated AS (
  SELECT
    product_code,
    product_name,
    brand,
    category,
    subcategory,
    ROW_NUMBER() OVER (
      PARTITION BY product_code
      ORDER BY silver_processed_at DESC
    ) AS rn
  FROM workspace.silver.dim_products
  WHERE product_code IS NOT NULL
),
validated AS (
  SELECT
    product_code,
    product_name,
    brand,
    category,
    subcategory
  FROM deduplicated
  WHERE rn = 1
    AND product_name IS NOT NULL
    AND brand IS NOT NULL
    AND category IS NOT NULL
)
SELECT
  *,
  current_timestamp() AS gold_processed_at
FROM validated;
