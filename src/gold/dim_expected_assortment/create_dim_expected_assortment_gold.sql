-- ============================================================================
-- TABLE: gold.dim_expected_assortment
-- Purpose: Define expected product assortment per PDV based on business rules
--          (category, store_size). Critical for Revenue Leakage analysis.
-- Author: Diego Mayorga
-- ============================================================================

CREATE OR REPLACE TABLE workspace.gold.dim_expected_assortment
USING DELTA
PARTITIONED BY (store_size)
COMMENT 'Optimized expected product assortment per PDV. Critical for Revenue Leakage analysis.'
AS
WITH active_pdvs AS (
    SELECT
        pdv_code,
        store_size,
        city
    FROM workspace.gold.dim_pdv
    WHERE is_active = TRUE
      AND pdv_code IS NOT NULL
      AND store_size IS NOT NULL
),
active_products AS (
    SELECT
        product_code,
        category,
        price_status
    FROM workspace.gold.dim_product
    WHERE price_status = 'ACTIVE'
      AND product_code IS NOT NULL
      AND category IS NOT NULL
),
-- Pre-filter products según store_size strategy para evitar combinaciones innecesarias
filtered_products AS (
    SELECT *
    FROM active_products
    WHERE (category IN ('beverages','dairy') AND 'small' IN ('small','large'))
       OR (category = 'ambient_culinary' AND 'medium' IN ('medium','large'))
       OR (category IN ('beverages','dairy','ambient_culinary') AND 'large' IN ('large'))
),
assortment_matrix AS (
    SELECT
        p.pdv_code,
        p.store_size,
        p.city,
        pr.product_code,
        pr.category,
        CASE
            WHEN p.store_size = 'small' AND pr.category IN ('beverages','dairy') THEN TRUE
            WHEN p.store_size = 'medium' AND pr.category = 'ambient_culinary' THEN TRUE
            WHEN p.store_size = 'large' AND pr.category IN ('beverages','dairy','ambient_culinary') THEN TRUE
            ELSE FALSE
        END AS expected_flag
    FROM active_pdvs p
    JOIN filtered_products pr
      ON (
            (p.store_size = 'small' AND pr.category IN ('beverages','dairy'))
         OR (p.store_size = 'medium' AND pr.category = 'ambient_culinary')
         OR (p.store_size = 'large' AND pr.category IN ('beverages','dairy','ambient_culinary'))
         )
),
with_id AS (
    SELECT
        ABS(HASH(CONCAT(pdv_code, '|', product_code))) % 2147483647 AS assortment_id,
        pdv_code,
        product_code,
        expected_flag,
        'store_size_strategy' AS assortment_reason,
        -- For simulation/historical alignment: fix valid_from_date to 2021-01-01
        -- This ensures all fact_pdv_monthly_health records (2021-2022) match expected assortment
        DATE('2021-01-01') AS valid_from_date,
        TRUE AS is_current,
        CURRENT_TIMESTAMP() AS gold_processed_at,
        store_size
    FROM assortment_matrix
)
SELECT
    assortment_id,
    pdv_code,
    product_code,
    expected_flag,
    assortment_reason,
    valid_from_date,
    is_current,
    gold_processed_at,
    store_size
FROM with_id;


-- ============================================================================
-- Notes:
-- - Idempotent and reproducible: safe to rerun daily.
-- - expected_flag = TRUE: product should be present in PDV per business strategy.
-- - Allows future versioning via is_current and valid_from_date.
-- - All fields are fully documented for business and technical stakeholders.
-- ============================================================================
