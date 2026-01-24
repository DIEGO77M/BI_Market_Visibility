-- ============================================================================
-- GOLD LAYER AUDIT: dim_expected_assortment
-- Combined logs version
-- ============================================================================
SELECT 'audit_structure' AS step_name,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
       CONCAT(
         'Total rows: ', COUNT(*),
         ', Unique PDVs: ', COUNT(DISTINCT pdv_code),
         ', Unique products: ', COUNT(DISTINCT product_code),
         ', Expected count: ', COUNT(CASE WHEN expected_flag = TRUE THEN 1 END),
         ', Not expected count: ', COUNT(CASE WHEN expected_flag = FALSE THEN 1 END),
         ', Unique assortment_ids: ', COUNT(DISTINCT assortment_id),
         ', ID uniqueness: ', CASE WHEN COUNT(*) = COUNT(DISTINCT assortment_id) THEN 'PASS' ELSE 'FAIL' END
       ) AS message
FROM workspace.gold.dim_expected_assortment

UNION ALL

SELECT 'audit_store_size_distribution' AS step_name,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
       CONCAT(
         'Store size: ', store_size,
         ', PDVs: ', COUNT(DISTINCT pdv_code),
         ', Products: ', COUNT(DISTINCT product_code),
         ', Expected products: ', COUNT(CASE WHEN expected_flag = TRUE THEN 1 END),
         ', Expected_flag %: ', ROUND(100.0 * COUNT(CASE WHEN expected_flag = TRUE THEN 1 END) / COUNT(*), 2)
       ) AS message
FROM workspace.gold.dim_expected_assortment
GROUP BY store_size

UNION ALL

SELECT 'audit_pdvs_zero_expected' AS step_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
       CONCAT(
         'PDVs with zero expected products: ', COUNT(*),
         ', PDVs with expected products: ', COUNT(CASE WHEN expected_products > 0 THEN 1 END)
       ) AS message
FROM (
    SELECT pdv_code,
           COUNT(CASE WHEN expected_flag = TRUE THEN 1 END) AS expected_products
    FROM workspace.gold.dim_expected_assortment
    GROUP BY pdv_code
) AS pdv_product_count
WHERE expected_products = 0

UNION ALL

SELECT 'audit_reconciliation_dim_pdv' AS step_name,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
       CONCAT('Active PDVs: ', COUNT(*)) AS message
FROM workspace.gold.dim_pdv
WHERE is_active = TRUE

UNION ALL

SELECT 'audit_reconciliation_dim_product' AS step_name,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
       CONCAT('Active products: ', COUNT(*)) AS message
FROM workspace.gold.dim_product
WHERE price_status = 'ACTIVE'

UNION ALL

SELECT 'audit_reconciliation_dim_expected_assortment_total' AS step_name,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
       CONCAT('Total PDVs in dim_expected_assortment: ', COUNT(DISTINCT pdv_code)) AS message
FROM workspace.gold.dim_expected_assortment

UNION ALL

SELECT 'audit_reconciliation_dim_expected_assortment_expected_true' AS step_name,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
       CONCAT('PDVs with expected_flag = TRUE: ', COUNT(DISTINCT pdv_code)) AS message
FROM workspace.gold.dim_expected_assortment;
