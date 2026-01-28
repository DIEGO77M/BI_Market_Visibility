-- ============================================================================
-- VALIDATION: Gold Layer - mart_revenue_leakage
-- ============================================================================
-- Purpose: Generate validation results as Delta table for external consumption
-- Execution: spark.sql() from orchestration notebook
-- Output: workspace.gold.validation_mart_revenue_leakage
-- Author: Diego Mayorga
-- Environment: Databricks Serverless SQL + Unity Catalog
-- ============================================================================

-- =========================
-- CREATE VALIDATION TABLE
-- =========================
CREATE TABLE IF NOT EXISTS workspace.gold.validation_mart_revenue_leakage (
    validation_id STRING COMMENT 'Unique validation execution ID',
    validation_name STRING COMMENT 'Validation test name',
    validation_category STRING COMMENT 'Category: structural, referential, business_logic, data_quality, summary',
    status STRING COMMENT 'PASS, FAIL, WARNING, INFO',
    severity STRING COMMENT 'CRITICAL, HIGH, MEDIUM, INFO',
    total_rows BIGINT COMMENT 'Total rows evaluated',
    failed_rows BIGINT COMMENT 'Number of rows that failed validation',
    failed_percentage DECIMAL(5,2) COMMENT 'Percentage of failed rows',
    metric_value STRING COMMENT 'Actual metric value',
    threshold_value STRING COMMENT 'Expected threshold',
    details STRING COMMENT 'Human-readable validation details',
    recommended_action STRING COMMENT 'Suggested remediation action',
    execution_timestamp TIMESTAMP COMMENT 'When validation was executed',
    audit_date_id DATE COMMENT 'Month being validated (if applicable)'
)
USING DELTA
PARTITIONED BY (status)
COMMENT 'Validation results for automated monitoring and alerting';

-- ============================================================================
-- LOAD VALIDATION RESULTS
-- ============================================================================
INSERT OVERWRITE workspace.gold.validation_mart_revenue_leakage
SELECT 
    validation_id,
    validation_name,
    validation_category,
    status,
    severity,
    total_rows,
    failed_rows,
    failed_percentage,
    metric_value,
    threshold_value,
    details,
    recommended_action,
    execution_timestamp,
    audit_date_id
FROM (
    -- =========================
    -- 1. ROW COUNT CHECK
    -- =========================
    SELECT 
        uuid() AS validation_id,
        'row_count_check' AS validation_name,
        'structural' AS validation_category,
        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        CASE WHEN COUNT(*) > 0 THEN 'INFO' ELSE 'CRITICAL' END AS severity,
        COUNT(*) AS total_rows,
        CASE WHEN COUNT(*) = 0 THEN COUNT(*) ELSE CAST(0 AS BIGINT) END AS failed_rows,
        CASE WHEN COUNT(*) = 0 THEN 100.0 ELSE 0.0 END AS failed_percentage,
        CAST(COUNT(*) AS STRING) AS metric_value,
        '> 0' AS threshold_value,
        CONCAT('Total records loaded: ', CAST(COUNT(*) AS STRING)) AS details,
        CASE WHEN COUNT(*) = 0 THEN 'Check DML load and source data' ELSE 'No action required' END AS recommended_action,
        CURRENT_TIMESTAMP() AS execution_timestamp,
        CAST(NULL AS DATE) AS audit_date_id
    FROM workspace.gold.mart_revenue_leakage

    UNION ALL

    -- =========================
    -- 2. PRIMARY KEY UNIQUENESS
    -- =========================
    SELECT 
        uuid() AS validation_id,
        'primary_key_uniqueness' AS validation_name,
        'structural' AS validation_category,
        CASE WHEN SUM(duplicate_flag) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        CASE WHEN SUM(duplicate_flag) = 0 THEN 'INFO' ELSE 'CRITICAL' END AS severity,
        COUNT(*) AS total_rows,
        SUM(duplicate_flag) AS failed_rows,
        ROUND(SUM(duplicate_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS failed_percentage,
        CAST(SUM(duplicate_flag) AS STRING) AS metric_value,
        '0' AS threshold_value,
        CONCAT('Duplicate PK records: ', CAST(SUM(duplicate_flag) AS STRING)) AS details,
        CASE WHEN SUM(duplicate_flag) > 0 THEN 'Review INSERT logic - use INSERT OVERWRITE' ELSE 'No action required' END AS recommended_action,
        CURRENT_TIMESTAMP() AS execution_timestamp,
        CAST(NULL AS DATE) AS audit_date_id
    FROM (
        SELECT 
            CASE WHEN COUNT(*) OVER (PARTITION BY audit_date_id, pdv_code, product_code) > 1 THEN 1 ELSE 0 END AS duplicate_flag
        FROM workspace.gold.mart_revenue_leakage
    )

    UNION ALL

    -- =========================
    -- 3. NULL BUSINESS KEYS
    -- =========================
    SELECT 
        uuid() AS validation_id,
        'null_business_keys' AS validation_name,
        'structural' AS validation_category,
        CASE WHEN SUM(null_flag) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        CASE WHEN SUM(null_flag) = 0 THEN 'INFO' ELSE 'CRITICAL' END AS severity,
        COUNT(*) AS total_rows,
        SUM(null_flag) AS failed_rows,
        ROUND(SUM(null_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS failed_percentage,
        CAST(SUM(null_flag) AS STRING) AS metric_value,
        '0' AS threshold_value,
        CONCAT('Records with NULL business keys: ', CAST(SUM(null_flag) AS STRING)) AS details,
        CASE WHEN SUM(null_flag) > 0 THEN 'Check INNER JOIN in base_data CTE' ELSE 'No action required' END AS recommended_action,
        CURRENT_TIMESTAMP() AS execution_timestamp,
        CAST(NULL AS DATE) AS audit_date_id
    FROM (
        SELECT 
            CASE WHEN audit_date_id IS NULL OR pdv_code IS NULL OR product_code IS NULL THEN 1 ELSE 0 END AS null_flag
        FROM workspace.gold.mart_revenue_leakage
    )

    UNION ALL

    -- =========================
    -- 4. FACTOR RANGE VALIDATION
    -- =========================
    SELECT 
        uuid() AS validation_id,
        'factor_range_validation' AS validation_name,
        'data_quality' AS validation_category,
        CASE WHEN SUM(invalid_flag) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        CASE WHEN SUM(invalid_flag) = 0 THEN 'INFO' ELSE 'HIGH' END AS severity,
        COUNT(*) AS total_rows,
        SUM(invalid_flag) AS failed_rows,
        ROUND(SUM(invalid_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS failed_percentage,
        CAST(SUM(invalid_flag) AS STRING) AS metric_value,
        '0.0 - 1.0' AS threshold_value,
        CONCAT('Factors out of valid range [0.0-1.0]: ', CAST(SUM(invalid_flag) AS STRING)) AS details,
        CASE WHEN SUM(invalid_flag) > 0 THEN 'Review CASE logic for factors in factors CTE' ELSE 'No action required' END AS recommended_action,
        CURRENT_TIMESTAMP() AS execution_timestamp,
        CAST(NULL AS DATE) AS audit_date_id
    FROM (
        SELECT 
            CASE WHEN stock_availability_factor NOT BETWEEN 0 AND 1 
                 OR price_competitiveness_factor NOT BETWEEN 0 AND 1 
                 OR execution_visibility_factor NOT BETWEEN 0 AND 1 
                 OR assortment_alignment_factor NOT BETWEEN 0 AND 1 
            THEN 1 ELSE 0 END AS invalid_flag
        FROM workspace.gold.mart_revenue_leakage
    )

    UNION ALL

    -- =========================
    -- 5. REVENUE LEAKAGE FORMULA VALIDATION
    -- =========================
    SELECT 
        uuid() AS validation_id,
        'revenue_leakage_formula_validation' AS validation_name,
        'business_logic' AS validation_category,
        CASE WHEN SUM(mismatch_flag) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        CASE WHEN SUM(mismatch_flag) = 0 THEN 'INFO' ELSE 'CRITICAL' END AS severity,
        COUNT(*) AS total_rows,
        SUM(mismatch_flag) AS failed_rows,
        ROUND(SUM(mismatch_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS failed_percentage,
        CAST(SUM(mismatch_flag) AS STRING) AS metric_value,
        '0' AS threshold_value,
        CONCAT('Revenue formula mismatch: ', CAST(SUM(mismatch_flag) AS STRING)) AS details,
        CASE WHEN SUM(mismatch_flag) > 0 THEN 'Review formula: (1-stock)*0.50 + (1-price)*0.35 + (1-exec)*0.15' ELSE 'No action required' END AS recommended_action,
        CURRENT_TIMESTAMP() AS execution_timestamp,
        CAST(NULL AS DATE) AS audit_date_id
    FROM (
        SELECT 
            CASE WHEN ABS(
                revenue_leakage_pct - 
                ROUND(
                    (1 - stock_availability_factor) * 0.50 +
                    (1 - price_competitiveness_factor) * 0.35 +
                    (1 - execution_visibility_factor) * 0.15, 4)
            ) > 0.01 THEN 1 ELSE 0 END AS mismatch_flag
        FROM workspace.gold.mart_revenue_leakage
    )

    UNION ALL

    -- =========================
    -- 6. ASSORTMENT REVENUE CONSISTENCY
    -- =========================
    SELECT 
        uuid() AS validation_id,
        'assortment_revenue_consistency' AS validation_name,
        'business_logic' AS validation_category,
        CASE WHEN SUM(inconsistent_flag) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        CASE WHEN SUM(inconsistent_flag) = 0 THEN 'INFO' ELSE 'CRITICAL' END AS severity,
        COUNT(*) AS total_rows,
        SUM(inconsistent_flag) AS failed_rows,
        ROUND(SUM(inconsistent_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS failed_percentage,
        CAST(SUM(inconsistent_flag) AS STRING) AS metric_value,
        '0' AS threshold_value,
        CONCAT('Assortment inconsistency: ', CAST(SUM(inconsistent_flag) AS STRING)) AS details,
        CASE WHEN SUM(inconsistent_flag) > 0 THEN 'Review CASE WHEN assortment_alignment_factor = 0 logic' ELSE 'No action required' END AS recommended_action,
        CURRENT_TIMESTAMP() AS execution_timestamp,
        CAST(NULL AS DATE) AS audit_date_id
    FROM (
        SELECT 
            CASE WHEN assortment_alignment_factor = 0.0 AND potential_revenue_lost_usd <> 0 
            THEN 1 ELSE 0 END AS inconsistent_flag
        FROM workspace.gold.mart_revenue_leakage
    )

    UNION ALL

    -- =========================
    -- 7. NULL CRITICAL FIELDS
    -- =========================
    SELECT 
        uuid() AS validation_id,
        'null_critical_fields' AS validation_name,
        'data_quality' AS validation_category,
        CASE WHEN SUM(null_flag) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        CASE WHEN SUM(null_flag) = 0 THEN 'INFO' ELSE 'CRITICAL' END AS severity,
        COUNT(*) AS total_rows,
        SUM(null_flag) AS failed_rows,
        ROUND(SUM(null_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS failed_percentage,
        CAST(SUM(null_flag) AS STRING) AS metric_value,
        '0' AS threshold_value,
        CONCAT('NULL critical fields: ', CAST(SUM(null_flag) AS STRING)) AS details,
        CASE WHEN SUM(null_flag) > 0 THEN 'Review LEFT JOIN and add COALESCE defaults' ELSE 'No action required' END AS recommended_action,
        CURRENT_TIMESTAMP() AS execution_timestamp,
        CAST(NULL AS DATE) AS audit_date_id
    FROM (
        SELECT 
            CASE WHEN stock_availability_factor IS NULL 
                 OR price_competitiveness_factor IS NULL 
                 OR execution_visibility_factor IS NULL 
                 OR assortment_alignment_factor IS NULL 
                 OR revenue_leakage_pct IS NULL 
                 OR potential_revenue_lost_usd IS NULL 
            THEN 1 ELSE 0 END AS null_flag
        FROM workspace.gold.mart_revenue_leakage
    )

    UNION ALL

    -- =========================
    -- 8. REFERENTIAL INTEGRITY - DIMENSIONS
    -- =========================
    SELECT 
        uuid() AS validation_id,
        'referential_integrity_dimensions' AS validation_name,
        'referential' AS validation_category,
        CASE 
            WHEN SUM(orphan_flag) = 0 THEN 'PASS'
            WHEN SUM(orphan_flag) * 100.0 / NULLIF(COUNT(*), 0) < 5 THEN 'WARNING'
            ELSE 'FAIL'
        END AS status,
        CASE 
            WHEN SUM(orphan_flag) = 0 THEN 'INFO'
            WHEN SUM(orphan_flag) * 100.0 / NULLIF(COUNT(*), 0) < 5 THEN 'MEDIUM'
            ELSE 'HIGH'
        END AS severity,
        COUNT(*) AS total_rows,
        SUM(orphan_flag) AS failed_rows,
        ROUND(SUM(orphan_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS failed_percentage,
        CAST(SUM(orphan_flag) AS STRING) AS metric_value,
        '< 5%' AS threshold_value,
        CONCAT('Orphan keys in dimensions: ', CAST(SUM(orphan_flag) AS STRING)) AS details,
        CASE 
            WHEN SUM(orphan_flag) * 100.0 / NULLIF(COUNT(*), 0) >= 5 THEN 'Review and refresh dimension tables'
            WHEN SUM(orphan_flag) > 0 THEN 'Monitor orphan keys - acceptable under 5%'
            ELSE 'No action required'
        END AS recommended_action,
        CURRENT_TIMESTAMP() AS execution_timestamp,
        CAST(NULL AS DATE) AS audit_date_id
    FROM (
        SELECT 
            CASE WHEN d.date_id IS NULL OR dp.pdv_code IS NULL OR p.product_code IS NULL 
            THEN 1 ELSE 0 END AS orphan_flag
        FROM workspace.gold.mart_revenue_leakage f
        LEFT JOIN workspace.gold.dim_date d ON f.audit_date_id = d.date_id
        LEFT JOIN workspace.gold.dim_pdv dp ON f.pdv_code = dp.pdv_code
        LEFT JOIN workspace.gold.dim_product p ON f.product_code = p.product_code
    )

    UNION ALL

    -- =========================
    -- 9. BATCH SUMMARY
    -- =========================
    SELECT 
        uuid() AS validation_id,
        'batch_summary' AS validation_name,
        'summary' AS validation_category,
        'INFO' AS status,
        'INFO' AS severity,
        COUNT(*) AS total_rows,
        CAST(0 AS BIGINT) AS failed_rows,
        0.0 AS failed_percentage,
        CONCAT('Dates: ', CAST(COUNT(DISTINCT audit_date_id) AS STRING),
               ' | PDVs: ', CAST(COUNT(DISTINCT pdv_code) AS STRING),
               ' | Products: ', CAST(COUNT(DISTINCT product_code) AS STRING)) AS metric_value,
        'N/A' AS threshold_value,
        CONCAT('Records: ', CAST(COUNT(*) AS STRING),
               ', Avg leakage: ', CAST(ROUND(AVG(revenue_leakage_pct), 4) AS STRING),
               ', Total loss: $', CAST(ROUND(SUM(potential_revenue_lost_usd), 2) AS STRING)) AS details,
        'No action required' AS recommended_action,
        CURRENT_TIMESTAMP() AS execution_timestamp,
        CAST(NULL AS DATE) AS audit_date_id
    FROM workspace.gold.mart_revenue_leakage
);