-- ============================================================================
-- VALIDATION: Gold Layer - fact_pdv_monthly_health
-- ============================================================================
-- Purpose: Generate validation results as Delta table for external consumption
-- Pattern: Single table with structured validation metrics
-- Execution: spark.sql() from orchestration notebook
-- Output: workspace.gold.validation_pdv_monthly_health
-- ============================================================================

-- =========================
-- CREATE VALIDATION TABLE
-- =========================
CREATE TABLE IF NOT EXISTS workspace.gold.validation_pdv_monthly_health (
    validation_id STRING COMMENT 'Unique validation execution ID',
    validation_name STRING COMMENT 'Validation test name',
    validation_category STRING COMMENT 'Category: structural, referential, business_logic, data_quality, summary',
    status STRING COMMENT 'PASS, FAIL, WARNING, INFO',
    severity STRING COMMENT 'CRITICAL, HIGH, MEDIUM, LOW, INFO',
    total_rows BIGINT COMMENT 'Total rows evaluated',
    failed_rows BIGINT COMMENT 'Number of rows that failed validation',
    failed_percentage DECIMAL(5,2) COMMENT 'Percentage of failed rows',
    metric_value STRING COMMENT 'Actual metric value',
    threshold_value STRING COMMENT 'Expected threshold',
    details STRING COMMENT 'Human-readable validation details',
    recommended_action STRING COMMENT 'Suggested remediation action',
    execution_timestamp TIMESTAMP COMMENT 'When validation was executed',
    year_month_id INT COMMENT 'Month being validated (if applicable)',
    silver_batch_id STRING COMMENT 'Source batch ID'
)
USING DELTA
PARTITIONED BY (status)
COMMENT 'Validation results for automated monitoring and alerting';

-- ============================================================================
-- LOAD VALIDATION RESULTS
-- ============================================================================
INSERT OVERWRITE workspace.gold.validation_pdv_monthly_health

-- =========================
-- CTE: Validation metrics
-- =========================
WITH validation_metrics AS (
    SELECT
        -- Row count
        COUNT(*) AS total_records,
        
        -- Primary key uniqueness
        COUNT(*) - COUNT(DISTINCT year_month_id, pdv_code, product_code) AS duplicate_keys,
        
        -- NULL business keys
        SUM(CASE WHEN year_month_id IS NULL OR pdv_code IS NULL OR product_code IS NULL THEN 1 ELSE 0 END) AS null_keys,
        
        -- Stock logic
        SUM(CASE 
            WHEN (closing_stock_units > 0 AND in_stock = FALSE) 
              OR (COALESCE(closing_stock_units, 0) <= 0 AND in_stock = TRUE)
            THEN 1 ELSE 0 
        END) AS stock_logic_errors,
        
        -- Coverage compliance logic
        SUM(CASE 
            WHEN (in_stock = TRUE AND in_expected_assortment = TRUE AND coverage_compliant = FALSE)
              OR (NOT (in_stock = TRUE AND in_expected_assortment = TRUE) AND coverage_compliant = TRUE)
            THEN 1 ELSE 0 
        END) AS compliance_logic_errors,
        
        -- Data confidence score
        SUM(CASE WHEN data_confidence_score < 0.0 OR data_confidence_score > 1.0 THEN 1 ELSE 0 END) AS score_out_of_range,
        SUM(CASE WHEN data_confidence_score = 0.2 THEN 1 ELSE 0 END) AS orphan_pdv_count,
        SUM(CASE WHEN data_confidence_score < 0.5 THEN 1 ELSE 0 END) AS low_quality_count,
        ROUND(AVG(data_confidence_score), 2) AS avg_confidence_score,
        
        -- Potential stockout days
        SUM(CASE WHEN COALESCE(closing_stock_units, 0) <= 0 AND potential_stockout_days IS NOT NULL THEN 1 ELSE 0 END) AS invalid_stockout_projection,
        
        -- Audit fields
        SUM(CASE WHEN silver_batch_id IS NULL OR gold_processed_at IS NULL THEN 1 ELSE 0 END) AS missing_audit_fields,
        
        -- Batch info
        MAX(silver_batch_id) AS current_batch_id,
        COUNT(DISTINCT year_month_id) AS months_processed,
        COUNT(DISTINCT pdv_code) AS pdvs_processed,
        COUNT(DISTINCT product_code) AS products_processed
        
    FROM workspace.gold.fact_pdv_monthly_health
),

-- =========================
-- CTE: Referential integrity
-- =========================
ref_integrity AS (
    SELECT
        -- dim_date
        COUNT(DISTINCT f.year_month_id) - COUNT(DISTINCT d.year_month) AS orphan_dates,
        
        -- dim_pdv
        SUM(CASE WHEN dp.pdv_code IS NULL THEN 1 ELSE 0 END) AS orphan_pdvs,
        
        -- dim_product
        SUM(CASE WHEN p.product_code IS NULL THEN 1 ELSE 0 END) AS orphan_products,
        
        -- Totals
        COUNT(*) AS total_for_ref_check
        
    FROM workspace.gold.fact_pdv_monthly_health f
    LEFT JOIN workspace.gold.dim_date d ON f.year_month_id = d.year_month
    LEFT JOIN workspace.gold.dim_pdv dp ON f.pdv_code = dp.pdv_code
    LEFT JOIN workspace.gold.dim_product p ON f.product_code = p.product_code
)

-- =========================
-- GENERATE VALIDATION ROWS
-- =========================
SELECT * FROM (

-- 1. ROW COUNT CHECK
SELECT 
    uuid() AS validation_id,
    'row_count_check' AS validation_name,
    'structural' AS validation_category,
    CASE WHEN vm.total_records > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    CASE WHEN vm.total_records > 0 THEN 'INFO' ELSE 'CRITICAL' END AS severity,
    vm.total_records AS total_rows,
    CASE WHEN vm.total_records = 0 THEN vm.total_records ELSE 0 END AS failed_rows,
    CASE WHEN vm.total_records = 0 THEN 100.0 ELSE 0.0 END AS failed_percentage,
    CAST(vm.total_records AS STRING) AS metric_value,
    '> 0' AS threshold_value,
    CONCAT('Total records loaded: ', CAST(vm.total_records AS STRING)) AS details,
    CASE WHEN vm.total_records = 0 THEN 'Check source data in silver.fact_sell_in' ELSE 'No action required' END AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    vm.current_batch_id AS silver_batch_id
FROM validation_metrics vm

UNION ALL

-- 2. PRIMARY KEY UNIQUENESS
SELECT 
    uuid() AS validation_id,
    'primary_key_uniqueness' AS validation_name,
    'structural' AS validation_category,
    CASE WHEN vm.duplicate_keys = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    CASE WHEN vm.duplicate_keys = 0 THEN 'INFO' ELSE 'CRITICAL' END AS severity,
    vm.total_records AS total_rows,
    vm.duplicate_keys AS failed_rows,
    ROUND(vm.duplicate_keys * 100.0 / NULLIF(vm.total_records, 0), 2) AS failed_percentage,
    CAST(vm.duplicate_keys AS STRING) AS metric_value,
    '0' AS threshold_value,
    CONCAT('Duplicate records found: ', CAST(vm.duplicate_keys AS STRING)) AS details,
    CASE WHEN vm.duplicate_keys > 0 THEN 'Review MERGE logic in DML - check ON clause keys' ELSE 'No action required' END AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    vm.current_batch_id AS silver_batch_id
FROM validation_metrics vm

UNION ALL

-- 3. NULL BUSINESS KEYS
SELECT 
    uuid() AS validation_id,
    'null_business_keys' AS validation_name,
    'structural' AS validation_category,
    CASE WHEN vm.null_keys = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    CASE WHEN vm.null_keys = 0 THEN 'INFO' ELSE 'CRITICAL' END AS severity,
    vm.total_records AS total_rows,
    vm.null_keys AS failed_rows,
    ROUND(vm.null_keys * 100.0 / NULLIF(vm.total_records, 0), 2) AS failed_percentage,
    CAST(vm.null_keys AS STRING) AS metric_value,
    '0' AS threshold_value,
    CONCAT('Records with NULL business keys: ', CAST(vm.null_keys AS STRING)) AS details,
    CASE WHEN vm.null_keys > 0 THEN 'Check JOIN conditions with dim_date in DML' ELSE 'No action required' END AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    vm.current_batch_id AS silver_batch_id
FROM validation_metrics vm

UNION ALL

-- 4. REFERENTIAL INTEGRITY - dim_date
SELECT 
    uuid() AS validation_id,
    'referential_integrity_dim_date' AS validation_name,
    'referential' AS validation_category,
    CASE WHEN ri.orphan_dates = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    CASE WHEN ri.orphan_dates = 0 THEN 'INFO' ELSE 'HIGH' END AS severity,
    ri.total_for_ref_check AS total_rows,
    ri.orphan_dates AS failed_rows,
    ROUND(ri.orphan_dates * 100.0 / NULLIF(ri.total_for_ref_check, 0), 2) AS failed_percentage,
    CAST(ri.orphan_dates AS STRING) AS metric_value,
    '0' AS threshold_value,
    CONCAT('Year-month values not found in dim_date: ', CAST(ri.orphan_dates AS STRING)) AS details,
    CASE WHEN ri.orphan_dates > 0 THEN 'Refresh dim_date or check year/month values in fact_sell_in' ELSE 'No action required' END AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    (SELECT current_batch_id FROM validation_metrics) AS silver_batch_id
FROM ref_integrity ri

UNION ALL

-- 5. REFERENTIAL INTEGRITY - dim_pdv
SELECT 
    uuid() AS validation_id,
    'referential_integrity_dim_pdv' AS validation_name,
    'referential' AS validation_category,
    CASE 
        WHEN ri.orphan_pdvs = 0 THEN 'PASS'
        WHEN ri.orphan_pdvs * 100.0 / NULLIF(ri.total_for_ref_check, 0) < 5 THEN 'WARNING'
        ELSE 'FAIL' 
    END AS status,
    CASE 
        WHEN ri.orphan_pdvs = 0 THEN 'INFO'
        WHEN ri.orphan_pdvs * 100.0 / NULLIF(ri.total_for_ref_check, 0) < 5 THEN 'MEDIUM'
        ELSE 'HIGH' 
    END AS severity,
    ri.total_for_ref_check AS total_rows,
    ri.orphan_pdvs AS failed_rows,
    ROUND(ri.orphan_pdvs * 100.0 / NULLIF(ri.total_for_ref_check, 0), 2) AS failed_percentage,
    CAST(ri.orphan_pdvs AS STRING) AS metric_value,
    '< 5%' AS threshold_value,
    CONCAT('PDVs not found in dim_pdv: ', CAST(ri.orphan_pdvs AS STRING), 
           ' (', CAST(ROUND(ri.orphan_pdvs * 100.0 / NULLIF(ri.total_for_ref_check, 0), 2) AS STRING), '%)') AS details,
    CASE 
        WHEN ri.orphan_pdvs * 100.0 / NULLIF(ri.total_for_ref_check, 0) >= 5 THEN 'Review and add missing PDVs to dim_pdv'
        WHEN ri.orphan_pdvs > 0 THEN 'Monitor orphan PDVs - acceptable under 5%'
        ELSE 'No action required' 
    END AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    (SELECT current_batch_id FROM validation_metrics) AS silver_batch_id
FROM ref_integrity ri

UNION ALL

-- 6. REFERENTIAL INTEGRITY - dim_product
SELECT 
    uuid() AS validation_id,
    'referential_integrity_dim_product' AS validation_name,
    'referential' AS validation_category,
    CASE WHEN ri.orphan_products = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    CASE WHEN ri.orphan_products = 0 THEN 'INFO' ELSE 'HIGH' END AS severity,
    ri.total_for_ref_check AS total_rows,
    ri.orphan_products AS failed_rows,
    ROUND(ri.orphan_products * 100.0 / NULLIF(ri.total_for_ref_check, 0), 2) AS failed_percentage,
    CAST(ri.orphan_products AS STRING) AS metric_value,
    '0' AS threshold_value,
    CONCAT('Products not found in dim_product: ', CAST(ri.orphan_products AS STRING)) AS details,
    CASE WHEN ri.orphan_products > 0 THEN 'Review and add missing products to dim_product' ELSE 'No action required' END AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    (SELECT current_batch_id FROM validation_metrics) AS silver_batch_id
FROM ref_integrity ri

UNION ALL

-- 7. STOCK LOGIC VALIDATION
SELECT 
    uuid() AS validation_id,
    'stock_logic_validation' AS validation_name,
    'business_logic' AS validation_category,
    CASE WHEN vm.stock_logic_errors = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    CASE WHEN vm.stock_logic_errors = 0 THEN 'INFO' ELSE 'CRITICAL' END AS severity,
    vm.total_records AS total_rows,
    vm.stock_logic_errors AS failed_rows,
    ROUND(vm.stock_logic_errors * 100.0 / NULLIF(vm.total_records, 0), 2) AS failed_percentage,
    CAST(vm.stock_logic_errors AS STRING) AS metric_value,
    '0' AS threshold_value,
    CONCAT('Records with inconsistent in_stock vs closing_stock_units: ', CAST(vm.stock_logic_errors AS STRING)) AS details,
    CASE WHEN vm.stock_logic_errors > 0 THEN 'Review in_stock CASE logic in DML' ELSE 'No action required' END AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    vm.current_batch_id AS silver_batch_id
FROM validation_metrics vm

UNION ALL

-- 8. COVERAGE COMPLIANCE LOGIC
SELECT 
    uuid() AS validation_id,
    'coverage_compliance_logic' AS validation_name,
    'business_logic' AS validation_category,
    CASE WHEN vm.compliance_logic_errors = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    CASE WHEN vm.compliance_logic_errors = 0 THEN 'INFO' ELSE 'CRITICAL' END AS severity,
    vm.total_records AS total_rows,
    vm.compliance_logic_errors AS failed_rows,
    ROUND(vm.compliance_logic_errors * 100.0 / NULLIF(vm.total_records, 0), 2) AS failed_percentage,
    CAST(vm.compliance_logic_errors AS STRING) AS metric_value,
    '0' AS threshold_value,
    CONCAT('Records with inconsistent coverage_compliant logic: ', CAST(vm.compliance_logic_errors AS STRING)) AS details,
    CASE WHEN vm.compliance_logic_errors > 0 THEN 'Review coverage_compliant CASE logic in DML' ELSE 'No action required' END AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    vm.current_batch_id AS silver_batch_id
FROM validation_metrics vm

UNION ALL

-- 9. DATA CONFIDENCE SCORE RANGE
SELECT 
    uuid() AS validation_id,
    'data_confidence_score_range' AS validation_name,
    'data_quality' AS validation_category,
    CASE WHEN vm.score_out_of_range = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    CASE WHEN vm.score_out_of_range = 0 THEN 'INFO' ELSE 'HIGH' END AS severity,
    vm.total_records AS total_rows,
    vm.score_out_of_range AS failed_rows,
    ROUND(vm.score_out_of_range * 100.0 / NULLIF(vm.total_records, 0), 2) AS failed_percentage,
    CAST(vm.score_out_of_range AS STRING) AS metric_value,
    '0.0 - 1.0' AS threshold_value,
    CONCAT('Scores out of valid range [0.0-1.0]: ', CAST(vm.score_out_of_range AS STRING),
           ' | Avg score: ', CAST(vm.avg_confidence_score AS STRING)) AS details,
    CASE WHEN vm.score_out_of_range > 0 THEN 'Review data_confidence_score CAST in DML' ELSE 'No action required' END AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    vm.current_batch_id AS silver_batch_id
FROM validation_metrics vm

UNION ALL

-- 10. ORPHAN PDV DETECTION
SELECT 
    uuid() AS validation_id,
    'orphan_pdv_detection' AS validation_name,
    'data_quality' AS validation_category,
    CASE 
        WHEN vm.orphan_pdv_count = 0 THEN 'PASS'
        WHEN vm.orphan_pdv_count * 100.0 / NULLIF(vm.total_records, 0) < 5 THEN 'WARNING'
        ELSE 'FAIL' 
    END AS status,
    CASE 
        WHEN vm.orphan_pdv_count = 0 THEN 'INFO'
        WHEN vm.orphan_pdv_count * 100.0 / NULLIF(vm.total_records, 0) < 5 THEN 'MEDIUM'
        ELSE 'HIGH' 
    END AS severity,
    vm.total_records AS total_rows,
    vm.orphan_pdv_count AS failed_rows,
    ROUND(vm.orphan_pdv_count * 100.0 / NULLIF(vm.total_records, 0), 2) AS failed_percentage,
    CAST(vm.orphan_pdv_count AS STRING) AS metric_value,
    '< 5%' AS threshold_value,
    CONCAT('Orphan PDVs detected (score=0.2): ', CAST(vm.orphan_pdv_count AS STRING),
           ' (', CAST(ROUND(vm.orphan_pdv_count * 100.0 / NULLIF(vm.total_records, 0), 2) AS STRING), '%)') AS details,
    CASE 
        WHEN vm.orphan_pdv_count * 100.0 / NULLIF(vm.total_records, 0) >= 5 THEN 'Add missing PDVs to dim_pdv'
        WHEN vm.orphan_pdv_count > 0 THEN 'Monitor orphan PDVs - acceptable under 5%'
        ELSE 'No action required' 
    END AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    vm.current_batch_id AS silver_batch_id
FROM validation_metrics vm

UNION ALL

-- 11. LOW DATA QUALITY RECORDS
SELECT 
    uuid() AS validation_id,
    'low_data_quality_records' AS validation_name,
    'data_quality' AS validation_category,
    CASE 
        WHEN vm.low_quality_count * 100.0 / NULLIF(vm.total_records, 0) < 10 THEN 'PASS'
        WHEN vm.low_quality_count * 100.0 / NULLIF(vm.total_records, 0) < 20 THEN 'WARNING'
        ELSE 'FAIL' 
    END AS status,
    CASE 
        WHEN vm.low_quality_count * 100.0 / NULLIF(vm.total_records, 0) < 10 THEN 'INFO'
        WHEN vm.low_quality_count * 100.0 / NULLIF(vm.total_records, 0) < 20 THEN 'MEDIUM'
        ELSE 'HIGH' 
    END AS severity,
    vm.total_records AS total_rows,
    vm.low_quality_count AS failed_rows,
    ROUND(vm.low_quality_count * 100.0 / NULLIF(vm.total_records, 0), 2) AS failed_percentage,
    CAST(vm.low_quality_count AS STRING) AS metric_value,
    '< 20%' AS threshold_value,
    CONCAT('Records with score < 0.5: ', CAST(vm.low_quality_count AS STRING),
           ' (', CAST(ROUND(vm.low_quality_count * 100.0 / NULLIF(vm.total_records, 0), 2) AS STRING), '%)') AS details,
    CASE 
        WHEN vm.low_quality_count * 100.0 / NULLIF(vm.total_records, 0) >= 20 THEN 'Investigate missing data in silver.fact_sell_in and dimensions'
        WHEN vm.low_quality_count > 0 THEN 'Monitor data quality - acceptable under 20%'
        ELSE 'No action required' 
    END AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    vm.current_batch_id AS silver_batch_id
FROM validation_metrics vm

UNION ALL

-- 12. POTENTIAL STOCKOUT DAYS VALIDATION
SELECT 
    uuid() AS validation_id,
    'potential_stockout_days_logic' AS validation_name,
    'business_logic' AS validation_category,
    CASE WHEN vm.invalid_stockout_projection = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    CASE WHEN vm.invalid_stockout_projection = 0 THEN 'INFO' ELSE 'MEDIUM' END AS severity,
    vm.total_records AS total_rows,
    vm.invalid_stockout_projection AS failed_rows,
    ROUND(vm.invalid_stockout_projection * 100.0 / NULLIF(vm.total_records, 0), 2) AS failed_percentage,
    CAST(vm.invalid_stockout_projection AS STRING) AS metric_value,
    '0' AS threshold_value,
    CONCAT('Records with no stock but stockout projection: ', CAST(vm.invalid_stockout_projection AS STRING)) AS details,
    CASE WHEN vm.invalid_stockout_projection > 0 THEN 'Review potential_stockout_days CASE logic in DML' ELSE 'No action required' END AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    vm.current_batch_id AS silver_batch_id
FROM validation_metrics vm

UNION ALL

-- 13. AUDIT FIELDS VALIDATION
SELECT 
    uuid() AS validation_id,
    'audit_fields_validation' AS validation_name,
    'structural' AS validation_category,
    CASE WHEN vm.missing_audit_fields = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    CASE WHEN vm.missing_audit_fields = 0 THEN 'INFO' ELSE 'MEDIUM' END AS severity,
    vm.total_records AS total_rows,
    vm.missing_audit_fields AS failed_rows,
    ROUND(vm.missing_audit_fields * 100.0 / NULLIF(vm.total_records, 0), 2) AS failed_percentage,
    CAST(vm.missing_audit_fields AS STRING) AS metric_value,
    '0' AS threshold_value,
    CONCAT('Records with missing audit fields: ', CAST(vm.missing_audit_fields AS STRING)) AS details,
    CASE WHEN vm.missing_audit_fields > 0 THEN 'Review silver_batch_id and gold_processed_at in DML' ELSE 'No action required' END AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    vm.current_batch_id AS silver_batch_id
FROM validation_metrics vm

UNION ALL

-- 14. BATCH SUMMARY
SELECT 
    uuid() AS validation_id,
    'batch_summary' AS validation_name,
    'summary' AS validation_category,
    'INFO' AS status,
    'INFO' AS severity,
    vm.total_records AS total_rows,
    0 AS failed_rows,
    0.0 AS failed_percentage,
    CONCAT('Months: ', CAST(vm.months_processed AS STRING), 
           ' | PDVs: ', CAST(vm.pdvs_processed AS STRING),
           ' | Products: ', CAST(vm.products_processed AS STRING)) AS metric_value,
    'N/A' AS threshold_value,
    CONCAT('Batch processed - ',
           'Records: ', CAST(vm.total_records AS STRING),
           ', Months: ', CAST(vm.months_processed AS STRING),
           ', PDVs: ', CAST(vm.pdvs_processed AS STRING),
           ', Products: ', CAST(vm.products_processed AS STRING),
           ', Avg quality: ', CAST(vm.avg_confidence_score AS STRING)) AS details,
    'No action required' AS recommended_action,
    CURRENT_TIMESTAMP() AS execution_timestamp,
    NULL AS year_month_id,
    vm.current_batch_id AS silver_batch_id
FROM validation_metrics vm

) final_validations;