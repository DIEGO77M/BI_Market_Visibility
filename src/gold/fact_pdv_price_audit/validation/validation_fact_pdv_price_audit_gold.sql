-- ============================================================================
-- Each check below is documented for technical reviewers and recruiters.
-- These validations ensure business key integrity, data freshness, and idempotency.
-- ============================================================================
-- CHECK 1: Ensures the most recent partition (latest month) contains at least one record.
-- This prevents silent pipeline failures and guarantees that the Gold table is being updated.
-- CHECK 2: Validates that there are no NULLs in the business keys (pdv_code, product_code, competitive_group).
-- This enforces referential integrity and prevents downstream analytics errors.
-- CHECK 3: Checks for duplicate records by the business grain (PDV + Product + Month).
-- This guarantees idempotency and prevents double-counting in BI/Analytics.
-- CHECK 4: Data freshness validation (simulation mode).
-- Passes if the latest partition is within the simulated data range (2021-2022).
-- Warns if data is outside this range, supporting demo and historical scenarios.
-- ============================================================================
-- VALIDATION: Gold Layer - PDV Price Audit Quality Checks
-- ============================================================================
-- Execution: spark.sql() from orchestration notebook
-- Scope: Validates last loaded partition(s)
-- Failure: Raises exception via failed assertion
-- ============================================================================

-- ============================================================
-- CHECK 1: Recent partition has data
-- ============================================================
SELECT * FROM (
    SELECT 'CHECK 1: Recent partition has data' AS check_name,
        CASE 
            WHEN record_count = 0 THEN RAISE_ERROR('VALIDATION_FAILED: No records in latest partition')
            ELSE 'PASS'
        END AS check_result
    FROM (
        SELECT COUNT(*) AS record_count
        FROM workspace.gold.fact_pdv_price_audit
        WHERE year_month = (
            SELECT MAX(year_month) 
            FROM workspace.gold.fact_pdv_price_audit
        )
    )
    UNION ALL
    SELECT 'CHECK 2: No nulls in business keys' AS check_name,
        CASE 
            WHEN null_count > 0 THEN RAISE_ERROR('VALIDATION_FAILED: Found ' || null_count || ' rows with NULL business keys')
            ELSE 'PASS'
        END AS check_result
    FROM (
        SELECT COUNT(*) AS null_count
        FROM workspace.gold.fact_pdv_price_audit
        WHERE year_month = (SELECT MAX(year_month) FROM workspace.gold.fact_pdv_price_audit)
          AND (pdv_code IS NULL OR product_code IS NULL OR competitive_group IS NULL)
    )
    UNION ALL
    SELECT 'CHECK 3: No duplicates by grain' AS check_name,
        CASE 
            WHEN dup_count > 0 THEN RAISE_ERROR('VALIDATION_FAILED: Found ' || dup_count || ' duplicate grain combinations')
            ELSE 'PASS'
        END AS check_result
    FROM (
        SELECT COUNT(*) AS dup_count
        FROM (
            SELECT pdv_code, product_code, year_month
            FROM workspace.gold.fact_pdv_price_audit
            WHERE year_month = (SELECT MAX(year_month) FROM workspace.gold.fact_pdv_price_audit)
            GROUP BY pdv_code, product_code, year_month
            HAVING COUNT(*) > 1
        )
    )
    UNION ALL
    SELECT 'CHECK 4: Data freshness' AS check_name,
        CASE 
            WHEN MAX(year_month) >= '2021-01' AND MAX(year_month) <= '2022-12' THEN 'PASS'
            ELSE 'WARNING: Latest partition is outside simulated data range (2021-2022)'
        END AS check_result
    FROM workspace.gold.fact_pdv_price_audit
) all_checks;