-- ============================================================================
-- DML: Gold Layer - fact_pdv_monthly_health
-- ============================================================================
-- Step 0: Align temporal grain (monthly)
-- Purpose:
-- dim_date is a daily-grain dimension. This CTE reduces it to a
-- unique monthly reference to avoid row multiplication.
-- ============================================================================
-- Step 1 & 2: Build and Load monthly PDV × Product health snapshot
-- Purpose: Deterministic monthly snapshot load. INSERT OVERWRITE guarantees idempotency and no duplicates.
-- The CTE must be immediately before the INSERT OVERWRITE for correct SQL syntax.
-- ============================================================================

WITH base_monthly_health AS (
    SELECT
        -- =====================================================
        -- Business Keys
        -- =====================================================
        MAKE_DATE(si.year, si.month, 1) AS date,
        si.pdv_code,
        si.product_code,

        ea.assortment_id AS expected_assortment_id,

        -- =====================================================
        -- Stock Metrics
        -- =====================================================
        si.closing_stock_units,
        si.days_of_inventory,


        -- Inventory Action Signal
        CASE
            WHEN si.closing_stock_units = 0 THEN 'STOCKOUT'
            WHEN si.days_of_inventory <= 15 THEN 'URGENT_REPLENISHMENT'
            WHEN si.days_of_inventory BETWEEN 16 AND 30 THEN 'REPLENISHMENT_SOON'
            WHEN si.stock_risk_level = 'OVERSTOCK' THEN 'PROMOTION_OR_STOP_SUPPLY'
            ELSE 'NO_ACTION'
        END AS inventory_action_signal,

        -- =====================================================
        -- Execution Metrics (PDV-level attributes)
        -- =====================================================
        CASE
            WHEN dp.merchandiser_code IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS merchandiser_executed,

        dp.has_planograms AS has_planogram_active,
        dp.has_additional_exhibitions AS has_exhibition_active,

        -- =====================================================
        -- Compliance Flags
        -- =====================================================
        CASE
            WHEN si.closing_stock_units > 0
            THEN TRUE
            ELSE FALSE
        END AS in_stock,

        COALESCE(ea.expected_flag, FALSE)
            AS in_expected_assortment,

        CASE
            WHEN si.closing_stock_units > 0
             AND COALESCE(ea.expected_flag, FALSE) = TRUE
            THEN TRUE
            ELSE FALSE
        END AS coverage_compliant,

        -- =====================================================
        -- Data Quality (baseline score)
        -- =====================================================
        CAST(1.0 AS DECIMAL(2,1)) AS data_confidence_score,

        -- =====================================================
        -- Audit Fields
        -- =====================================================
        si.silver_batch_id,
        CURRENT_TIMESTAMP() AS gold_processed_at

    FROM workspace.silver.fact_sell_in si

    -- Mandatory dimensions
    INNER JOIN workspace.gold.dim_product pr
        ON pr.product_code = si.product_code

    -- Optional PDV attributes
    LEFT JOIN workspace.gold.dim_pdv dp
        ON dp.pdv_code = si.pdv_code
       AND dp.is_active = TRUE

    -- Expected assortment (business key join only for simulation)
    LEFT JOIN workspace.gold.dim_expected_assortment ea
        ON ea.pdv_code = si.pdv_code
       AND ea.product_code = si.product_code
       AND ea.is_current = TRUE
)
INSERT OVERWRITE workspace.gold.fact_pdv_monthly_health
SELECT
    date,
    pdv_code,
    product_code,
    expected_assortment_id,

    closing_stock_units,
    days_of_inventory,
    inventory_action_signal,

    merchandiser_executed,
    has_planogram_active,
    has_exhibition_active,

    in_stock,
    in_expected_assortment,
    coverage_compliant,

    data_confidence_score,

    silver_batch_id,
    gold_processed_at
FROM base_monthly_health;
