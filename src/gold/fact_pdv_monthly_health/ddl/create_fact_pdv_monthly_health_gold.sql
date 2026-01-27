-- ============================================================================
-- DDL: Gold Layer - fact_pdv_monthly_health
-- ============================================================================
-- Canonical source for Revenue Leakage analytics
-- Grain: PDV + Product + Month (year_month_id)
-- ============================================================================

CREATE TABLE IF NOT EXISTS workspace.gold.fact_pdv_monthly_health (
    -- =========================
    -- Business Keys
    -- =========================
    year_month_id INT COMMENT 'Logical FK to dim_date.year_month',
    pdv_code STRING COMMENT 'Logical FK to dim_pdv.pdv_code',
    product_code STRING COMMENT 'Logical FK to dim_product.product_code',
    expected_assortment_id INT COMMENT 'Logical FK to dim_expected_assortment.assortment_id',

    -- =========================
    -- Stock (monthly snapshot)
    -- =========================
    closing_stock_units INT COMMENT 'End-of-month stock units',
    days_of_inventory INT COMMENT 'Estimated number of inventory days available',
    potential_stockout_days INT COMMENT 'Projection: number of days until stockout',

    -- =========================
    -- In-store Execution
    -- =========================
    merchandiser_executed BOOLEAN COMMENT 'Indicates whether the merchandiser visited during the month',
    has_planogram_active BOOLEAN COMMENT 'Indicates if a planogram is active in the store',
    has_exhibition_active BOOLEAN COMMENT 'Indicates if an in-store exhibition is active',

    -- =========================
    -- Commercial Compliance
    -- =========================
    in_stock BOOLEAN COMMENT 'TRUE if closing_stock_units > 0',
    in_expected_assortment BOOLEAN COMMENT 'TRUE if the product belongs to the expected assortment',
    coverage_compliant BOOLEAN COMMENT 'TRUE if product is in stock and in expected assortment',

    -- =========================
    -- Data Quality
    -- =========================
    data_confidence_score DECIMAL(3,1) COMMENT 'Data confidence score ranging from 0.0 to 1.0',

    -- =========================
    -- Audit Fields
    -- =========================
    silver_batch_id STRING COMMENT 'Batch identifier inherited from Silver layer',
    gold_processed_at TIMESTAMP COMMENT 'Gold layer processing timestamp'
)
USING DELTA
PARTITIONED BY (year_month_id)
COMMENT 'Monthly snapshot of PDV–Product health, including stock, execution, compliance, and data quality';
