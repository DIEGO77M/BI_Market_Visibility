-- DDL: Gold Layer - mart_revenue_leakage
-- Author: BI Market Visibility Project
-- Description: Canonical fact table for Revenue Leakage analytics at PDV + Product + Date grain.

CREATE TABLE gold.mart_revenue_leakage (
    -- =========================
    -- Business Keys
    -- =========================
    audit_date_id DATE COMMENT 'Audit date (FK to dim_date.date_id)',
    pdv_code STRING COMMENT 'Point of sale code (FK to dim_pdv.pdv_code)',
    product_code STRING COMMENT 'Product code (FK to dim_product.product_code)',

    -- =========================
    -- Revenue Leakage Factors
    -- =========================
    stock_availability_factor DECIMAL(3,2) COMMENT 'Stock availability factor (0-1), derived from inventory and in_stock status',
    price_competitiveness_factor DECIMAL(3,2) COMMENT 'Price competitiveness factor (0-1), derived from price audit vs competition',
    execution_visibility_factor DECIMAL(3,2) COMMENT 'Execution/visibility factor (0-1), derived from in-store execution metrics',
    assortment_alignment_factor DECIMAL(3,2) COMMENT 'Assortment alignment factor (0-1), derived from assortment compliance',

    -- =========================
    -- Composite Score
    -- =========================
    revenue_leakage_pct DECIMAL(3,2) COMMENT 'Composite revenue leakage percentage (0-1), weighted by business rules',

    -- =========================
    -- Monetization
    -- =========================
    potential_revenue_lost_usd DECIMAL(10,2) COMMENT 'Estimated potential revenue lost in USD',
    
    -- =========================
    -- Product Context
    -- =========================
    brand STRING COMMENT 'Brand (from dim_product)',
    category STRING COMMENT 'Category (from dim_product)',

    -- =========================
    -- PDV Context
    -- =========================
    chain STRING COMMENT 'Retail chain (from dim_pdv)',
    channel STRING COMMENT 'Sales channel (from dim_pdv)',
    store_size STRING COMMENT 'Store size (from dim_pdv)',
    city STRING COMMENT 'City (from dim_pdv)',

    -- =========================
    -- Health & Compliance
    -- =========================
    in_stock BOOLEAN COMMENT 'TRUE if product is in stock (from fact_pdv_monthly_health)',
    coverage_compliant BOOLEAN COMMENT 'TRUE if product is in expected assortment and in stock (from fact_pdv_monthly_health)',

    -- =========================
    -- Audit Fields
    -- =========================
    gold_processed_at TIMESTAMP COMMENT 'Gold layer processing timestamp',

    -- =========================
    -- Constraints
    -- =========================
    CONSTRAINT pk_mart_revenue_leakage PRIMARY KEY (audit_date_id, pdv_code, product_code)
)
-- Partitioning suggestion for large-scale DWs:
-- PARTITION BY (audit_date_id) -- or by year_month if supported
-- Index suggestion:
-- CREATE INDEX idx_mart_revenue_leakage_pdv_product_date ON gold.mart_revenue_leakage (pdv_code, product_code, audit_date_id);
-- Table comment: Revenue leakage fact table at PDV + Product + Date grain. Supports root-cause and monetization analytics.
