-- ============================================================================
-- DDL: Gold Layer - PDV Price Audit Fact Table
-- ============================================================================
-- Grain: PDV + Product + Month
-- Source: silver.fact_price_audit
-- Execution: spark.sql() from orchestration notebook
-- Idempotency: Ensured via partition overwrite in DML
-- ============================================================================

CREATE TABLE IF NOT EXISTS workspace.gold.fact_pdv_price_audit (
    -- Business Keys
    pdv_code STRING,
    product_code STRING,
    competitive_group STRING,
    
    -- Price Metrics
    observed_price DECIMAL(10,2),
    has_promotion BOOLEAN,
    promotional_price DECIMAL(10,2),
    effective_price DECIMAL(10,2),
    
    -- Competitive Metrics
    avg_competitor_price DECIMAL(10,2),
    price_vs_competitor_pct DOUBLE,
    is_price_above_competition BOOLEAN,
    competitive_position STRING,
    price_index DOUBLE,
    
    -- Audit Metadata
    audit_date DATE,
    days_since_audit INT,
    data_confidence_score DOUBLE,
    
    -- Lineage
    silver_batch_id STRING,
    gold_processed_at TIMESTAMP,
    
    -- Partition Key
    year_month STRING
)
USING DELTA
PARTITIONED BY (year_month)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);