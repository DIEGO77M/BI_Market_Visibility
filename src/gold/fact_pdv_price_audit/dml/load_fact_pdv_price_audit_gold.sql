-- ============================================================================
-- DML: Gold Layer - PDV Price Audit Load
-- ============================================================================
-- Idempotency: INSERT OVERWRITE by partition
-- Execution: spark.sql() from orchestration notebook
-- ============================================================================

INSERT OVERWRITE workspace.gold.fact_pdv_price_audit PARTITION (year_month)

WITH deduplicated_silver AS (
    -- Keep latest audit per PDV + Product + Month
    SELECT
        pdv_code,
        product_code,
        competitive_group,
        price,
        has_promotion,
        promotional_price,
        date AS audit_date,
        silver_batch_id,
        DATE_FORMAT(date, 'yyyy-MM') AS year_month,
        ROW_NUMBER() OVER (
            PARTITION BY pdv_code, product_code, DATE_FORMAT(date, 'yyyy-MM')
            ORDER BY date DESC, silver_processed_at DESC
        ) AS rn
    FROM workspace.silver.fact_price_audit
    WHERE date IS NOT NULL
),

base_prices AS (
    SELECT
        pdv_code,
        product_code,
        competitive_group,
        audit_date,
        year_month,
        price AS observed_price,
        has_promotion,
        promotional_price,
        COALESCE(NULLIF(promotional_price, 0), price) AS effective_price,
        silver_batch_id
    FROM deduplicated_silver
    WHERE rn = 1
),

-- Calculate competitive metrics EXCLUDING own PDV
competitor_avg AS (
    SELECT
        b1.pdv_code,
        b1.product_code,
        b1.year_month,
        AVG(b2.effective_price) AS avg_competitor_price,
        COUNT(b2.pdv_code) AS competitor_count
    FROM base_prices b1
    INNER JOIN base_prices b2
        ON b1.competitive_group = b2.competitive_group
        AND b1.product_code = b2.product_code
        AND b1.year_month = b2.year_month
        AND b1.pdv_code != b2.pdv_code  -- Exclude self
    GROUP BY b1.pdv_code, b1.product_code, b1.year_month
),

metrics AS (
    SELECT
        bp.*,
        ca.avg_competitor_price,
        ca.competitor_count,
        -- Single calculation, reused via column reference
        CASE 
            WHEN ca.avg_competitor_price IS NOT NULL 
            THEN ((bp.effective_price - ca.avg_competitor_price) / ca.avg_competitor_price) * 100
        END AS price_vs_competitor_pct,
        CASE 
            WHEN ca.avg_competitor_price IS NOT NULL 
            THEN (bp.effective_price / ca.avg_competitor_price) * 100
        END AS price_index
    FROM base_prices bp
    LEFT JOIN competitor_avg ca USING (pdv_code, product_code, year_month)
)

SELECT
    pdv_code,
    product_code,
    competitive_group,
    observed_price,
    has_promotion,
    promotional_price,
    effective_price,
    
    -- Competitive metrics (already calculated)
    avg_competitor_price,
    price_vs_competitor_pct,
    effective_price > avg_competitor_price AS is_price_above_competition,
    
    CASE
        WHEN price_vs_competitor_pct > 5 THEN 'ABOVE_MARKET'
        WHEN price_vs_competitor_pct BETWEEN -5 AND 5 THEN 'MATCH_MARKET'
        WHEN price_vs_competitor_pct < -5 THEN 'BELOW_MARKET'
    END AS competitive_position,
    
    price_index,
    
    -- Audit metadata
    audit_date,
    DATEDIFF(LAST_DAY(audit_date), audit_date) AS days_since_audit,
    CASE
        WHEN DATEDIFF(LAST_DAY(audit_date), audit_date) <= 7 THEN 1.0
        WHEN DATEDIFF(LAST_DAY(audit_date), audit_date) <= 30 THEN 0.7
        ELSE 0.4
    END AS data_confidence_score,
    
    -- Lineage
    silver_batch_id,
    CURRENT_TIMESTAMP() AS gold_processed_at,
    year_month
    
FROM metrics;