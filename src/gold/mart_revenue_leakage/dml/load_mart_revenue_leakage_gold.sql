/* =========================================================
   Gold Layer Load
   Table: gold.mart_revenue_leakage
   Grain: PDV + Product + Month (monthly snapshot)
   Date usage: Full audit_date
   Author: Diego Mayorga
   ========================================================= */

WITH base_data AS (

    SELECT
        -- Business keys
        pa.audit_date AS audit_date_id,
        pa.pdv_code,
        pa.product_code,

        -- Product context
        pr.brand,
        pr.category,
        pr.price_list,

        -- PDV context
        pdv.chain,
        pdv.channel,
        pdv.store_size,
        pdv.city,

        -- Monthly health
        mh.closing_stock_units,
        mh.days_of_inventory,
        mh.has_planogram_active,
        mh.has_exhibition_active,
        mh.in_stock,
        mh.coverage_compliant,
        mh.in_expected_assortment,

        -- Price audit
        pa.observed_price,

        -- Expected assortment
        ea.expected_flag

    FROM (
        SELECT *, YEAR(audit_date) AS pa_year, MONTH(audit_date) AS pa_month
        FROM workspace.gold.fact_pdv_price_audit
    ) pa

    INNER JOIN workspace.gold.dim_pdv pdv
        ON pa.pdv_code = pdv.pdv_code
       AND pdv.is_active = TRUE

    INNER JOIN workspace.gold.dim_product pr
        ON pa.product_code = pr.product_code

    LEFT JOIN workspace.gold.dim_expected_assortment ea
        ON pa.pdv_code = ea.pdv_code
       AND pa.product_code = ea.product_code
       AND ea.is_current = TRUE

    LEFT JOIN (
        SELECT *, YEAR(date) AS mh_year, MONTH(date) AS mh_month
        FROM workspace.gold.fact_pdv_monthly_health
    ) mh
        ON pa.pdv_code = mh.pdv_code
       AND pa.product_code = mh.product_code
       AND pa.pa_year = mh.mh_year
       AND pa.pa_month = mh.mh_month

    WHERE pa.audit_date IS NOT NULL
),

factors AS (

    SELECT
        *,

        /* -------------------------------
           Stock Availability Factor
           ------------------------------- */
        CASE
            WHEN closing_stock_units = 0 THEN 0.0
            WHEN days_of_inventory < 10 THEN 0.8
            ELSE 1.0
        END AS stock_availability_factor,

        /* -------------------------------
           Price Competitiveness Factor
           ------------------------------- */
        CASE
            WHEN observed_price BETWEEN price_list * 0.85 AND price_list * 1.15 THEN 1.0
            WHEN observed_price > price_list * 1.15 THEN 0.0
            ELSE 0.5
        END AS price_competitiveness_factor,

        /* -------------------------------
           Execution Visibility Factor
           ------------------------------- */
        CASE
            WHEN has_planogram_active = TRUE
             AND has_exhibition_active = TRUE THEN 1.0
            WHEN has_planogram_active = TRUE
              OR has_exhibition_active = TRUE THEN 0.5
            ELSE 0.0
        END AS execution_visibility_factor,

        /* -------------------------------
           Assortment Alignment Factor
           ------------------------------- */
        CASE
            WHEN expected_flag = TRUE THEN 1.0
            ELSE 0.0
        END AS assortment_alignment_factor

    FROM base_data
)

INSERT OVERWRITE gold.mart_revenue_leakage
(
    audit_date_id,
    pdv_code,
    product_code,

    stock_availability_factor,
    price_competitiveness_factor,
    execution_visibility_factor,
    assortment_alignment_factor,

    revenue_leakage_pct,
    potential_revenue_lost_usd,

    brand,
    category,
    chain,
    channel,
    store_size,
    city,

    in_stock,
    coverage_compliant,

    gold_processed_at
)

SELECT
    audit_date_id,
    pdv_code,
    product_code,

    stock_availability_factor,
    price_competitiveness_factor,
    execution_visibility_factor,
    assortment_alignment_factor,

    /* -------------------------------
       Revenue Leakage Score (0–1)
       Only real loss factors included
       ------------------------------- */
    ROUND(
          (1 - stock_availability_factor)  * 0.50
        + (1 - price_competitiveness_factor) * 0.35
        + (1 - execution_visibility_factor)  * 0.15
    , 4) AS revenue_leakage_pct,

    /* -------------------------------
       Monetized Revenue Loss (USD)
       No loss if strategic assortment
       ------------------------------- */
    CASE
        WHEN assortment_alignment_factor = 0.0 THEN 0.0
        ELSE
            COALESCE(closing_stock_units, 0)
            * GREATEST(price_list - observed_price, 0)
    END AS potential_revenue_lost_usd,

    brand,
    category,
    chain,
    channel,
    store_size,
    city,

    in_stock,
    coverage_compliant,

    CURRENT_TIMESTAMP AS gold_processed_at

FROM factors;
