CREATE OR REPLACE TABLE workspace.gold.dim_pdv
USING DELTA
PARTITIONED BY (is_active)
COMMENT 'POS dimension with SCD Type 1. Represents current store configuration.'
AS
-- Store size mapping: business-driven logic, not technical normalization
WITH store_size_map AS (
  SELECT
    'independent_supermarket' AS sub_channel_norm,
    'small' AS store_size
  UNION ALL SELECT 'convenience_store', 'small'
  UNION ALL SELECT 'supermarket_chain', 'medium'
  UNION ALL SELECT 'hypermarket', 'large'
),
validated AS (
  SELECT
    pdv_code,
    store_name,
    chain,
    channel,
    UPPER(chain) || '|' || UPPER(channel) AS chain_channel_id,
    UPPER(chain) || '|' || UPPER(channel) || '|' || COALESCE(UPPER(merchandiser_code), 'NA') AS chain_channel_merchandiser_id,
    city,
    -- Business rule: derive store_size from sub_channel, fallback to 'unknown' for data governance
    COALESCE(ssm.store_size, 'unknown') AS store_size,
    is_active,
    -- Drill-down only: coordinates are not used for analytics/modeling
    TRY_CAST(latitude AS DECIMAL(10,7)) AS latitude,
    TRY_CAST(longitude AS DECIMAL(10,7)) AS longitude,
    merchandiser_code,
    merchandiser_name,
    has_additional_exhibitions,
    has_commercial_activities,
    has_planograms,
    assignment_complete,
    current_timestamp() AS gold_processed_at,
    -- SCD1: always keep latest state per pdv_code (no history)
    ROW_NUMBER() OVER (PARTITION BY pdv_code ORDER BY current_timestamp() DESC) AS rn
  FROM workspace.silver.dim_pdv sp
  LEFT JOIN store_size_map ssm ON lower(sp.sub_channel) = ssm.sub_channel_norm
  WHERE pdv_code IS NOT NULL
)
SELECT * EXCEPT(rn)
FROM validated
WHERE rn = 1
  -- Data quality: only valid coordinates allowed in Gold
  AND TRY_CAST(latitude AS DECIMAL(10,7)) IS NOT NULL
  AND TRY_CAST(longitude AS DECIMAL(10,7)) IS NOT NULL