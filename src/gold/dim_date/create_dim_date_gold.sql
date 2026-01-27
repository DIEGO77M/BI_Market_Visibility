CREATE OR REPLACE TABLE workspace.gold.dim_date
USING DELTA
COMMENT 'Date dimension for Revenue Leakage analytics'
AS
SELECT
  d AS date_id,
  year(d) AS year,
  month(d) AS month,
  CAST(date_format(d, 'yyyyMM') AS INT) AS year_month,
  quarter(d) AS quarter,
  (weekday(d) + 1) AS day_of_week,  
  (weekday(d) IN (0, 6)) AS is_weekend,  
  current_timestamp() AS gold_processed_at
FROM (
  SELECT explode(sequence(
    DATE('2019-01-01'),
    date_add(current_date(), 365)
  )) AS d
)
WHERE d IS NOT NULL;