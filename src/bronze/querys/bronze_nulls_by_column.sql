-- Bronze Layer Example Query: Nulls and Empties by Key Column
-- Purpose: Counts null or empty values in key business columns for technical data quality monitoring.
-- Only for technical monitoring, not business logic.

SELECT
    SUM(CASE WHEN "Code (eLeader)" IS NULL OR "Code (eLeader)" = '' THEN 1 ELSE 0 END) AS empty_business_key,
    SUM(CASE WHEN "Store Name" IS NULL OR "Store Name" = '' THEN 1 ELSE 0 END) AS empty_store_name
FROM workspace.bronze.master_pdv;
