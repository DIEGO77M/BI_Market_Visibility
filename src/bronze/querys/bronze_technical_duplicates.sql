-- Bronze Layer Example Query: Technical Duplicates by Batch and Business Key
-- Purpose: Detects duplicate records by business key and batch for ingestion quality monitoring.
-- Only for technical monitoring, not business logic.

SELECT "Code (eLeader)", _batch_id, COUNT(*) AS duplicates
FROM workspace.bronze.master_pdv
GROUP BY "Code (eLeader)", _batch_id
HAVING COUNT(*) > 1;
