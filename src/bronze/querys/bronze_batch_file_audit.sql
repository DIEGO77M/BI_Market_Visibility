-- Bronze Layer Example Query: Batch and File Audit
-- Purpose: Audits record counts by batch and source file for technical traceability.
-- Only for technical monitoring, not business logic.

SELECT _batch_id, _source_file, COUNT(*) AS records
FROM workspace.bronze.master_pdv
GROUP BY _batch_id, _source_file
ORDER BY records DESC;
