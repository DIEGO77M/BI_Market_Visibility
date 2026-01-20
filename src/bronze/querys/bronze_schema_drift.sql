-- Bronze Layer Example Query: Schema Drift Detection
-- Purpose: Detects extra columns in the Bronze table that are not part of the expected schema.
-- Only for technical monitoring, not business logic.

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'master_pdv' AND table_schema = 'bronze'
  AND column_name NOT IN (
    'Code (eLeader)', 'Store Name', 'Channel', 'Sub Channel', 'Chain',
    'Neighborhood', 'City', 'Parish', 'Country', 'Latitude', 'Longitude',
    'Type of Service', 'Status', 'Supervisor Code', 'Supervisor Name',
    'Merchandiser Code', 'Merchandiser Name', 'CODE PO',
    'Aditional_Exhibitions', 'Commercial Activities', 'Planograms',
    'Store SAP Code', 'Sales Rep', '_ingestion_timestamp', '_load_date', '_source_file', '_batch_id'
  );
