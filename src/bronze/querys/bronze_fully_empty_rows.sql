-- Bronze Layer Example Query: Fully Empty Rows Detection
-- Purpose: Detects rows where all business columns are empty or null (potential ingestion or file issues).
-- Only for technical monitoring, not business logic.

SELECT COUNT(*) AS fully_empty_rows
FROM workspace.bronze.master_pdv
WHERE
    TRIM(
        COALESCE("Code (eLeader)", '') ||
        COALESCE("Store Name", '') ||
        COALESCE("Channel", '') ||
        COALESCE("Sub Channel", '') ||
        COALESCE("Chain", '') ||
        COALESCE("Neighborhood", '') ||
        COALESCE("City", '') ||
        COALESCE("Parish", '') ||
        COALESCE("Country", '') ||
        COALESCE("Latitude", '') ||
        COALESCE("Longitude", '') ||
        COALESCE("Type of Service", '') ||
        COALESCE("Status", '') ||
        COALESCE("Supervisor Code", '') ||
        COALESCE("Supervisor Name", '') ||
        COALESCE("Merchandiser Code", '') ||
        COALESCE("Merchandiser Name", '') ||
        COALESCE("CODE PO", '') ||
        COALESCE("Aditional_Exhibitions", '') ||
        COALESCE("Commercial Activities", '') ||
        COALESCE("Planograms", '') ||
        COALESCE("Store SAP Code", '') ||
        COALESCE("Sales Rep", '')
    ) = '';
