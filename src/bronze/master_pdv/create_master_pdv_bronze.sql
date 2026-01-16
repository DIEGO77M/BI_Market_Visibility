-- DDL for Bronze Table: workspace.bronze.master_pdv
-- Author: Diego Mayorga
-- Context: Databricks Serverless + Unity Catalog
-- Purpose: Explicit schema, partitioning, and properties for raw data ingestion (no transformation)

CREATE TABLE IF NOT EXISTS workspace.bronze.master_pdv (
    `Code (eLeader)` STRING,
    `Store Name` STRING,
    `Channel` STRING,
    `Sub Channel` STRING,
    `Chain` STRING,
    `Neighborhood` STRING,
    `City` STRING,
    `Parish` STRING,
    `Country` STRING,
    `Latitude` STRING,
    `Longitude` STRING,
    `Type of Service` STRING,
    `Status` STRING,
    `Supervisor Code` STRING,
    `Supervisor Name` STRING,
    `Merchandiser Code` STRING,
    `Merchandiser Name` STRING,
    `CODE PO` STRING,
    `Aditional_Exhibitions` STRING,
    `Commercial Activities` STRING,
    `Planograms` STRING,
    `Store SAP Code` STRING,
    `Sales Rep` STRING,
    _ingestion_timestamp TIMESTAMP,
    _load_date DATE,
    _source_file STRING,
    _batch_id STRING
)
USING DELTA
PARTITIONED BY (_load_date)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

