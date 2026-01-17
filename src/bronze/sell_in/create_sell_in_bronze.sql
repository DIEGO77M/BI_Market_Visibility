-- DDL for Bronze Table: workspace.bronze.sell_in
-- Author: Diego Mayorga
-- Context: Databricks Serverless + Unity Catalog
-- Purpose: Explicit schema for raw sell_in ingestion, partitioned for performance and traceability

CREATE TABLE IF NOT EXISTS workspace.bronze.sell_in (
    `Year` STRING,
    `Month` STRING,
    `PDV_Code` STRING,
    `Product_Code` STRING,
    `Opening_Stock_Units` STRING,
    `Sell_In_Units` STRING,
    `Returns_Units` STRING,
    `Closing_Stock_Units` STRING,
    `Days_of_Inventory` STRING,
    `Inventory_Turnover` STRING,
    `Replenishment_Flag` STRING,
    `Stock_Risk_Level` STRING,
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
