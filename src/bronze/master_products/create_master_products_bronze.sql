-- DDL for Bronze Table: workspace.bronze.master_products
-- Author: Diego Mayorga
-- Context: Databricks Serverless + Unity Catalog
-- Purpose: Explicit schema, partitioning, and properties for raw data ingestion (no transformation)

CREATE TABLE IF NOT EXISTS workspace.bronze.master_products (
    `Product_Code` STRING,
    `Product_Name` STRING,
    `Brand` STRING,
    `Segment` STRING,
    `Subsegment` STRING,
    `Category` STRING,
    `Subcategory` STRING,
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
