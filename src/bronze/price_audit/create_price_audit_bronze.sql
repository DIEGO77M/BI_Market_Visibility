-- DDL for Bronze Table: workspace.bronze.price_audit
-- Author: Diego Mayorga
-- Context: Databricks Serverless + Unity Catalog
-- Purpose: Explicit schema, partitioning, and properties for raw data ingestion (no transformation)

CREATE TABLE IF NOT EXISTS workspace.bronze.price_audit (
    `Fecha` STRING,
    `Nombre del punto de venta` STRING,
    `Cod_PDV` STRING,
    `Nombre_Producto` STRING,
    `Cod_Producto` STRING,
    `Precio` STRING,
    `Tiene este producto una promocion?` STRING,
    `Promotional Price` STRING,
    `Comentarios` STRING,
    `Competitive_Group` STRING,
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
