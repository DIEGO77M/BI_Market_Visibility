# Data Directory

## Medallion Architecture

This directory follows the **Medallion Architecture** pattern with three layers:

### 📂 Raw
- **Purpose:** Store original source data files
- **Format:** Any format (CSV, JSON, Excel, etc.)
- **Processing:** None - exact copy of source
- **Usage:** Reference only, never modified

### 🥉 Bronze Layer
- **Purpose:** Raw data ingestion into lakehouse
- **Format:** Parquet/Delta format
- **Processing:** Minimal - append timestamps, source metadata
- **Data Quality:** No validation
- **Schema:** Flexible, may include all source columns

### 🥈 Silver Layer
- **Purpose:** Cleaned and validated data
- **Format:** Parquet/Delta format
- **Processing:** 
  - Remove duplicates
  - Handle nulls
  - Standardize formats
  - Data type corrections
  - Basic business rules
- **Data Quality:** Validated, enforced constraints
- **Schema:** Structured, documented

### 🥇 Gold Layer
- **Purpose:** Business-level aggregations
- **Format:** Parquet/Delta format optimized for BI
- **Processing:**
  - Aggregations
  - Denormalization for BI
  - KPI calculations
  - Dimension modeling (star/snowflake schema)
- **Data Quality:** Business-ready, conformed
- **Schema:** Optimized for analytics and reporting

## Data Flow

```
Source Systems → raw/ → Bronze → Silver → Gold → Power BI
```

## Best Practices

1. **Never modify bronze data** - It's your source of truth
2. **Document all transformations** in notebooks
3. **Version control schemas** in docs/
4. **Implement data quality checks** at each layer
5. **Use partitioning** for large datasets
6. **Add .gitkeep files** if committing empty directories

## Sample Data

If using public datasets, add samples in `raw/` with clear documentation of:
- Source URL
- Download date
- License information
- Data dictionary reference
