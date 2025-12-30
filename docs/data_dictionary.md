# Data Dictionary

## Overview

This document provides detailed information about all data entities, attributes, and relationships in the BI Market Visibility project.

---

## Bronze Layer Tables

### raw_[table_name]

| Column Name | Data Type | Description | Source | Nullable | Example |
|-------------|-----------|-------------|--------|----------|---------|
| column_1 | string | [Description] | [Source system] | Yes/No | [Sample value] |
| column_2 | integer | [Description] | [Source system] | Yes/No | [Sample value] |
| ingestion_timestamp | timestamp | Timestamp when record was ingested | System | No | 2024-01-15 10:30:00 |
| source_file | string | Original source file name | System | No | data_20240115.csv |

**Table Purpose:** [Describe the purpose and use case]

**Data Quality Rules:**
- Rule 1: [Description]
- Rule 2: [Description]

---

## Silver Layer Tables

### silver_master_pdv

| Column Name | Data Type | Description | Transformation | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| [PDV_KEY] | string | Point of sale code (standardized) | TRIM + UPPER | No | PDV001 |
| [PDV_NAME] | string | Point of sale name (standardized) | TRIM + UPPER | No | SUPER MARKET ABC |
| [Additional columns from source] | various | Preserved from Bronze | Standardized text | Varies | - |
| [KEY]_is_valid | boolean | Validation flag for critical field | Quality check | No | true |
| quality_score | double | Overall quality score (0-100) | (valid_checks/total)*100 | No | 97.5 |
| processing_timestamp | timestamp | Silver processing time | System | No | 2025-12-30 10:35:00 |
| silver_layer_version | string | Silver layer version | System | No | v1.0 |
| processing_date | date | Processing date | System | No | 2025-12-30 |

**Table Purpose:** Standardized point-of-sale master data for analytics

**Transformations Applied:**
1. Text standardization (TRIM, UPPER) on all string columns
2. Deduplication by PDV key (keep most recent)
3. Quality validation flags added
4. Quality score calculation

**Data Quality Rules:**
- PDV key must be unique
- Critical text fields must not be null
- All text standardized to uppercase

**Validation Checks:**
- PDV_CODE_is_valid: Validates non-null PDV code
- quality_score: 0-100 scale based on all validation checks

---

### silver_master_products

| Column Name | Data Type | Description | Transformation | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| [PRODUCT_KEY] | string | Product code (standardized) | TRIM + UPPER | No | PROD123 |
| [PRODUCT_NAME] | string | Product name (standardized) | TRIM + UPPER | No | COFFEE 500G |
| [PRICE_COLUMNS] | decimal(18,2) | Product prices (cleaned) | Remove negatives, round to 2 decimals | Yes | 12.50 |
| [Additional columns] | various | Preserved from Bronze | Standardized | Varies | - |
| [KEY]_is_valid | boolean | Validation flag | Quality check | No | true |
| quality_score | double | Overall quality score (0-100) | Calculated | No | 98.0 |
| processing_timestamp | timestamp | Silver processing time | System | No | 2025-12-30 10:35:00 |
| silver_layer_version | string | Silver layer version | System | No | v1.0 |
| processing_date | date | Processing date | System | No | 2025-12-30 |

**Table Purpose:** Standardized product master data with clean prices

**Transformations Applied:**
1. Text standardization on product names and codes
2. Price validation (remove negatives, handle nulls)
3. Price rounding to 2 decimals
4. Deduplication by product key
5. Quality score calculation

**Data Quality Rules:**
- Product key must be unique
- Prices must be >= 0 or null
- All prices rounded to 2 decimals

**Validation Checks:**
- PRODUCT_CODE_is_valid: Validates non-null product code
- Price validation: Removes negative values
- quality_score: Overall data quality metric

---

### silver_price_audit

| Column Name | Data Type | Description | Transformation | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| [PDV_KEY] | string | Point of sale code | From Bronze | No | PDV001 |
| [PRODUCT_KEY] | string | Product code | From Bronze | No | PROD123 |
| [PRICE_COLUMNS] | decimal(18,2) | Audited prices | Round to 2 decimals, remove negatives | Yes | 15.99 |
| [DATE_COLUMNS] | date | Audit dates | Standardized | Yes | 2025-01-15 |
| year_month | string | Partition column | From Bronze | No | 2025-01 |
| [KEY]_is_valid | boolean | Validation flags | Quality check | No | true |
| quality_score | double | Quality score (0-100) | Calculated | No | 99.0 |
| processing_timestamp | timestamp | Processing time | System | No | 2025-12-30 10:35:00 |
| silver_layer_version | string | Version | System | No | v1.0 |
| processing_date | date | Processing date | System | No | 2025-12-30 |

**Table Purpose:** Clean price audit data for price variance analysis

**Transformations Applied:**
1. Remove records with all null critical fields
2. Price validation and rounding
3. Date standardization
4. Quality score calculation
5. Filter out invalid records

**Data Quality Rules:**
- At least one critical field must be non-null
- Prices must be positive or null
- Records with all nulls removed

**Validation Checks:**
- Critical columns (PDV, Product, Price) validated
- quality_score reflects overall completeness
- Partitioned by year_month (preserved from Bronze)

---

### silver_sell_in

| Column Name | Data Type | Description | Transformation | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| [PDV_KEY] | string | Point of sale code | From Bronze | No | PDV001 |
| [PRODUCT_KEY] | string | Product code | From Bronze | No | PROD123 |
| [QUANTITY_COLUMNS] | integer | Sales quantities | Clean (nulls→0, negatives→0) | No | 100 |
| [VALUE_COLUMNS] | decimal(18,2) | Sales values | Clean, round to 2 decimals | No | 1250.00 |
| unit_price_calculated | decimal(18,2) | Derived unit price | value / quantity | Yes | 12.50 |
| has_sales | boolean | Sales flag | quantity > 0 | No | true |
| year | integer | Partition column | From Bronze | No | 2025 |
| [KEY]_is_valid | boolean | Validation flags | Quality check | No | true |
| quality_score | double | Quality score (0-100) | Calculated | No | 100.0 |
| processing_timestamp | timestamp | Processing time | System | No | 2025-12-30 10:35:00 |
| silver_layer_version | string | Version | System | No | v1.0 |
| processing_date | date | Processing date | System | No | 2025-12-30 |

**Table Purpose:** Standardized sales transactions with derived metrics

**Transformations Applied:**
1. Quantity cleaning (nulls→0, negatives→0)
2. Value cleaning and rounding
3. Unit price calculation (derived metric)
4. Business flag: has_sales
5. Quality score calculation

**Data Quality Rules:**
- Quantities must be >= 0
- Values must be >= 0
- Unit price only calculated when both qty and value > 0

**Validation Checks:**
- QUANTITY_is_valid: Validates non-null quantity
- VALUE_is_valid: Validates non-null value
- has_sales: Business flag for records with actual sales
- quality_score: Overall data quality
- Partitioned by year (preserved from Bronze)

---

## Gold Layer Tables

### fact_[table_name]

| Column Name | Data Type | Description | Business Logic | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| fact_key | bigint | Surrogate key | Auto-generated | No | 123456789 |
| dim_key_1 | integer | Foreign key to dimension | Lookup | No | 101 |
| measure_1 | decimal(18,2) | [Description] | [Calculation] | Yes | 1234.56 |
| created_date | date | Record creation date | System | No | 2024-01-15 |

**Table Purpose:** [Describe the purpose and use case]

**Grain:** [Describe the grain/level of detail]

**Business Logic:**
- Logic 1: [Description]
- Logic 2: [Description]

**Relationships:**
- Related to: `dim_[dimension_name]` via `dim_key_1`

---

### dim_[dimension_name]

| Column Name | Data Type | Description | Business Logic | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| dim_key | integer | Primary key | Auto-generated | No | 101 |
| attribute_1 | string | [Description] | [Business logic] | No | [Sample value] |
| attribute_2 | string | [Description] | [Business logic] | Yes | [Sample value] |
| is_active | boolean | Active flag | SCD Type 2 | No | true |
| effective_date | date | Start date of validity | SCD Type 2 | No | 2024-01-01 |
| end_date | date | End date of validity | SCD Type 2 | Yes | 2024-12-31 |

**Table Purpose:** [Describe the purpose and use case]

**Dimension Type:** Type 1 / Type 2 / Type 3

**Hierarchies:**
- Hierarchy 1: Level 1 → Level 2 → Level 3

---

## Key Performance Indicators (KPIs)

### KPI 1: [KPI Name]

- **Definition:** [Clear definition]
- **Formula:** `[Mathematical formula]`
- **Target:** [Target value or threshold]
- **Source Tables:** [List of tables used]
- **Refresh Frequency:** Daily/Weekly/Monthly

### KPI 2: [KPI Name]

- **Definition:** [Clear definition]
- **Formula:** `[Mathematical formula]`
- **Target:** [Target value or threshold]
- **Source Tables:** [List of tables used]
- **Refresh Frequency:** Daily/Weekly/Monthly

---

## Data Lineage

```
Source System A → Bronze.raw_table_a → Silver.cleaned_table_a → Gold.fact_sales
Source System B → Bronze.raw_table_b → Silver.cleaned_table_b → Gold.dim_product
```

---

## Data Refresh Schedule

| Layer | Table | Refresh Frequency | Estimated Runtime | Dependencies |
|-------|-------|-------------------|-------------------|--------------|
| Bronze | raw_[table] | Daily 6:00 AM | 10 min | Source system |
| Silver | cleaned_[table] | Daily 6:15 AM | 15 min | Bronze layer |
| Gold | fact_[table] | Daily 6:35 AM | 20 min | Silver layer |

---

## Business Glossary

| Term | Definition | Example |
|------|------------|---------|
| [Term 1] | [Clear business definition] | [Example usage] |
| [Term 2] | [Clear business definition] | [Example usage] |

---

## Change Log

| Date | Version | Author | Changes |
|------|---------|--------|---------|
| 2024-01-15 | 1.0 | [Your Name] | Initial version |

---

## Notes

- All timestamps are in UTC
- Currency values are in USD unless specified
- Date format: YYYY-MM-DD
- Null vs Empty String: [Clarify handling]
