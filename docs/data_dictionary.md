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

### cleaned_[table_name]

| Column Name | Data Type | Description | Transformation | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| column_1 | string | [Description] | [Transformation applied] | Yes/No | [Sample value] |
| column_2 | integer | [Description] | [Transformation applied] | Yes/No | [Sample value] |
| updated_timestamp | timestamp | Last update timestamp | System | No | 2024-01-15 10:35:00 |

**Table Purpose:** [Describe the purpose and use case]

**Transformations Applied:**
1. [Transformation 1]
2. [Transformation 2]

**Data Quality Rules:**
- Rule 1: [Description]
- Rule 2: [Description]

**Validation Checks:**
- Check 1: [Description]
- Check 2: [Description]

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
