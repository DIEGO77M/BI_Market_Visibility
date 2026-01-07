### silver_drift_history (Audit Table)

| Column Name      | Data Type | Description                                 | Example |
|------------------|-----------|---------------------------------------------|---------|
| table_name       | string    | Silver table monitored                      | silver_master_pdv |
| drift_type       | string    | Type of drift detected (schema/volume)      | schema/volume |
| severity         | string    | Severity of drift (HIGH/MEDIUM/LOW)         | HIGH |
| details          | string    | JSON with drift details                     | {"missing_columns": ["price"]} |
| baseline_json    | string    | JSON with baseline schema/metrics           | {"schema": {...}, "metrics": {...}} |
| timestamp        | timestamp | Detection time                              | 2026-01-05 10:00:00 |

**Table Purpose:** Audit and observability of all schema, quality, and volume drift events detected in Silver layer. Used for executive monitoring and operational traceability.

**Refresh Frequency:** After each Silver write (post-write hook or manual)

**Notes:**
- Only metadata is stored (no data scan)
- Table is append-only for full auditability
# Data Dictionary

## Overview

This document provides detailed information about all data entities, attributes, and relationships in the BI Market Visibility project.

---

## Bronze Layer Tables


### bronze_master_pdv

| Column Name              | Data Type | Description                        | Source         | Nullable | Example           |
|-------------------------|-----------|------------------------------------|----------------|----------|-------------------|
| Code (eLeader)          | string    | Point of sale unique identifier    | master_pdv_raw | No       | PDV0001           |
| Store Name              | string    | Point of sale name                 | master_pdv_raw | No       | Store_001         |
| Channel                 | string    | Sales channel                      | master_pdv_raw | Yes      | Direct Trade      |
| Sub Channel             | string    | Subdivision of channel             | master_pdv_raw | Yes      | Independent Supermarket |
| Chain                   | string    | Retail chain name                  | master_pdv_raw | Yes      | Independent       |
| Neighborhood            | string    | Neighborhood                       | master_pdv_raw | Yes      | Brown's Town      |
| City                    | string    | City                               | master_pdv_raw | Yes      | Brown's Town      |
| Parish                  | string    | Parish/region                      | master_pdv_raw | Yes      | St Ann            |
| Country                 | string    | Country                            | master_pdv_raw | Yes      | Jamaica           |
| Latitude                | string    | Store latitude                     | master_pdv_raw | Yes      | 180.562.475       |
| Longitude               | string    | Store longitude                    | master_pdv_raw | Yes      | -768.061.162      |
| Type of Service         | string    | Store service type                 | master_pdv_raw | Yes      | MERCHANDISER      |
| Status                  | string    | Store status                       | master_pdv_raw | Yes      | ACTIVE            |
| Supervisor Code         | string    | Supervisor code                    | master_pdv_raw | Yes      | SUP-003           |
| Supervisor Name         | string    | Supervisor name                    | master_pdv_raw | Yes      | Michael Brown     |
| Merchandiser Code       | string    | Merchandiser code                  | master_pdv_raw | Yes      | MER-008           |
| Merchandiser Name       | string    | Merchandiser name                  | master_pdv_raw | Yes      | Leroy Campbell    |
| CODE PO                 | string    | Purchase order code                | master_pdv_raw | Yes      | PO_001            |
| Aditional_Exhibitions   | string    | Additional exhibitions             | master_pdv_raw | Yes      | Yes               |
| Commercial Activities   | string    | Commercial activities              | master_pdv_raw | Yes      | No                |
| Planograms              | string    | Planogram compliance               | master_pdv_raw | Yes      | Yes               |
| Store SAP Code          | string    | SAP code for store                 | master_pdv_raw | Yes      | 3031244           |
| Sales Rep               | string    | Sales representative               | master_pdv_raw | Yes      | Shakeera Marshall |
| _metadata_ingestion_timestamp | timestamp | Data ingestion time           | System         | No       | 2025-12-31 03:00:00 |
| _metadata_source_file   | string    | Source file path                   | System         | No       | /Volumes/.../master_pdv_raw.csv |
| _metadata_batch_id      | string    | Batch identifier                   | System         | No       | 20251231_030000_bronze_ingestion |
| _metadata_ingestion_date| date      | Ingestion date                     | System         | No       | 2025-12-31        |

**Table Purpose:** Store master data for point-of-sale locations, used for geographic and organizational analysis.

**Data Quality Rules:**
- Code (eLeader) must be unique and not null
- Store Name must not be null
- Latitude/Longitude should be valid coordinates if present

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

### Dimensions

#### gold_dim_date

| Column Name | Data Type | Description | Business Logic | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| date_sk | integer | Surrogate key (YYYYMMDD) | date_format("yyyyMMdd") | No | 20260106 |
| date | date | Calendar date | Source | No | 2026-01-06 |
| year | integer | Calendar year | year(date) | No | 2026 |
| quarter | integer | Quarter (1-4) | quarter(date) | No | 1 |
| month | integer | Month (1-12) | month(date) | No | 1 |
| week | integer | ISO week of year | weekofyear(date) | No | 2 |
| day_of_month | integer | Day of month (1-31) | dayofmonth(date) | No | 6 |
| day_of_week | integer | Day of week (1=Sun, 7=Sat) | dayofweek(date) | No | 3 |
| year_month | string | Year-Month | YYYY-MM format | No | 2026-01 |
| year_quarter | string | Year-Quarter | YYYY-Qn format | No | 2026-Q1 |
| month_name | string | Full month name | MMMM format | No | January |
| month_name_short | string | Abbreviated month | MMM format | No | Jan |
| day_name | string | Full day name | EEEE format | No | Tuesday |
| day_name_short | string | Abbreviated day | EEE format | No | Tue |
| is_weekend | boolean | Weekend flag | Sat or Sun | No | false |
| is_month_end | boolean | Last day of month | date = last_day(date) | No | false |
| is_month_start | boolean | First day of month | day_of_month = 1 | No | false |
| fiscal_year | integer | Fiscal year | = calendar year | No | 2026 |
| fiscal_quarter | integer | Fiscal quarter | = calendar quarter | No | 1 |

**Table Purpose:** Calendar dimension for time-based analysis and reporting

**Dimension Type:** Static (Type 1 - regenerated annually)

**Date Range:** 2022-01-01 to 2026-12-31

**Refresh Frequency:** Annual or when extending date range

---

#### gold_dim_product

| Column Name | Data Type | Description | Business Logic | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| product_sk | string(16) | Surrogate key | SHA-256 hash | No | a1b2c3d4e5f67890 |
| product_code | string | Business key | From Silver | No | PROD001 |
| product_name | string | Product description | From Silver | No | COFFEE 500G |
| brand | string | Brand name | Tracked attribute | No | NESCAFE |
| segment | string | Product segment | Tracked attribute | No | BEVERAGES |
| category | string | Product category | Tracked attribute | No | HOT DRINKS |
| valid_from | date | Version start date | SCD2 logic | No | 2026-01-06 |
| valid_to | date | Version end date | 9999-12-31 for current | No | 9999-12-31 |
| is_current | boolean | Active version flag | SCD2 logic | No | true |

**Table Purpose:** Product master dimension with full change history

**Dimension Type:** SCD Type 2 (Slowly Changing)

**Tracked Attributes:** product_name, brand, segment, category

**Query Patterns:**
- Current: `WHERE is_current = true`
- Point-in-time: `WHERE date BETWEEN valid_from AND valid_to`

---

#### gold_dim_pdv

| Column Name | Data Type | Description | Business Logic | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| pdv_sk | string(16) | Surrogate key | SHA-256 hash | No | f6e5d4c3b2a19087 |
| pdv_code | string | Business key | code_eleader from Silver | No | PDV001 |
| pdv_name | string | Store name | Tracked attribute | No | SUPER MART |
| channel | string | Sales channel | Tracked attribute | No | MODERN TRADE |
| region | string | Geographic region | Tracked attribute | No | NORTH |
| city | string | City | Tracked attribute | No | KINGSTON |
| format | string | Store format | Tracked attribute | No | SUPERMARKET |
| latitude | double | Geographic latitude | Informational | Yes | 18.0179 |
| longitude | double | Geographic longitude | Informational | Yes | -76.8099 |
| valid_from | date | Version start date | SCD2 logic | No | 2026-01-06 |
| valid_to | date | Version end date | 9999-12-31 for current | No | 9999-12-31 |
| is_current | boolean | Active version flag | SCD2 logic | No | true |

**Table Purpose:** Point of Sale (store) dimension with change history

**Dimension Type:** SCD Type 2 (Slowly Changing)

**Tracked Attributes:** pdv_name, channel, region, city, format

---

### Fact Tables

#### gold_fact_sell_in

| Column Name | Data Type | Description | Business Logic | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| date_sk | integer | FK to dim_date (partition) | Lookup | No | 20260106 |
| product_sk | string | FK to dim_product | Lookup (is_current=true) | Yes | a1b2c3d4e5f67890 |
| pdv_sk | string | FK to dim_pdv | Lookup (is_current=true) | Yes | f6e5d4c3b2a19087 |
| transaction_date | date | Transaction date | From Silver | No | 2026-01-06 |
| quantity_sell_in | integer | Units sold (aggregated) | SUM(quantity) at grain | No | 150 |
| value_sell_in | double | Revenue (aggregated) | SUM(value) at grain | No | 1875.00 |
| unit_price_sell_in | double | Average unit price | value / quantity | Yes | 12.50 |
| transactions_count | integer | Number of transactions | COUNT at grain | No | 3 |
| product_code | string | Natural key (degenerate) | For debugging | No | PROD001 |
| pdv_code | string | Natural key (degenerate) | For debugging | No | PDV001 |
| processing_timestamp | timestamp | ETL timestamp | System | No | 2026-01-06 08:00:00 |

**Table Purpose:** Sell-in transactions (manufacturer → retailer)

**Grain:** date × product × pdv

**Partition:** date_sk

**Business Use:** Measure commercial push, track sales volume

---

#### gold_fact_price_audit

| Column Name | Data Type | Description | Business Logic | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| date_sk | integer | FK to dim_date (partition) | Lookup | No | 20260106 |
| product_sk | string | FK to dim_product | Lookup | Yes | a1b2c3d4e5f67890 |
| pdv_sk | string | FK to dim_pdv | Lookup | Yes | f6e5d4c3b2a19087 |
| audit_date | date | Price observation date | From Silver | No | 2026-01-06 |
| observed_price | double | Actual price at PDV | AVG if multiple per day | No | 14.99 |
| avg_market_price | double | Market average (product/month) | AVG across all PDVs | No | 15.50 |
| price_variance | double | Difference from market | observed - avg_market | No | -0.51 |
| price_index | double | Price competitiveness | (observed/avg) × 100 | No | 96.71 |
| is_above_market | boolean | Above market flag | observed > avg_market | No | false |
| is_below_market | boolean | Below market flag | observed < avg_market | No | true |
| market_observations | integer | Market sample size | COUNT for avg calculation | No | 45 |
| product_code | string | Natural key | For debugging | No | PROD001 |
| pdv_code | string | Natural key | For debugging | No | PDV001 |
| year_month | string | Period for grouping | YYYY-MM format | No | 2026-01 |
| processing_timestamp | timestamp | ETL timestamp | System | No | 2026-01-06 08:00:00 |

**Table Purpose:** Price observations for market visibility

**Grain:** audit_date × product × pdv

**Partition:** date_sk

**Price Index Interpretation:**
- < 85: Significantly below market
- 85-95: Below market (competitive)
- 95-105: At market
- 105-115: Above market (premium)
- > 115: Significantly above market

---

#### gold_fact_stock (Estimated)

| Column Name | Data Type | Description | Business Logic | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| date_sk | integer | FK to dim_date (partition) | Lookup | No | 20260106 |
| product_sk | string | FK to dim_product | Lookup | Yes | a1b2c3d4e5f67890 |
| pdv_sk | string | FK to dim_pdv | Lookup | Yes | f6e5d4c3b2a19087 |
| stock_date | date | Stock position date | From fact_sell_in | No | 2026-01-06 |
| opening_stock | integer | Estimated opening stock | Prior day closing | No | 120 |
| closing_stock | integer | Estimated closing stock | cum_sell_in - cum_sell_out | No | 100 |
| stock_days | double | Days of inventory | closing / avg_daily_demand | No | 8.5 |
| stock_out_flag | boolean | Stock-out indicator | closing <= 0 OR stock_days <= 0 | No | false |
| overstock_flag | boolean | Overstock indicator | stock_days > 45 | No | false |
| is_healthy_stock | boolean | Healthy stock range | 0 < stock_days <= 45 | No | true |
| cumulative_sell_in | integer | Cumulative sell-in | Running sum | No | 500 |
| cumulative_sell_out_estimated | integer | Estimated cumulative sell-out | cum_sell_in × 0.85 | No | 425 |
| avg_daily_sell_out_estimated | double | 30-day avg daily demand | Rolling avg | No | 11.76 |
| daily_sell_in | integer | Daily sell-in quantity | From fact_sell_in | No | 25 |
| product_code | string | Natural key | For debugging | No | PROD001 |
| pdv_code | string | Natural key | For debugging | No | PDV001 |
| estimation_method | string | Documents formula | Transparency | No | SELL_OUT_RATE=0.85 |
| processing_timestamp | timestamp | ETL timestamp | System | No | 2026-01-06 08:00:00 |

**Table Purpose:** Estimated stock position for risk management

**Grain:** date × product × pdv

**Partition:** date_sk

**⚠️ Estimation Assumptions:**
- Sell-out rate: 85% of sell-in
- Stock-out threshold: 0 days
- Overstock threshold: 45 days
- Lookback period: 30 days

---

### KPI Tables (Denormalized)

#### gold_kpi_market_visibility_daily

| Column Name | Data Type | Description | Business Logic | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| date_sk | integer | Date key (partition) | Grain | No | 20260106 |
| product_sk | string | Product key | Grain | Yes | a1b2c3d4e5f67890 |
| channel | string | Sales channel | Grain | No | MODERN TRADE |
| product_code | string | Product code | Denormalized | Yes | PROD001 |
| product_name | string | Product name | Denormalized | Yes | COFFEE 500G |
| brand | string | Brand | Denormalized | Yes | NESCAFE |
| category | string | Category | Denormalized | Yes | HOT DRINKS |
| date | date | Calendar date | Denormalized | No | 2026-01-06 |
| year_month | string | Period | Denormalized | No | 2026-01 |
| **Sell-In KPIs** |
| total_sell_in_qty | integer | Total units sold | SUM(quantity_sell_in) | Yes | 1500 |
| total_sell_in_value | double | Total revenue | SUM(value_sell_in) | Yes | 18750.00 |
| avg_unit_price_sell_in | double | Avg unit price | AVG(unit_price) | Yes | 12.50 |
| estimated_sell_out_qty | integer | Est. consumer sales | sell_in × 0.85 | Yes | 1275 |
| sell_in_sell_out_ratio | double | Push/Pull balance | sell_in / sell_out | Yes | 1.18 |
| pdv_count_with_sell_in | integer | Active stores | COUNT DISTINCT(pdv) | Yes | 35 |
| **Price KPIs** |
| avg_observed_price | double | Avg price observed | AVG(observed_price) | Yes | 14.99 |
| market_avg_price | double | Market average | AVG(avg_market_price) | Yes | 15.50 |
| price_competitiveness_index | double | Price index | AVG(price_index) | Yes | 96.71 |
| min_price_index | double | Lowest price index | MIN(price_index) | Yes | 85.00 |
| max_price_index | double | Highest price index | MAX(price_index) | Yes | 115.00 |
| pdv_above_market | integer | Stores above market | COUNT(is_above_market) | Yes | 12 |
| pdv_below_market | integer | Stores below market | COUNT(is_below_market) | Yes | 23 |
| **Stock KPIs** |
| total_closing_stock | integer | Total inventory | SUM(closing_stock) | Yes | 5000 |
| avg_stock_days | double | Avg days of stock | AVG(stock_days) | Yes | 12.5 |
| total_pdv_count | integer | Total stores | COUNT DISTINCT(pdv) | Yes | 50 |
| pdv_with_stock_out | integer | Stores with stock-out | COUNT(stock_out_flag) | Yes | 5 |
| pdv_with_overstock | integer | Stores with overstock | COUNT(overstock_flag) | Yes | 8 |
| pdv_with_healthy_stock | integer | Stores healthy | COUNT(is_healthy_stock) | Yes | 37 |
| availability_rate | double | % stores with stock | (total - stock_out) / total × 100 | Yes | 90.00 |
| lost_sales_qty_estimated | double | Est. lost units | stock_out_pdvs × avg_demand | Yes | 125.5 |
| lost_sales_value_estimated | double | Est. lost revenue | lost_qty × avg_price | Yes | 1880.75 |
| **Composite KPIs** |
| stock_health_score | double | Health score (0-100) | Weighted composite | Yes | 78.50 |
| **Alert Flags** |
| alert_stock_out | boolean | Stock-out alert | Any stock-out detected | No | true |
| alert_price_anomaly | boolean | Price alert | Index < 85 OR > 115 | No | false |
| alert_overstock | boolean | Overstock alert | > 30% stores overstocked | No | false |
| alert_low_availability | boolean | Availability alert | Rate < 80% | No | false |
| processing_timestamp | timestamp | ETL timestamp | System | No | 2026-01-06 08:00:00 |

**Table Purpose:** Daily operational KPIs for market visibility dashboards

**Grain:** date × product × channel

**Partition:** date_sk

**Designed For:** Power BI Direct Query, Streamlit dashboards, daily alerts

---

#### gold_kpi_market_share

| Column Name | Data Type | Description | Business Logic | Nullable | Example |
|-------------|-----------|-------------|----------------|----------|---------|
| year_month | string | Period (partition) | Grain | No | 2026-01 |
| brand | string | Product brand | Grain | No | NESCAFE |
| category | string | Product category | Grain | No | HOT DRINKS |
| channel | string | Sales channel | Grain | No | MODERN TRADE |
| **Absolute Metrics** |
| monthly_sell_in_qty | integer | Monthly units sold | SUM(total_sell_in_qty) | Yes | 45000 |
| monthly_sell_in_value | double | Monthly revenue | SUM(total_sell_in_value) | Yes | 562500.00 |
| monthly_sell_out_qty | integer | Est. monthly sell-out | SUM(estimated_sell_out_qty) | Yes | 38250 |
| monthly_lost_sales | double | Est. monthly lost sales | SUM(lost_sales_value) | Yes | 15000.00 |
| active_products | integer | Active product count | COUNT DISTINCT(product_sk) | Yes | 12 |
| **Share Metrics (Portfolio)** |
| portfolio_share_qty_pct | double | % of total portfolio units | (brand_qty / total_qty) × 100 | Yes | 25.50 |
| portfolio_share_value_pct | double | % of total portfolio value | (brand_value / total_value) × 100 | Yes | 28.75 |
| channel_share_of_brand_pct | double | % of brand in channel | (channel_qty / brand_qty) × 100 | Yes | 45.00 |
| **Trend Metrics** |
| share_change_qty_pp | double | MoM share change (pp) | current - previous month | Yes | +1.25 |
| share_change_value_pp | double | MoM value share change | current - previous month | Yes | +1.50 |
| sell_in_growth_pct | double | MoM sell-in growth % | ((current - prev) / prev) × 100 | Yes | +5.25 |
| share_trend | string | Trend direction | GAINING/STABLE/LOSING | Yes | GAINING |
| **Context Metrics** |
| avg_price_index | double | Avg price competitiveness | AVG(price_competitiveness_index) | Yes | 98.50 |
| avg_availability | double | Avg availability rate | AVG(availability_rate) | Yes | 92.00 |
| total_market_qty | integer | Total market units | SUM all brands | Yes | 176470 |
| total_market_value | double | Total market value | SUM all brands | Yes | 1960000.00 |
| brand_total_qty | integer | Brand total units | SUM across channels | Yes | 45000 |
| brand_total_value | double | Brand total value | SUM across channels | Yes | 562500.00 |
| observation_count | integer | Data points | COUNT | Yes | 1500 |
| processing_timestamp | timestamp | ETL timestamp | System | No | 2026-01-06 08:00:00 |

**Table Purpose:** Monthly market share analysis and trend detection

**Grain:** year_month × brand × category × channel

**Partition:** year_month

**⚠️ Important:** This is **portfolio share**, not true market share (requires competitor data)

**Trend Interpretation:**
- GAINING: share_change > +1pp
- STABLE: -1pp to +1pp
- LOSING: share_change < -1pp

---

## Key Performance Indicators (KPIs)

### KPI 1: Availability Rate

- **Definition:** Percentage of stores with product in stock
- **Formula:** `(Total PDVs - PDVs with Stock-Out) / Total PDVs × 100`
- **Target:** ≥ 95%
- **Source Tables:** gold_fact_stock, gold_kpi_market_visibility_daily
- **Refresh Frequency:** Daily
- **Alert Threshold:** < 80%

### KPI 2: Price Competitiveness Index

- **Definition:** Product price relative to market average
- **Formula:** `(Observed Price / Market Average Price) × 100`
- **Target:** 95-105 (at market)
- **Source Tables:** gold_fact_price_audit, gold_kpi_market_visibility_daily
- **Refresh Frequency:** Daily
- **Alert Threshold:** < 85 or > 115

### KPI 3: Sell-In / Sell-Out Ratio

- **Definition:** Balance between manufacturer push and consumer pull
- **Formula:** `Sell-In Quantity / Estimated Sell-Out Quantity`
- **Target:** 1.0 (balanced)
- **Source Tables:** gold_fact_sell_in, gold_kpi_market_visibility_daily
- **Refresh Frequency:** Daily
- **Alert Threshold:** > 1.5 (overstock risk) or < 0.7 (stock-out risk)

### KPI 4: Lost Sales Estimated

- **Definition:** Revenue at risk due to stock-outs
- **Formula:** `Stock-Out PDVs × Avg Daily Demand × Avg Unit Price`
- **Target:** Minimize
- **Source Tables:** gold_fact_stock, gold_kpi_market_visibility_daily
- **Refresh Frequency:** Daily
- **Alert Threshold:** > $10,000 daily

### KPI 5: Stock Health Score

- **Definition:** Composite inventory health score
- **Formula:** `(Availability Rate × 0.4) + (Days-of-Stock Score × 0.3) + (No-Overstock Score × 0.3)`
- **Target:** ≥ 80
- **Source Tables:** gold_kpi_market_visibility_daily
- **Refresh Frequency:** Daily
- **Alert Threshold:** < 60

### KPI 6: Portfolio Share (by Quantity)

- **Definition:** Brand's share of total portfolio units sold
- **Formula:** `(Brand Quantity / Total Portfolio Quantity) × 100`
- **Target:** Varies by brand
- **Source Tables:** gold_kpi_market_share
- **Refresh Frequency:** Monthly
- **Trend Alert:** Losing > 2pp MoM

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
| Sell-In | Units sold from manufacturer to retailer (not consumer) | 150 units sold to PDV001 in Jan 2026 |
| Sell-Out | Estimated units sold from retailer to consumer (not always observed) | 120 units estimated for PDV001 in Jan 2026 |
| SCD2 (Slowly Changing Dimension Type 2) | Dimension modeling technique that tracks historical changes with `valid_from`, `valid_to`, `is_current` | Store changes region, both versions kept with different validity dates |
| Portfolio Share | % of total sales volume/value represented by the company’s brands (not full market share) | 25.5% portfolio share for NESCAFE in HOT DRINKS |
| Availability Rate | % of stores (PDVs) with at least one unit in stock during the period | 92% availability for NESCAFE in Jan 2026 |
| Price Competitiveness Index | Ratio of observed price to market average, expressed as a percentage | 98.5 means price is 1.5% below market average |
| Gold Layer | Analytics-ready tables (star schema, SCD2, pre-aggregated KPIs) for BI consumption | gold_fact_sell_in, gold_kpi_market_share |
| Silver Layer | Standardized, deduplicated, quality-checked tables (no business KPIs) | silver_master_pdv, silver_price_audit |
| Bronze Layer | Raw ingested data with audit metadata, no business logic | bronze_master_pdv, bronze_sell_in |
| PDV | Punto de Venta (Point of Sale) – retail location tracked in the model | PDV001, SUPERMARKET ABC |
| KPI | Key Performance Indicator, pre-aggregated in Gold layer for BI | availability_rate, portfolio_share_qty_pct |
| Partition | Table division for performance, typically by date_sk or year_month | gold_fact_sell_in partitioned by date_sk |
| Surrogate Key | Hash-based unique identifier for dimension/fact rows | pdv_sk, product_sk |
| DirectQuery | Power BI mode that queries data live from Databricks Gold tables | Power BI dashboard refreshes in <2s |

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
