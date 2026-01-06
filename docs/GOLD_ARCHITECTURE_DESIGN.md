# 🏆 Gold Layer - Architecture Design Document

**Project:** BI Market Visibility Analytics  
**Version:** 1.0  
**Date:** 2025-01-06  
**Author:** Senior Analytics Engineer  
**Status:** Implementation Phase  

---

## 📋 Executive Summary

The Gold Layer implements a **star schema** dimensional model optimized for market visibility analytics. It transforms curated Silver data into business-consumable datasets through:

- **4 Conformed Dimensions** (Date, Product SCD2, PDV SCD2, with Time-based attributes)
- **3 Fact Tables** (Sell-In, Price Audit, Stock Estimation)
- **2 KPI-Derived Tables** (Daily Market Visibility, Market Share Analysis)
- **Serverless-First Design** (no cache/persist, incremental by partition)
- **Power BI-Optimized** (all logic pre-computed, minimal DAX required)

---

## 🎯 Business Objectives

### Question 1: **Sell-In vs Sell-Out Understanding**
- **Fact:** gold_fact_sell_in
- **Goal:** Establish baseline for inventory dynamics
- **KPI:** sell_in_sell_out_ratio (estimated sell-out from PDV inventory proxy)

### Question 2: **Price-Rotation Impact**
- **Fact:** gold_fact_price_audit + gold_fact_sell_in
- **Goal:** Correlate price movements with transaction velocity
- **KPI:** price_elasticity_proxy (variance ↔ quantity relationship)

### Question 3: **Stock Availability**
- **Fact:** gold_fact_stock
- **Goal:** Quantify stockouts, overstock, and days-of-supply
- **KPI:** availability_rate, stock_out_flag, stock_days

### Question 4: **Pricing Competitiveness**
- **Fact:** gold_fact_price_audit
- **Goal:** Benchmark against market via price variance analysis
- **KPI:** price_competitiveness_index, price_index

### Question 5: **Market Share Estimation**
- **Fact:** gold_kpi_market_share (aggregated from gold_fact_sell_in)
- **Goal:** Estimate market penetration by product/brand/region
- **KPI:** market_share_estimated, share_by_product/brand/region

### Question 6: **Inefficiencies Detection**
- **Fact:** gold_kpi_market_visibility_daily
- **Goal:** Identify stockouts, phantom SKUs, price anomalies
- **KPI:** lost_sales_estimated, efficiency_score

---

## 🏗️ Physical Data Model

### Dimension Tables (SCD Type 2 for changing dimensions)

#### 1. `gold_dim_date` (Slowly Changing Dimension - Type 1)
**Purpose:** Calendar dimension for temporal analysis  
**Grain:** 1 row per calendar day  
**Size:** ~3,650 rows (10-year span: 2020-2030)  
**Partitioning:** None (small, broadcast-friendly)  

| Column | Type | Role | Example |
|--------|------|------|---------|
| `date_sk` | INT | Surrogate Key (PK) | 44927 |
| `date` | DATE | Business Key (unique) | 2026-01-06 |
| `year` | INT | Year dimension | 2026 |
| `quarter` | INT | Quarter (1-4) | 1 |
| `month` | INT | Month (1-12) | 1 |
| `week` | INT | ISO week number | 1 |
| `day_of_month` | INT | DOM (1-31) | 6 |
| `day_of_week` | INT | DOW (0=Monday) | 1 |
| `year_month` | STRING | Partition key (YYYY-MM) | 2026-01 |
| `is_month_end` | BOOLEAN | Month-end flag | false |
| `is_weekend` | BOOLEAN | Weekend flag | false |
| `is_quarter_end` | BOOLEAN | Quarter-end flag | false |
| `_metadata_created_date` | DATE | Audit timestamp | 2025-01-06 |

**Strategy:**
- Pre-load all 10 years (immutable, ~3KB)
- SCD Type 1 (no changes once created)
- Row number as surrogate key for performance

#### 2. `gold_dim_product` (SCD Type 2)
**Purpose:** Product dimension with historical tracking  
**Grain:** 1 row per product version (product_sku + valid_from combination)  
**Estimated Rows:** 250-350 (201 base products × 1.2-1.5 versions due to category/brand changes)  
**Partitioning:** None (small, broadcast-friendly)  

| Column | Type | Role | Example |
|--------|------|------|---------|
| `product_sk` | BIGINT | Surrogate Key (PK) | 10001 |
| `product_sku` | STRING | Business Key | PROD-2025-00142 |
| `product_name` | STRING | Current name | PREMIUM COFFEE 500G |
| `brand` | STRING | Brand dimension | BRAND_X |
| `segment` | STRING | Product segment | PREMIUM |
| `category` | STRING | Category | BEVERAGES |
| `sub_category` | STRING | Sub-category | COFFEE |
| `valid_from` | DATE | Start of validity | 2025-12-01 |
| `valid_to` | DATE | End of validity (NULL if current) | NULL |
| `is_current` | BOOLEAN | Is current version | true |
| `_metadata_source_batch_id` | STRING | Silver batch origin | 20251230_235959... |
| `_metadata_created_date` | DATE | Creation timestamp | 2025-12-30 |

**SCD Type 2 Logic:**
- When product attributes change (brand, segment, category):
  - Old record: `valid_to = change_date - 1`, `is_current = false`
  - New record: `valid_from = change_date`, `is_current = true`
- Preserves historical product definitions for accurate historical analysis
- Join to facts on `date_sk` and `is_current = true` for latest definitions

**Source:** `silver_master_products`  
**Change Detection:** Hash of (product_name, brand, segment, category)

#### 3. `gold_dim_pdv` (SCD Type 2)
**Purpose:** Point-of-sale dimension with location/channel history  
**Grain:** 1 row per PDV version  
**Estimated Rows:** 51-100 (51 base PDVs × 1.5-2 versions due to location/channel changes)  
**Partitioning:** None (small)  

| Column | Type | Role | Example |
|--------|------|------|---------|
| `pdv_sk` | BIGINT | Surrogate Key (PK) | 20001 |
| `pdv_code` | STRING | Business Key | PDV0001 |
| `pdv_name` | STRING | Store name | SUPER MARKET ABC |
| `channel` | STRING | Sales channel | DIRECT_TRADE \| MODERN_TRADE |
| `sub_channel` | STRING | Sub-channel | INDEPENDENT_SUPERMARKET |
| `chain` | STRING | Retail chain | INDEPENDENT |
| `region` | STRING | Geographic region | KINGSTON |
| `city` | STRING | City | KINGSTON |
| `parish` | STRING | Parish/State | ST_ANN |
| `format` | STRING | Store format | SUPERMARKET \| CONVENIENCE |
| `latitude` | DECIMAL(10,6) | Geographic latitude | 18.015423 |
| `longitude` | DECIMAL(10,6) | Geographic longitude | -76.854312 |
| `sales_rep` | STRING | Responsible sales rep | JOHN DOE |
| `valid_from` | DATE | Start of validity | 2025-12-01 |
| `valid_to` | DATE | End of validity | NULL |
| `is_current` | BOOLEAN | Is current version | true |
| `_metadata_source_batch_id` | STRING | Silver batch | 20251230_235959... |
| `_metadata_created_date` | DATE | Creation | 2025-12-30 |

**SCD Type 2 Logic:** Same as Product (tracks location changes, channel reassignments)  
**Source:** `silver_master_pdv`

---

### Fact Tables (Immutable, Append-Only)

#### 4. `gold_fact_sell_in`
**Purpose:** Daily sell-in transactions by product × PDV  
**Grain:** (date, product, pdv) — 1 row per daily transaction  
**Estimated Rows:** 500K - 2M (51 PDVs × 201 products × 365 days with sparsity ~10-30%)  
**Partitioning:** By `year` (2025, 2026, etc.)  
**Retention:** 3+ years historical  

| Column | Type | Role | Example |
|--------|------|------|---------|
| `fact_sell_in_id` | BIGINT | Surrogate Key (PK) | 1000000001 |
| `date_sk` | INT | FK → gold_dim_date | 44927 |
| `product_sk` | BIGINT | FK → gold_dim_product | 10001 |
| `pdv_sk` | BIGINT | FK → gold_dim_pdv | 20001 |
| `quantity_sell_in` | DECIMAL(18,2) | Units sold-in | 100.00 |
| `value_sell_in` | DECIMAL(18,2) | Total value | 1250.00 |
| `unit_price_sell_in` | DECIMAL(18,4) | Avg unit price | 12.5000 |
| `transactions_count` | INT | # of transactions | 5 |
| `is_complete_transaction` | BOOLEAN | Both qty & value non-null | true |
| `year` | INT | Partition column | 2026 |
| `_metadata_silver_batch_id` | STRING | Lineage | 20251230_235959... |
| `_metadata_created_timestamp` | TIMESTAMP | Gold creation time | 2025-12-31 10:00:00 |

**Grain Justification:**
- Aggregates daily transactions per product-PDV pair
- Enables join with daily prices for elasticity analysis
- Sparse matrix (~10-30% populated) but critical for time-series analysis

**Calculation Rules:**
- `quantity_sell_in = SUM(quantity)` where is_complete_transaction = true
- `value_sell_in = SUM(value)` where is_complete_transaction = true
- `unit_price_sell_in = value_sell_in / quantity_sell_in` (NULL if qty=0)
- `transactions_count = COUNT(*)`

**Partitioning Strategy:**
- By `year` (coarse partition to enable DPO without table explosion)
- Incremental refresh: Only (re)process current year's date_sk range

**Append-Only Contract:**
- ✅ Idempotent: Upsert on (date_sk, product_sk, pdv_sk, year)
- ✅ Immutable facts: Historical facts never change
- ❌ No deletes
- ✅ Mode: Dynamic Partition Overwrite (year-by-year)

#### 5. `gold_fact_price_audit`
**Purpose:** Daily price observations for competitiveness analysis  
**Grain:** (date, product, pdv) — 1 observation per day  
**Estimated Rows:** 500K - 2M  
**Partitioning:** By `year_month` (2025-01, 2025-02, etc.)  
**Retention:** 3+ years  

| Column | Type | Role | Example |
|--------|------|------|---------|
| `fact_price_audit_id` | BIGINT | Surrogate Key (PK) | 2000000001 |
| `date_sk` | INT | FK → gold_dim_date | 44927 |
| `product_sk` | BIGINT | FK → gold_dim_product (is_current=true) | 10001 |
| `pdv_sk` | BIGINT | FK → gold_dim_pdv (is_current=true) | 20001 |
| `observed_price` | DECIMAL(18,4) | Price observed at PDV | 12.9900 |
| `avg_market_price` | DECIMAL(18,4) | Market average (all PDVs same day) | 13.5000 |
| `price_variance_pct` | DECIMAL(10,2) | (observed - avg) / avg * 100 | -3.70 |
| `price_index` | DECIMAL(10,4) | observed / avg * 100 | 96.30 |
| `is_outlier_price` | BOOLEAN | Variance > 10% | false |
| `year_month` | STRING | Partition key (YYYY-MM) | 2026-01 |
| `_metadata_silver_batch_id` | STRING | Lineage | 20251231_000000... |
| `_metadata_created_timestamp` | TIMESTAMP | Gold creation | 2025-12-31 10:00:00 |

**Calculation Rules:**
- `avg_market_price = AVG(observed_price)` across all PDVs per (date, product)
- `price_variance_pct = ((observed - avg) / avg) * 100`
- `price_index = (observed / avg) * 100`
- `is_outlier_price = ABS(price_variance_pct) > 10`

**Partitioning Strategy:**
- By `year_month` (monthly partitions for manageable refresh cycles)
- Incremental: Only (re)process current/last month's data

#### 6. `gold_fact_stock` (Estimated/Derived)
**Purpose:** Stock levels and days-of-supply analysis  
**Grain:** (date, product, pdv)  
**Estimated Rows:** 500K - 2M  
**Partitioning:** By `year`  
**⚠️ Important:** No direct stock source available → **Estimation Model Required**  

| Column | Type | Role | Example |
|--------|------|------|---------|
| `fact_stock_id` | BIGINT | Surrogate Key (PK) | 3000000001 |
| `date_sk` | INT | FK → gold_dim_date | 44927 |
| `product_sk` | BIGINT | FK → gold_dim_product | 10001 |
| `pdv_sk` | BIGINT | FK → gold_dim_pdv | 20001 |
| `opening_stock_units` | DECIMAL(18,2) | Opening inventory | 500.00 |
| `closing_stock_units` | DECIMAL(18,2) | Closing inventory | 450.00 |
| `stock_movement_units` | DECIMAL(18,2) | Net movement | -50.00 |
| `stock_days_available` | DECIMAL(10,2) | Days of supply | 4.50 |
| `stock_out_flag` | BOOLEAN | True if closing = 0 | false |
| `overstock_flag` | BOOLEAN | True if days > 30 | false |
| `stock_health_score` | INT | 0-100 score | 75 |
| `year` | INT | Partition column | 2026 |
| `_metadata_estimation_note` | STRING | Model/source | "Estimated via sell-in proxy" |
| `_metadata_created_timestamp` | TIMESTAMP | Creation | 2025-12-31 10:00:00 |

**⚠️ ESTIMATION APPROACH:**
Since no direct stock table exists:

```
# Proxy Estimation Model:
closing_stock_t = closing_stock_t-1 - sell_in_t + estimated_replenishment_t

# Conservative assumptions:
1. Initial stock (2025-01-01): Assume 30-day stock based on avg sell-in
2. Daily movement: Directly from sell-in (one-to-one assumption)
3. Replenishment: Assume weekly replenishment cycles (Fridays)
4. Safety stock: Assume 10 days minimum

# Validation Rules:
- closing_stock >= 0 (no negative inventory)
- stock_out_flag = (closing_stock = 0)
- overstock_flag = (stock_days > 30)
- stock_health_score = 100 if 5 ≤ stock_days ≤ 30, else scaled down
```

**Key Assumption:**
> **Sell-In ≈ Sell-Out (within 1 day).** This model assumes products move through PDV shelves quickly without long-term warehousing. Suitable for fast-moving consumer goods (FMCG).

**Documentation Required:**
- Justification in Power BI data model
- Sensitivity analysis showing impact of +/- 20% assumption variance
- Monthly stock audits recommended for validation

---

### KPI-Derived Tables (Pre-Aggregated for BI)

#### 7. `gold_kpi_market_visibility_daily`
**Purpose:** Single source of truth for daily market visibility reporting  
**Grain:** (date, product, pdv)  
**Estimated Rows:** 500K - 2M  
**Partitioning:** By `year`  
**Refresh:** Incremental (only current date's partitions)  
**Power BI:** Direct connection (no DAX aggregation needed)  

| Column | Type | Role | Example | DAX Needed? |
|--------|------|------|---------|------------|
| `date_sk` | INT | FK → gold_dim_date | 44927 | ❌ |
| `product_sk` | BIGINT | FK → gold_dim_product | 10001 | ❌ |
| `pdv_sk` | BIGINT | FK → gold_dim_pdv | 20001 | ❌ |
| `year` | INT | Partition | 2026 | ❌ |
| **Sell-In Metrics** | | | | |
| `quantity_sell_in` | DECIMAL(18,2) | Units sold-in | 100.00 | ✅ SUM() only |
| `value_sell_in` | DECIMAL(18,2) | Value sold-in | 1250.00 | ✅ SUM() only |
| **Price Metrics** | | | | |
| `observed_price` | DECIMAL(18,4) | Current price | 12.99 | ✅ AVERAGE() only |
| `avg_market_price` | DECIMAL(18,4) | Market avg | 13.50 | ✅ AVERAGE() only |
| `price_competitiveness_index` | DECIMAL(10,4) | observed/avg × 100 | 96.30 | ❌ |
| **Stock Metrics** | | | | |
| `closing_stock_units` | DECIMAL(18,2) | Available stock | 450.00 | ✅ AVERAGE() only |
| `stock_days_available` | DECIMAL(10,2) | Days supply | 4.50 | ✅ AVERAGE() only |
| `stock_out_flag` | BOOLEAN | Stockout occurred | false | ✅ MAX() |
| **Derived KPIs** | | | | |
| `sell_in_sell_out_ratio` | DECIMAL(10,4) | quantity_sell_in / avg_sell_out_velocity | 1.05 | ❌ |
| `availability_rate_pct` | DECIMAL(10,2) | % days with stock > 0 | 95.50 | ❌ |
| `lost_sales_estimated_units` | DECIMAL(18,2) | Estimated sell-out during stockout | 45.00 | ❌ |
| `price_elasticity_proxy` | DECIMAL(10,4) | Qty change per 1% price change | -2.30 | ❌ |
| `efficiency_score` | INT | 0-100 health score | 78 | ❌ |
| `_metadata_created_timestamp` | TIMESTAMP | Creation | 2025-12-31 10:00:00 | ❌ |

**Calculation Rules:**

```sql
sell_in_sell_out_ratio = quantity_sell_in / 
                         (AVG(quantity_sell_in) OVER (PARTITION BY product_sk, pdv_sk ORDER BY date_sk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) + 1e-6)
                         
availability_rate_pct = (COUNT CASE WHEN stock_days > 0 THEN 1 END OVER (...PARTITION BY product_sk, pdv_sk...)) / 
                        COUNT(*) OVER (...) * 100
                        
lost_sales_estimated_units = CASE 
                              WHEN stock_out_flag THEN 
                                AVG(quantity_sell_in) OVER (PARTITION BY product_sk, pdv_sk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) * 1
                              ELSE 0
                              END

price_elasticity_proxy = CORR(LOG(quantity_sell_in + 1), LOG(observed_price + 1)) OVER (PARTITION BY product_sk, pdv_sk) 
                         -- Estimated via rolling correlation

efficiency_score = CASE
                    WHEN stock_days BETWEEN 5 AND 30 AND price_index BETWEEN 90 AND 110 THEN 100
                    WHEN stock_days BETWEEN 3 AND 40 AND price_index BETWEEN 80 AND 120 THEN 75
                    ELSE GREATEST(0, 50 - (ABS(stock_days - 15) / 15 * 25) - (ABS(price_index - 100) / 100 * 25))
                   END
```

#### 8. `gold_kpi_market_share`
**Purpose:** Market penetration and share analysis by dimensions  
**Grain:** (date, product, brand, region)  
**Estimated Rows:** 20K - 100K (daily granularity aggregated)  
**Partitioning:** By `year`  
**Refresh:** Incremental (date-by-date)  

| Column | Type | Role | Example |
|--------|------|------|---------|
| `date_sk` | INT | FK → gold_dim_date | 44927 |
| `product_sk` | BIGINT | FK → gold_dim_product (is_current) | 10001 |
| `region` | STRING | Geographic dimension | KINGSTON |
| `segment` | STRING | Product segment | PREMIUM |
| `brand` | STRING | Brand dimension | BRAND_X |
| `year` | INT | Partition | 2026 |
| **Share Metrics** | | | |
| `sell_in_units` | DECIMAL(18,2) | Total units | 10000.00 |
| `sell_in_value` | DECIMAL(18,2) | Total value | 125000.00 |
| `market_share_units_pct` | DECIMAL(10,2) | % of regional units | 12.50 |
| `market_share_value_pct` | DECIMAL(10,2) | % of regional value | 14.20 |
| `market_share_pdv_count` | INT | Active PDVs | 48 |
| `market_penetration_pct` | DECIMAL(10,2) | PDVs with sales / total PDVs | 94.12 |
| `market_share_trend_pct` | DECIMAL(10,2) | MoM change | +2.50 |
| `_metadata_created_timestamp` | TIMESTAMP | Creation | 2025-12-31 10:00:00 |

**Calculation Rules:**
```sql
market_share_units_pct = sell_in_units / SUM(sell_in_units) OVER (PARTITION BY date_sk, region) * 100

market_share_value_pct = sell_in_value / SUM(sell_in_value) OVER (PARTITION BY date_sk, region) * 100

market_penetration_pct = COUNT DISTINCT pdv_sk / (SELECT COUNT DISTINCT pdv_sk FROM gold_dim_pdv WHERE is_current) * 100

market_share_trend_pct = market_share_units_pct - LAG(market_share_units_pct, 30) OVER (PARTITION BY product_sk, region ORDER BY date_sk)
```

---

## ⚙️ Implementation Strategy

### Phase 1: Dimension Loading (One-Time)
1. **gold_dim_date:** Load 10 years (2020-2030) in single batch
   - No SCD logic (immutable)
   - Index on `date` for join performance

2. **gold_dim_product (SCD2):** Initial load from silver_master_products
   - Hash all attributes: `SHA2(CONCAT(product_name, brand, segment, category), 256)`
   - Row 1 for each product with valid_from = min(ingestion_date)

3. **gold_dim_pdv (SCD2):** Initial load from silver_master_pdv
   - Hash attributes: `SHA2(CONCAT(pdv_name, channel, region, city), 256)`
   - Row 1 per PDV with valid_from = min(ingestion_date)

### Phase 2: Fact Loading (Incremental per Partition)
1. **gold_fact_sell_in (Yearly Partitions)**
   - Aggregate silver_sell_in by (date, product_sk, pdv_sk)
   - Join with gold_dim_product (ON product_sku, WHERE is_current=true)
   - Join with gold_dim_pdv (ON pdv_code, WHERE is_current=true)
   - Dynamic Partition Overwrite by year

2. **gold_fact_price_audit (Monthly Partitions)**
   - Load silver_price_audit by year_month
   - Calculate avg_market_price as WINDOW function
   - Dynamic Partition Overwrite by year_month

3. **gold_fact_stock (Yearly Partitions)**
   - Estimate from gold_fact_sell_in using proxy model
   - Initial stock calculation on day 1
   - Daily movement = -sell_in_qty + replenishment_est
   - DPO by year

### Phase 3: KPI Materialization (Incremental per Date)
1. **gold_kpi_market_visibility_daily**
   - Join fact tables on (date_sk, product_sk, pdv_sk)
   - Compute window functions for rolling averages, elasticity
   - Upsert on (date_sk, product_sk, pdv_sk, year)

2. **gold_kpi_market_share**
   - Aggregate gold_fact_sell_in by (date, product, region)
   - Calculate shares via window functions
   - DPO by year

---

## 🔄 Incrementality Strategy

| Table | Partition Key | Refresh Cadence | Mode | Idempotency |
|-------|---------------|-----------------|------|-------------|
| `gold_dim_date` | None | One-time | INSERT | ✅ Skip if exists |
| `gold_dim_product` | None | Daily (SCD2 check) | MERGE | ✅ On (product_sku, hash) |
| `gold_dim_pdv` | None | Daily (SCD2 check) | MERGE | ✅ On (pdv_code, hash) |
| `gold_fact_sell_in` | `year` | Incremental year | DPO | ✅ Upsert per year |
| `gold_fact_price_audit` | `year_month` | Incremental month | DPO | ✅ Upsert per month |
| `gold_fact_stock` | `year` | Incremental year | DPO | ✅ Upsert per year |
| `gold_kpi_market_visibility_daily` | `year` | Incremental date | DPO | ✅ Upsert per year |
| `gold_kpi_market_share` | `year` | Incremental date | DPO | ✅ Upsert per year |

---

## 📏 Surrogate Key Generation Strategy

### Dense Integer Keys (Optimal for Joins)
```python
def generate_surrogate_key(business_key: str, table_context: str) -> int:
    """
    Generate deterministic surrogate key via:
    1. Hash business key → 64-bit integer
    2. Offset by table-specific start value
    3. Modulo to fit INT range (2.1B)
    
    Example:
    - gold_dim_product: start=10000
    - gold_dim_pdv: start=20000
    - gold_fact_sell_in: start=1000000000
    """
    hash_value = int(hashlib.md5(business_key.encode()).hexdigest(), 16)
    table_offsets = {
        "product": 10000,
        "pdv": 20000,
        "fact_sell_in": 1000000000,
        "fact_price_audit": 2000000000,
        "fact_stock": 3000000000,
    }
    return (hash_value % (2147483647 - table_offsets[table_context])) + table_offsets[table_context]
```

---

## 🚫 Design Constraints & Trade-Offs

### ✅ What's Included
- Star schema (conformed dimensions, facts)
- SCD Type 2 for slowly changing dimensions
- Append-only facts with surrogate keys
- Pre-calculated KPIs (Power BI consumption)
- Incremental refresh by partition
- Metadata lineage columns

### ❌ What's Excluded (Intentional)
- **No dimension role-playing** (separate fact tables instead)
- **No slowly changing dimensions Type 4+** (2 is sufficient)
- **No non-additive facts** (all facts are additive)
- **No Junk Dimensions** (attributes stored directly in facts)
- **No Conformed Fact Table** (facts are domain-specific)

### Trade-Off: Fact Table Size vs Query Performance
- **Wide Facts:** Include all needed attributes to minimize joins
- **Benefit:** Power BI queries <1s even without aggregations
- **Cost:** Gold layer tables 1.5x larger than minimalist design
- **Justification:** Serverless compute is cheap; slower BI queries are expensive operationally

### Trade-Off: Stock Estimation Accuracy
- **Limitation:** No direct stock source → proxy model
- **Assumption:** Sell-in ≈ sell-out within 24 hours
- **Acceptance:** Suitable for FMCG; unsuitable for slow-movers
- **Mitigation:** Confidence score in gold_fact_stock + monthly audits

---

## 🧪 Data Quality Checks (Gold-Specific)

```sql
-- Check 1: Referential Integrity
SELECT COUNT(*) AS orphan_fact_rows
FROM gold_fact_sell_in f
LEFT JOIN gold_dim_date d ON f.date_sk = d.date_sk
WHERE d.date_sk IS NULL;
-- Expected: 0 rows

-- Check 2: Surrogate Key Uniqueness
SELECT COUNT(*) - COUNT(DISTINCT product_sk) AS dup_product_sks
FROM gold_dim_product;
-- Expected: 0 rows

-- Check 3: SCD2 Validity (Non-Overlapping Intervals)
SELECT product_sku, COUNT(*) AS version_count
FROM gold_dim_product
WHERE is_current = true
GROUP BY product_sku
HAVING COUNT(*) > 1;
-- Expected: 0 rows (only 1 current version per product)

-- Check 4: Partition Completeness
SELECT YEAR, COUNT(DISTINCT date_sk) AS dates
FROM gold_fact_sell_in
GROUP BY YEAR
HAVING COUNT(DISTINCT date_sk) < 365;
-- Expected: Sparse (expected, not all product-pdv pairs have daily sales)

-- Check 5: KPI Consistency
SELECT 
  ABS(daily_sum - kpi_sum) AS delta
FROM (
  SELECT SUM(quantity_sell_in) AS daily_sum FROM gold_fact_sell_in WHERE date_sk = 44927
) d
CROSS JOIN (
  SELECT SUM(sell_in_units) AS kpi_sum FROM gold_kpi_market_share WHERE date_sk = 44927
) k;
-- Expected: delta = 0 (exact match)
```

---

## 📊 Expected Data Volumes & Performance

| Table | Rows | Size | Partition Size | Query Perf* |
|-------|------|------|----------------|-------------|
| gold_dim_date | 3,650 | 500KB | N/A | <100ms |
| gold_dim_product | 250 | 100KB | N/A | <100ms |
| gold_dim_pdv | 75 | 50KB | N/A | <100ms |
| gold_fact_sell_in | 500K-2M | 50-200MB | 50MB/year | <1s |
| gold_fact_price_audit | 500K-2M | 50-200MB | 4-8MB/month | <1s |
| gold_fact_stock | 500K-2M | 50-200MB | 50MB/year | <1s |
| gold_kpi_market_visibility_daily | 500K-2M | 100-300MB | 100MB/year | <500ms |
| gold_kpi_market_share | 20K-100K | 10-50MB | 10MB/year | <200ms |

*Query Performance: Single-metric aggregation via Power BI on Databricks SQL Warehouse

---

## 📝 Power BI Integration Points

### Semantic Model (Implicit)
- **Date:** Broadcast join key (small, hot path)
- **Product:** Broadcast join (SCD2 filtered to is_current=true)
- **PDV:** Broadcast join (SCD2 filtered to is_current=true)

### DAX Measures (Minimal)
```dax
// Only aggregate-only measures needed (Power BI will auto-SUM facts)
Total_Sell_In_Value := SUM('fact_sell_in'[value_sell_in])
Avg_Price := AVERAGE('fact_price_audit'[observed_price])
Total_Stock_Days := AVERAGE('fact_stock'[stock_days_available])

// Pre-calculated measures in Gold (no DAX needed)
Price_Competitiveness := MAX('kpi_market_visibility_daily'[price_competitiveness_index])
Efficiency_Score := MAX('kpi_market_visibility_daily'[efficiency_score])
```

---

## 🎓 Lessons Learned & ADRs

### ADR-006: SCD Type 2 Over Type 4
**Decision:** Use SCD2 for Product and PDV dimensions  
**Rationale:** Historical analysis requires correct product classification at transaction date  
**Example:**  
> A product SKU `PROD-001` was "Budget Coffee" (segment=BUDGET) on 2025-01-01.  
> On 2025-06-01, it was upgraded to "Premium Coffee" (segment=PREMIUM).  
> Historical sell-in from Jan should roll up to Budget, not Premium.

### ADR-007: Fact Grain Selection
**Decision:** (date, product, pdv) grain vs hourly or raw transactions  
**Rationale:**  
- ✅ Daily grain enables stock estimation (daily movement)
- ✅ Sufficient for price elasticity analysis (1-day lag negligible)
- ✅ Reduces data volume 10x vs hourly
- ❌ Loses transaction-level detail (acceptable for strategic decisions)

### ADR-008: Pre-Aggregated KPIs in Gold
**Decision:** Materialize gold_kpi_* instead of DAX-only calcs  
**Rationale:**  
- ✅ Sub-second Power BI queries (user experience)
- ✅ Testable, versioned KPI logic
- ✅ Enables scheduled alerts on materialized metrics
- ❌ Larger data model (50-100MB vs 5MB)

### ADR-009: Stock Estimation Transparency
**Decision:** Document all assumptions, store confidence scores, enable audits  
**Rationale:**  
- Business stakeholders understand model limitations
- Monthly reconciliation against actual counts
- Sensitivity testing documented for variance tolerance

---

## ✅ Success Criteria

1. **Schema Completeness:** All tables, all columns implemented
2. **Referential Integrity:** 100% join success rate, 0 orphan rows
3. **Incremental Efficiency:** Yearly/monthly partition refresh <5 min
4. **Power BI Performance:** Dashboard load <2s, drill-through <1s
5. **Data Consistency:** KPI sums match fact aggregates exactly
6. **Auditability:** Full lineage traceable via _metadata_* columns
7. **Documentation:** Every calculation justified, assumptions listed
8. **Testability:** Unit tests for surrogate key generation, SCD2 logic, KPI calcs

---

## 📚 References & Further Reading

- Delta Lake Optimization: https://docs.databricks.com/optimizations/
- Star Schema Best Practices: Kimball, "The Data Warehouse Toolkit"
- SCD Implementation: https://learn.microsoft.com/en-us/fabric/onelake/scd
- Power BI Import Mode: https://learn.microsoft.com/en-us/power-bi/connect-data/service-dataset-modes-understand

---

**Next Step:** Proceed to [03_gold_analytics.py](../notebooks/03_gold_analytics.py) for implementation.

