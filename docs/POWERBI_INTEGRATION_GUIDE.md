# Power BI Integration Guide - Gold Layer

**Project:** BI Market Visibility  
**Version:** 1.0  
**Date:** 2025-01-06  
**Audience:** Power BI Developers, Analytics Consumers  

---

## 📊 Overview

This guide explains how to connect Power BI to the Gold layer for market visibility analytics.

**Key Benefits:**
- ✅ Pre-calculated KPIs (minimal DAX required)
- ✅ Star schema (simple drill-through navigation)
- ✅ SCD Type 2 dimensions (historical accuracy)
- ✅ Append-only facts (incremental refresh friendly)
- ✅ Sub-second query performance (Serverless Databricks)

---

## 🔌 Connection Setup

### Data Source Configuration

#### 1. Databricks Connection
In Power BI Desktop:
1. **Get Data** → **Databricks**
2. Enter Databricks workspace URL:
   ```
   https://<workspace-region>.cloud.databricks.com
   ```
3. Select **Lake House** connection mode
4. Authenticate with Databricks PAT token

#### 2. Schema Selection
Select tables from `default` schema:
- ✅ `gold_dim_date`
- ✅ `gold_dim_product`
- ✅ `gold_dim_pdv`
- ✅ `gold_fact_sell_in`
- ✅ `gold_fact_price_audit`
- ✅ `gold_fact_stock`
- ✅ `gold_kpi_market_visibility_daily` ← **Primary KPI table**
- ✅ `gold_kpi_market_share` ← **Secondary KPI table**

---

## 🎯 Semantic Model Design

### Relationships (Star Schema)

```
            gold_dim_date
                 ↑
                 │ (1:M)
                 │
gold_kpi_market_visibility_daily
      ↓         │         ↓
    (M:1)      │        (M:1)
      │        │         │
gold_dim_   gold_dim_   gold_dim_
product     pdv         (via KPI)
(SCD2)      (SCD2)
```

### Dimension Relationships

#### 1. Date Relationship
- **Fact Column:** `gold_kpi_market_visibility_daily[date_sk]`
- **Dimension Column:** `gold_dim_date[date_sk]`
- **Cardinality:** Many-to-One
- **Join Type:** Left (all KPIs have valid date_sk)
- **Cross Filter:** Both directions

#### 2. Product Relationship
- **Fact Column:** `gold_kpi_market_visibility_daily[product_sk]`
- **Dimension Column:** `gold_dim_product[product_sk]` **(filter: is_current = True)**
- **Cardinality:** Many-to-One
- **Join Type:** Left
- **Cross Filter:** Both directions
- **⚠️ IMPORTANT:** Filter dimension to `is_current = TRUE` to avoid double-counting in SCD2

#### 3. PDV Relationship
- **Fact Column:** `gold_kpi_market_visibility_daily[pdv_sk]`
- **Dimension Column:** `gold_dim_pdv[pdv_sk]` **(filter: is_current = True)**
- **Cardinality:** Many-to-One
- **Join Type:** Left
- **Cross Filter:** Both directions
- **⚠️ IMPORTANT:** Filter to current versions only

---

## 📐 Data Model Configuration (Power BI Desktop)

### Step 1: Import Tables
1. Import all 8 tables
2. Set `date` column in `gold_dim_date` as **Date Table**
   - Right-click table → **Mark as Date Table**
   - Select `date` as date column

### Step 2: Configure Relationships
1. Disable auto-relationships (to avoid many-to-many traps)
2. Manually create 3 relationships (date, product, pdv)
3. Set cardinality to M:1
4. Set cross-filter to **Both**

### Step 3: Hide Technical Columns
Hide in model view:
- `date_sk`, `product_sk`, `pdv_sk` (used only for joins)
- `_metadata_created_timestamp`, `_metadata_source_batch_id` (audit columns)
- `attribute_hash` (SCD tracking)
- `year` (partition column, used for incremental refresh only)

### Step 4: Configure Column Visibility
Show in model view:
- **Dimensions:** Natural language columns (product_name, pdv_name, region, city, etc.)
- **Facts:** Business-meaningful metrics (quantity_sell_in, value_sell_in, observed_price, etc.)
- **KPIs:** Pre-calculated metrics (efficiency_score, availability_rate_pct, market_share_pct, etc.)

---

## 📈 Recommended Measures (DAX)

### Simple Aggregate Measures (Auto-Sum)
Power BI will automatically aggregate these correctly:

```dax
// NO DAX needed - let Power BI SUM automatically
[Total Sell-In Units] := SUM('gold_kpi_market_visibility_daily'[quantity_sell_in])
[Total Sell-In Value] := SUM('gold_kpi_market_visibility_daily'[value_sell_in])
[Average Price] := AVERAGE('gold_kpi_market_visibility_daily'[observed_price])
[Total Stock] := SUM('gold_kpi_market_visibility_daily'[closing_stock_units])
```

### Pre-Calculated KPIs (Already Computed in Gold)
Since these are pre-aggregated in the Gold layer, use **MAX** or **AVERAGE** for rollup:

```dax
// These are pre-calculated in Gold Layer - just aggregate them
[Efficiency Score] := MAX('gold_kpi_market_visibility_daily'[efficiency_score])
[Availability Rate] := AVERAGE('gold_kpi_market_visibility_daily'[availability_rate_pct])
[Price Competitiveness Index] := AVERAGE('gold_kpi_market_visibility_daily'[price_competitiveness_index])
[Stock Days Available] := AVERAGE('gold_kpi_market_visibility_daily'[stock_days_available])

[Market Share %] := AVERAGE('gold_kpi_market_share'[market_share_units_pct])
[Market Penetration %] := AVERAGE('gold_kpi_market_share'[market_penetration_pct])
```

### Context-Aware Measures (Optional DAX)
These are optional if deeper analysis needed:

```dax
// Stockout Indicator
[Stockout Flag] := MAX('gold_kpi_market_visibility_daily'[stock_out_flag])

// Lost Sales Estimation
[Est. Lost Sales Units] := SUM('gold_kpi_market_visibility_daily'[lost_sales_estimated_units])

// Sell-In/Sell-Out Ratio
[Sell-In/Out Ratio] := AVERAGE('gold_kpi_market_visibility_daily'[sell_in_sell_out_ratio])
```

---

## 📊 Recommended Visualizations

### Page 1: Market Overview
- **KPI Cards:** Total Sell-In (Units & Value), Avg Price, Availability Rate
- **Line Chart:** Sell-In Quantity by Date (time series)
- **Stacked Bar:** Sell-In by Region (geographic breakdown)
- **Clustered Bar:** Market Share by Product/Brand

### Page 2: Price Analysis
- **Line Chart:** Price Index Trend (observed vs market average)
- **Scatter Plot:** Price vs Quantity (elasticity proxy)
- **Heatmap:** Price Variance by Product × Region (outlier detection)
- **Card:** Price Competitiveness Index (scalar KPI)

### Page 3: Stock & Availability
- **Area Chart:** Stock Days Trend
- **KPI Cards:** Availability Rate %, Stockout Count, Overstock Count
- **Matrix:** Stock Health Score by Product × PDV (drill-down enabled)
- **Map:** Stockouts by Location (using PDV latitude/longitude)

### Page 4: Market Share
- **Pie Chart:** Market Share by Brand (current period)
- **Line Chart:** Market Share Trend by Product (30-day moving average)
- **Clustered Bar:** Market Penetration by Region
- **Card:** Market Share Trend % (growth/decline)

---

## 🔄 Incremental Refresh Setup

Power BI can efficiently refresh only changed data via incremental refresh.

### Prerequisites
1. **Storage Mode:** Set fact tables to **Import** (not DirectQuery)
2. **Partition Column:** Use `year` (for `gold_fact_*` tables)

### Configuration Steps
1. Right-click table → **Incremental Refresh**
2. Set parameters:
   - **Detection Partition:** `year`
   - **Full Refresh Range:** Last 3 years
   - **Incremental Refresh:** Last 1 month

### Refresh Schedule
```
Daily 08:00 UTC - Incremental refresh (current month)
Sunday 02:00 UTC - Full refresh (3-year history)
```

**Expected Refresh Times:**
- Incremental: <2 minutes (only current month's partitions)
- Full: <15 minutes (3 years of data)

---

## ⚠️ Important Considerations

### 1. SCD Type 2 Handling
**Problem:** Dimensions contain multiple versions per business key (valid_from/valid_to intervals).

**Solution:** Always filter to `is_current = TRUE` in relationships:
- Prevents double-counting in aggregations
- Ensures current product classifications used
- Historical analysis automatically correct (facts reference historical versions via dates)

```dax
// CORRECT: Filter to current version only
[Current Product Count] := CALCULATE(
    DISTINCTCOUNT('gold_dim_product'[product_sk]),
    FILTER('gold_dim_product', 'gold_dim_product'[is_current] = TRUE)
)

// INCORRECT: Counts all versions (inflates metrics)
[Wrong Count] := DISTINCTCOUNT('gold_dim_product'[product_sk])
```

### 2. Stock Estimation Limitations
**Assumption:** Sell-In ≈ Sell-Out (within 1 day)  
**Suitable for:** FMCG (fast-moving consumer goods)  
**Not suitable for:** Slow-movers, seasonal products, long shelf-life items

**Validation:** Monthly stock audits recommended to compare actual vs estimated

**User Guidance:**
> "Stock availability estimates are based on historical sell-in patterns. May not reflect actual physical inventory. Use for trend analysis; validate critical decisions with physical counts."

### 3. Query Performance
**Fact Table Size:** 500K - 2M rows per year  
**Dimension Size:** 50-350 rows per dimension  
**Expected Query Time:** <1s for single metric, <2s for dashboard load

**Optimization Tips:**
- Use slicers (date range, region) to reduce fact table cardinality
- Avoid cross-filters on low-cardinality columns (causes Cartesian joins)
- Use **Performance Analyzer** in Power BI Desktop to identify slow queries

### 4. Refresh Logic
**Fact Tables:** Dynamic Partition Overwrite (yearly partitions)
- Re-running same year overwrites completely
- Idempotent: safe to run multiple times
- No merge logic needed (facts immutable)

**Dimensions:** SCD2 logic (insert new versions, close old)
- Running daily enables rapid fact updates
- Queries automatically see current dimensions
- Historical analysis preserved via date_sk

---

## 📋 Pre-Launch Checklist

- [ ] All 8 tables connected and visible
- [ ] 3 relationships created (date, product, pdv)
- [ ] Date table marked and configured
- [ ] Dimensions filtered to `is_current = TRUE` in relationships
- [ ] Technical columns hidden from model view
- [ ] DAX measures created for key KPIs
- [ ] Test queries execute in <2s
- [ ] Visualizations configured and styled
- [ ] Incremental refresh schedule set
- [ ] Documentation updated with business glossary
- [ ] Users trained on navigating SCD2 dimensions

---

## 🔍 Troubleshooting

### Issue: Measures Sum to Wrong Totals
**Cause:** Dimension not filtered to current versions (SCD2 duplication)  
**Solution:** Add filter: `is_current = TRUE` to dimension in relationship

### Issue: Blank Cells in Fact Aggregations
**Cause:** Outer joins on mismatched keys (orphan facts)  
**Solution:** Run validation: `SELECT COUNT(*) FROM fact WHERE {fk} NOT IN (SELECT pk FROM dim)`

### Issue: Slow Dashboard Load (>5s)
**Cause:** Large fact tables without partitioning; cross-filters causing Cartesian joins  
**Solution:**
1. Add slicers for date range (reduces fact cardinality)
2. Remove cross-filters on non-key columns
3. Use DirectQuery for specific large tables
4. Archive old years (pre-2023) to separate dataset

### Issue: Inconsistent Values Between Refreshes
**Cause:** Running both old and new batch logic; SCD2 duplicates not closed properly  
**Solution:**
1. Clear partition cache (Power BI Desktop → Options → Data → Clear Cache)
2. Run full refresh (not incremental) to rebuild from Gold
3. Verify Gold layer `is_current = TRUE` filters active

---

## 📞 Support & Escalation

| Issue | Owner | Contact |
|-------|-------|---------|
| Data accuracy / Gold layer logic | Analytics Engineering | #data-platform |
| Power BI model / DAX | Power BI Developer | #analytics-bi |
| Incremental refresh failures | Databricks Admin | #cloud-support |
| Business metric definitions | Product Analyst | #product-analytics |

---

## 📚 Related Documentation

- [Gold Layer Architecture Design](./GOLD_ARCHITECTURE_DESIGN.md)
- [Data Dictionary](./data_dictionary.md)
- [Medallion Architecture Overview](../README.md)

