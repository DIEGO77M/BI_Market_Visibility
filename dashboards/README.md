# 📊 Power BI Dashboards

Interactive business intelligence dashboards connecting to Gold layer Delta tables for real-time market visibility analytics.


## 🛡️ Data Quality & Monitoring Integration

**Silver drift monitoring** is fully integrated: all Gold tables and Power BI dashboards are fed from Silver datasets with schema, quality, and volume drift monitoring. Data quality KPIs and freshness indicators are available for executive review.

**Checklist:**
- [x] Gold tables only consume Silver with validated quality
- [x] Data quality and drift KPIs available in dashboard
- [x] Monitoring results (silver_drift_history) can be visualized or queried from Power BI
- [x] Documentation and lineage traceable from dashboard to source

## 📁 Dashboard Files

### `market_visibility.pbix` (Planned)

**Purpose:** Executive dashboard for market penetration, product performance, and pricing analytics.

**Pages:**
1. **Executive Overview** - High-level KPIs and trends
2. **Sales Performance** - Sell-in analysis by PDV, product, time period
3. **Price Analysis** - Price audit observations and variance tracking
4. **Distribution Coverage** - PDV geographic and category distribution
5. **Data Quality** - Freshness indicators and validation metrics

**Data Model:**
- Star schema with fact and dimension tables from Gold layer
- Relationships: `fact_sales[pdv_id] → dim_pdv[pdv_id]`
- Date table with fiscal calendar support

---

## 📊 Key Metrics (DAX)

### Sales Metrics

```dax
Total Sales = SUM(fact_sales[sales_amount])

YTD Sales = TOTALYTD([Total Sales], dim_date[date])

Sales Growth % = 
VAR CurrentSales = [Total Sales]
VAR PreviousSales = CALCULATE([Total Sales], DATEADD(dim_date[date], -1, YEAR))
RETURN DIVIDE(CurrentSales - PreviousSales, PreviousSales, 0)
```

### Price Metrics

```dax
Average Price = AVERAGE(fact_price_audit[observed_price])

Price Variance % = 
VAR AvgPrice = [Average Price]
VAR TargetPrice = MAX(dim_product[suggested_price])
RETURN DIVIDE(AvgPrice - TargetPrice, TargetPrice, 0)
```

### Distribution Metrics

```dax
Active PDVs = DISTINCTCOUNT(fact_sales[pdv_id])

Distribution % = 
DIVIDE(
    [Active PDVs],
    CALCULATE(COUNTROWS(dim_pdv), ALL(fact_sales)),
    0
)
```

---

## 🎨 Design Principles

### User Experience
- ✅ Mobile-optimized layouts for executive access
- ✅ Consistent color scheme (brand colors)
- ✅ Tooltips with contextual information
- ✅ Drill-through for detailed exploration

### Performance
- ✅ Import mode for sub-second response
- ✅ Aggregations on large fact tables
- ✅ Incremental refresh by date (90 days full, 2 years rolling)
- ✅ Query folding validated via DAX Studio

### Governance
- ✅ Row-level security by region (if needed)
- ✅ Data source credentials in Power BI Service
- ✅ Scheduled refresh: Daily at 7:00 AM
- ✅ Refresh failure alerts to email

---

## 🚀 Deployment

### Local Development

1. Install Power BI Desktop (latest version)
2. Open `.pbix` file
3. Update data source connection:
   ```
   Databricks SQL Warehouse
   Server: dbc-fd5c2cc6-9b83.cloud.databricks.com
   HTTP Path: /sql/1.0/warehouses/...
   Catalog: workspace
   Schema: gold
   ```
4. Enter credentials (PAT token)
5. Refresh data

### Power BI Service

1. Publish to workspace: `BI_Market_Visibility`
2. Configure gateway (if on-premises data)
3. Set refresh schedule:
   ```
   Frequency: Daily
   Time: 7:00 AM (America/Bogota)
   Email on failure: diego.mayorgacapera@gmail.com
   ```
4. Share with stakeholders (Viewer role)

---

## 📸 Screenshots

*Coming soon: Add screenshots after dashboard development*

### Folder Structure

```
dashboards/
├── market_visibility.pbix      # Main dashboard file
├── screenshots/                # Preview images for README
│   ├── executive_overview.png
│   ├── sales_performance.png
│   ├── price_analysis.png
│   └── distribution.png
└── README.md                   # This file
```

---

## 🔧 Troubleshooting

### Connection Issues

**Error:** "Cannot connect to data source"
**Solution:** 
1. Verify SQL Warehouse is running in Databricks
2. Check PAT token is valid (Settings → User Settings → Access Tokens)
3. Validate HTTP path matches warehouse endpoint

### Slow Performance

**Error:** Visuals take >3 seconds to load
**Solution:**
1. Enable aggregations on large fact tables
2. Review DAX measures for efficiency (avoid CALCULATE inside iterators)
3. Reduce visual count per page (<20 recommended)
4. Use performance analyzer to identify bottlenecks

### Data Not Refreshing

**Error:** "Last refresh failed"
**Solution:**
1. Check Gold layer tables were updated (Databricks job logs)
2. Verify gateway is online (Power BI Service → Settings → Gateways)
3. Review refresh history for specific error message

---

## 💡 Next Steps

- [ ] Complete Gold layer dimensional modeling
- [ ] Build initial dashboard with 5 pages
- [ ] Add screenshots to `screenshots/` folder
- [ ] Conduct user acceptance testing with stakeholders
- [ ] Document DAX measures library
- [ ] Set up automated testing via Tabular Editor

---

**Author:** Diego Mayorga | [GitHub](https://github.com/DIEGO77M/BI_Market_Visibility)  
**Last Updated:** 2025-12-31
