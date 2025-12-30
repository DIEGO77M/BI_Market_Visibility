# Power BI Dashboards

## Dashboard Files

### market_visibility.pbix
Main Power BI dashboard for Market Visibility analysis.

**Pages:**
1. **Overview:** Executive summary with key KPIs
2. **Detailed Analysis:** Drill-down views and trends
3. **Comparisons:** Period-over-period comparisons
4. **Data Quality:** Data freshness and quality metrics

**Key Features:**
- Interactive filters by date, region, category
- Drill-through capabilities
- Bookmarks for different views
- Mobile layout optimized

## Data Sources

The dashboard connects to the following data sources:

- **Gold Layer Tables:**
  - `fact_[table_name]`
  - `dim_[dimension_name]`

**Connection Type:** Import/DirectQuery
**Refresh Schedule:** Daily at 7:00 AM

## DAX Measures

### Key Measures

```dax
Total Sales = SUM(fact_sales[sales_amount])

YTD Sales = TOTALYTD([Total Sales], dim_date[date])

Sales Growth % = 
DIVIDE(
    [Total Sales] - [Total Sales PY],
    [Total Sales PY],
    0
)
```

See complete DAX documentation in [dax_measures.md](dax_measures.md)

## Screenshots

Preview of key dashboard pages:

### Overview Page
![Overview](screenshots/dashboard_overview.png)

### Detailed Analysis
![Details](screenshots/dashboard_details.png)

### Comparisons
![Comparisons](screenshots/dashboard_comparisons.png)

## Performance Optimization

1. **Data Model:**
   - Star schema design
   - Aggregations for large fact tables
   - Disabled auto date/time

2. **Visuals:**
   - Limited number of visuals per page
   - Optimized DAX measures
   - Appropriate visual types

3. **Refresh:**
   - Incremental refresh configured
   - Partition by date range

## Usage Instructions

### Opening the Dashboard

1. Install Power BI Desktop (latest version)
2. Open `market_visibility.pbix`
3. Update data source connections if needed
4. Refresh data

### Publishing to Power BI Service

1. Save the .pbix file
2. File → Publish → Publish to Power BI
3. Select workspace
4. Configure scheduled refresh in service

### Troubleshooting

**Issue:** Data source connection error
- **Solution:** Update data source credentials in Transform Data → Data Source Settings

**Issue:** Slow performance
- **Solution:** Check query folding in Power Query, optimize DAX measures

**Issue:** Visuals not displaying
- **Solution:** Verify data is loaded, check filters

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2024-01-15 | [Your Name] | Initial dashboard |

## Contact

For dashboard issues or enhancement requests, contact [Your Email]
