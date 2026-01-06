# 📊 Power BI Dashboards

**Connection:** Direct to Gold layer Delta tables via Databricks connector

## Setup

1. Open Power BI Desktop
2. **Get Data** → **Azure Databricks**
3. Enter Databricks workspace URL + token
4. Import Gold tables: `gold_dim_*`, `gold_fact_*`, `gold_kpi_*`

## Data Model

**Star Schema** (8 tables):
- 3 Dimensions: Date, Product (SCD2), PDV (SCD2)
- 3 Facts: Sell-In, Price Audit, Stock
- 2 KPIs: Market Visibility, Market Share

**Important:** Filter dimensions by `is_current = TRUE` for SCD2 handling.

## Key DAX Measures

```dax
Total Sales = SUM(gold_fact_sell_in[total_value])
Avg Price Index = AVERAGE(gold_fact_price_audit[price_index])
Market Share % = SUM(gold_kpi_market_share[market_share_pct])
```

## Screenshots

Dashboard screenshots will be added in `screenshots/` folder.

See [Power BI Integration Guide](../docs/POWERBI_INTEGRATION_GUIDE.md) for detailed setup.
