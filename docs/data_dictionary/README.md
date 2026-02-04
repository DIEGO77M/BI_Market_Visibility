
# Data Dictionary – BI Market Visibility

> Formal definitions, business rules, ownership, SLA, and audience for all key tables, fields, and metrics.

---

## Table Overview & Ownership

| Table                        | Grain                | Owner      | SLA (Freshness) | Audience         | Description                                 |
|------------------------------|----------------------|------------|-----------------|------------------|---------------------------------------------|
| bronze_master_products       | Product              | Data Eng   | Daily           | BI, Analyst      | Raw product master data                     |
| bronze_master_pdv            | Point of Sale (PDV)  | Data Eng   | Daily           | BI, Analyst      | Raw POS master data                         |
| bronze_price_audit           | Product–PDV–Date     | Data Eng   | Daily           | BI, Analyst      | Raw price audit data                        |
| bronze_sell_in               | Product–PDV–Month    | Data Eng   | Daily           | BI, Analyst      | Raw sell-in data                            |
| silver_dim_product           | Product              | Analytics  | Daily           | BI, Analyst      | Cleaned/enriched product dimension          |
| silver_dim_pdv               | PDV                  | Analytics  | Daily           | BI, Analyst      | Cleaned/enriched PDV dimension              |
| silver_fact_price_audit      | Product–PDV–Date     | Analytics  | Daily           | BI, RGM, Exec    | Cleaned/enriched price audit fact           |
| silver_fact_sell_in          | Product–PDV–Month    | Analytics  | Daily           | BI, RGM, Exec    | Cleaned/enriched sell-in fact               |
| gold_fact_pdv_monthly_health | Product–PDV–Month    | BI Lead    | Monthly         | Exec, RGM, BI    | Executive-ready monthly health metrics      |
| gold_fact_pdv_price_audit    | Product–PDV–Month    | BI Lead    | Monthly         | Exec, RGM, BI    | Price audit KPIs and competitive metrics    |
| gold_mart_revenue_leakage    | Product–PDV–Date     | BI Lead    | Monthly         | Exec, Finance    | Revenue leakage analytics                   |

---

## Column-Level Definitions & Business Rules

### bronze_price_audit

| Column                        | Type         | Nullable | Business Rule                        | Example   | Source                |
|-------------------------------|--------------|----------|--------------------------------------|-----------|-----------------------|
| Fecha                         | STRING       | NOT NULL | Valid date format (YYYY-MM-DD)       | 2023-05-01| price_audit_raw       |
| Cod_PDV                       | STRING       | NOT NULL | Must exist in master_pdv             | 1001      | price_audit_raw       |
| Cod_Producto                  | STRING       | NOT NULL | Must exist in master_products        | P123      | price_audit_raw       |
| Precio                        | DECIMAL(10,2)| NOT NULL | > 0                                  | 12.99     | price_audit_raw       |
| Promotional Price             | DECIMAL(10,2)| NULL     | If promo, must be < Precio           | 10.99     | price_audit_raw       |
| Competitive_Group             | STRING       | NULL     | Must match allowed values            | "A"       | price_audit_raw       |
| Tiene este producto una promocion?| STRING   | NULL     | Yes/No                              | Yes       | price_audit_raw       |

### gold_fact_pdv_monthly_health

| Column                    | Type         | Nullable | Business Rule                                  | Example   | Source                        |
|---------------------------|--------------|----------|------------------------------------------------|-----------|-------------------------------|
| date                      | DATE         | NOT NULL | Must exist in dim_date                         | 2023-05-01| silver_fact_sell_in           |
| pdv_code                  | STRING       | NOT NULL | Must exist in dim_pdv                          | 1001      | silver_fact_sell_in           |
| product_code              | STRING       | NOT NULL | Must exist in dim_product                      | P123      | silver_fact_sell_in           |
| closing_stock_units       | INT          | NOT NULL | >= 0                                           | 120        | silver_fact_sell_in           |
| days_of_inventory         | INT          | NOT NULL | >= 0                                           | 15         | silver_fact_sell_in           |
| inventory_action_signal   | STRING       | NOT NULL | Must be in allowed signals                     | STOCKOUT   | calculated                    |
| data_confidence_score     | DECIMAL(3,1) | NOT NULL | 0.0 <= score <= 1.0                            | 0.95       | calculated                    |
| coverage_compliant        | BOOLEAN      | NOT NULL | TRUE if in_stock and in_expected_assortment    | TRUE       | calculated                    |
| gold_processed_at         | TIMESTAMP    | NOT NULL | Audit field                                    | 2026-02-03 | gold pipeline                  |

---

## Metric Definitions (Formal)

| Metric Name                | Definition                                              | Formula                                      | Periodicity | Aggregation Level | Exclusions                |
|----------------------------|--------------------------------------------------------|-----------------------------------------------|-------------|-------------------|---------------------------|
| Inventory Turnover         | Efficiency of inventory usage                          | Sell_In_Units / Avg_Inventory                | Monthly     | PDV–Product–Month | Null/zero inventory       |
| Promo Rate                 | % of products sold under promotion                     | SUM(Promo_Sales) / SUM(Total_Sales)          | Monthly     | PDV–Product–Month | Null/zero sales           |
| Price Index vs Competition | Relative price vs competitive group                    | Price / Avg_Competitor_Price                  | Monthly     | PDV–Product–Month | Null/zero competitor price|
| Stock Risk Level           | Categorization of inventory risk                       | CASE WHEN ...                                 | Monthly     | PDV–Product–Month | Null/zero inventory       |
| Revenue Leakage %          | Composite score of lost revenue opportunity            | Weighted sum of risk factors                  | Monthly     | PDV–Product–Date  | Null/zero sales/inventory |

---

## Data Quality Flags & Critical Fields

- All tables include audit fields (_ingestion_timestamp, _load_date, _source_file, _batch_id)
- Data quality flags: nulls, duplicates, referential integrity, business rule violations
- Critical fields: price, promotional_price, inventory_action_signal, data_confidence_score

---

## Documentation

- [Project Architecture](../architecture/README.md)
- [Technical Specs](../technical_specs/README.md)

---

## License

MIT
