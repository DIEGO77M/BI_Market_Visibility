# 📊 Data Directory

**Medallion Architecture Layers** (Unity Catalog managed)

```
Bronze → Silver → Gold → Power BI
(raw)    (clean)  (star)
```

## Tables Location

All tables stored in **Unity Catalog**: `workspace.default.*`

| Layer | Tables | Format |
|-------|--------|--------|
| Bronze | `bronze_master_pdv`, `bronze_master_products`, `bronze_price_audit`, `bronze_sell_in` | Delta |
| Silver | `silver_master_pdv`, `silver_master_products`, `silver_price_audit`, `silver_sell_in` | Delta |
| Gold | `gold_dim_*`, `gold_fact_*`, `gold_kpi_*` (8 tables) | Delta |

## Subfolders

- `bronze/` - Local samples (optional)
- `silver/` - Local samples (optional)  
- `gold/` - Local samples (optional)

**Note:** Production data lives in Databricks Unity Catalog, not local files.
