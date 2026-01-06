# 📓 Databricks Notebooks

**Medallion Architecture Pipeline:** Bronze → Silver → Gold

| # | Notebook | Purpose | Runtime |
|---|----------|---------|---------|
| 1 | [01_bronze_ingestion.py](01_bronze_ingestion.py) | Raw data → Delta Lake | ~3 min |
| 2 | [02_silver_standardization.py](02_silver_standardization.py) | Clean & validate | ~3 min |
| 3 | [03_gold_analytics.py](03_gold_analytics.py) | Star schema & KPIs | ~5 min |

## Quick Execution

```bash
# Run in Databricks Workspace (in order):
01_bronze_ingestion.py
02_silver_standardization.py
03_gold_analytics.py
```

## Output Tables

| Layer | Tables | Description |
|-------|--------|-------------|
| **Bronze** | 4 | Raw snapshots with `_metadata_*` columns |
| **Silver** | 4 | Validated, deduplicated, snake_case |
| **Gold** | 8 | Star schema (3 dims + 3 facts + 2 KPIs) |

See [Architecture Docs](../docs/) for design decisions.
