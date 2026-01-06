# 📓 Notebooks

Medallion Architecture pipeline: **Bronze → Silver → Gold**

---

## Pipeline Flow

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ 01_bronze_       │────▶│ 02_silver_       │────▶│ 03_gold_         │
│ ingestion.py     │     │ standardization  │     │ orchestrator.py  │
│                  │     │ .py              │     │                  │
│ • CSV/Excel read │     │ • Deduplication  │     │ • Dims (SCD2)    │
│ • Delta write    │     │ • snake_case     │     │ • Facts          │
│ • Audit metadata │     │ • Quality flags  │     │ • KPIs           │
│                  │     │                  │     │ • Validation     │
│ (~4 min)         │     │ (~3 min)         │     │ (~10 min)        │
└──────────────────┘     └──────────────────┘     └──────────────────┘
```

---

## Gold Layer: Anti-Saturation Design

```
gold/
├── 03_gold_orchestrator.py      # ← Run this (controls everything)
├── dimensions/                   
│   ├── gold_dim_date.py         # Static calendar
│   ├── gold_dim_product.py      # SCD Type 2
│   └── gold_dim_pdv.py          # SCD Type 2
├── facts/
│   ├── gold_fact_sell_in.py     
│   ├── gold_fact_price_audit.py 
│   └── gold_fact_stock.py       # Derived from sell-in
├── kpis/
│   ├── gold_kpi_market_visibility.py
│   └── gold_kpi_market_share.py
└── validation/
    └── gold_validation.py       # Read-only checks
```

**Why modular?** One notebook = One table = Independent failure isolation

---

## Quick Execution

```bash
# Option A: Full pipeline (recommended)
databricks bundle run full_pipeline

# Option B: Gold only (orchestrator)
# Open 03_gold_orchestrator.py → Set "Execute Pipeline" = "yes" → Run All
```

---

## Output Tables

| Layer | Count | Tables |
|-------|-------|--------|
| Bronze | 4 | `bronze_master_pdv`, `bronze_master_products`, `bronze_price_audit`, `bronze_sell_in` |
| Silver | 4 | `silver_*` (same names, standardized) |
| Gold | 8 | 3 dims + 3 facts + 2 KPIs |

---

See [Architecture Docs](../docs/) for detailed ADRs.
