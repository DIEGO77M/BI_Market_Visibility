# 📓 Databricks Notebooks

This directory contains all transformation logic for the **Medallion Architecture** pipeline. Each notebook is designed for **Databricks Serverless** execution with Unity Catalog integration.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              NOTEBOOK PIPELINE                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   bronze/                     silver/                    gold/                   │
│   ┌──────────────────┐       ┌──────────────────┐       ┌──────────────────┐   │
│   │ 01_bronze_       │       │ 02_silver_       │       │ 03_gold_         │   │
│   │ ingestion.py     │──────▶│ standardization  │──────▶│ orchestrator.py  │   │
│   │                  │       │ .py              │       │                  │   │
│   │ • CSV/Excel read │       │ • Deduplication  │       │ • Central config │   │
│   │ • Delta write    │       │ • snake_case     │       │ • Execution ctrl │   │
│   │ • Audit columns  │       │ • Quality flags  │       │                  │   │
│   └──────────────────┘       └──────────────────┘       └────────┬─────────┘   │
│                                                                   │              │
│                                                    ┌──────────────┴──────────┐  │
│                                                    ▼              ▼          ▼  │
│                                              dimensions/      facts/      kpis/ │
│                                              ┌──────────┐   ┌─────────┐  ┌────┐ │
│                                              │dim_date  │   │fact_    │  │kpi_│ │
│                                              │dim_prod  │   │sell_in  │  │mkt │ │
│                                              │dim_pdv   │   │fact_    │  │vis │ │
│                                              │          │   │price    │  │kpi_│ │
│                                              │          │   │fact_    │  │mkt │ │
│                                              │          │   │stock    │  │shr │ │
│                                              └──────────┘   └─────────┘  └────┘ │
│                                                    │              │          │   │
│                                                    └──────────────┴──────────┘   │
│                                                               ▼                  │
│                                                    validation/gold_validation.py │
│                                                    (read-only integrity checks)  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
notebooks/
├── bronze/
│   └── 01_bronze_ingestion.py          # Raw data ingestion
│
├── silver/
│   └── 02_silver_standardization.py    # Cleaning & validation
│
└── gold/                               # ⭐ Modular anti-saturation design
    ├── 03_gold_orchestrator.py         # Central configuration & execution
    │
    ├── dimensions/                     # Conformed dimensions (SCD handling)
    │   ├── gold_dim_date.py            # Static 10-year calendar
    │   ├── gold_dim_product.py         # SCD Type 2 with surrogate keys
    │   └── gold_dim_pdv.py             # SCD Type 2 with surrogate keys
    │
    ├── facts/                          # Transactional facts
    │   ├── gold_fact_sell_in.py        # Shipment transactions
    │   ├── gold_fact_price_audit.py    # Price observations with index
    │   └── gold_fact_stock.py          # Derived inventory proxy
    │
    ├── kpis/                           # Pre-aggregated business metrics
    │   ├── gold_kpi_market_visibility.py   # Daily operational KPIs
    │   └── gold_kpi_market_share.py        # Monthly trend KPIs
    │
    └── validation/
        └── gold_validation.py          # Read-only post-execution checks
```

---

## Layer Details

### Bronze Layer (`bronze/`)

| Notebook | Purpose | Write Pattern | Runtime |
|----------|---------|---------------|---------|
| `01_bronze_ingestion.py` | Raw data ingestion from CSV/Excel | Dimensions: Full Overwrite, Facts: Append | ~3-4 min |

**Why this design:**
- **Full Overwrite for Dimensions:** Master data is small, ensures consistency
- **Append for Facts:** Preserves history, enables incremental processing
- **Audit columns added:** `ingestion_timestamp`, `source_file`, `ingestion_date`

**Source formats handled:**
- CSV (semicolon and comma delimited)
- Excel (.xlsx) via pandas workaround (Serverless limitation)

---

### Silver Layer (`silver/`)

| Notebook | Purpose | Write Pattern | Runtime |
|----------|---------|---------------|---------|
| `02_silver_standardization.py` | Deduplication, standardization, quality flags | Merge/Overwrite | ~3 min |

**Why this design:**
- **Business key deduplication:** Deterministic ordering (latest wins)
- **snake_case standardization:** Consistent column naming
- **Quality flags, not imputation:** `is_complete`, `is_valid` columns preserve transparency
- **Partitioning preserved:** Same partitions as Bronze for query optimization

**Key transformations:**
1. Remove duplicates by business key
2. Standardize column names (snake_case)
3. Cast explicit data types
4. Add quality indicator columns

---

### Gold Layer (`gold/`)

**Design Pattern:** Anti-saturation modular architecture

| Subfolder | Notebooks | Purpose |
|-----------|-----------|---------|
| `dimensions/` | 3 notebooks | Conformed dimensions (SCD Type 2) |
| `facts/` | 3 notebooks | Transactional facts with foreign keys |
| `kpis/` | 2 notebooks | Pre-aggregated business metrics |
| `validation/` | 1 notebook | Read-only integrity checks |

**Why modular design:**
```
┌─────────────────────────────────────────────────────────────┐
│               ANTI-SATURATION PRINCIPLE                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Traditional:           Modular (This Project):             │
│  ┌───────────────┐      ┌─────┐ ┌─────┐ ┌─────┐            │
│  │ gold_all.py   │      │dim_1│ │dim_2│ │dim_3│            │
│  │               │      └──┬──┘ └──┬──┘ └──┬──┘            │
│  │ • dim_date    │         │       │       │                │
│  │ • dim_product │      ┌──┴──┐ ┌──┴──┐ ┌──┴──┐            │
│  │ • dim_pdv     │      │fact1│ │fact2│ │fact3│            │
│  │ • fact_sell   │      └─────┘ └─────┘ └─────┘            │
│  │ • fact_price  │                                          │
│  │ • kpi_market  │      Benefits:                           │
│  │               │      ✅ Independent failure isolation    │
│  │ If ONE fails: │      ✅ Parallel execution possible      │
│  │ ALL fail ❌    │      ✅ Easier debugging & maintenance  │
│  └───────────────┘      ✅ Granular retry on failure       │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## Execution Guide

### Option A: Databricks Workflows (Recommended)

```bash
# Deploy and run via Databricks CLI
databricks bundle deploy --target dev
databricks bundle run full_pipeline
```

Workflow definition: `.databricks/workflows/full_pipeline.yml`

### Option B: Orchestrator (Development)

```python
# In Databricks workspace:
# 1. Open notebooks/gold/03_gold_orchestrator.py
# 2. Set widget "Execute Pipeline" = "yes"
# 3. Run All Cells
```

### Option C: Manual (Learning/Debugging)

Execute in order respecting dependencies:

```
Phase 1: Bronze (no dependencies)
└── 01_bronze_ingestion.py

Phase 2: Silver (depends on Bronze)
└── 02_silver_standardization.py

Phase 3: Gold Dimensions (depends on Silver, parallel)
├── gold_dim_date.py      ─┐
├── gold_dim_product.py   ─┼── Can run in parallel
└── gold_dim_pdv.py       ─┘

Phase 4: Gold Facts (depends on Dimensions, parallel)
├── gold_fact_sell_in.py      ─┐
├── gold_fact_price_audit.py  ─┼── Can run in parallel
└── gold_fact_stock.py        ─┘

Phase 5: Gold KPIs (depends on Facts, parallel)
├── gold_kpi_market_visibility.py  ─┐
└── gold_kpi_market_share.py       ─┘  Can run in parallel

Phase 6: Validation (depends on all Gold)
└── gold_validation.py (read-only)
```

---

## Output Tables

| Layer | Tables | Unity Catalog Location |
|-------|--------|----------------------|
| **Bronze** | 4 tables | `workspace.default.bronze_*` |
| **Silver** | 4 tables | `workspace.default.silver_*` |
| **Gold** | 8 tables | `workspace.default.gold_*` |

### Table Details

| Table | Type | Grain | Purpose |
|-------|------|-------|---------|
| `gold_dim_date` | Dimension | date | Calendar attributes (10-year range) |
| `gold_dim_product` | Dimension (SCD2) | product_sk | Product master with history |
| `gold_dim_pdv` | Dimension (SCD2) | pdv_sk | Store master with history |
| `gold_fact_sell_in` | Fact | date × product × pdv | Shipment transactions |
| `gold_fact_price_audit` | Fact | date × product × pdv | Price observations |
| `gold_fact_stock` | Fact | date × product × pdv | Derived inventory |
| `gold_kpi_market_visibility` | KPI | date × product × channel | Daily ops metrics |
| `gold_kpi_market_share` | KPI | month × brand × channel | Monthly trends |

---

## Key Design Decisions

### Why SCD Type 2 for Product/PDV?

| Alternative | Decision | Rationale |
|-------------|----------|-----------|
| SCD Type 1 (overwrite) | ❌ | Loses history of category/segment changes |
| SCD Type 3 (previous value) | ❌ | Limited to one change, not flexible |
| **SCD Type 2** | ✅ | Full history preservation, supports time-travel analysis |

### Why Pre-Aggregated KPIs?

| Alternative | Decision | Rationale |
|-------------|----------|-----------|
| DAX measures only | ❌ | Complex DAX, slow at scale |
| **Pre-aggregated tables** | ✅ | Simple DAX, fast queries, single source of truth |

### Why Derived Stock Table?

**Business context:** No sell-out data available.

**Solution:** Estimate stock from sell-in patterns:
```
stock_proxy = cumulative_sell_in - estimated_sell_out
```

**Trade-off:** Approximation, not actual inventory. Clearly documented in ADRs.

---

## Related Documentation

- [BRONZE_ARCHITECTURE_DECISIONS.md](../docs/BRONZE_ARCHITECTURE_DECISIONS.md) - 5 ADRs
- [SILVER_ARCHITECTURE_DECISIONS.md](../docs/SILVER_ARCHITECTURE_DECISIONS.md) - 9 ADRs
- [GOLD_ARCHITECTURE_DECISIONS.md](../docs/GOLD_ARCHITECTURE_DECISIONS.md) - 14 ADRs
- [POWERBI_INTEGRATION_GUIDE.md](../docs/POWERBI_INTEGRATION_GUIDE.md) - BI setup
- [data_dictionary.md](../docs/data_dictionary.md) - Schema definitions
