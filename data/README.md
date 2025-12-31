# 📊 Data Directory - Medallion Architecture

Enterprise-grade data lakehouse organized into Bronze-Silver-Gold layers following industry best practices.

---

## 🏗️ Architecture Overview

```
Source Systems → raw/ → Bronze → Silver → Gold → Power BI
                         ↓         ↓        ↓
                     Immutable  Validated  Aggregated
```

---

## 📂 Layer Definitions

### 🗃️ `/raw` - Source Data Files

**Purpose:** Store original files exactly as received from source systems

| Characteristic | Description |
|----------------|-------------|
| **Format** | Any (CSV, Excel, JSON, Parquet) |
| **Processing** | None - exact copy preserved |
| **Schema Evolution** | Free-form, no enforcement |
| **Usage** | Reference only, never modified |
| **Governance** | Read-only after initial load |

**Best Practice:** Treat as immutable archive for audit and replay scenarios.

---

### 🥉 `/bronze` - Raw Delta Tables

**Purpose:** Ingested raw data with minimal transformation (Unity Catalog managed)

| Characteristic | Description |
|----------------|-------------|
| **Format** | Delta Lake (Parquet + transaction log) |
| **Processing** | Add metadata columns only (`_metadata_*`) |
| **Schema Evolution** | Flexible, preserves source structure |
| **Data Quality** | No validation (accept all) |
| **Partitioning** | Optional (e.g., `year_month` for time series) |

**Tables Created:**
- `workspace.default.bronze_master_pdv` (51 rows)
- `workspace.default.bronze_master_products` (201 rows)
- `workspace.default.bronze_price_audit` (1,200+ rows, partitioned)
- `workspace.default.bronze_sell_in` (400+ rows, partitioned)

**Key Feature:** ACID transactions with time travel capability

---

### 🥈 `/silver` - Cleaned & Validated

**Purpose:** Apply business rules, standardize schemas, enforce data quality

| Characteristic | Description |
|----------------|-------------|
| **Format** | Delta Lake with enforced schema |
| **Processing** | Deduplication, validation, standardization |
| **Schema Evolution** | Controlled via migrations |
| **Data Quality** | Validated (e.g., prices > 0, no nulls in keys) |
| **Partitioning** | Optimized for query patterns |

**Transformations:**
- Remove duplicates (by business key)
- Handle nulls and invalid values
- Standardize text (trim, uppercase codes)
- Data type corrections
- Derived columns (e.g., `unit_price`, `year`, `month`)

**Output Tables:**
- `workspace.default.silver_master_pdv`
- `workspace.default.silver_master_products`
- `workspace.default.silver_price_audit`
- `workspace.default.silver_sell_in`

---

### 🥇 `/gold` - Analytics-Ready

**Purpose:** Business-level aggregations optimized for BI tools (Planned)

| Characteristic | Description |
|----------------|-------------|
| **Format** | Delta Lake with star schema |
| **Processing** | Aggregations, KPIs, denormalization |
| **Schema Evolution** | Business-driven via semantic layer |
| **Data Quality** | Conformed dimensions, referential integrity |
| **Optimization** | Z-ordering, indexing for BI queries |

**Planned Models:**
- `gold_fact_sales` - Transaction-level fact
- `gold_dim_pdv` - Point of sale dimension
- `gold_dim_product` - Product hierarchy
- `gold_dim_date` - Calendar dimension

---

## 🔐 Governance & Security

**Unity Catalog Integration:**
- All Bronze/Silver/Gold tables managed via Unity Catalog
- GRANT/REVOKE permissions at catalog/schema/table level
- Audit logs for access tracking
- Data lineage automatically captured

**TBLPROPERTIES:**
```sql
'delta.enableChangeDataFeed' = 'true'
'quality.owner' = 'diego.mayorgacapera@gmail.com'
'quality.tier' = 'bronze|silver|gold'
```

---

## 📊 Data Flow Example

**Price Audit Journey:**

1. **Raw:** `data/raw/Price_Audit/audit_202101.xlsx`
2. **Bronze:** `bronze_price_audit` (24 Excel files unionized)
   - Added: `_metadata_ingestion_timestamp`, `_metadata_source_file`, `_metadata_batch_id`
3. **Silver:** `silver_price_audit` 
   - Filtered invalid prices
   - Standardized PDV/Product codes
   - Added `year`, `month` partitions
4. **Gold:** `gold_fact_price_observations` (Planned)
   - Joined with dimensions
   - Aggregated by PDV/Product/Month
   - Pre-calculated price variance metrics

---

## 💡 Design Decisions

**Why Medallion Architecture?**
- ✅ **Separation of Concerns:** Ingestion vs transformation vs analytics
- ✅ **Fault Tolerance:** Bronze failures don't corrupt Silver/Gold
- ✅ **Incremental Complexity:** Each layer adds value progressively
- ✅ **Auditability:** Full lineage from raw → gold

**Why Delta Lake?**
- ✅ **ACID Transactions:** Prevent partial writes
- ✅ **Time Travel:** Rollback to any version via `VERSION AS OF`
- ✅ **Schema Evolution:** Add/modify columns without rewrite
- ✅ **Performance:** Z-ordering, compaction, statistics

**Why Unity Catalog?**
- ✅ **Centralized Governance:** Single source of truth for metadata
- ✅ **Fine-Grained Access:** Column-level security
- ✅ **Automatic Lineage:** Track data flow across pipelines

---

## 🚀 Best Practices

1. ✅ **Never modify Bronze** - It's your source of truth
2. ✅ **Document transformations** - ADRs explain WHY, not just HOW
3. ✅ **Version control schemas** - Track changes in `docs/data_dictionary.md`
4. ✅ **Implement quality checks** - Fail fast on critical validations
5. ✅ **Use partitioning wisely** - Optimize for query patterns, not arbitrary fields

---

**Author:** Diego Mayorga | [GitHub](https://github.com/DIEGO77M/BI_Market_Visibility)  
**Last Updated:** 2025-12-31
