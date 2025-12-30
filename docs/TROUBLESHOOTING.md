# Troubleshooting Guide - Bronze Layer Implementation

**Author:** Diego Mayorga  
**Date:** 2025-12-30  
**Project:** BI Market Visibility Analysis

---

## Overview

This document captures **real challenges encountered** during Bronze layer implementation and their solutions. These issues represent common scenarios in enterprise data engineering projects.

---

## 🔴 Critical Issues Resolved

### Issue #1: File Access Denied in Databricks Workspace

**Error:**
```
[FAILED_READ_FILE.NO_HINT] Error while reading file 
dbfs:/Workspace/Users/.../master_pdv_raw.csv
```

**Root Cause:**
- Attempted to read from `/Workspace/` paths directly
- Databricks workspace paths are not accessible via DBFS protocol
- Public DBFS root is disabled for security in the workspace

**Solution Attempted #1 (Failed):**
- Added `file://` prefix to workspace paths
- Error: `SecurityException: Cannot use WorkspaceLocalFileSystem - local filesystem access is forbidden`

**Solution Attempted #2 (Failed):**
- Tried to use `/FileStore/` in DBFS
- Error: `Public DBFS root is disabled`

**Final Solution (Success):**
- Migrated to **Unity Catalog Volumes** (modern enterprise approach)
- Created volume: `workspace.default.bi_market_raw`
- Uploaded files to: `/Volumes/workspace/default/bi_market_raw/`
- **Key Learning:** Unity Catalog Volumes is the professional standard for file storage in Databricks

**Code Fix:**
```python
# ❌ Old approach (workspace paths)
BASE_PATH = "/Workspace/Users/.../files"

# ✅ New approach (Unity Catalog Volumes)
RAW_VOLUME = "/Volumes/workspace/default/bi_market_raw"
MASTER_PDV_PATH = f"{RAW_VOLUME}/Master_PDV/master_pdv_raw.csv"
```

**Prevention:**
- Always use Unity Catalog Volumes for data files in production
- Reserve workspace paths only for notebooks and code
- Check workspace security settings before architecting storage

---

### Issue #2: Unity Catalog Function Compatibility

**Error:**
```
[UC_COMMAND_NOT_SUPPORTED.WITH_RECOMMENDATION] 
The command(s): input_file_name are not supported in Unity Catalog. 
Please use _metadata.file_path instead.
```

**Root Cause:**
- Legacy PySpark function `input_file_name()` not supported in Unity Catalog
- Unity Catalog has stricter governance and requires new metadata access patterns

**Solution:**
```python
# ❌ Old (legacy PySpark)
.withColumn("source_file", input_file_name())

# ✅ New (Unity Catalog compatible)
.withColumn("source_file", col("_metadata.file_path"))
```

**Key Learning:**
- Unity Catalog introduces breaking changes for better governance
- Always check Unity Catalog compatibility for PySpark functions
- Use `_metadata` pseudo-column for file metadata in UC

**Prevention:**
- Review Databricks Unity Catalog migration guide
- Test with Unity Catalog enabled from the start
- Avoid deprecated functions in new projects

---

### Issue #3: Invalid Characters in Column Names

**Error:**
```
[DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES] 
Found invalid character(s) among ' ,;{}()\n\t=' in the column names.
Invalid column names: Code (eLeader); Store Name; Sales Rep.
```

**Root Cause:**
- Raw CSV files contain column names with:
  - Spaces: `Store Name`
  - Parentheses: `Code (eLeader)`
  - Periods: `Sales Rep.`
  - Semicolons in data (not actual column names)
- Delta Lake's default mode doesn't allow these characters

**Solution:**
- Enabled **Column Mapping** feature in Delta Lake
- Applied to all table writes

```python
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \  # KEY FIX
    .saveAsTable(table_name)
```

**Key Learning:**
- Bronze layer should preserve raw column names (minimal transformation)
- Column Mapping allows special characters without breaking Delta
- Alternative: Sanitize column names (loses traceability to source)

**Prevention:**
- Enable Column Mapping from the start for raw data ingestion
- Document original vs. sanitized column names if renaming
- Consider data contracts with source systems for clean column names

---

### Issue #4: Column Name Too Long

**Error:**
```
AnalysisException: Invalid input: Field managedcatalog.ColumnInfo.name: 
At columns.0: name "Code (eLeader);Store Name;Channel;..." too long. 
Maximum length is 255 characters.
```

**Root Cause:**
- CSV was being read with **wrong delimiter**
- Master_PDV uses semicolon (`;`) as delimiter, not comma
- Spark treated entire header row as single column name (exceeded 255 char limit)

**Investigation Process:**
1. Checked raw CSV file structure
2. Identified delimiter: `Code (eLeader);Store Name;Channel;...`
3. Realized semicolon is field separator, not data

**Solution:**
```python
# ❌ Wrong (default comma delimiter)
spark.read.csv(path, header=True)

# ✅ Correct (explicit semicolon delimiter)
spark.read.csv(path, header=True, sep=";")
```

**Key Learning:**
- **Never assume CSV delimiter** - always verify source files
- Common delimiters: `,` (US), `;` (Europe/Latin America), `|`, `\t`
- File extension `.csv` doesn't guarantee comma delimiter

**Prevention:**
- Document delimiter for each source in data dictionary
- Add delimiter validation in data quality checks
- Use `spark.read.option("sep", ";")` explicitly, never rely on defaults

---

## ⚠️ Moderate Issues

### Issue #5: Schema Inference with Special Characters

**Challenge:**
- `inferSchema=True` sometimes misinterprets columns with special chars
- Excel files may have merged cells or formatting that breaks schema

**Solution:**
```python
# For CSV with known issues
df = spark.read.csv(
    path,
    header=True,
    inferSchema=True,
    sep=";",
    encoding="UTF-8",  # Explicit encoding
    quote='"',         # Handle quoted fields
    escape='\\'        # Handle escaped characters
)

# For Excel files
df = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("treatEmptyValuesAsNulls", "true")  # Clean empty strings
    .load(path)
```

**Key Learning:**
- Always specify encoding explicitly (UTF-8, Latin-1, etc.)
- Excel files need `spark-excel` library
- Test schema inference with sample before full load

---

### Issue #6: Dynamic Column Detection

**Challenge:**
- Hardcoded column names fail when source schema changes
- Different sources have different naming conventions (case, spaces)

**Solution - Dynamic Key Column Detection:**
```python
# ❌ Hardcoded (brittle)
key_columns = ["product_id", "pdv_id"]

# ✅ Dynamic (robust)
# Find date column (case-insensitive)
date_column = None
for col_name in df.columns:
    if 'date' in col_name.lower() or 'fecha' in col_name.lower():
        date_column = col_name
        break

# Use actual first column as key
key_columns = [df.columns[0], "year"]
```

**Key Learning:**
- Bronze layer must handle schema variability
- Use dynamic column detection for flexibility
- Log actual detected columns for troubleshooting

---

### Issue #7: File Path Organization

**Challenge:**
- Files uploaded to volume root without folder structure
- Difficult to manage multiple sources
- Path inconsistencies between local and Databricks

**Solution - Proper Folder Structure:**
```
/Volumes/workspace/default/bi_market_raw/
├── Master_PDV/
│   └── master_pdv_raw.csv
├── Master_Products/
│   └── product_master_raw.csv
├── Price_Audit/
│   ├── Price_Audit_2021_01.xlsx
│   └── ... (24 files)
└── Sell-In/
    ├── Sell_In_2021.xlsx
    └── Sell_In_2022.xlsx
```

**Key Learning:**
- Organize by data source, not by file type
- Mirror source system structure in Bronze
- Maintain consistent structure between environments

---

## 🛠️ Best Practices Learned

### 1. Path Configuration Strategy

**Problem:** Hardcoded paths break across environments

**Solution:**
```python
# Environment-aware path configuration
CATALOG = "workspace"
SCHEMA = "default"
RAW_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/bi_market_raw"

# Source-specific paths
MASTER_PDV_PATH = f"{RAW_VOLUME}/Master_PDV/master_pdv_raw.csv"
```

**Benefits:**
- Easy to switch between dev/staging/prod
- Single source of truth for paths
- Clear separation of concerns

---

### 2. Unity Catalog Table Naming

**Problem:** Inconsistent table references cause errors

**Solution:**
```python
# ❌ Path-based (legacy)
.save("/path/to/table")
deltaTable = DeltaTable.forPath(spark, path)

# ✅ Name-based (Unity Catalog)
.saveAsTable("catalog.schema.table")
deltaTable = DeltaTable.forName(spark, "catalog.schema.table")
```

**Benefits:**
- Centralized metadata management
- Built-in governance and lineage
- Multi-cloud compatibility

---

### 3. Audit Columns for Traceability

**Implementation:**
```python
def add_audit_columns(df):
    return df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", col("_metadata.file_path")) \
        .withColumn("ingestion_date", lit(datetime.now().strftime("%Y-%m-%d")))
```

**Benefits:**
- Track when and where data came from
- Debug data issues faster
- Enable time-travel queries
- Audit compliance

---

### 4. Data Quality Checks Before Write

**Implementation:**
```python
def validate_data_quality(df, source_name, key_columns):
    # Check for nulls in keys
    # Check for duplicates
    # Log warnings (don't fail - Bronze is raw data)
    pass
```

**Benefits:**
- Early detection of data issues
- Visibility into source data quality
- Documentation of known issues

---

## 📚 Key Takeaways

### For Bronze Layer Development:

1. **Storage:**
   - ✅ Use Unity Catalog Volumes
   - ❌ Avoid workspace paths or legacy DBFS

2. **Schema Handling:**
   - ✅ Enable Column Mapping for raw data
   - ✅ Use dynamic column detection
   - ❌ Don't hardcode column names

3. **Delimiters:**
   - ✅ Always verify and specify explicitly
   - ✅ Document in data dictionary
   - ❌ Never assume comma for CSV

4. **Unity Catalog:**
   - ✅ Use `saveAsTable()` for managed tables
   - ✅ Use `_metadata` instead of `input_file_name()`
   - ✅ Use `DeltaTable.forName()` not `forPath()`

5. **File Organization:**
   - ✅ Maintain clear folder structure
   - ✅ Group by source system
   - ✅ Mirror local and cloud structure

---

## 🔍 Debugging Checklist

When encountering issues, check:

- [ ] File exists in correct Volume path (use `databricks fs ls`)
- [ ] Delimiter is specified correctly for CSV
- [ ] Unity Catalog compatibility of functions used
- [ ] Column Mapping enabled for special characters
- [ ] Table name uses full 3-level namespace (catalog.schema.table)
- [ ] Schema inference working (print schema after read)
- [ ] Audit columns added without errors
- [ ] Network/permissions to access Databricks workspace

---

## 📖 Related Documentation

- [Development Setup Guide](DEVELOPMENT_SETUP.md) - Initial configuration
- [Integration Architecture](INTEGRATION_ARCHITECTURE.md) - System overview
- [Data Dictionary](data_dictionary.md) - Source schemas
- [Quick Reference](QUICK_REFERENCE.md) - Common commands

---

## 💡 Questions for Code Reviews

When reviewing Bronze layer code, ask:

1. Are Unity Catalog Volumes being used for file storage?
2. Is Column Mapping enabled for raw data with special characters?
3. Are CSV delimiters explicitly specified?
4. Are column names dynamically detected instead of hardcoded?
5. Are audit columns being added consistently?
6. Is error handling appropriate for Bronze (log warnings, don't fail)?
7. Are paths configurable across environments?

---

**Last Updated:** 2025-12-30  
**Status:** Active - reflects actual issues encountered during development  
**Lessons Applied:** All solutions implemented in `notebooks/01_bronze_ingestion.py`
