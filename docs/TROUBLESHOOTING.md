# Troubleshooting Guide - Bronze Layer Implementation

**Author:** Diego Mayorga  
**Date:** 2025-12-30  
**Project:** BI Market Visibility Analysis

---

## Overview

This document captures **technical challenges** encountered during Bronze layer implementation. These represent common configuration issues and data quality problems in enterprise data engineering.

---

## 🔴 Critical Issues Resolved

### Issue #1: DBFS Public Access Disabled

**Error:**
```
[FAILED_READ_FILE] Error while reading file dbfs:/...
Public DBFS root is disabled. Access is denied.
```

**Root Cause:**
- Databricks workspace has DBFS public access disabled for security
- Cannot use traditional `dbfs:/FileStore/` paths
- Legacy DBFS approach no longer supported in modern workspaces

**Solution:**
- Use **Unity Catalog Volumes** (modern standard)
- Created volume: `workspace.default.bi_market_raw`
- Organized files: `/Volumes/workspace/default/bi_market_raw/Master_PDV/`

**Code:**
```python
RAW_VOLUME = "/Volumes/workspace/default/bi_market_raw"
MASTER_PDV_PATH = f"{RAW_VOLUME}/Master_PDV/master_pdv_raw.csv"
```

**Key Learning:**
- Unity Catalog Volumes is required in secured Databricks environments
- Provides better governance, lineage, and access control
- Standard approach for modern data platforms

---

### Issue #2: Unity Catalog Function Compatibility

**Error:**
```
[UC_COMMAND_NOT_SUPPORTED.WITH_RECOMMENDATION] 
The command(s): input_file_name are not supported in Unity Catalog. 
Please use _metadata.file_path instead.
```

**Root Cause:**
- Unity Catalog requires updated metadata access patterns
- Legacy `input_file_name()` function deprecated

**Solution:**
```python
# ✅ Unity Catalog compatible
.withColumn("source_file", col("_metadata.file_path"))
```

**Key Learning:**
- Unity Catalog introduces breaking changes for governance
- Always use `_metadata` pseudo-column for file metadata

---

### Issue #3: Invalid Characters in Column Names

**Error:**
```
[DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES] 
Found invalid character(s) among ' ,;{}()\n\t=' in column names.
Invalid: Code (eLeader); Store Name; Sales Rep.
```

**Root Cause:**
- Source CSV contains columns with spaces, parentheses, periods
- Delta Lake default mode doesn't allow these characters

**Solution:**
```python
df.write \
    .option("delta.columnMapping.mode", "name") \
    .saveAsTable(table_name)
```

**Key Learning:**
- Bronze layer should preserve raw column names
- Column Mapping allows special characters without data loss
- Essential for maintaining traceability to source systems

---

### Issue #4: Incorrect CSV Delimiter

**Error:**
```
AnalysisException: name "Code (eLeader);Store Name;Channel..." too long. 
Maximum length is 255 characters.
```

**Root Cause:**
- Master_PDV uses semicolon (`;`) delimiter, not comma
- Default Spark CSV reader assumes comma
- Entire header row parsed as single column name

**Solution:**
```python
spark.read.csv(path, header=True, sep=";")  # Explicit delimiter
```

**Key Learning:**
- Never assume CSV delimiter - always verify source files
- Common delimiters: `,` (US), `;` (Europe/Latin America), `|`, `\t`
- Document delimiter in data dictionary

---

### Issue #5: Excel File Reading in Databricks

**Challenge:**
- Excel files require external library support
- Databricks Community/Standard workspaces don't allow cluster Maven configuration
- `spark-excel` requires cluster-level installation

**Solution:**
```python
# Use pandas + openpyxl (Python libraries)
%pip install openpyxl pandas

def read_excel_files(path_pattern, spark_session):
    excel_files = glob.glob(path_pattern)
    dfs_pandas = [pd.read_excel(f, engine='openpyxl') for f in excel_files]
    combined_df = pd.concat(dfs_pandas, ignore_index=True)
    return spark_session.createDataFrame(combined_df)
```

**Key Learning:**
- Python libraries (pip) can be installed in notebooks
- Pandas suitable for files <1GB (acceptable for most Bronze ingestion)
- Maintains file lineage via metadata columns

---

### Issue #6: Dynamic Schema Handling

**Challenge:**
- Source systems use different column naming conventions
- Hardcoded column names break when schemas change
- Case sensitivity varies across sources

**Solution:**
```python
# Dynamic date column detection (case-insensitive)
date_column = None
for col_name in df.columns:
    if 'date' in col_name.lower() or 'fecha' in col_name.lower():
        date_column = col_name
        break
```

**Key Learning:**
- Bronze layer must handle schema variability
- Use dynamic column detection for robustness
- Support multiple languages (date/fecha, product/producto)

---

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

## 🛠️ Best Practices Implemented

### 1. Unity Catalog Volumes for File Storage
```python
RAW_VOLUME = "/Volumes/workspace/default/bi_market_raw"
```

### 2. Delta Column Mapping
```python
.option("delta.columnMapping.mode", "name")
```

### 3. Explicit CSV Delimiters
```python
spark.read.csv(path, sep=";")
```

### 4. Dynamic Column Detection
```python
date_col = next((c for c in df.columns if 'date' in c.lower()), None)
```

### 5. Audit Columns
```python
.withColumn("ingestion_timestamp", current_timestamp())
.withColumn("source_file", col("_metadata.file_path"))
```

---

## 🔍 Configuration Checklist

- [ ] Unity Catalog Volume created
- [ ] Files uploaded with folder structure
- [ ] CSV delimiters verified
- [ ] Column Mapping enabled
- [ ] Python libraries installed (`openpyxl`, `pandas`)
- [ ] Unity Catalog functions used
- [ ] Audit columns added

---

## 📖 Related Documentation

- [Development Setup](DEVELOPMENT_SETUP.md)
- [Integration Architecture](INTEGRATION_ARCHITECTURE.md)
- [Data Dictionary](data_dictionary.md)

---

**Last Updated:** 2025-12-30  
**Status:** Production-ready
