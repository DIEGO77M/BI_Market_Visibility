# Test Suite: Market Visibility Project

> Automated test coverage for all business and technical layers. Ensures data quality, transformation logic, and pipeline integrity for analytics engineering in retail/FMCG.

---

## Quick Start

Run all tests:

```bash
pytest src/tests/
```

Run by test type:

```bash
pytest src/tests/data_quality/
pytest src/tests/integration/
pytest src/tests/unit/
```

---

## Features

- Business-impact data quality validation
- End-to-end pipeline and join checks
- Unit tests for all transformation logic
- Naming and structure aligned with Medallion Architecture

---

## Structure

| Folder         | Purpose                                  |
|---------------|------------------------------------------|
| data_quality/ | Business-impact data quality checks       |
| integration/  | End-to-end pipeline and join validation   |
| unit/         | Transformation and logic unit tests       |

---

## Test Coverage

### Data Quality (`data_quality/`)

| File                                 | Description                                                      |
|--------------------------------------|------------------------------------------------------------------|
| test_bronze_price_audit_quality.py   | Validates technical and business quality for Bronze price audit  |
| test_gold_dim_product_quality.py     | Ensures Gold product dimension meets business/modeling standards |
| test_silver_fact_price_audit_quality.py | Checks Silver fact price audit for deduplication, normalization, business rules |

### Integration (`integration/`)

| File                                 | Description                                                      |
|--------------------------------------|------------------------------------------------------------------|
| test_bronze_to_silver_price_audit.py | Verifies transformation and data flow Bronze → Silver (price audit) |
| test_silver_to_gold_dim_product.py   | Validates join/enrichment Silver → Gold (product dimension)      |

### Unit (`unit/`)

| File                                 | Description                                                      |
|--------------------------------------|------------------------------------------------------------------|
| test_bronze_price_audit.py           | Unit tests for Bronze price audit ingestion/validation           |
| test_gold_dim_product.py             | Unit tests for Gold product dimension transformation             |
| test_silver_dim_pdv_transformations.py | Tests Silver PDV dimension cleaning/standardization             |
| test_silver_fact_price_audit.py      | Unit tests for Silver fact price audit transformation            |

---

## Configuration

| Variable | Description                | Default |
|----------|----------------------------|---------|
| PYTHONPATH | Project root for imports  | .       |
| DATABRICKS_PROFILE | Databricks CLI profile | DTB_Market_Visibility |

---

## Documentation

- [Project Architecture](../../docs/architecture/)
- [Data Dictionary](../../docs/data_dictionary/)
- [Technical Specs](../../docs/technical_specs/)

---

## Contributing

1. Add new tests in the appropriate folder.
2. Use naming: `test_<layer>_<entity>[_quality|_integration].py`
3. Document new business rules or edge cases in test docstrings.
4. Prefer business-impact scenarios over generic technical checks.

---

## License

MIT
