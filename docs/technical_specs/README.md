# Technical Specs

> Implementation details, environment, and operational requirements for the BI Market Visibility project.

---

## Environment

- **Platform:** Databricks Serverless (Unity Catalog)
- **Runtime:** Databricks Runtime 13.3 LTS (Spark 3.4, Delta Lake 2.4)
- **Deployment:** Databricks Asset Bundles (`databricks bundle deploy`)
- **Storage:** Delta Lake (all layers)
- **Languages:** Python (PySpark), SQL
- **Orchestration:**
    - Databricks Jobs and Workflows: All pipelines are orchestrated as jobs with explicit dependencies and execution order
    - Notebooks: Modular, idempotent, deterministic
    - Python scripts: Used for simulation and batch automation
    - Retries: Configurable per job, default 3 retries on failure
    - SLA: Gold tables available by day 3 of each month; alerts on delay

---

## Data Flow & Processing

- **Bronze:** Raw file ingestion, technical validation, and monitoring
- **Silver:** Cleaning, deduplication, business rules, enrichment
- **Gold:** KPI calculation, business logic, executive-ready facts

---

## Security & Governance

- **Unity Catalog:** Centralized data governance for all layers
- **Row-level security:** Implemented via Unity Catalog access policies (e.g., restrict Gold to Exec, Silver to BI)
- **Column masking:** Sensitive fields (e.g., price, margin) masked for non-privileged roles
- **Access patterns:**
    - Gold: Read-only for business users, full access for BI/RGM
    - Silver: Read/write for analytics, restricted for business users
    - Bronze: Read/write for Data Engineering only
- **Audit trails:** All access and changes logged via Unity Catalog

---

## Quality & Monitoring

- All validation and drift results are stored in Delta tables
- Automated alerting for schema drift, referential integrity, and business rule violations
- No data is dropped; all issues are flagged for auditability

---

## Deployment & Operations

- All code and configs are versioned in Git
- Deployment is automated via Databricks Asset Bundles
- Notebooks and scripts are idempotent and deterministic
- No use of classic clusters, cache(), or manual broadcast

---

## Documentation

- [Project Architecture](../architecture/README.md)
- [Data Dictionary](../data_dictionary/README.md)

---

## License

MIT
