# BI_Market_Visibility
Market Visibility platform for Retail/FMCG, built on Databricks Medallion Architecture. Enables robust data ingestion, standardization, and advanced analytics for price, margin, and channel insights. Delivers traceable KPIs and dimensional models for executive decision-making.

# Ingestion Notebook Decision

## Decision: Use Jupyter Notebook for Bronze Ingestion

**Context:**
The ingestion process for the Bronze layer (master_pdv) was originally implemented as a Python script (.py). However, Databricks Workflows natively recognizes notebooks (.ipynb) as tasks, providing better integration, logging, and monitoring capabilities.

**Decision:**
The ingestion script was converted to a Jupyter notebook to be used directly as a task in Databricks Workflows.

**Justification:**
- Native compatibility with Databricks orchestration (Workflows)
- Improved traceability, logging, and error handling
- Direct visualization and step-by-step debugging in Databricks UI
- Avoids the need for an extra wrapper notebook or custom Python task configuration

**Trade-offs:**
- Slightly less convenient for pure code versioning compared to .py scripts
- Requires maintaining notebook format for orchestration tasks

**Business Impact:**
This approach ensures robust, auditable, and maintainable ingestion pipelines, aligned with Databricks best practices for enterprise analytics projects.
