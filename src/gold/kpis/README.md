# 🎯 KPIs Gold Layer - Market Visibility & Revenue Leakage


## Overview

This folder contains **7 strategic KPIs at Senior BI level** designed for:
- Executive decision-making with real business impact
- Data-driven prioritization (not intuition)
- Specific action by functional area
- End-to-end orientation (not decorative metrics)

**Focus:** Market Visibility + Revenue Leakage  
**Compatibility:** 100% Databricks Serverless + Unity Catalog  
**Refresh:** Monthly (aligned with Gold layer grain)

---


## KPIs Portfolio Summary

| # | KPI Name | Business Question | Primary Owner | Complexity |
|---|----------|------------------|---------------|-----------|
| 1 | [Recovery Opportunity Score](./kpi_01_recovery_opportunity_score/) | Where are the highest-impact, actionable opportunities to recover revenue, especially due to stock or execution issues? | Commercial Strategy | ⭐⭐⭐ |
| 2 | [Price Elasticity Impact](./kpi_02_price_elasticity_impact/) | Which products show significant price sensitivity, and how does price change affect revenue? | Pricing Team | ⭐⭐⭐⭐|
| 3 | [Execution ROI](./kpi_03_execution_roi/) | What is the real business impact of merchandiser visits and execution, and where is execution-driven revenue loss highest? | Trade Marketing | ⭐⭐⭐|
| 4 | [Channel & Chain Performance](./kpi_04_channel_chain_performance/) | Which channels or chains are driving the most revenue leakage, and where should commercial focus be prioritized? | Commercial Strategy | ⭐⭐⭐ |
| 5 | [Product Portfolio Health](./kpi_05_product_portfolio_health/) | Which products are healthy, at risk, or critical, and what specific actions (strengthen, maintain, discontinue) should be taken for each? | Category Management | ⭐⭐⭐ |
| 6 | [Competitive Position Velocity](./kpi_06_competitive_position_velocity/) | How fast and where are our products losing or recovering competitive position at the point of sale, and where is urgent intervention required? | Pricing & Commercial Intelligence | ⭐⭐⭐⭐⭐ |
| 7 | [Perfect Storm Index](./kpi_07_perfect_storm_index/) | Where do all critical factors (stock, price, execution, assortment) fail simultaneously, creating maximum risk and requiring cross-functional intervention? | General Manager | ⭐⭐⭐ |

---


## Quick Start Guide

### 1. Recommended sequential execution

```sql
-- Prerequisite: Ensure mart_revenue_leakage is up to date
SELECT MAX(audit_date_id) FROM workspace.gold.mart_revenue_leakage;

-- KPI 1: Recovery Opportunity Score
%run /Workspace/src/gold/kpis/kpi_01_recovery_opportunity_score/ddl/create_kpi_recovery_opportunity_score.sql
```
-- KPI 2: Price Elasticity Impact  
%run /Workspace/src/gold/kpis/kpi_02_price_elasticity_impact/ddl/create_kpi_price_elasticity_impact.sql

-- KPI 3: Execution ROI
%run /Workspace/src/gold/kpis/kpi_03_execution_roi/ddl/create_kpi_execution_roi.sql

-- KPI 4: Channel & Chain Performance
%run /Workspace/src/gold/kpis/kpi_04_channel_chain_performance/ddl/create_kpi_channel_chain_performance.sql

-- KPI 5: Product Portfolio Health
%run /Workspace/src/gold/kpis/kpi_05_product_portfolio_health/ddl/create_kpi_product_portfolio_health.sql

-- KPI 6: Competitive Position Velocity
%run /Workspace/src/gold/kpis/kpi_06_competitive_position_velocity/ddl/create_kpi_competitive_position_velocity.sql

-- KPI 7: Perfect Storm Index
%run /Workspace/src/gold/kpis/kpi_07_perfect_storm_index/ddl/create_kpi_perfect_storm_index.sql
```

### 2. Validación post-ejecución

```sql
-- Verificar que todas las tablas existen y tienen datos
SELECT 'kpi_recovery_opportunity_score' AS kpi, COUNT(*) AS row_count FROM workspace.gold.kpi_recovery_opportunity_score UNION ALL
SELECT 'kpi_price_elasticity_impact', COUNT(*) FROM workspace.gold.kpi_price_elasticity_impact UNION ALL
SELECT 'kpi_execution_roi', COUNT(*) FROM workspace.gold.kpi_execution_roi UNION ALL
SELECT 'kpi_channel_chain_performance', COUNT(*) FROM workspace.gold.kpi_channel_chain_performance UNION ALL
SELECT 'kpi_product_portfolio_health', COUNT(*) FROM workspace.gold.kpi_product_portfolio_health UNION ALL
SELECT 'kpi_competitive_position_velocity', COUNT(*) FROM workspace.gold.kpi_competitive_position_velocity UNION ALL
SELECT 'kpi_perfect_storm_index', COUNT(*) FROM workspace.gold.kpi_perfect_storm_index;
```

---


## 🗂️ Folder Structure

```
kpis/
├── kpi_01_recovery_opportunity_score/
│   ├── ddl/
│   │   └── create_kpi_recovery_opportunity_score.sql
│   └── README.md
├── kpi_02_price_elasticity_impact/
│   ├── ddl/
│   │   └── create_kpi_price_elasticity_impact.sql
│   └── README.md
├── kpi_03_execution_roi/
│   ├── ddl/
│   │   └── create_kpi_execution_roi.sql
│   └── README.md
├── kpi_04_channel_chain_performance/
│   ├── ddl/
│   │   └── create_kpi_channel_chain_performance.sql
│   └── README.md
├── kpi_05_product_portfolio_health/
│   ├── ddl/
│   │   └── create_kpi_product_portfolio_health.sql
│   └── README.md
├── kpi_06_competitive_position_velocity/
│   ├── ddl/
│   │   └── create_kpi_competitive_position_velocity.sql
│   └── README.md
├── kpi_07_perfect_storm_index/
│   ├── ddl/
│   │   └── create_kpi_perfect_storm_index.sql
│   └── README.md
└── README.md (this file)
```

---


## 💼 Business Use Cases by Role

### Commercial Director / CEO
**Priority KPIs:** 4, 7
- KPI 4: Strategic view of performance by channel/chain
- KPI 7: Identify crises that require escalation

**Morning Dashboard:**
```sql
-- Executive Summary: Perfect Storms + Channel Performance
SELECT 
  ps.risk_tier,
  COUNT(*) AS cases,
  SUM(ps.potential_revenue_lost_usd) AS total_at_risk
FROM workspace.gold.kpi_perfect_storm_index ps
WHERE ps.date = '2024-05-01'
GROUP BY ps.risk_tier
ORDER BY total_at_risk DESC;
```

### Pricing Manager
**Priority KPIs:** 2, 6
- KPI 2: Price-revenue elasticity by product
- KPI 6: Competitive deterioration speed

**Weekly Review:**
```sql
-- Products losing competitive ground fast
SELECT *
FROM workspace.gold.kpi_competitive_position_velocity
WHERE competitive_trend = 'Deteriorating'
  AND velocity_magnitude = 'High Velocity'
  AND date = '2024-05-01'
ORDER BY price_gap_velocity DESC
LIMIT 20;
```

### Trade Marketing Manager
**Priority KPIs:** 3
- KPI 3: Merchandiser and execution ROI

**Monthly Review:**
```sql
-- Merchandiser performance ranking
SELECT 
  merchandiser_name,
  visit_coverage_pct,
  execution_driven_loss_usd,
  revenue_at_risk_per_visit_usd
FROM workspace.gold.kpi_execution_roi
WHERE date = '2024-05-01'
ORDER BY execution_driven_loss_usd ASC
LIMIT 10;
```

### Category Manager
**Priority KPIs:** 5
- KPI 5: Product portfolio health

**Portfolio Review:**
```sql
-- Products to discontinue vs invest
SELECT 
  product_health_status,
  strategic_value,
  COUNT(*) AS product_count,
  SUM(total_revenue_at_risk_usd) AS total_revenue_risk
FROM workspace.gold.kpi_product_portfolio_health
WHERE date = '2024-05-01'
GROUP BY product_health_status, strategic_value
ORDER BY total_revenue_risk DESC;
```

### Supply Chain Manager
**Priority KPIs:** 1, 7
- KPI 1: Recovery opportunities (many are stock issues)
- KPI 7: Perfect storms including stock failures

**Daily Operations:**
```sql
-- Urgent stock issues (Quick Wins)
SELECT *
FROM workspace.gold.kpi_recovery_opportunity_score
WHERE recovery_action_type = 'Quick Win - Restock'
  AND date = '2024-05-01'
ORDER BY recovery_priority_score DESC
LIMIT 50;
```

---


## 📈 Recommended Dashboards

### Dashboard 1: Executive Overview
**Objective:** 30-second view for CEO/Commercial Director

**Cards:**
- Total Revenue at Risk (from KPI 1)
- # Perfect Storms (from KPI 7)
- Top 3 Chains at Risk (from KPI 4)

**Charts:**
- Stacked bar: Revenue leakage by driver (from KPI 4)
- Trend line: Perfect Storms over time (from KPI 7)

### Dashboard 2: Pricing Intelligence
**Objective:** Pricing team actions

**Cards:**
- # High Price Sensitivity products (from KPI 2)
- # Deteriorating positions (from KPI 6)

**Charts:**
- Scatter: Price gap vs Revenue loss (from KPI 2)
- Velocity heatmap (from KPI 6)

### Dashboard 3: Execution Excellence
**Objective:** Optimize merchandisers

**Cards:**
- Avg visit coverage % (from KPI 3)
- Execution-driven loss (from KPI 3)

**Charts:**
- Bar: Merchandiser ranking (from KPI 3)
- Map: Execution loss by city (from KPI 3)

### Dashboard 4: Action Prioritization
**Objective:** Worklist for operational teams

**Tables:**
- Top 100 by Recovery Priority Score (from KPI 1)
- Perfect Storms requiring war room (from KPI 7)
- Products to discontinue (from KPI 5)

---

## 🔧 Technical Notes


### Serverless Compatibility
✅ All KPIs are 100% compatible with Databricks Serverless:
- Do not use initial WITH (incompatible with serverless)
- Do not use INSERT after CREATE (incompatible with external orchestrators)
- Use CREATE OR REPLACE TABLE ... AS SELECT
- Partitioned by `date` for performance

### Dependencies
```
mart_revenue_leakage (mandatory for 1,4,5,7)
    ↓
fact_pdv_price_audit (mandatory for 2,6)
fact_pdv_monthly_health (mandatory for 3)
dim_product (optional, enrichment)
dim_pdv (optional, enrichment)
```

### Refresh Schedule
**Recommended:** Monthly, 2nd day of the month (after Gold layer is closed)

```python
# Databricks Workflow
{
  "name": "Gold_KPIs_Monthly_Refresh",
  "schedule": "0 2 2 * *",  # 2 AM on the 2nd day of each month
  "tasks": [
    {"task_key": "kpi_01", "notebook_path": ".../kpi_01.../ddl/..."},
    {"task_key": "kpi_02", "notebook_path": ".../kpi_02.../ddl/...", "depends_on": ["kpi_01"]},
    # ... etc
  ]
}
```

---

## 📚 Additional Resources


### Documentation
- Each KPI has a detailed README.md in its folder
- See `/docs/architecture/` for dependency diagram
- See `/docs/data_dictionary/` for field definitions

### Support
- **Owner:** BI Team
- **Slack:** #bi-market-visibility
- **Email:** bi-team@company.com

### Training
- **Video tutorial:** [Loom/YouTube link]
- **Workshop slides:** [Google Slides link]
- **Office hours:** Fridays 10-11 AM

---

## 🚀 Roadmap


### Completed ✅
- KPIs 1-7 designed and implemented
- Complete documentation
- Serverless compatibility

### In Progress 🔄
- Dashboards in Power BI / Tableau
- Automatic alerts via email
- Integration with n8n for workflows

### Planned 📋
- KPI 8: Customer Lifetime Value at Risk
- KPI 9: Promotion Effectiveness Score
- KPI 10: Market Share Momentum Index

---

**Version:** 1.0  
**Last Updated:** 2025-01-28  
**Maintained by:** BI Team
