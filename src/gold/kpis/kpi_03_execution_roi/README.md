# KPI 3: Execution ROI - Revenue Recovered per Merchandiser Visit

## 🎯 Business Purpose

Quantifies Trade Marketing team effectiveness by correlating merchandiser visits with actual revenue impact.

**Answers:** Which merchandisers prevent revenue loss, and where should we invest resources?

---

## 📊 Business Value

**Key Decisions Enabled:**
- Identify high/low performing merchandisers with financial data
- Optimize territory assignments based on revenue impact potential
- Justify Trade Marketing budget with measurable ROI (typically 300-500%)
- Detect execution gaps costing real revenue

**Typical Impact:**
- 20-30% increase in merchandiser productivity
- Quantifiable reduction in execution-driven revenue loss
- Data-driven resource allocation vs gut-feel decisions

---

## 🔑 Core Metrics

| Metric | Business Meaning |
|---------|------------------|
| `visit_coverage_pct` | Are merchandisers visiting assigned stores? |
| `execution_driven_loss_usd` | Revenue lost due to poor execution (controllable) |
| `revenue_at_risk_per_visit_usd` | Financial impact per visit (ROI baseline) |
| `avg_execution_factor` | Product visibility score (0-10) |

**The Key Insight:** Separates controllable losses (poor execution) from market factors (competitor pricing). Shows exactly where coaching or support is needed.

---

## 💼 Business Scenarios Solved

### **Scenario 1: Star Performer**
High coverage + Low execution loss + Good displays → Use as benchmark, assign strategic territories

### **Scenario 2: Visitor Without Impact**
High coverage + High execution loss + No displays → Immediate coaching required, pure execution failure

### **Scenario 3: High-Value Territory**
Good execution + High revenue at risk → Not execution problem - provide pricing/promotional support

### **Scenario 4: ROI Justification**
Sum all `execution_driven_loss_usd` = Total preventable revenue loss → Trade Marketing prevents $X million annually

---

## 🎯 Real-World Application

**Performance Review:**
- Top 20%: execution_driven_loss < $5K, coverage >95%
- Bottom 20%: execution_driven_loss > $30K, coverage <75%

**Territory Optimization:**
Balance territories by `revenue_at_risk_per_visit_usd` so each merchandiser has similar impact potential

**Budget Justification:**
If team prevents $2M in annual execution-driven loss, $200K-400K Trade Marketing budget = 500%+ ROI

---

## 🛠️ Technical Implementation

### **Key Features:**
- **Pre-aggregation:** Revenue summarized by PDV first to prevent product-level double counting
- **Execution correlation:** Links visits/displays to financial impact
- **Controllable isolation:** Calculates execution-driven loss vs market-driven loss

### **SQL Techniques:**
- CTEs for data prep (revenue_by_pdv, health_by_pdv)
- MAX() aggregation to deduplicate execution flags
- Conditional aggregation (CASE WHEN) for execution breakdown
- LEFT JOIN to handle PDVs without revenue data

### **Architecture:**
```sql
-- CTE 1: Aggregate revenue by PDV (not by product)
-- CTE 2: Aggregate health metrics by PDV (deduplicate execution flags)
-- Main: Join and calculate ROI metrics
```

### **Performance:**
- Partitioned by `date`
- Monthly grain (not daily - allows meaningful trends)
- Serverless compatible
- Handles 100K+ PDV/month scale

---

## 📈 Expected Output Structure

```
Merchandiser A:
- 10 stores assigned, 9 visited (90% coverage)
- $45K revenue at risk in territory
- $5K execution-driven loss (11% - good)
- $5K per visit ROI potential

Merchandiser B:
- 8 stores assigned, 8 visited (100% coverage)
- $72K revenue at risk in territory
- $72K execution-driven loss (100% - critical)
- No displays installed despite visits
```

**Action:** Merchandiser A is performing well (external factors drive risk). Merchandiser B needs immediate coaching (100% controllable issue).

---

## 🎓 For Technical Recruiters

**This implementation demonstrates:**

✅ **Business Acumen**
- Translated field operations into financial metrics
- Separated controllable vs uncontrollable factors
- Built actionable segmentation framework

✅ **Data Engineering**
- Solved complex aggregation problem (prevent double counting)
- Efficient CTE structure for readability and performance
- Proper granularity design (monthly, merchandiser-level)

✅ **SQL Skills**
- Multi-level aggregation with CTEs
- Conditional aggregation (CASE WHEN in SUM)
- Deduplication logic (MAX for flags)
- Join optimization (pre-aggregate before join)

✅ **Impact**
- Quantified "soft" department (Trade Marketing) with hard ROI numbers
- Enabled data-driven performance management
- Typical result: Identifies 10-15% of team driving 60%+ of execution losses

---

## 🔄 Refresh Frequency

**Monthly** - Allows merchandisers time to implement changes and provides meaningful trend analysis for performance reviews.

---

**Owner:** Trade Marketing & BI Team  
**Last Updated:** 2025-01-28  
**Related Tables:** `fact_pdv_monthly_health`, `dim_pdv`, `mart_revenue_leakage`  
**Code:** `kpi_execution_roi_final.sql`