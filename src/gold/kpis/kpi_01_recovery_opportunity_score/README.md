
# KPI: Revenue Recovery Opportunity Score

---

## 🏆 Overview

The Revenue Recovery Opportunity Score is a decision-driven KPI designed to prioritize commercial and operational actions at the **PDV–product–month** level by quantifying where lost revenue can be recovered fastest and with the highest impact.

> This KPI does not measure execution, stock, or pricing in isolation. Instead, it translates multiple operational signals into a single economic prioritization score, enabling business teams to focus on recoverable revenue, not just observed issues.

---

## 💼 Business Problem Addressed

In retail and CPG environments, revenue loss is often visible but not prioritized:

- Many PDVs show issues simultaneously
- Resources are limited (merchandisers, pricing actions, negotiations)
- Not all problems have the same economic urgency or ease of recovery

**Key business question:**

> “If we can only act on a limited number of PDVs this month, where should we intervene to recover the most revenue, fastest?”

This KPI answers that question.

---

## 📏 What This KPI Measures (Conceptually)

The KPI measures the **economic priority of intervention**, not operational compliance. It combines four dimensions:

**1️⃣ Revenue at Risk (Magnitude)**
- `potential_revenue_lost_usd`: Quantifies how much money is at stake if the issue persists

**2️⃣ Leakage Intensity**
- `revenue_leakage_pct`: Indicates how severe the revenue loss is relative to potential

**3️⃣ Recoverability Factors**
- `stock_availability_factor`, `price_competitiveness_factor`: Identify whether the issue is a quick win or structurally complex

**4️⃣ Actionable Root Cause Signals**
- `execution_visibility_factor`, `assortment_alignment_factor`: Pinpoint where the business must act (execution, price, stock, assortment)

---

## 📝 KPI Output

The KPI produces:

- **Recovery Priority Score (0–100):** A normalized score ranking PDV–product combinations by intervention urgency
- **Recovery Action Type:**
	- Quick Win – Restock
	- Price Adjustment Needed
	- Execution Gap
	- Assortment Mismatch
	- Under Control
- **Revenue Loss Decomposition:**
	- Stock-driven loss
	- Price-driven loss
	- Execution-driven loss
	- Assortment-driven loss

This allows teams to understand both “how urgent” and “why”.

---

## ⚡ Why Execution Gaps Often Dominate

In many real datasets, results show:

- Stock = Available
- Price = Competitive
- Assortment = Aligned
- Execution = Weak

This is not a modeling issue — it is a business insight.

**Interpretation:**

Products are correctly listed, priced, and supplied, but are not being effectively executed at point of sale (visibility, placement, activation).

Execution gaps are among the most expensive and most recoverable sources of revenue leakage in retail.

---

## 🔄 How Actions Affect the KPI Over Time

This KPI is self-regulating.

**Before Action:**
- Low execution visibility factor
- High execution-driven revenue loss
- High recovery priority score
- PDV appears at the top of the intervention list

**After Corrective Action (e.g. execution fixed):**
- Execution visibility factor increases
- Execution-driven loss decreases
- Recovery priority score drops
- PDV naturally exits the priority ranking

> The KPI does not measure the action itself — it measures the economic result of the action.

This ensures that:
- Teams are rewarded for outcomes, not activities
- Attention shifts automatically to the next highest-value opportunity

---

## 🧭 Decisions Enabled by This KPI

- ✔ Prioritized merchandiser routing
- ✔ Focus on revenue recovery, not generic compliance
- ✔ Identification of quick wins vs structural issues
- ✔ Efficient allocation of commercial resources
- ✔ Clear linkage between operational fixes and financial impact

---

## 🎓 Why This KPI Is Senior-Level

This is not a descriptive KPI. **It is a decision engine.**

| Traditional KPI                | Revenue Recovery Opportunity Score         |
|-------------------------------|-------------------------------------------|
| Are we compliant?             | Where should we act first?                |
| What is wrong?                | What costs us the most if ignored?        |
| Did execution happen?         | What action recovers revenue fastest?     |

It aligns operations, finance, and commercial strategy into a single prioritization framework.

---

## ⚙️ Technical Characteristics

- Monthly grain (aligned with business planning cycles)
- Built on Gold-layer curated data
- Fully compatible with Databricks Serverless + Unity Catalog
- Orchestrator-friendly (idempotent, no side effects)
- No synthetic or invented fields
- Designed for BI dashboards and operational consumption