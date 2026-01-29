# KPI 7: Perfect Storm Index

## 🎯 Business Purpose


Identifies PDV-products where **ALL** factors fail simultaneously:
- ❌ Stock out
- ❌ Non-competitive price
- ❌ Poor execution
- ❌ Assortment mismatch


**This KPI answers:** Where do I have the perfect storm that amplifies my losses?

## 📊 Business Value


**Enabled decision:**
- Ultra-prioritized list for immediate intervention
- Identify "perfect storms" that exponentially amplify losses
- Assign **cross-functional** resources (not just one area)
- War rooms with multiple departments


**Expected impact:**
- Solve systemic (not isolated) problems
- Maximize ROI of interventions (fix 4 problems at once)
- Avoid catastrophic losses

## 🔑 Key Metrics

| Metric | Description |
|---------|-------------|
| `risk_tier` | Perfect Storm (4) / Critical (3) / High (2) / Medium (1) / Under Control (0) |
| `failing_factors_count` | Number of factors failing simultaneously |
| `intervention_complexity_score` | 0-100, indicates how many teams are needed |
| `required_teams` | List of teams that must intervene |
| `problem_amplification_factor` | 2^n where n = # of factors failing (exponential effect) |

## 🎬 Use Cases


### Use Case 1: War room prioritization
**Problem:** Which cases require cross-functional war room?
**Solution:** Filter `risk_tier = 'Perfect Storm - All 4 Factors Failing'` OR `risk_tier = 'Critical Storm - 3 Factors Failing'`.

### Use Case 2: Resource allocation
**Problema:** Tengo 1 persona de pricing, 1 de supply chain, 1 de trade marketing.
**Solución:** 
- Asignarlos juntos a `Perfect Storm` cases (máximo impacto)
- No dispersarlos en problemas de 1 factor

### Use Case 3: Amplification awareness
**Problema:** ¿Vale la pena resolver este caso complejo?
**Solución:** 
- Si `problem_amplification_factor = 16` (4 factores), la pérdida es 16x peor que un problema aislado
- ROI de resolverlo es exponencial

### Use Case 4: Executive escalation
**Problema:** ¿Qué casos escalar a CEO/GM?
**Solución:** 
- `risk_tier = 'Perfect Storm'` + `potential_revenue_lost_usd > $10K`
- Estos requieren decisiones de alto nivel

## 📈 Expected Output Sample

```
date       | pdv_code | product_code | brand  | risk_tier                        | failing_factors_count | potential_revenue_lost_usd | intervention_complexity_score | required_teams                                        | problem_amplification_factor
-----------|----------|--------------|--------|----------------------------------|----------------------|---------------------------|------------------------------|------------------------------------------------------|-----------------------------
2024-05-01 | PDV_001  | PROD_A123    | BrandX | Perfect Storm - All 4 Factors    | 4                    | 8,500.00                  | 100                          | Supply Chain, Pricing, Trade Marketing, Category Mgmt | 16.00
2024-05-01 | PDV_002  | PROD_B456    | BrandY | Critical Storm - 3 Factors       | 3                    | 5,200.00                  | 75                           | Supply Chain, Pricing, Trade Marketing               | 8.00
2024-05-01 | PDV_003  | PROD_C789    | BrandZ | High Risk - 2 Factors Failing    | 2                    | 3,100.00                  | 50                           | Pricing, Trade Marketing                             | 4.00
2024-05-01 | PDV_004  | PROD_D012    | BrandA | Medium Risk - 1 Factor Failing   | 1                    | 1,200.00                  | 25                           | Supply Chain                                         | 2.00
```

## 🔄 Refresh Frequency

**Monthly** - Aligned with `mart_revenue_leakage` grain.

## 🛠️ Technical Notes

- **Serverless Compatible:** ✅ Yes
- **Partitioned by:** `date`
- **Key insight:** `problem_amplification_factor = 2^(failing_factors_count)` representa efecto compuesto
- **Filters only records with revenue loss:** `WHERE potential_revenue_lost_usd > 0`

## 📊 Visualization Recommendations

1. **Alert Card Dashboard:**
   ```
   🔴 Perfect Storms: 12 casos (XXX,XXX USD at risk)
   🟠 Critical Storms: 35 casos (XXX,XXX USD at risk)
   🟡 High Risk: 120 casos (XXX,XXX USD at risk)
   ```

2. **Sankey diagram:** 
   - Flow from total PDV-products → risk_tier → required_teams

3. **Bubble chart:**
   - X: `failing_factors_count`
   - Y: `potential_revenue_lost_usd`
   - Size: `problem_amplification_factor`
   - Color: `risk_tier`

4. **Table:** Top 50 Perfect Storms ordenados por `potential_revenue_lost_usd DESC`

## 💡 Business Insights

### Perfect Storm (4 factors)
- **Frecuencia esperada:** <5% de casos
- **Impacto:** 60-80% de revenue at risk total
- **Acción:** War room inmediato, CEO involvement
- **SLA:** 48 horas para plan de acción

### Critical Storm (3 factors)
- **Frecuencia esperada:** 10-15% de casos
- **Impacto:** 20-30% de revenue at risk total
- **Acción:** Cross-functional team, VP involvement
- **SLA:** 1 semana para plan de acción

### High Risk (2 factors)
- **Frecuencia esperada:** 25-30% de casos
- **Impacto:** 15-20% de revenue at risk total
- **Acción:** Coordinación entre 2 áreas
- **SLA:** 2 semanas

### Medium Risk (1 factor)
- **Frecuencia esperada:** 40-50% de casos
- **Impacto:** 5-10% de revenue at risk total
- **Acción:** Área específica individual
- **SLA:** 1 mes

## 🚨 War Room Protocol

**When Perfect Storm is detected:**

1. **Immediate (Day 1):**
   - Alert to all `required_teams` heads
   - Block calendars for war room session
   - Pull detailed data for affected PDV-product

2. **War Room Session (Day 2):**
   - Review root causes por cada factor
   - Define owner por cada factor
   - Set deadlines (7-14 días)

3. **Execution (Day 3-14):**
   - Daily standups con owners
   - Track progress on each factor
   - Remove blockers

4. **Validation (Day 15-30):**
   - Re-measure factors post-intervention
   - Confirm `risk_tier` improved
   - Document lessons learned

## 🎯 Success Metrics

**Goal:** Reduce Perfect Storms by 80% within 60 days

**Tracking:**
```sql
SELECT 
  date,
  SUM(CASE WHEN risk_tier = 'Perfect Storm - All 4 Factors Failing' THEN 1 ELSE 0 END) AS perfect_storm_count,
  SUM(CASE WHEN risk_tier = 'Critical Storm - 3 Factors Failing' THEN 1 ELSE 0 END) AS critical_count
FROM workspace.gold.kpi_perfect_storm_index
GROUP BY date
ORDER BY date
```

---
**Owner:** General Manager / Operations Director + BI Team  
**Last Updated:** 2025-01-28  
**Related Tables:** `mart_revenue_leakage`  
**Escalation Required:** YES for Perfect Storm cases
