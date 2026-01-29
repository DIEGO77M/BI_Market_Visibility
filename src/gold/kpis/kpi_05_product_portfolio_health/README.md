# KPI 5: Product Portfolio Health - Winners vs Losers

## 🎯 Business Purpose


Identifies products that are losing share due to avoidable causes:
- Recurrent stock out
- Non-competitive price
- Lack of execution
- Mismatch with expected assortment


**This KPI answers:** Which products should I strengthen, maintain, or discontinue?

## 📊 Business Value


**Enabled decision:**
- Redefine active portfolio by PDV (discontinue/strengthen)
- Identify star products vs problem products
- Optimize assortment and shelf space
- Prioritize launches and discontinuations


**Expected impact:**
- Concentrate resources on products with the greatest potential
- Free up space/capital from "loser" products
- Improve sell-out and turnover

## 🔑 Key Metrics

| Metric | Description |
|---------|-------------|
| `product_health_status` | Critical / At Risk / Monitor / Healthy / Stable |
| `recommended_action` | Specific action for each product |
| `strategic_value` | Strategic / Important / Standard Product |
| `stock_availability_pct` | % of PDVs where the product has stock |
| `total_revenue_at_risk_usd` | Potential revenue lost for this product |

## 🎬 Use Cases


### Use Case 1: Portfolio rationalization
**Problem:** I have 500 SKUs, which ones to discontinue?
**Solution:** 
- Filtrar `product_health_status = 'Critical - High Loss'` Y `recommended_action = 'Consider Portfolio Exit'`
- Validar si son strategic products antes de descontinuar

### Use Case 2: Focus on winners
**Problema:** ¿En qué productos invertir en marketing?
**Solución:** 
- Filtrar `product_health_status = 'Healthy'` Y `strategic_value = 'Strategic Product'`
- Estos son los productos que ya funcionan, amplificarlos

### Use Case 3: Turnaround plan
**Problema:** Productos "At Risk", ¿se pueden salvar?
**Solución:** 
- Si `recommended_action = 'Improve Distribution'` → Problema de supply chain (solucionable)
- Si `recommended_action = 'Review Pricing'` → Problema de competitividad (requiere análisis)
- Si `recommended_action = 'Reassess Assortment Fit'` → Producto en PDVs incorrectos

### Use Case 4: Category management
**Problema:** ¿Cómo optimizar mi categoría de bebidas?
**Solución:** `GROUP BY category` y ver distribución de health_status para identificar categorías problemáticas

## 📈 Expected Output Sample

```
date       | product_code | brand  | category | product_health_status | total_revenue_at_risk_usd | stock_availability_pct | recommended_action       | strategic_value
-----------|--------------|--------|----------|----------------------|--------------------------|----------------------|-------------------------|------------------
2024-05-01 | PROD_A123    | BrandX | Cat1     | Critical - High Loss | 12,500.00                | 45.5                 | Improve Distribution    | Strategic Product
2024-05-01 | PROD_B456    | BrandY | Cat2     | At Risk              | 5,200.00                 | 72.3                 | Review Pricing          | Important Product
2024-05-01 | PROD_C789    | BrandZ | Cat1     | Healthy              | 500.00                   | 95.1                 | Maintain & Grow         | Strategic Product
2024-05-01 | PROD_D012    | BrandA | Cat3     | Monitor Closely      | 1,800.00                 | 68.0                 | Strengthen Execution    | Standard Product
```

## 🔄 Refresh Frequency

**Monthly** - Aligned with `mart_revenue_leakage` grain.

## 🛠️ Technical Notes

- **Serverless Compatible:** ✅ Yes
- **Partitioned by:** `date`
- **Aggregation level:** `product_code` (with brand, category, subcategory)
- **Key logic:** Combina magnitude (`total_revenue_at_risk_usd`) con severity (`avg_leakage_pct`)

## 📊 Visualization Recommendations

1. **BCG Matrix adapted:** 
   - X: `stock_availability_pct`
   - Y: `total_revenue_at_risk_usd`
   - Size: `total_pdvs_exposed`
   - Color: `product_health_status`

2. **Sankey diagram:** `brand` → `category` → `product_health_status`

3. **Table:** Top 20 products por `total_revenue_at_risk_usd` con `recommended_action`

4. **Trend:** Evolution of `product_health_status` distribution over time

## 💡 Business Insights

### Critical - High Loss
- **Acción:** Intervención urgente o discontinuación
- **Owner:** Category Manager + Supply Chain
- **Timeline:** 30 días

### At Risk
- **Acción:** Plan de recuperación
- **Owner:** Brand Manager
- **Timeline:** 60 días

### Monitor Closely
- **Acción:** Tracking preventivo
- **Owner:** BI Team
- **Timeline:** Ongoing

### Healthy
- **Acción:** Replicar best practices
- **Owner:** Commercial Excellence
- **Timeline:** Ongoing

## 🎯 Portfolio Optimization Framework

| Health Status | Strategic Value | Action |
|--------------|----------------|--------|
| Critical | Strategic | Urgent turnaround plan |
| Critical | Important | Assess if worth saving |
| Critical | Standard | Discontinue |
| At Risk | Strategic | Reinforce |
| At Risk | Important | Monitor |
| At Risk | Standard | Evaluate |
| Healthy | Strategic | Invest and grow |
| Healthy | Important | Maintain |
| Healthy | Standard | Harvest |

---
**Owner:** Category Management & BI Team  
**Last Updated:** 2025-01-28  
**Related Tables:** `mart_revenue_leakage`
