# KPI 4: Channel & Chain Performance Matrix

## 🎯 Business Purpose


Segments revenue leakage by channel and chain to:
- Identify where the biggest problem is
- Understand which lever to attack by segment
- Allocate resources in a differentiated way


**This KPI answers:** In which channels/chains am I losing the most and why?

## 📊 Business Value


**Enabled decision:**
- Differentiated commercial strategy by channel/chain
- Investment prioritization: where to allocate my budget?
- Identify chains with the greatest opportunity for improvement
- Negotiation with retailers based on data


**Expected impact:**
- Focus 80% of resources on 20% of critical channels
- Personalize strategy: pricing for some, execution for others
- Improve retail relationship by showing objective gaps

## 🔑 Key Metrics

| Metric | Description |
|---------|-------------|
| `performance_tier` | Critical / Poor / Average / Good Performance |
| `primary_issue` | Stock / Price / Execution / Mixed Issues |
| `total_revenue_lost_usd` | Total revenue lost in that channel-chain |
| `oos_driven_loss_usd` | Specific loss due to out-of-stock |
| `price_driven_loss_usd` | Specific loss due to non-competitive price |
| `execution_driven_loss_usd` | Specific loss due to poor execution |

## 🎬 Use Cases


### Use Case 1: Channel prioritization
**Problem:** Where to focus my commercial team?
**Solution:** Filter `performance_tier IN ('Critical', 'Poor')` and sort by `total_revenue_lost_usd DESC`.

### Use Case 2: Estrategia por cadena
**Problema:** Cadena A vs Cadena B, ¿qué hacer diferente?
**Solución:** 
- Si Cadena A: `primary_issue = 'Price Issue'` → Negociar precios
- Si Cadena B: `primary_issue = 'Execution Issue'` → Reforzar merchandising

### Use Case 3: Budget allocation
**Problema:** Tengo $100K, ¿cómo los distribuyo?
**Solución:** Proporcional a `total_revenue_lost_usd` pero ajustado por `primary_issue` (stock es más barato de resolver que precio).

### Use Case 4: Comparación temporal
**Problema:** ¿Mejoró mi estrategia en Modern Trade vs Traditional?
**Solución:** Comparar `avg_leakage_rate` mes vs mes por canal.

## 📈 Expected Output Sample

```
date       | channel        | chain      | total_pdvs | total_revenue_lost_usd | avg_leakage_rate | performance_tier     | primary_issue  | oos_driven_loss_usd | price_driven_loss_usd
-----------|----------------|------------|------------|------------------------|-----------------|---------------------|----------------|-------------------|---------------------
2024-05-01 | Modern Trade   | ChainA     | 120        | 45,000.00              | 0.28            | Poor Performance    | Price Issue    | 8,500.00          | 25,000.00
2024-05-01 | Modern Trade   | ChainB     | 95         | 32,000.00              | 0.22            | Poor Performance    | Stock Issue    | 18,000.00         | 9,000.00
2024-05-01 | Traditional    | ChainC     | 450        | 28,000.00              | 0.15            | Average Performance | Execution Issue| 5,000.00          | 8,000.00
2024-05-01 | E-commerce     | ChainD     | 12         | 12,000.00              | 0.35            | Critical Performance| Mixed Issues   | 4,000.00          | 5,000.00
```

## 🔄 Refresh Frequency

**Monthly** - Aligned with `mart_revenue_leakage` grain.

## 🛠️ Technical Notes

- **Serverless Compatible:** ✅ Yes
- **Partitioned by:** `date`
- **Aggregation level:** `channel` + `chain`
- **Key logic:** `primary_issue` usa GREATEST() para identificar driver dominante

## 📊 Visualization Recommendations

1. **Heatmap:** Channels (rows) vs Chains (columns) coloreado por `total_revenue_lost_usd`
2. **Stacked bar chart:** Por channel, stack = driver type (stock, price, execution)
3. **Scatter plot:** `avg_leakage_rate` (X) vs `total_revenue_lost_usd` (Y), size = `total_pdvs`, color = `primary_issue`
4. **Performance matrix:** 2x2 (Magnitude vs Severity) con channels posicionados

## 💡 Business Insights

**Critical Performance + Stock Issue:**
- Problema de supply chain o forecasting
- Acción inmediata con Operations

**Poor Performance + Price Issue:**
- Problema comercial o de competitividad
- Acción con Pricing team y Key Account Managers

**Average Performance + Execution Issue:**
- Problema de Trade Marketing
- Acción con merchandisers y en-store activation

**Good Performance:**
- Mantener y replicar best practices
- Liberar recursos para segmentos críticos

## 🎯 Recommended Actions by Primary Issue

| Primary Issue | Recommended Action | Owner |
|---------------|-------------------|-------|
| Stock Issue | Forecast review, safety stock adjustment | Supply Chain |
| Price Issue | Price list adjustment, promotional strategy | Pricing/Commercial |
| Execution Issue | Merchandiser reallocation, training | Trade Marketing |
| Mixed Issues | Cross-functional war room | General Manager |

---
**Owner:** Commercial Strategy & BI Team  
**Last Updated:** 2025-01-28  
**Related Tables:** `mart_revenue_leakage`
