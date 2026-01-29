
# KPI 6: Competitive Position Erosion Velocity

## 🎯 Business Purpose

Quantifies the speed and scope at which our products are losing or recovering competitive position at the point of sale. This KPI goes beyond static price gap analysis, enabling the business to detect and respond to negative market trends before they result in significant revenue loss.

**This KPI answers:** Where and how fast are we losing (or regaining) competitive ground, and which products or channels require immediate intervention?

## 📊 Business Value

**Enabled decision:**
- Early identification of products and PDVs with rapid competitive deterioration
- Prioritization of pricing and commercial actions by velocity and impact
- Proactive mitigation of revenue leakage before it escalates
- Strategic focus on recovery opportunities and market defense

**Expected impact:**
- Reduce time-to-action from months to days for pricing and commercial teams
- Prevent margin erosion and volume loss by acting on early warning signals
- Strengthen competitive intelligence and market responsiveness

## 🔑 Key Metrics

| Metric | Description |
|--------|-------------|
| `pct_pdvs_deteriorating` | % of PDVs where the product is deteriorating |
| `avg_velocity_pct` | Average monthly change in price gap vs. competition |
| `has_critical_alert` | Flag for urgent action (price gap >10% and position worsened) |
| `overall_trend` | Strategic summary (Widespread Deterioration, Recovery, Stable, Mixed) |
| `recommended_action` | Data-driven next step (Immediate Price Review, Strategic Intervention, etc.) |

## 🎬 Use Cases

### Use Case 1: Early warning for pricing
**Problem:** Price gaps are widening, but the business reacts too late.
**Solution:**
- Filter `has_critical_alert = 1` and `overall_trend = 'Widespread Deterioration'`
- Trigger immediate price review and commercial intervention

### Use Case 2: Channel or region prioritization
**Problem:** Limited resources to address all market issues simultaneously.
**Solution:**
- Group by `channel` or `region` and sort by `pct_pdvs_deteriorating`
- Focus on channels/regions with highest velocity of position loss

### Use Case 3: Recovery opportunity identification
**Problem:** Some products are regaining competitive position, but not leveraged.
**Solution:**
- Filter `overall_trend = 'Recovery'` and `avg_velocity_pct < 0`
- Allocate marketing or trade support to reinforce positive momentum

### Use Case 4: Executive dashboarding
**Problem:** Need to communicate market threats and opportunities to leadership.
**Solution:**
- Visualize trend of `pct_pdvs_deteriorating` and `has_critical_alert` over time
- Highlight products with persistent or high-velocity erosion

## 📈 Expected Output Sample

```
date       | product_code | brand  | pct_pdvs_deteriorating | avg_velocity_pct | has_critical_alert | overall_trend           | recommended_action
-----------|--------------|--------|------------------------|------------------|--------------------|-------------------------|---------------------------------
2024-05-01 | PROD_A123    | BrandX | 62.5                   | +4.2             | 1                  | Widespread Deterioration| Immediate Price Review Required
2024-05-01 | PROD_B456    | BrandY | 12.0                   | -1.1             | 0                  | Stable Position         | Maintain Vigilance
```

## 🔄 Refresh Frequency

**Monthly** - Aligned with `fact_pdv_price_audit` grain.

## 🛠️ Technical Notes

- **Serverless Compatible:** ✅ Yes
- **Partitioned by:** `date`
- **Aggregation level:** `product_code` (with brand, category, subcategory)
- **Key logic:** Tracks monthly change in price gap and classifies competitive trend and velocity
- **Alert logic:** Flags high-velocity deteriorations (price gap >10% and position worsened)

## 📊 Visualization Recommendations

1. **Heatmap:**
	- X: `date`
	- Y: `product_code` or `brand`
	- Color: `pct_pdvs_deteriorating` or `avg_velocity_pct`

2. **Line chart:**
	- Track `avg_velocity_pct` over time for top products

3. **Table:**
	- Top 20 products by `pct_pdvs_deteriorating` with `has_critical_alert`

4. **Alert dashboard:**
	- List all products with `has_critical_alert = 1` and recommended action

## 💡 Business Insights

### Widespread Deterioration
- **Action:** Immediate price review and commercial defense
- **Owner:** Pricing & Commercial Intelligence Team
- **Timeline:** 7 days

### Recovery
- **Action:** Reinforce positive momentum, allocate support
- **Owner:** Brand/Channel Manager
- **Timeline:** 30 days

### Stable
- **Action:** Monitor, maintain vigilance
- **Owner:** BI Team
- **Timeline:** Ongoing

### Mixed
- **Action:** Deep dive by segment, targeted interventions
- **Owner:** Commercial Excellence
- **Timeline:** Ongoing

## 🎯 Competitive Position Response Framework

| Overall Trend | Critical Alert | Action |
|---------------|---------------|--------|
| Widespread Deterioration | Yes | Immediate price review, defend market share |
| Widespread Deterioration | No  | Monitor, prepare intervention |
| Recovery                | Yes | Reinforce, communicate success |
| Recovery                | No  | Maintain, monitor |
| Stable                  | Any | Maintain vigilance |
| Mixed                   | Any | Segment and prioritize |

---
**Owner:** Pricing & Commercial Intelligence Team  
**Last Updated:** 2026-01-28  
**Related Tables:** `fact_pdv_price_audit`, `dim_product`, `dim_pdv`
**Key Filter:** `competitive_trend = 'Deteriorating'` AND `velocity_magnitude = 'High Velocity'`
