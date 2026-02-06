# OMOP CDM Dashboard Design Guide

## Dashboard 1: Population Health Overview

![Population Health Dashboard](file:///home/taivo/.gemini/antigravity/brain/a624020c-f2c9-48ac-9fe6-9ff0dfdba481/dashboard1_population_health_1770404950849.png)

### Layout Structure

#### Row 1: KPI Cards (5 metrics)
| Metric | Icon | Color |
|--------|------|-------|
| Total Patients | 👥 | Blue (#2196F3) |
| Total Visits | 🏥 | Green (#4CAF50) |
| Diagnoses | 📋 | Orange (#FF9800) |
| Prescriptions | 💊 | Purple (#9C27B0) |
| Avg Age | ⏱️ | Teal (#009688) |

#### Row 2: Demographics (2 pie charts)
- **Gender Distribution**: Donut chart (Male: Blue, Female: Pink)
- **Age Groups**: Donut chart (4 segments with distinct colors)

#### Row 3: Visit Trend (Line chart)
- **X-axis**: Months (Jan - Dec)
- **Y-axis**: Patient count
- **3 Lines**:
  - Inpatient (Blue)
  - Outpatient (Green)
  - Emergency (Red)

#### Row 4: Top Rankings (2 bar charts)
- **Top 10 Conditions**: Horizontal bars (Blue gradient)
- **Top 10 Medications**: Horizontal bars (Purple gradient)

### Data Sources
| Chart | Query File | CSV Export |
|-------|-----------|------------|
| KPI Cards | `KPI_SUMMARY` | KPI_SUMMARY.csv |
| Demographics | `DEMOGRAPHICS` | DEMOGRAPHICS.csv |
| Visit Trend | `VISIT_TREND` | VISIT_TREND.csv |
| Top Conditions | `TOP_CONDITIONS` | TOP_CONDITIONS.csv |
| Top Medications | `TOP_DRUGS` | TOP_DRUGS.csv |

---

## Dashboard 2: Clinical Analytics - Cohort & Comorbidity

![Clinical Analytics Dashboard](file:///home/taivo/.gemini/antigravity/brain/a624020c-f2c9-48ac-9fe6-9ff0dfdba481/dashboard2_clinical_analytics_1770404977194.png)

### Layout Structure

#### Row 1: Cohort Prevalence (Horizontal bar chart)
5 major disease cohorts with percentages:
- Diabetes (Blue)
- Hypertension (Green)
- Heart Disease (Orange)
- Respiratory (Red)
- Cancer (Purple)

#### Row 2: Comorbidity Heatmap (8x8 matrix)
- **Axes**: Top 8 diseases
- **Color Scale**: Light yellow (low) → Dark red (high)
- **Values**: Patient co-occurrence counts
- **Use case**: Identify disease correlations

#### Row 3: Risk Analysis (2 charts)
**Left - Polypharmacy Pie Chart:**
- 1 drug: 40%
- 2-4 drugs: 35%
- 5-9 drugs: 20%
- 10+ drugs (High Risk): 5%

**Right - Conditions by Age (Stacked bars):**
- 4 age groups (0-17, 18-39, 40-64, 65+)
- Top 5 conditions stacked

#### Row 4: Disease-Drug Flow (Sankey diagram)
- **Left**: Top 5 diseases
- **Right**: Top 10 medications
- **Flows**: Patient volumes
- **Colors**: Disease-specific

### Data Sources
| Chart | Query File | CSV Export |
|-------|-----------|------------|
| Cohort Prevalence | `COHORT_SUMMARY` | COHORT_SUMMARY.csv |
| Comorbidity Matrix | `COMORBIDITY_MATRIX` | COMORBIDITY_MATRIX.csv |
| Polypharmacy | `POLYPHARMACY` | POLYPHARMACY.csv |
| Conditions by Age | `CONDITION_BY_AGE` | CONDITION_BY_AGE.csv |
| Drug-Disease Flow | `DRUG_CONDITION_MAP` | DRUG_CONDITION_MAP.csv |

---

## Implementation Steps

### Option 1: Amazon QuickSight

1. **Create Data Source**
   ```
   Data source: Amazon Athena
   Database: healthcare
   Tables: person, visit_occurrence, condition_occurrence, drug_exposure, concept
   ```

2. **Create Datasets**
   - Run each query from `analytics/dashboard_queries.sql`
   - Import as SPICE datasets for faster performance

3. **Build Visuals**
   - Dashboard 1: 5 sheets (KPIs, Demographics, Trend, Conditions, Drugs)
   - Dashboard 2: 5 sheets (Cohorts, Heatmap, Polypharmacy, Age, Sankey)

4. **Apply Filters**
   - Date range selector
   - Gender filter
   - Age group filter

### Option 2: Tableau

1. **Connect to AWS Athena**
   ```
   Connection → Amazon Athena
   Database: healthcare
   ```

2. **Create Calculated Fields**
   ```
   Age: YEAR(TODAY()) - [year_of_birth]
   Age Group: IF [Age] < 18 THEN '0-17' ELSEIF [Age] < 40 THEN '18-39' ...
   ```

3. **Build Dashboards**
   - Use provided mockups as design reference
   - Match color schemes exactly

### Option 3: PowerBI

1. **Get Data from Athena**
   ```
   Get Data → More → Amazon Athena
   DirectQuery mode for real-time data
   ```

2. **Create Measures**
   ```DAX
   Total Patients = COUNTROWS(person)
   Avg Age = AVERAGE(YEAR(TODAY()) - person[year_of_birth])
   ```

3. **Design Pages**
   - Page 1: Population Health
   - Page 2: Clinical Analytics

---

## Color Palettes

### Dashboard 1 (Medical Blues & Greens)
```
Primary:   #1E88E5 (Blue)
Success:   #43A047 (Green)
Warning:   #FB8C00 (Orange)
Info:      #7B1FA2 (Purple)
Accent:    #00897B (Teal)
Background: #FFFFFF (White)
Text:      #263238 (Dark Gray)
```

### Dashboard 2 (Clinical Purples & Reds)
```
Primary:   #6A1B9A (Purple)
Secondary: #EF6C00 (Orange)
Alert:     #D32F2F (Red)
Accent:    #1976D2 (Blue)
Heatmap:   #FFEB3B → #D32F2F (Yellow to Red)
Background: #FAFAFA (Light Gray)
Text:      #212121 (Black)
```

---

## Best Practices

### Performance
- ✅ Import CSV to SPICE (QuickSight) for <1s load time
- ✅ Use Athena partitioning for large datasets
- ✅ Limit initial data to last 12 months

### UX Design
- ✅ Keep KPIs above the fold
- ✅ Use consistent color coding across dashboards
- ✅ Add tooltips with detailed metrics
- ✅ Enable drill-down from high-level to detail

### Accessibility
- ✅ Use colorblind-friendly palettes
- ✅ Add data labels on charts
- ✅ Ensure 4.5:1 contrast ratio for text

---

## Sample Dashboard URLs (After Deployment)

```
Dashboard 1: https://quicksight.aws.amazon.com/...../population-health
Dashboard 2: https://quicksight.aws.amazon.com/...../clinical-analytics
```

**Share with**: Clinical researchers, hospital administrators, data analysts
