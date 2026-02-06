-- ============================================================
-- OMOP CDM - 2 BEST DASHBOARDS
-- Based on OHDSI/OMOP-Queries standard patterns
-- Database: healthcare (AWS Athena)
-- ============================================================

-- ============================================================
-- DASHBOARD 1: POPULATION HEALTH OVERVIEW
-- Visual: Executive summary of patient population
-- Charts: 6 visualizations
-- ============================================================

-- [1.1] KPI_SUMMARY - Key Performance Indicators (5 Cards)
-- Purpose: High-level metrics at a glance
SELECT 
    (SELECT COUNT(DISTINCT person_id) FROM healthcare.person) as total_patients,
    (SELECT COUNT(*) FROM healthcare.visit_occurrence) as total_visits,
    (SELECT COUNT(*) FROM healthcare.condition_occurrence) as total_conditions,
    (SELECT COUNT(*) FROM healthcare.drug_exposure) as total_prescriptions,
    (SELECT ROUND(AVG(YEAR(CURRENT_DATE) - year_of_birth), 1) FROM healthcare.person) as avg_age;


-- [1.2] GENDER_DISTRIBUTION - Patient count by gender (Pie Chart)
-- Based on OHDSI query: Number of patients of specific gender
SELECT 
    c.concept_name as gender,
    COUNT(p.person_id) as patient_count,
    ROUND(COUNT(p.person_id) * 100.0 / SUM(COUNT(p.person_id)) OVER(), 1) as percentage
FROM healthcare.person p
INNER JOIN healthcare.concept c ON p.gender_concept_id = c.concept_id
GROUP BY c.concept_name
ORDER BY patient_count DESC;


-- [1.3] AGE_DISTRIBUTION - Patients grouped by year of birth (Bar Chart)
-- Based on OHDSI query: Number of patients grouped by year of birth
SELECT 
    CASE 
        WHEN YEAR(CURRENT_DATE) - year_of_birth < 18 THEN '0-17'
        WHEN YEAR(CURRENT_DATE) - year_of_birth BETWEEN 18 AND 39 THEN '18-39'
        WHEN YEAR(CURRENT_DATE) - year_of_birth BETWEEN 40 AND 64 THEN '40-64'
        ELSE '65+'
    END as age_group,
    COUNT(person_id) as patient_count,
    ROUND(COUNT(person_id) * 100.0 / SUM(COUNT(person_id)) OVER(), 1) as percentage
FROM healthcare.person
GROUP BY 1
ORDER BY 1;


-- [1.4] VISIT_MONTHLY_PATTERN - Visits aggregated by month (Line Chart)
-- Purpose: Seasonal patterns across all years
SELECT 
    MONTH(visit_start_date) as month_num,
    CASE MONTH(visit_start_date)
        WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar'
        WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun'
        WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep'
        WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
    END as month_name,
    COUNT(*) as total_visits,
    COUNT(DISTINCT person_id) as unique_patients
FROM healthcare.visit_occurrence
WHERE visit_start_date IS NOT NULL
GROUP BY 1, 2
ORDER BY 1;


-- [1.5] VISIT_TYPE_BREAKDOWN - Visit types distribution (Pie Chart)
-- Purpose: Inpatient vs Outpatient vs Emergency
SELECT 
    c.concept_name as visit_type,
    COUNT(*) as visit_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
FROM healthcare.visit_occurrence v
INNER JOIN healthcare.concept c ON v.visit_concept_id = c.concept_id
GROUP BY c.concept_name
ORDER BY visit_count DESC;


-- [1.6] LOCATION_DISTRIBUTION - Patients by state (Map/Bar Chart)
-- Based on OHDSI query: Number of patients grouped by residence state
SELECT 
    COALESCE(l.state, 'Unknown') as state,
    COUNT(*) as patient_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
FROM healthcare.person p
LEFT JOIN healthcare.location l ON p.location_id = l.location_id
GROUP BY 1
ORDER BY patient_count DESC
LIMIT 15;


-- ============================================================
-- DASHBOARD 2: CLINICAL ANALYTICS
-- Visual: Deep clinical insights for healthcare analysis
-- Charts: 6 visualizations
-- ============================================================

-- [2.1] TOP_CONDITIONS - Most frequent conditions (Horizontal Bar)
-- Based on OHDSI C01/C02: Find condition by concept
SELECT 
    c.concept_id as condition_concept_id,
    c.concept_name as condition_name,
    c.concept_code as condition_code,
    c.vocabulary_id as vocabulary,
    COUNT(*) as occurrences,
    COUNT(DISTINCT co.person_id) as patients,
    ROUND(COUNT(DISTINCT co.person_id) * 100.0 / (SELECT COUNT(*) FROM healthcare.person), 2) as prevalence_pct
FROM healthcare.condition_occurrence co
INNER JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
WHERE c.domain_id = 'Condition'
GROUP BY 1, 2, 3, 4
ORDER BY occurrences DESC
LIMIT 15;


-- [2.2] TOP_DRUGS - Most prescribed medications (Horizontal Bar)
-- Purpose: Drug utilization analysis
SELECT 
    c.concept_id as drug_concept_id,
    c.concept_name as drug_name,
    c.concept_code as drug_code,
    c.vocabulary_id as vocabulary,
    COUNT(*) as prescriptions,
    COUNT(DISTINCT de.person_id) as patients,
    ROUND(COUNT(DISTINCT de.person_id) * 100.0 / (SELECT COUNT(*) FROM healthcare.person), 2) as usage_pct
FROM healthcare.drug_exposure de
INNER JOIN healthcare.concept c ON de.drug_concept_id = c.concept_id
WHERE c.domain_id = 'Drug'
GROUP BY 1, 2, 3, 4
ORDER BY prescriptions DESC
LIMIT 15;


-- [2.3] GENDER_AGE_CONDITIONS - Conditions by gender and age (Stacked Bar)
-- Based on OHDSI query: Patients by gender stratified by year of birth
SELECT 
    gc.concept_name as gender,
    CASE 
        WHEN YEAR(CURRENT_DATE) - p.year_of_birth < 18 THEN '0-17'
        WHEN YEAR(CURRENT_DATE) - p.year_of_birth BETWEEN 18 AND 39 THEN '18-39'
        WHEN YEAR(CURRENT_DATE) - p.year_of_birth BETWEEN 40 AND 64 THEN '40-64'
        ELSE '65+'
    END as age_group,
    cc.concept_name as condition_name,
    COUNT(*) as count
FROM healthcare.condition_occurrence co
INNER JOIN healthcare.person p ON co.person_id = p.person_id
INNER JOIN healthcare.concept gc ON p.gender_concept_id = gc.concept_id
INNER JOIN healthcare.concept cc ON co.condition_concept_id = cc.concept_id
WHERE cc.concept_id IN (
    SELECT condition_concept_id 
    FROM healthcare.condition_occurrence 
    GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 5
)
GROUP BY 1, 2, 3
ORDER BY 1, 2, count DESC;


-- [2.4] DISEASE_COHORTS - Major disease groups prevalence (Bar Chart)
-- Purpose: Chronic disease burden analysis
WITH patient_total AS (SELECT COUNT(*) as cnt FROM healthcare.person)
SELECT 
    'Diabetes' as cohort,
    COUNT(DISTINCT co.person_id) as patients,
    ROUND(COUNT(DISTINCT co.person_id) * 100.0 / (SELECT cnt FROM patient_total), 1) as prevalence_pct
FROM healthcare.condition_occurrence co
INNER JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
WHERE LOWER(c.concept_name) LIKE '%diabetes%'

UNION ALL

SELECT 'Hypertension', COUNT(DISTINCT co.person_id),
       ROUND(COUNT(DISTINCT co.person_id) * 100.0 / (SELECT cnt FROM patient_total), 1)
FROM healthcare.condition_occurrence co
INNER JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
WHERE LOWER(c.concept_name) LIKE '%hypertension%'
   OR LOWER(c.concept_name) LIKE '%hypertensive%'

UNION ALL

SELECT 'Heart Disease', COUNT(DISTINCT co.person_id),
       ROUND(COUNT(DISTINCT co.person_id) * 100.0 / (SELECT cnt FROM patient_total), 1)
FROM healthcare.condition_occurrence co
INNER JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
WHERE LOWER(c.concept_name) LIKE '%heart%'
   OR LOWER(c.concept_name) LIKE '%cardiac%'
   OR LOWER(c.concept_name) LIKE '%coronary%'

UNION ALL

SELECT 'Respiratory', COUNT(DISTINCT co.person_id),
       ROUND(COUNT(DISTINCT co.person_id) * 100.0 / (SELECT cnt FROM patient_total), 1)
FROM healthcare.condition_occurrence co
INNER JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
WHERE LOWER(c.concept_name) LIKE '%asthma%'
   OR LOWER(c.concept_name) LIKE '%copd%'
   OR LOWER(c.concept_name) LIKE '%bronchitis%'

UNION ALL

SELECT 'Cancer', COUNT(DISTINCT co.person_id),
       ROUND(COUNT(DISTINCT co.person_id) * 100.0 / (SELECT cnt FROM patient_total), 1)
FROM healthcare.condition_occurrence co
INNER JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
WHERE LOWER(c.concept_name) LIKE '%cancer%'
   OR LOWER(c.concept_name) LIKE '%malignant%'
   OR LOWER(c.concept_name) LIKE '%carcinoma%'

ORDER BY patients DESC;


-- [2.5] COMORBIDITY_PAIRS - Disease co-occurrence (Heatmap/Table)
-- Purpose: Identify common comorbidities
WITH top_conditions AS (
    SELECT 
        co.condition_concept_id,
        c.concept_name,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn
    FROM healthcare.condition_occurrence co
    INNER JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
    WHERE c.domain_id = 'Condition'
    GROUP BY 1, 2
)
SELECT 
    tc1.concept_name as condition_1,
    tc2.concept_name as condition_2,
    COUNT(DISTINCT co1.person_id) as patients_with_both
FROM healthcare.condition_occurrence co1
INNER JOIN healthcare.condition_occurrence co2 
    ON co1.person_id = co2.person_id 
    AND co1.condition_concept_id < co2.condition_concept_id
INNER JOIN top_conditions tc1 ON co1.condition_concept_id = tc1.condition_concept_id AND tc1.rn <= 8
INNER JOIN top_conditions tc2 ON co2.condition_concept_id = tc2.condition_concept_id AND tc2.rn <= 8
GROUP BY 1, 2
ORDER BY patients_with_both DESC
LIMIT 20;


-- [2.6] POLYPHARMACY_ANALYSIS - Drug count per patient (Pie Chart)
-- Purpose: Identify patients at risk of polypharmacy
SELECT 
    CASE 
        WHEN drug_count = 1 THEN '1 drug (Low)'
        WHEN drug_count BETWEEN 2 AND 4 THEN '2-4 drugs (Normal)'
        WHEN drug_count BETWEEN 5 AND 9 THEN '5-9 drugs (Moderate)'
        ELSE '10+ drugs (High Risk)'
    END as risk_category,
    COUNT(*) as patients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
FROM (
    SELECT 
        de.person_id, 
        COUNT(DISTINCT de.drug_concept_id) as drug_count 
    FROM healthcare.drug_exposure de
    INNER JOIN healthcare.concept c ON de.drug_concept_id = c.concept_id
    WHERE c.domain_id = 'Drug'
    GROUP BY 1
)
GROUP BY 1
ORDER BY 
    CASE risk_category
        WHEN '1 drug (Low)' THEN 1
        WHEN '2-4 drugs (Normal)' THEN 2
        WHEN '5-9 drugs (Moderate)' THEN 3
        ELSE 4
    END;


-- ============================================================
-- DASHBOARD SUMMARY
-- ============================================================
-- 
-- DASHBOARD 1: POPULATION HEALTH OVERVIEW (6 charts)
--   [1.1] KPI_SUMMARY           → 5 Card metrics
--   [1.2] GENDER_DISTRIBUTION   → Pie Chart
--   [1.3] AGE_DISTRIBUTION      → Bar Chart
--   [1.4] VISIT_MONTHLY_PATTERN → Line Chart
--   [1.5] VISIT_TYPE_BREAKDOWN  → Pie Chart
--   [1.6] LOCATION_DISTRIBUTION → Map/Bar Chart
-- 
-- DASHBOARD 2: CLINICAL ANALYTICS (6 charts)
--   [2.1] TOP_CONDITIONS        → Horizontal Bar (Top 15)
--   [2.2] TOP_DRUGS             → Horizontal Bar (Top 15)
--   [2.3] GENDER_AGE_CONDITIONS → Stacked Bar
--   [2.4] DISEASE_COHORTS       → Bar Chart (5 groups)
--   [2.5] COMORBIDITY_PAIRS     → Heatmap/Table
--   [2.6] POLYPHARMACY_ANALYSIS → Pie Chart
-- 
-- ============================================================
-- NOTES:
-- - All queries use INNER JOIN with concept table (OHDSI standard)
-- - Filters on domain_id ensure correct concept types
-- - Follows OHDSI/OMOP-Queries patterns
-- ============================================================
