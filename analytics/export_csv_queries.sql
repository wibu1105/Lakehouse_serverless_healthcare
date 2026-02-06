-- ============================================================
-- EXPORT QUERIES TO S3 AS CSV FILES
-- Mỗi query được lưu thành file .csv với tên rõ ràng
-- ============================================================

-- ============================================================
-- POPULATION
-- ============================================================

-- File: DS_POP_01_SUMMARY.csv
UNLOAD (
    SELECT 
        COUNT(DISTINCT person_id) as total_patients,
        COUNT(DISTINCT CASE WHEN gender_concept_id = 8507 THEN person_id END) as male_count,
        COUNT(DISTINCT CASE WHEN gender_concept_id = 8532 THEN person_id END) as female_count,
        MIN(year_of_birth) as oldest_birth_year,
        MAX(year_of_birth) as youngest_birth_year,
        ROUND(AVG(YEAR(CURRENT_DATE) - year_of_birth), 1) as avg_age
    FROM healthcare.person
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_POP_01_SUMMARY'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- File: DS_POP_02_AGE_DISTRIBUTION.csv
UNLOAD (
    SELECT 
        CASE 
            WHEN YEAR(CURRENT_DATE) - year_of_birth < 18 THEN '0-17'
            WHEN YEAR(CURRENT_DATE) - year_of_birth BETWEEN 18 AND 30 THEN '18-30'
            WHEN YEAR(CURRENT_DATE) - year_of_birth BETWEEN 31 AND 45 THEN '31-45'
            WHEN YEAR(CURRENT_DATE) - year_of_birth BETWEEN 46 AND 60 THEN '46-60'
            WHEN YEAR(CURRENT_DATE) - year_of_birth BETWEEN 61 AND 75 THEN '61-75'
            ELSE '75+'
        END as age_group,
        COUNT(*) as patient_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM healthcare.person
    GROUP BY 1
    ORDER BY 1
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_POP_02_AGE_DISTRIBUTION'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- File: DS_POP_03_GENDER_DISTRIBUTION.csv
UNLOAD (
    SELECT 
        CASE gender_concept_id 
            WHEN 8507 THEN 'Male'
            WHEN 8532 THEN 'Female'
            ELSE 'Other'
        END as gender,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM healthcare.person
    GROUP BY 1
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_POP_03_GENDER_DISTRIBUTION'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- File: DS_POP_04_RACE_DISTRIBUTION.csv
UNLOAD (
    SELECT 
        COALESCE(c.concept_name, 'Unknown') as race,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM healthcare.person p
    LEFT JOIN healthcare.concept c ON p.race_concept_id = c.concept_id
    GROUP BY 1
    ORDER BY count DESC
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_POP_04_RACE_DISTRIBUTION'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- ============================================================
-- VISITS
-- ============================================================

-- File: DS_VIS_01_TYPE_DISTRIBUTION.csv
UNLOAD (
    SELECT 
        COALESCE(c.concept_name, 'Unknown') as visit_type,
        COUNT(*) as visit_count,
        COUNT(DISTINCT person_id) as unique_patients,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM healthcare.visit_occurrence v
    LEFT JOIN healthcare.concept c ON v.visit_concept_id = c.concept_id
    GROUP BY 1
    ORDER BY visit_count DESC
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_VIS_01_TYPE_DISTRIBUTION'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- File: DS_VIS_02_MONTHLY_TREND.csv
UNLOAD (
    SELECT 
        DATE_FORMAT(visit_start_date, '%Y-%m') as month,
        COUNT(*) as total_visits,
        COUNT(DISTINCT person_id) as unique_patients,
        COUNT(CASE WHEN visit_concept_id = 9201 THEN 1 END) as inpatient,
        COUNT(CASE WHEN visit_concept_id = 9202 THEN 1 END) as outpatient,
        COUNT(CASE WHEN visit_concept_id = 9203 THEN 1 END) as emergency
    FROM healthcare.visit_occurrence
    GROUP BY 1
    ORDER BY 1
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_VIS_02_MONTHLY_TREND'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- File: DS_VIS_03_YEARLY_TREND.csv
UNLOAD (
    SELECT 
        YEAR(visit_start_date) as year,
        COUNT(*) as total_visits,
        COUNT(DISTINCT person_id) as unique_patients
    FROM healthcare.visit_occurrence
    GROUP BY 1
    ORDER BY 1
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_VIS_03_YEARLY_TREND'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- File: DS_VIS_04_AVG_LOS.csv
UNLOAD (
    SELECT 
        YEAR(visit_start_date) as year,
        ROUND(AVG(DATE_DIFF('day', visit_start_date, visit_end_date)), 2) as avg_los_days,
        COUNT(*) as inpatient_count
    FROM healthcare.visit_occurrence
    WHERE visit_concept_id = 9201
      AND visit_end_date IS NOT NULL
    GROUP BY 1
    ORDER BY 1
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_VIS_04_AVG_LOS'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- ============================================================
-- CONDITIONS
-- ============================================================

-- File: DS_COND_01_TOP20.csv
UNLOAD (
    SELECT 
        c.concept_name as condition_name,
        c.concept_code as snomed_code,
        COUNT(*) as occurrence_count,
        COUNT(DISTINCT co.person_id) as patient_count,
        ROUND(COUNT(DISTINCT co.person_id) * 100.0 / (SELECT COUNT(*) FROM healthcare.person), 2) as prevalence_pct
    FROM healthcare.condition_occurrence co
    JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
    GROUP BY c.concept_name, c.concept_code
    ORDER BY occurrence_count DESC
    LIMIT 20
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_COND_01_TOP20'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- File: DS_COND_02_BY_AGE_GROUP.csv
UNLOAD (
    SELECT 
        CASE 
            WHEN YEAR(co.condition_start_date) - p.year_of_birth < 18 THEN '0-17'
            WHEN YEAR(co.condition_start_date) - p.year_of_birth BETWEEN 18 AND 40 THEN '18-40'
            WHEN YEAR(co.condition_start_date) - p.year_of_birth BETWEEN 41 AND 60 THEN '41-60'
            ELSE '60+'
        END as age_at_diagnosis,
        c.concept_name as condition_name,
        COUNT(*) as count
    FROM healthcare.condition_occurrence co
    JOIN healthcare.person p ON co.person_id = p.person_id
    JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
    WHERE c.concept_id IN (
        SELECT condition_concept_id 
        FROM healthcare.condition_occurrence 
        GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 10
    )
    GROUP BY 1, 2
    ORDER BY 2, 1
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_COND_02_BY_AGE_GROUP'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- File: DS_COND_04_COMORBIDITY.csv
UNLOAD (
    SELECT 
        c1.concept_name as condition_1,
        c2.concept_name as condition_2,
        COUNT(DISTINCT co1.person_id) as co_occurrence_count
    FROM healthcare.condition_occurrence co1
    JOIN healthcare.condition_occurrence co2 
        ON co1.person_id = co2.person_id 
        AND co1.condition_concept_id < co2.condition_concept_id
    JOIN healthcare.concept c1 ON co1.condition_concept_id = c1.concept_id
    JOIN healthcare.concept c2 ON co2.condition_concept_id = c2.concept_id
    WHERE co1.condition_concept_id IN (
        SELECT condition_concept_id FROM healthcare.condition_occurrence 
        GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 10
    )
    AND co2.condition_concept_id IN (
        SELECT condition_concept_id FROM healthcare.condition_occurrence 
        GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 10
    )
    GROUP BY 1, 2
    ORDER BY co_occurrence_count DESC
    LIMIT 50
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_COND_04_COMORBIDITY'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- ============================================================
-- DRUGS
-- ============================================================

-- File: DS_DRUG_01_TOP20.csv
UNLOAD (
    SELECT 
        c.concept_name as drug_name,
        c.concept_code as rxnorm_code,
        COUNT(*) as prescription_count,
        COUNT(DISTINCT de.person_id) as patient_count,
        ROUND(COUNT(DISTINCT de.person_id) * 100.0 / (SELECT COUNT(*) FROM healthcare.person), 2) as usage_rate_pct
    FROM healthcare.drug_exposure de
    JOIN healthcare.concept c ON de.drug_concept_id = c.concept_id
    GROUP BY c.concept_name, c.concept_code
    ORDER BY prescription_count DESC
    LIMIT 20
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_DRUG_01_TOP20'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- File: DS_DRUG_02_MONTHLY_TREND.csv
UNLOAD (
    SELECT 
        DATE_FORMAT(drug_exposure_start_date, '%Y-%m') as month,
        COUNT(*) as total_prescriptions,
        COUNT(DISTINCT person_id) as unique_patients
    FROM healthcare.drug_exposure
    WHERE drug_exposure_start_date IS NOT NULL
    GROUP BY 1
    ORDER BY 1
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_DRUG_02_MONTHLY_TREND'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- File: DS_DRUG_04_POLYPHARMACY.csv
UNLOAD (
    SELECT 
        drug_count_range,
        COUNT(*) as patient_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM (
        SELECT 
            person_id,
            CASE 
                WHEN COUNT(DISTINCT drug_concept_id) = 1 THEN '1 drug'
                WHEN COUNT(DISTINCT drug_concept_id) BETWEEN 2 AND 4 THEN '2-4 drugs'
                WHEN COUNT(DISTINCT drug_concept_id) BETWEEN 5 AND 9 THEN '5-9 drugs'
                ELSE '10+ drugs'
            END as drug_count_range
        FROM healthcare.drug_exposure
        GROUP BY person_id
    )
    GROUP BY drug_count_range
    ORDER BY 1
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_DRUG_04_POLYPHARMACY'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- ============================================================
-- COHORTS
-- ============================================================

-- File: DS_COH_05_ALL_COHORTS_SUMMARY.csv
UNLOAD (
    SELECT 'Diabetes' as cohort, COUNT(DISTINCT co.person_id) as size
    FROM healthcare.condition_occurrence co
    JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
    WHERE LOWER(c.concept_name) LIKE '%diabetes%'
    UNION ALL
    SELECT 'Hypertension', COUNT(DISTINCT co.person_id)
    FROM healthcare.condition_occurrence co
    JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
    WHERE LOWER(c.concept_name) LIKE '%hypertension%'
    UNION ALL
    SELECT 'Heart Disease', COUNT(DISTINCT co.person_id)
    FROM healthcare.condition_occurrence co
    JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
    WHERE LOWER(c.concept_name) LIKE '%heart%' OR LOWER(c.concept_name) LIKE '%cardiac%'
    UNION ALL
    SELECT 'Lung Disease', COUNT(DISTINCT co.person_id)
    FROM healthcare.condition_occurrence co
    JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
    WHERE LOWER(c.concept_name) LIKE '%lung%' OR LOWER(c.concept_name) LIKE '%pulmonary%'
    UNION ALL
    SELECT 'Cancer', COUNT(DISTINCT co.person_id)
    FROM healthcare.condition_occurrence co
    JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
    WHERE LOWER(c.concept_name) LIKE '%cancer%' OR LOWER(c.concept_name) LIKE '%carcinoma%'
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_COH_05_ALL_COHORTS_SUMMARY'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- File: DS_COH_04_MULTI_MORBIDITY.csv
UNLOAD (
    SELECT 
        chronic_condition_count as number_of_conditions,
        COUNT(*) as patient_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM (
        SELECT 
            co.person_id,
            COUNT(DISTINCT co.condition_concept_id) as chronic_condition_count
        FROM healthcare.condition_occurrence co
        GROUP BY co.person_id
    )
    GROUP BY chronic_condition_count
    ORDER BY 1
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_COH_04_MULTI_MORBIDITY'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- ============================================================
-- DATA QUALITY
-- ============================================================

-- File: DS_DQ_01_TABLE_COUNTS.csv
UNLOAD (
    SELECT 'Person' as table_name, COUNT(*) as row_count FROM healthcare.person
    UNION ALL SELECT 'Visit Occurrence', COUNT(*) FROM healthcare.visit_occurrence
    UNION ALL SELECT 'Condition Occurrence', COUNT(*) FROM healthcare.condition_occurrence
    UNION ALL SELECT 'Drug Exposure', COUNT(*) FROM healthcare.drug_exposure
    UNION ALL SELECT 'Procedure Occurrence', COUNT(*) FROM healthcare.procedure_occurrence
    UNION ALL SELECT 'Measurement', COUNT(*) FROM healthcare.measurement
    UNION ALL SELECT 'Observation', COUNT(*) FROM healthcare.observation
    UNION ALL SELECT 'Condition Era', COUNT(*) FROM healthcare.condition_era
    UNION ALL SELECT 'Drug Era', COUNT(*) FROM healthcare.drug_era
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_DQ_01_TABLE_COUNTS'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- File: DS_DQ_02_COMPLETENESS.csv
UNLOAD (
    SELECT 
        'Patients with Visits' as metric,
        COUNT(DISTINCT v.person_id) as count,
        ROUND(COUNT(DISTINCT v.person_id) * 100.0 / (SELECT COUNT(*) FROM healthcare.person), 2) as percentage
    FROM healthcare.visit_occurrence v
    UNION ALL
    SELECT 
        'Patients with Conditions',
        COUNT(DISTINCT person_id),
        ROUND(COUNT(DISTINCT person_id) * 100.0 / (SELECT COUNT(*) FROM healthcare.person), 2)
    FROM healthcare.condition_occurrence
    UNION ALL
    SELECT 
        'Patients with Drugs',
        COUNT(DISTINCT person_id),
        ROUND(COUNT(DISTINCT person_id) * 100.0 / (SELECT COUNT(*) FROM healthcare.person), 2)
    FROM healthcare.drug_exposure
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_DQ_02_COMPLETENESS'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- ============================================================
-- PROCEDURES & LABS
-- ============================================================

-- File: DS_PROC_01_TOP20.csv
UNLOAD (
    SELECT 
        c.concept_name as procedure_name,
        COUNT(*) as procedure_count,
        COUNT(DISTINCT po.person_id) as patient_count
    FROM healthcare.procedure_occurrence po
    JOIN healthcare.concept c ON po.procedure_concept_id = c.concept_id
    GROUP BY c.concept_name
    ORDER BY procedure_count DESC
    LIMIT 20
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_PROC_01_TOP20'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- File: DS_LAB_01_TOP20.csv
UNLOAD (
    SELECT 
        c.concept_name as measurement_name,
        COUNT(*) as measurement_count,
        ROUND(AVG(CAST(value_as_number AS DOUBLE)), 2) as avg_value,
        ROUND(MIN(CAST(value_as_number AS DOUBLE)), 2) as min_value,
        ROUND(MAX(CAST(value_as_number AS DOUBLE)), 2) as max_value
    FROM healthcare.measurement m
    JOIN healthcare.concept c ON m.measurement_concept_id = c.concept_id
    WHERE value_as_number IS NOT NULL
    GROUP BY c.concept_name
    ORDER BY measurement_count DESC
    LIMIT 20
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/csv/DS_LAB_01_TOP20'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE');


-- ============================================================
-- SAU KHI CHẠY TOÀN BỘ QUERIES, CHECK S3:
-- aws s3 ls s3://omop-cdm-lakehouse-904557616804/analytics/csv/
--
-- KẾT QUẢ:
-- DS_POP_01_SUMMARY/
-- DS_POP_02_AGE_DISTRIBUTION/
-- DS_VIS_01_TYPE_DISTRIBUTION/
-- DS_COND_01_TOP20/
-- DS_DRUG_01_TOP20/
-- ... (mỗi folder chứa file .csv)
--
-- DOWNLOAD VỀ LOCAL:
-- aws s3 sync s3://omop-cdm-lakehouse-904557616804/analytics/csv/ ./dashboard_data/
-- ============================================================
