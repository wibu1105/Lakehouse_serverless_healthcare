# Hướng dẫn Export Kết quả Query vào S3 cho Dashboard

## Phương pháp 1: Sử dụng Athena UNLOAD (Khuyến nghị)

### 1.1. Cú pháp UNLOAD

```sql
UNLOAD (
    -- Query của bạn
    SELECT 
        COUNT(DISTINCT person_id) as total_patients,
        COUNT(DISTINCT CASE WHEN gender_concept_id = 8507 THEN person_id END) as male_count,
        COUNT(DISTINCT CASE WHEN gender_concept_id = 8532 THEN person_id END) as female_count
    FROM healthcare.person
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/population_summary/'
WITH (
    format = 'PARQUET',
    compression = 'SNAPPY'
);
```

### 1.2. Export tất cả Dashboard Queries

```sql
-- 1. Population Summary
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
TO 's3://omop-cdm-lakehouse-904557616804/analytics/population_summary/'
WITH (format = 'PARQUET', compression = 'SNAPPY');

-- 2. Age Distribution
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
TO 's3://omop-cdm-lakehouse-904557616804/analytics/age_distribution/'
WITH (format = 'PARQUET', compression = 'SNAPPY');

-- 3. Visit Type Distribution
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
TO 's3://omop-cdm-lakehouse-904557616804/analytics/visit_type_distribution/'
WITH (format = 'PARQUET', compression = 'SNAPPY');

-- 4. Monthly Visit Trend
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
TO 's3://omop-cdm-lakehouse-904557616804/analytics/monthly_visit_trend/'
WITH (format = 'PARQUET', compression = 'SNAPPY');

-- 5. Top 20 Conditions
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
TO 's3://omop-cdm-lakehouse-904557616804/analytics/top_conditions/'
WITH (format = 'PARQUET', compression = 'SNAPPY');

-- 6. Top 20 Drugs
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
TO 's3://omop-cdm-lakehouse-904557616804/analytics/top_drugs/'
WITH (format = 'PARQUET', compression = 'SNAPPY');

-- 7. Comorbidity Matrix
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
    LIMIT 30
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/comorbidity_matrix/'
WITH (format = 'PARQUET', compression = 'SNAPPY');

-- 8. Data Quality Summary
UNLOAD (
    SELECT 'Person' as table_name, COUNT(*) as row_count FROM healthcare.person
    UNION ALL
    SELECT 'Visit Occurrence', COUNT(*) FROM healthcare.visit_occurrence
    UNION ALL
    SELECT 'Condition Occurrence', COUNT(*) FROM healthcare.condition_occurrence
    UNION ALL
    SELECT 'Drug Exposure', COUNT(*) FROM healthcare.drug_exposure
    UNION ALL
    SELECT 'Procedure Occurrence', COUNT(*) FROM healthcare.procedure_occurrence
    UNION ALL
    SELECT 'Measurement', COUNT(*) FROM healthcare.measurement
)
TO 's3://omop-cdm-lakehouse-904557616804/analytics/data_quality_summary/'
WITH (format = 'PARQUET', compression = 'SNAPPY');
```

---

## Phương pháp 2: Tạo Glue Job tự động Export

### 2.1. Tạo file Python Glue Job

```python
# File: analytics/export_dashboard_data.py

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_output_path'])
job.init(args['JOB_NAME'], args)

S3_OUTPUT = args['s3_output_path']

# 1. Population Summary
pop_summary = spark.sql("""
    SELECT 
        COUNT(DISTINCT person_id) as total_patients,
        COUNT(DISTINCT CASE WHEN gender_concept_id = 8507 THEN person_id END) as male_count,
        COUNT(DISTINCT CASE WHEN gender_concept_id = 8532 THEN person_id END) as female_count
    FROM healthcare.person
""")
pop_summary.write.mode("overwrite").parquet(f"{S3_OUTPUT}/population_summary/")

# 2. Age Distribution
age_dist = spark.sql("""
    SELECT 
        CASE 
            WHEN YEAR(CURRENT_DATE) - year_of_birth < 18 THEN '0-17'
            WHEN YEAR(CURRENT_DATE) - year_of_birth BETWEEN 18 AND 30 THEN '18-30'
            WHEN YEAR(CURRENT_DATE) - year_of_birth BETWEEN 31 AND 45 THEN '31-45'
            WHEN YEAR(CURRENT_DATE) - year_of_birth BETWEEN 46 AND 60 THEN '46-60'
            WHEN YEAR(CURRENT_DATE) - year_of_birth BETWEEN 61 AND 75 THEN '61-75'
            ELSE '75+'
        END as age_group,
        COUNT(*) as patient_count
    FROM healthcare.person
    GROUP BY 1
""")
age_dist.write.mode("overwrite").parquet(f"{S3_OUTPUT}/age_distribution/")

# 3. Top Conditions
top_conditions = spark.sql("""
    SELECT 
        c.concept_name as condition_name,
        COUNT(*) as occurrence_count,
        COUNT(DISTINCT co.person_id) as patient_count
    FROM healthcare.condition_occurrence co
    JOIN healthcare.concept c ON co.condition_concept_id = c.concept_id
    GROUP BY c.concept_name
    ORDER BY occurrence_count DESC
    LIMIT 20
""")
top_conditions.write.mode("overwrite").parquet(f"{S3_OUTPUT}/top_conditions/")

# 4. Top Drugs
top_drugs = spark.sql("""
    SELECT 
        c.concept_name as drug_name,
        COUNT(*) as prescription_count,
        COUNT(DISTINCT de.person_id) as patient_count
    FROM healthcare.drug_exposure de
    JOIN healthcare.concept c ON de.drug_concept_id = c.concept_id
    GROUP BY c.concept_name
    ORDER BY prescription_count DESC
    LIMIT 20
""")
top_drugs.write.mode("overwrite").parquet(f"{S3_OUTPUT}/top_drugs/")

print("✓ All dashboard data exported to S3")
job.commit()
```

### 2.2. Upload và chạy Glue Job

```bash
# Upload script
aws s3 cp analytics/export_dashboard_data.py s3://omop-cdm-lakehouse-904557616804/scripts/

# Chạy job
aws glue start-job-run \
  --job-name export-dashboard-data \
  --arguments '{"--s3_output_path":"s3://omop-cdm-lakehouse-904557616804/analytics/"}'
```

---

## Phương pháp 3: Tạo Glue Tables trỏ vào S3 Export

Sau khi export, tạo external tables:

```sql
-- Tạo table từ Parquet đã export
CREATE EXTERNAL TABLE analytics.population_summary (
    total_patients BIGINT,
    male_count BIGINT,
    female_count BIGINT,
    oldest_birth_year INT,
    youngest_birth_year INT,
    avg_age DOUBLE
)
STORED AS PARQUET
LOCATION 's3://omop-cdm-lakehouse-904557616804/analytics/population_summary/';

CREATE EXTERNAL TABLE analytics.age_distribution (
    age_group STRING,
    patient_count BIGINT,
    percentage DOUBLE
)
STORED AS PARQUET
LOCATION 's3://omop-cdm-lakehouse-904557616804/analytics/age_distribution/';

CREATE EXTERNAL TABLE analytics.top_conditions (
    condition_name STRING,
    snomed_code STRING,
    occurrence_count BIGINT,
    patient_count BIGINT,
    prevalence_pct DOUBLE
)
STORED AS PARQUET
LOCATION 's3://omop-cdm-lakehouse-904557616804/analytics/top_conditions/';
```

---

## Kết nối với Dashboard Tools

### QuickSight
1. **Data Source**: Athena
2. **Database**: `analytics` (hoặc query trực tiếp từ `healthcare`)
3. **Tables**: Chọn các tables đã export
4. **SPICE**: Import vào SPICE để tăng tốc

### Tableau
1. **Connect**: Amazon Athena
2. **Database**: `analytics`
3. **Custom SQL**: Hoặc dùng query trực tiếp

### PowerBI
1. **Get Data** → Amazon Athena
2. **DirectQuery** hoặc **Import**
3. Point to S3 Parquet files

---

## Script tự động hóa (Recommended)

```bash
#!/bin/bash
# File: scripts/export_analytics.sh

BUCKET="omop-cdm-lakehouse-904557616804"
OUTPUT_PATH="s3://$BUCKET/analytics"

echo "Exporting dashboard data to S3..."

# Chạy tất cả UNLOAD queries
athena_queries=(
    "population_summary"
    "age_distribution"
    "visit_type_distribution"
    "monthly_visit_trend"
    "top_conditions"
    "top_drugs"
    "comorbidity_matrix"
    "data_quality_summary"
)

for query in "${athena_queries[@]}"; do
    echo "Exporting $query..."
    # Gọi Athena API hoặc chạy qua AWS CLI
done

echo "✓ Export complete! Data available at: $OUTPUT_PATH"
```

---

## Best Practices

1. **Partition by date**: Thêm partition để track historical data
   ```sql
   TO 's3://.../analytics/top_conditions/date=2026-02-06/'
   ```

2. **Schedule với EventBridge**: Tự động export hàng ngày
   ```bash
   # Cron: 0 2 * * * (2am daily)
   ```

3. **Compress**: Dùng SNAPPY hoặc GZIP
   ```sql
   WITH (format = 'PARQUET', compression = 'SNAPPY')
   ```

4. **Incremental updates**: Chỉ export data mới
   ```sql
   WHERE condition_start_date >= CURRENT_DATE - INTERVAL '7' DAY
   ```
