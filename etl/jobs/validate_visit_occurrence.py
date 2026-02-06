"""
Validate Visit Occurrence
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0)

This validation job runs after Job3 (Visit Occurrence):
- Verifies visit_occurrence table exists with data
- Validates mapping rate >= 95%
- Checks foreign key integrity to person table
- Validates visit dates

If validation fails, the job raises an exception to stop the pipeline.
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'silver_path',
    'database_name'
])

job.init(args['JOB_NAME'], args)

# Configuration
SILVER_PATH = args['silver_path']
DATABASE_NAME = args['database_name']
MIN_MAPPING_RATE = 95.0  # Minimum acceptable mapping rate

# Configure Iceberg
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", SILVER_PATH)
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")


class ValidationError(Exception):
    """Custom exception for validation failures."""
    pass


def load_iceberg_table(table_name):
    """Load table from Silver layer."""
    full_table = f"glue_catalog.{DATABASE_NAME}.{table_name}"
    return spark.read.format("iceberg").load(full_table)


def validate_foreign_key_person(visit_df, person_df):
    """Check that all person_ids in visit_occurrence exist in person."""
    visit_persons = visit_df.select("person_id").distinct()
    valid_persons = person_df.select("person_id").distinct()
    
    orphans = visit_persons.join(valid_persons, "person_id", "left_anti").count()
    total = visit_persons.count()
    
    if orphans > 0:
        rate = (1 - orphans / total) * 100 if total > 0 else 0
        return f"CRITICAL: {orphans} visits reference non-existent persons (FK integrity: {rate:.1f}%)"
    
    return None


def validate_visit_dates(visit_df):
    """Check visit date consistency."""
    issues = []
    
    # Start <= End
    invalid_range = visit_df.filter(
        F.col("visit_start_date") > F.col("visit_end_date")
    ).count()
    
    if invalid_range > 0:
        issues.append(f"CRITICAL: {invalid_range} visits have start > end date")
    
    # Null dates
    null_start = visit_df.filter(F.col("visit_start_date").isNull()).count()
    if null_start > 0:
        issues.append(f"CRITICAL: {null_start} visits have NULL start_date")
    
    return issues


def validate_mapping_rate(visit_df, all_visits_df):
    """Check mapping rate compared to source visits."""
    mapped_count = visit_df.count()
    source_count = all_visits_df.count()
    
    rate = (mapped_count / source_count * 100) if source_count > 0 else 0
    
    print(f"\n📊 Mapping Statistics:")
    print(f"  • Source visits (all_visits): {source_count:,}")
    print(f"  • Mapped visits (visit_occurrence): {mapped_count:,}")
    print(f"  • Mapping rate: {rate:.1f}%")
    
    if rate < MIN_MAPPING_RATE:
        return f"CRITICAL: Mapping rate {rate:.1f}% is below {MIN_MAPPING_RATE}% threshold"
    
    return None


def main():
    """Main validation process."""
    print(f"\n{'#'*60}")
    print(f"# Validate Visit Occurrence (Job3 Output)")
    print(f"# Started at: {datetime.now()}")
    print(f"{'#'*60}\n")
    
    issues = []
    
    # Load tables
    try:
        visit_occurrence = load_iceberg_table("visit_occurrence")
        person = load_iceberg_table("person")
        all_visits = load_iceberg_table("all_visits")
    except Exception as e:
        raise ValidationError(f"Failed to load tables: {str(e)}")
    
    visit_count = visit_occurrence.count()
    
    print(f"📊 visit_occurrence: {visit_count:,} rows")
    
    if visit_count == 0:
        raise ValidationError("CRITICAL: visit_occurrence table is empty")
    
    # Run validations
    print("\n📋 Running Validations...")
    
    # 1. FK to person
    issue = validate_foreign_key_person(visit_occurrence, person)
    if issue:
        issues.append(issue)
        print(f"  ✗ {issue}")
    else:
        print(f"  ✓ FK to person: PASS")
    
    # 2. Date validation
    date_issues = validate_visit_dates(visit_occurrence)
    issues.extend(date_issues)
    if date_issues:
        for issue in date_issues:
            print(f"  ✗ {issue}")
    else:
        print(f"  ✓ Visit dates: PASS")
    
    # 3. Mapping rate
    issue = validate_mapping_rate(visit_occurrence, all_visits)
    if issue:
        issues.append(issue)
        print(f"  ✗ {issue}")
    else:
        print(f"  ✓ Mapping rate: PASS")
    
    # Summary
    print(f"\n{'='*60}")
    print("Validation Summary")
    print(f"{'='*60}")
    
    critical_issues = [i for i in issues if 'CRITICAL' in i]
    if critical_issues:
        print(f"\n❌ VALIDATION FAILED!")
        for issue in critical_issues:
            print(f"  • {issue}")
        raise ValidationError(f"Visit occurrence validation failed: {len(critical_issues)} critical issues")
    
    print(f"\n✓ Visit occurrence validation completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
