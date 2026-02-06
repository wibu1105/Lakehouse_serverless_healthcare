"""
Validate Visits Layer
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0)

This validation job runs after Job1 (Visit Grouping):
- Verifies all_visits table exists with data
- Validates visit_id uniqueness
- Checks visit_id_mapping completeness
- Validates visit concept IDs

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

# Configure Iceberg
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", SILVER_PATH)
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Valid visit concept IDs
VALID_VISIT_CONCEPTS = {9201, 9202, 9203}  # inpatient, outpatient, emergency


class ValidationError(Exception):
    """Custom exception for validation failures."""
    pass


def load_iceberg_table(table_name):
    """Load table from Silver layer."""
    full_table = f"glue_catalog.{DATABASE_NAME}.{table_name}"
    return spark.read.format("iceberg").load(full_table)


def validate_visit_id_uniqueness(visits_df):
    """Check that visit_id is unique."""
    total_count = visits_df.count()
    unique_count = visits_df.select("visit_id").distinct().count()
    
    if total_count != unique_count:
        duplicates = total_count - unique_count
        return f"CRITICAL: {duplicates} duplicate visit_ids found"
    return None


def validate_visit_concepts(visits_df):
    """Check that visit_concept_id values are valid."""
    invalid_concepts = visits_df.filter(
        ~F.col("visit_concept_id").isin(list(VALID_VISIT_CONCEPTS))
    ).count()
    
    if invalid_concepts > 0:
        return f"WARNING: {invalid_concepts} visits have invalid concept IDs"
    return None


def validate_date_consistency(visits_df):
    """Check that visit_start <= visit_end."""
    invalid_dates = visits_df.filter(
        F.col("visit_start_datetime") > F.col("visit_end_datetime")
    ).count()
    
    if invalid_dates > 0:
        return f"CRITICAL: {invalid_dates} visits have start > end datetime"
    return None


def validate_mapping_completeness(visits_df, mapping_df):
    """Check that all visits have mappings."""
    visit_ids = visits_df.select("visit_id").distinct()
    mapped_ids = mapping_df.select("visit_id").distinct()
    
    unmapped = visit_ids.join(mapped_ids, "visit_id", "left_anti").count()
    
    if unmapped > 0:
        return f"WARNING: {unmapped} visits have no encounter mappings"
    return None


def main():
    """Main validation process."""
    print(f"\n{'#'*60}")
    print(f"# Validate Visits (Job1 Output)")
    print(f"# Started at: {datetime.now()}")
    print(f"{'#'*60}\n")
    
    issues = []
    
    # Load tables
    try:
        all_visits = load_iceberg_table("all_visits")
        visit_mapping = load_iceberg_table("visit_id_mapping")
    except Exception as e:
        raise ValidationError(f"Failed to load visits tables: {str(e)}")
    
    visit_count = all_visits.count()
    mapping_count = visit_mapping.count()
    
    print(f"📊 Table Statistics:")
    print(f"  • all_visits: {visit_count:,} rows")
    print(f"  • visit_id_mapping: {mapping_count:,} rows")
    
    # Check tables are populated
    if visit_count == 0:
        raise ValidationError("CRITICAL: all_visits table is empty")
    
    if mapping_count == 0:
        raise ValidationError("CRITICAL: visit_id_mapping table is empty")
    
    # Run validations
    print("\n📋 Running Validations...")
    
    # 1. Uniqueness check
    issue = validate_visit_id_uniqueness(all_visits)
    if issue:
        issues.append(issue)
        print(f"  ✗ {issue}")
    else:
        print(f"  ✓ visit_id uniqueness: PASS")
    
    # 2. Concept ID check
    issue = validate_visit_concepts(all_visits)
    if issue:
        issues.append(issue)
        print(f"  ⚠ {issue}")
    else:
        print(f"  ✓ visit_concept_id validity: PASS")
    
    # 3. Date consistency
    issue = validate_date_consistency(all_visits)
    if issue:
        issues.append(issue)
        print(f"  ✗ {issue}")
    else:
        print(f"  ✓ Date consistency: PASS")
    
    # 4. Mapping completeness
    issue = validate_mapping_completeness(all_visits, visit_mapping)
    if issue:
        issues.append(issue)
        print(f"  ⚠ {issue}")
    else:
        print(f"  ✓ Mapping completeness: PASS")
    
    # Summary
    print(f"\n{'='*60}")
    print("Validation Summary")
    print(f"{'='*60}")
    
    critical_issues = [i for i in issues if 'CRITICAL' in i]
    if critical_issues:
        print(f"\n❌ VALIDATION FAILED!")
        for issue in critical_issues:
            print(f"  • {issue}")
        raise ValidationError(f"Visits validation failed: {len(critical_issues)} critical issues")
    
    warnings = [i for i in issues if 'WARNING' in i]
    if warnings:
        print(f"\n⚠ Warnings ({len(warnings)}):")
        for w in warnings:
            print(f"  • {w}")
    
    print(f"\n✓ Visits validation completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
