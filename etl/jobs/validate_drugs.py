"""
Validate Drug Exposure
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0)

This validation job runs after Job5 (Drug Exposure):
- Verifies drug_exposure table exists with data
- Validates foreign key integrity
- Checks date consistency
- Validates drug type concept IDs

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

# Valid drug type concept IDs
VALID_DRUG_TYPES = {38000177, 38000179}  # Prescription, Immunization


class ValidationError(Exception):
    """Custom exception for validation failures."""
    pass


def load_iceberg_table(table_name):
    """Load table from Silver layer."""
    full_table = f"glue_catalog.{DATABASE_NAME}.{table_name}"
    return spark.read.format("iceberg").load(full_table)


def validate_drug_exposure_ids(drug_df):
    """Check drug_exposure_id uniqueness."""
    total = drug_df.count()
    unique = drug_df.select("drug_exposure_id").distinct().count()
    
    if total != unique:
        return f"CRITICAL: {total - unique} duplicate drug_exposure_ids"
    return None


def validate_foreign_keys(drug_df, person_df, visit_df):
    """Validate FK to person and visit_occurrence."""
    issues = []
    
    # FK to person
    drug_persons = drug_df.select("person_id").distinct()
    valid_persons = person_df.select("person_id").distinct()
    orphans = drug_persons.join(valid_persons, "person_id", "left_anti").count()
    
    if orphans > 0:
        issues.append(f"CRITICAL: {orphans} drug exposures reference non-existent persons")
    
    # FK to visit (optional)
    drug_visits = drug_df.filter(F.col("visit_occurrence_id").isNotNull()) \
                         .select("visit_occurrence_id").distinct()
    valid_visits = visit_df.select("visit_occurrence_id").distinct()
    orphan_visits = drug_visits.join(valid_visits, "visit_occurrence_id", "left_anti").count()
    
    if orphan_visits > 0:
        issues.append(f"WARNING: {orphan_visits} drug exposures reference non-existent visits")
    
    return issues


def validate_dates(drug_df):
    """Check date consistency."""
    issues = []
    
    # Null start date
    null_start = drug_df.filter(F.col("drug_exposure_start_date").isNull()).count()
    if null_start > 0:
        issues.append(f"CRITICAL: {null_start} drug exposures have NULL start_date")
    
    # Start > End
    both_dates = drug_df.filter(
        F.col("drug_exposure_start_date").isNotNull() & 
        F.col("drug_exposure_end_date").isNotNull()
    )
    invalid = both_dates.filter(
        F.col("drug_exposure_start_date") > F.col("drug_exposure_end_date")
    ).count()
    
    if invalid > 0:
        issues.append(f"WARNING: {invalid} drug exposures have start > end date")
    
    return issues


def validate_type_concepts(drug_df):
    """Check drug_type_concept_id values."""
    invalid = drug_df.filter(
        ~F.col("drug_type_concept_id").isin(list(VALID_DRUG_TYPES))
    ).count()
    
    if invalid > 0:
        return f"WARNING: {invalid} drug exposures have non-standard type concept IDs"
    return None


def main():
    """Main validation process."""
    print(f"\n{'#'*60}")
    print(f"# Validate Drug Exposure (Job5 Output)")
    print(f"# Started at: {datetime.now()}")
    print(f"{'#'*60}\n")
    
    issues = []
    
    # Load tables
    try:
        drug_exposure = load_iceberg_table("drug_exposure")
        person = load_iceberg_table("person")
        visit_occurrence = load_iceberg_table("visit_occurrence")
    except Exception as e:
        raise ValidationError(f"Failed to load tables: {str(e)}")
    
    drug_count = drug_exposure.count()
    print(f"📊 drug_exposure: {drug_count:,} rows")
    
    if drug_count == 0:
        raise ValidationError("CRITICAL: drug_exposure table is empty")
    
    # Run validations
    print("\n📋 Running Validations...")
    
    # 1. ID uniqueness
    issue = validate_drug_exposure_ids(drug_exposure)
    if issue:
        issues.append(issue)
        print(f"  ✗ {issue}")
    else:
        print(f"  ✓ drug_exposure_id uniqueness: PASS")
    
    # 2. Foreign keys
    fk_issues = validate_foreign_keys(drug_exposure, person, visit_occurrence)
    issues.extend(fk_issues)
    if fk_issues:
        for issue in fk_issues:
            print(f"  ✗ {issue}")
    else:
        print(f"  ✓ Foreign key integrity: PASS")
    
    # 3. Date validation
    date_issues = validate_dates(drug_exposure)
    issues.extend(date_issues)
    if date_issues:
        for issue in date_issues:
            print(f"  ⚠ {issue}")
    else:
        print(f"  ✓ Date consistency: PASS")
    
    # 4. Type concepts
    issue = validate_type_concepts(drug_exposure)
    if issue:
        issues.append(issue)
        print(f"  ⚠ {issue}")
    else:
        print(f"  ✓ Type concept IDs: PASS")
    
    # Summary
    print(f"\n{'='*60}")
    print("Validation Summary")
    print(f"{'='*60}")
    
    critical_issues = [i for i in issues if 'CRITICAL' in i]
    if critical_issues:
        print(f"\n❌ VALIDATION FAILED!")
        for issue in critical_issues:
            print(f"  • {issue}")
        raise ValidationError(f"Drug exposure validation failed: {len(critical_issues)} critical issues")
    
    warnings = [i for i in issues if 'WARNING' in i]
    if warnings:
        print(f"\n⚠ Warnings ({len(warnings)}):")
        for w in warnings:
            print(f"  • {w}")
    
    print(f"\n✓ Drug exposure validation completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
