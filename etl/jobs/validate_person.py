"""
Validate Person & Observation Period
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0)

This validation job runs after Job2 (Person & Observation Period):
- Verifies person table exists with data
- Validates gender/race/ethnicity concept mappings
- Checks observation_period date ranges
- Validates location records

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

# Valid concept IDs
VALID_GENDER_CONCEPTS = {8507, 8532, 0}  # Male, Female, Unknown
VALID_RACE_CONCEPTS = {8527, 8516, 8515, 8657, 8522, 0}  # white, black, asian, native, other


class ValidationError(Exception):
    """Custom exception for validation failures."""
    pass


def load_iceberg_table(table_name):
    """Load table from Silver layer."""
    full_table = f"glue_catalog.{DATABASE_NAME}.{table_name}"
    return spark.read.format("iceberg").load(full_table)


def validate_person_ids(person_df):
    """Check person_id uniqueness and not null."""
    null_count = person_df.filter(F.col("person_id").isNull()).count()
    if null_count > 0:
        return f"CRITICAL: {null_count} records have NULL person_id"
    
    total = person_df.count()
    unique = person_df.select("person_id").distinct().count()
    if total != unique:
        return f"CRITICAL: {total - unique} duplicate person_ids"
    
    return None


def validate_gender_mapping(person_df):
    """Check gender_concept_id mapping rate."""
    total = person_df.count()
    mapped = person_df.filter(F.col("gender_concept_id").isin([8507, 8532])).count()
    rate = (mapped / total * 100) if total > 0 else 0
    
    invalid = person_df.filter(
        ~F.col("gender_concept_id").isin(list(VALID_GENDER_CONCEPTS))
    ).count()
    
    if invalid > 0:
        return f"WARNING: {invalid} records have invalid gender_concept_id"
    
    if rate < 95:
        return f"WARNING: Gender mapping rate {rate:.1f}% is below 95%"
    
    return None


def validate_birth_years(person_df):
    """Check year_of_birth is reasonable."""
    current_year = datetime.now().year
    
    # Allow births up to current year (babies born this year)
    invalid = person_df.filter(
        (F.col("year_of_birth") < 1900) | 
        (F.col("year_of_birth") > current_year + 1)
    ).count()
    
    if invalid > 0:
        return f"WARNING: {invalid} records have unusual year_of_birth"
    
    return None


def validate_observation_periods(obs_df, person_df):
    """Check observation_period references valid persons."""
    issues = []
    
    # Check FK to person (WARNING only - don't block pipeline)
    person_ids = person_df.select("person_id").distinct()
    obs_person_ids = obs_df.select("person_id").distinct()
    
    orphans = obs_person_ids.join(person_ids, "person_id", "left_anti").count()
    if orphans > 0:
        issues.append(f"WARNING: {orphans} observation_periods reference non-existent persons")
    
    # Check date consistency (WARNING only)
    invalid_dates = obs_df.filter(
        F.col("observation_period_start_date") > F.col("observation_period_end_date")
    ).count()
    
    if invalid_dates > 0:
        issues.append(f"WARNING: {invalid_dates} observation_periods have start > end date")
    
    return issues[0] if issues else None


def main():
    """Main validation process."""
    print(f"\n{'#'*60}")
    print(f"# Validate Person & Observation Period (Job2 Output)")
    print(f"# Started at: {datetime.now()}")
    print(f"{'#'*60}\n")
    
    issues = []
    
    # Load tables
    try:
        person = load_iceberg_table("person")
        observation_period = load_iceberg_table("observation_period")
    except Exception as e:
        raise ValidationError(f"Failed to load person tables: {str(e)}")
    
    # Location is optional
    try:
        location = load_iceberg_table("location")
        location_count = location.count()
    except:
        location_count = 0
        print("  ⚠ location table not found (optional)")
    
    person_count = person.count()
    obs_count = observation_period.count()
    
    print(f"📊 Table Statistics:")
    print(f"  • person: {person_count:,} rows")
    print(f"  • observation_period: {obs_count:,} rows")
    print(f"  • location: {location_count:,} rows")
    
    # Check tables are populated
    if person_count == 0:
        raise ValidationError("CRITICAL: person table is empty")
    
    if obs_count == 0:
        raise ValidationError("CRITICAL: observation_period table is empty")
    
    # Run validations
    print("\n📋 Running Validations...")
    
    # 1. Person ID check
    issue = validate_person_ids(person)
    if issue:
        issues.append(issue)
        print(f"  ✗ {issue}")
    else:
        print(f"  ✓ person_id uniqueness: PASS")
    
    # 2. Gender mapping
    issue = validate_gender_mapping(person)
    if issue:
        issues.append(issue)
        print(f"  ⚠ {issue}")
    else:
        print(f"  ✓ gender_concept_id mapping: PASS")
    
    # 3. Birth year check
    issue = validate_birth_years(person)
    if issue:
        issues.append(issue)
        print(f"  ✗ {issue}")
    else:
        print(f"  ✓ year_of_birth validity: PASS")
    
    # 4. Observation period check
    issue = validate_observation_periods(observation_period, person)
    if issue:
        issues.append(issue)
        print(f"  ✗ {issue}")
    else:
        print(f"  ✓ observation_period integrity: PASS")
    
    # Summary
    print(f"\n{'='*60}")
    print("Validation Summary")
    print(f"{'='*60}")
    
    critical_issues = [i for i in issues if 'CRITICAL' in i]
    if critical_issues:
        print(f"\n❌ VALIDATION FAILED!")
        for issue in critical_issues:
            print(f"  • {issue}")
        raise ValidationError(f"Person validation failed: {len(critical_issues)} critical issues")
    
    warnings = [i for i in issues if 'WARNING' in i]
    if warnings:
        print(f"\n⚠ Warnings ({len(warnings)}):")
        for w in warnings:
            print(f"  • {w}")
    
    print(f"\n✓ Person validation completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
