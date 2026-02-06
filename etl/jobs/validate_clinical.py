"""
Validate Clinical Events
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0)

This validation job runs after Job4 (Clinical Events):
- Verifies condition_occurrence, procedure_occurrence tables
- Verifies measurement, observation tables
- Validates foreign key integrity
- Checks date consistency

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


class ValidationError(Exception):
    """Custom exception for validation failures."""
    pass


def load_iceberg_table(table_name):
    """Load table from Silver layer."""
    full_table = f"glue_catalog.{DATABASE_NAME}.{table_name}"
    return spark.read.format("iceberg").load(full_table)


def validate_foreign_keys(df, table_name, person_df, visit_df):
    """Validate FK to person and visit_occurrence."""
    issues = []
    
    # FK to person
    df_persons = df.select("person_id").distinct()
    valid_persons = person_df.select("person_id").distinct()
    orphan_persons = df_persons.join(valid_persons, "person_id", "left_anti").count()
    
    if orphan_persons > 0:
        issues.append(f"{table_name}: {orphan_persons} orphan person_id references")
    
    # FK to visit (optional - may be null)
    if "visit_occurrence_id" in df.columns:
        df_visits = df.filter(F.col("visit_occurrence_id").isNotNull()) \
                      .select("visit_occurrence_id").distinct()
        valid_visits = visit_df.select("visit_occurrence_id").distinct()
        orphan_visits = df_visits.join(valid_visits, "visit_occurrence_id", "left_anti").count()
        
        if orphan_visits > 0:
            issues.append(f"{table_name}: {orphan_visits} orphan visit_occurrence_id references")
    
    return issues


def validate_clinical_table(df, table_name, id_col, start_col, end_col=None):
    """Validate a clinical table."""
    issues = []
    
    # ID uniqueness
    total = df.count()
    unique = df.select(id_col).distinct().count()
    if total != unique:
        issues.append(f"CRITICAL: {table_name} has {total - unique} duplicate {id_col}")
    
    # Null person_id
    null_person = df.filter(F.col("person_id").isNull()).count()
    if null_person > 0:
        issues.append(f"CRITICAL: {table_name} has {null_person} NULL person_id")
    
    # Date consistency
    if end_col:
        both_dates = df.filter(
            F.col(start_col).isNotNull() & F.col(end_col).isNotNull()
        )
        invalid = both_dates.filter(F.col(start_col) > F.col(end_col)).count()
        if invalid > 0:
            issues.append(f"WARNING: {table_name} has {invalid} records with start > end date")
    
    return issues


def main():
    """Main validation process."""
    print(f"\n{'#'*60}")
    print(f"# Validate Clinical Events (Job4 Output)")
    print(f"# Started at: {datetime.now()}")
    print(f"{'#'*60}\n")
    
    issues = []
    
    # Load reference tables
    try:
        person = load_iceberg_table("person")
        visit_occurrence = load_iceberg_table("visit_occurrence")
    except Exception as e:
        raise ValidationError(f"Failed to load reference tables: {str(e)}")
    
    # Load and validate clinical tables
    tables_config = {
        'condition_occurrence': {
            'id_col': 'condition_occurrence_id',
            'start_col': 'condition_start_date',
            'end_col': 'condition_end_date'
        },
        'procedure_occurrence': {
            'id_col': 'procedure_occurrence_id',
            'start_col': 'procedure_date',
            'end_col': 'procedure_end_date'
        },
        'measurement': {
            'id_col': 'measurement_id',
            'start_col': 'measurement_date',
            'end_col': None
        },
        'observation': {
            'id_col': 'observation_id',
            'start_col': 'observation_date',
            'end_col': None
        }
    }
    
    print("📊 Table Statistics:")
    for table_name, config in tables_config.items():
        try:
            df = load_iceberg_table(table_name)
            count = df.count()
            print(f"  • {table_name}: {count:,} rows")
            
            if count == 0:
                issues.append(f"WARNING: {table_name} is empty")
                continue
            
            # Validate table
            table_issues = validate_clinical_table(
                df, table_name,
                config['id_col'],
                config['start_col'],
                config['end_col']
            )
            issues.extend(table_issues)
            
            # Validate FKs
            fk_issues = validate_foreign_keys(df, table_name, person, visit_occurrence)
            issues.extend(fk_issues)
            
        except Exception as e:
            issues.append(f"CRITICAL: Failed to load {table_name}: {str(e)}")
    
    # Summary
    print(f"\n{'='*60}")
    print("Validation Summary")
    print(f"{'='*60}")
    
    critical_issues = [i for i in issues if 'CRITICAL' in i]
    if critical_issues:
        print(f"\n❌ VALIDATION FAILED!")
        for issue in critical_issues:
            print(f"  • {issue}")
        raise ValidationError(f"Clinical events validation failed: {len(critical_issues)} critical issues")
    
    warnings = [i for i in issues if 'WARNING' in i]
    if warnings:
        print(f"\n⚠ Warnings ({len(warnings)}):")
        for w in warnings:
            print(f"  • {w}")
    else:
        print("\n✓ All clinical tables validated successfully")
    
    print(f"\n✓ Clinical events validation completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
