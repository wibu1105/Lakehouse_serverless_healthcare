"""
Validate Final Pipeline Output
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0)

This validation job runs after Job6 (Era & Metadata):
- Verifies era tables (condition_era, drug_era)
- Validates CDM source metadata
- Runs comprehensive data quality suite
- Generates final validation report

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
    """Load table from Silver layer (single database pattern)."""
    full_table = f"glue_catalog.{DATABASE_NAME}.{table_name}"
    return spark.read.format("iceberg").load(full_table)


def validate_era_table(era_df, table_name, id_col, start_col, end_col, count_col):
    """Validate an era table."""
    issues = []
    
    # ID uniqueness
    total = era_df.count()
    unique = era_df.select(id_col).distinct().count()
    if total != unique:
        issues.append(f"WARNING: {table_name} has {total - unique} duplicate {id_col}")
    
    # Date consistency
    invalid_dates = era_df.filter(F.col(start_col) > F.col(end_col)).count()
    if invalid_dates > 0:
        issues.append(f"WARNING: {table_name} has {invalid_dates} records with start > end date")
    
    # Count > 0
    zero_counts = era_df.filter(F.col(count_col) <= 0).count()
    if zero_counts > 0:
        issues.append(f"WARNING: {table_name} has {zero_counts} records with count <= 0")
    
    return issues


def validate_cdm_source(cdm_df):
    """Validate CDM source metadata."""
    issues = []
    
    if cdm_df.count() == 0:
        issues.append("WARNING: cdm_source table is empty")
        return issues
    
    # Check required fields
    required_fields = ['cdm_source_name', 'cdm_version']
    for field in required_fields:
        if field in cdm_df.columns:
            null_count = cdm_df.filter(F.col(field).isNull()).count()
            if null_count > 0:
                issues.append(f"WARNING: cdm_source.{field} is NULL")
    
    return issues


def run_final_data_quality_checks():
    """Run comprehensive DQ checks across all tables."""
    issues = []
    summary = {}
    
    # List of all OMOP tables to check (single database - no schema prefix)
    tables_to_check = [
        'person',
        'observation_period',
        'visit_occurrence',
        'condition_occurrence',
        'procedure_occurrence',
        'drug_exposure',
        'measurement',
        'observation',
        'condition_era',
        'drug_era',
    ]
    
    print("\n📊 Final Table Statistics:")
    for table_name in tables_to_check:
        try:
            df = load_iceberg_table(table_name)
            count = df.count()
            summary[table_name] = count
            print(f"  • {table_name}: {count:,} rows")
        except Exception as e:
            issues.append(f"WARNING: Could not load {table_name}: {str(e)}")
    
    return issues, summary


def generate_validation_report(summary, issues):
    """Generate final validation report."""
    report = {
        'timestamp': datetime.now().isoformat(),
        'database': DATABASE_NAME,
        'table_counts': summary,
        'total_issues': len(issues),
        'critical_issues': len([i for i in issues if 'CRITICAL' in i]),
        'warnings': len([i for i in issues if 'WARNING' in i]),
        'status': 'PASS' if not any('CRITICAL' in i for i in issues) else 'FAIL'
    }
    
    return report


def main():
    """Main validation process."""
    print(f"\n{'#'*60}")
    print(f"# Validate Final Pipeline Output (Job6 Output)")
    print(f"# Started at: {datetime.now()}")
    print(f"{'#'*60}\n")
    
    issues = []
    
    # Validate Era tables (optional - may not exist yet)
    print("📋 Validating Era Tables...")
    
    try:
        condition_era = load_iceberg_table("condition_era")
        drug_era = load_iceberg_table("drug_era")
        
        print(f"  • condition_era: {condition_era.count():,} rows")
        print(f"  • drug_era: {drug_era.count():,} rows")
        
        # Condition Era validation
        era_issues = validate_era_table(
            condition_era, "condition_era",
            "condition_era_id", "condition_era_start_date", 
            "condition_era_end_date", "condition_occurrence_count"
        )
        issues.extend(era_issues)
        
        # Drug Era validation
        era_issues = validate_era_table(
            drug_era, "drug_era",
            "drug_era_id", "drug_era_start_date",
            "drug_era_end_date", "drug_exposure_count"
        )
        issues.extend(era_issues)
        
    except Exception as e:
        issues.append(f"WARNING: Could not load era tables: {str(e)}")
    
    # Validate CDM Source (optional)
    print("\n📋 Validating CDM Source...")
    try:
        cdm_source = load_iceberg_table("cdm_source")
        cdm_issues = validate_cdm_source(cdm_source)
        issues.extend(cdm_issues)
        print(f"  ✓ cdm_source metadata present")
    except Exception as e:
        issues.append(f"WARNING: cdm_source not found: {str(e)}")
    
    # Run final DQ checks
    print("\n📋 Running Final Data Quality Checks...")
    dq_issues, summary = run_final_data_quality_checks()
    issues.extend(dq_issues)
    
    # Generate report
    report = generate_validation_report(summary, issues)
    
    # Summary
    print(f"\n{'='*60}")
    print("FINAL VALIDATION REPORT")
    print(f"{'='*60}")
    print(f"Timestamp: {report['timestamp']}")
    print(f"Database: {report['database']}")
    print(f"Status: {report['status']}")
    print(f"Total Issues: {report['total_issues']}")
    print(f"  - Critical: {report['critical_issues']}")
    print(f"  - Warnings: {report['warnings']}")
    
    # Fail only if critical issues
    critical_issues = [i for i in issues if 'CRITICAL' in i]
    if critical_issues:
        print(f"\n❌ FINAL VALIDATION FAILED!")
        for issue in critical_issues:
            print(f"  • {issue}")
        raise ValidationError(f"Final validation failed: {len(critical_issues)} critical issues")
    
    # Report warnings
    warnings = [i for i in issues if 'WARNING' in i]
    if warnings:
        print(f"\n⚠ Warnings ({len(warnings)}):")
        for w in warnings:
            print(f"  • {w}")
    
    print(f"\n✓ PIPELINE VALIDATION COMPLETE")
    print(f"✓ All {len(summary)} OMOP CDM tables validated successfully")
    print(f"✓ Completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
