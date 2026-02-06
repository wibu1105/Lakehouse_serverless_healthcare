"""
Validate Bronze Layer
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0)

This validation job runs after RawToBronze:
- Verifies all Bronze tables exist with data
- Validates schema columns
- Checks data quality metrics

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
    'bronze_path',
    'database_name'
])

job.init(args['JOB_NAME'], args)

# Configuration
BRONZE_PATH = args['bronze_path']
DATABASE_NAME = args['database_name']

# Essential Bronze tables that MUST have data
ESSENTIAL_TABLES = {
    'patients': ['Id', 'BIRTHDATE', 'GENDER'],
    'encounters': ['Id', 'START', 'STOP', 'PATIENT', 'ENCOUNTERCLASS'],
    'conditions': ['START', 'PATIENT', 'ENCOUNTER', 'CODE'],
    'medications': ['START', 'PATIENT', 'ENCOUNTER', 'CODE'],
}

# Optional tables (may be empty but should exist)
OPTIONAL_TABLES = [
    'procedures', 'observations', 'immunizations', 'allergies',
    'careplans', 'devices', 'organizations', 'providers',
    'payers', 'payer_transitions'
]


class ValidationError(Exception):
    """Custom exception for validation failures."""
    pass


def validate_table_exists(table_name):
    """Check if table exists and has data."""
    table_path = f"{BRONZE_PATH}{table_name}/"
    
    try:
        df = spark.read.parquet(table_path)
        row_count = df.count()
        return row_count, df.columns
    except Exception as e:
        return -1, []


def validate_required_columns(df, table_name, required_cols):
    """Validate required columns exist and are not all null."""
    issues = []
    
    for col in required_cols:
        if col not in df.columns:
            issues.append(f"{table_name}: Missing required column '{col}'")
        else:
            null_count = df.filter(F.col(col).isNull()).count()
            total_count = df.count()
            if total_count > 0 and null_count == total_count:
                issues.append(f"{table_name}.{col}: All values are NULL")
    
    return issues


def validate_metadata_columns(df, table_name):
    """Validate ingestion metadata columns exist."""
    required_metadata = ['ingestion_date', 'source_file']
    missing = [col for col in required_metadata if col not in df.columns]
    
    if missing:
        return [f"{table_name}: Missing metadata columns: {missing}"]
    return []


def main():
    """Main validation process."""
    print(f"\n{'#'*60}")
    print(f"# Validate Bronze Layer")
    print(f"# Started at: {datetime.now()}")
    print(f"# Bronze Path: {BRONZE_PATH}")
    print(f"{'#'*60}\n")
    
    all_issues = []
    validation_results = []
    
    # Validate essential tables
    print("📋 Validating Essential Tables...")
    for table_name, required_cols in ESSENTIAL_TABLES.items():
        row_count, columns = validate_table_exists(table_name)
        
        if row_count == -1:
            all_issues.append(f"CRITICAL: {table_name} does not exist or is unreadable")
            validation_results.append({
                'table': table_name,
                'status': 'MISSING',
                'rows': 0
            })
            continue
        
        if row_count == 0:
            all_issues.append(f"CRITICAL: {table_name} is empty")
            validation_results.append({
                'table': table_name,
                'status': 'EMPTY',
                'rows': 0
            })
            continue
        
        # Load and validate columns
        df = spark.read.parquet(f"{BRONZE_PATH}{table_name}/")
        
        # Check required columns
        col_issues = validate_required_columns(df, table_name, required_cols)
        all_issues.extend(col_issues)
        
        # Check metadata columns
        meta_issues = validate_metadata_columns(df, table_name)
        all_issues.extend(meta_issues)
        
        status = 'PASS' if not col_issues and not meta_issues else 'WARN'
        validation_results.append({
            'table': table_name,
            'status': status,
            'rows': row_count
        })
        
        print(f"  ✓ {table_name}: {row_count:,} rows")
    
    # Validate optional tables (warnings only)
    print("\n📋 Checking Optional Tables...")
    for table_name in OPTIONAL_TABLES:
        row_count, _ = validate_table_exists(table_name)
        
        if row_count == -1:
            print(f"  ⚠ {table_name}: Not found (optional)")
        elif row_count == 0:
            print(f"  ⚠ {table_name}: Empty (optional)")
        else:
            print(f"  ✓ {table_name}: {row_count:,} rows")
    
    # Summary
    print(f"\n{'='*60}")
    print("Validation Summary")
    print(f"{'='*60}")
    
    passed = sum(1 for r in validation_results if r['status'] == 'PASS')
    warned = sum(1 for r in validation_results if r['status'] == 'WARN')
    failed = sum(1 for r in validation_results if r['status'] in ['MISSING', 'EMPTY'])
    
    print(f"PASS: {passed} | WARN: {warned} | FAIL: {failed}")
    
    # Fail if critical issues
    critical_issues = [i for i in all_issues if 'CRITICAL' in i]
    if critical_issues:
        print(f"\n❌ VALIDATION FAILED!")
        for issue in critical_issues:
            print(f"  • {issue}")
        raise ValidationError(f"Bronze validation failed: {len(critical_issues)} critical issues")
    
    # Report warnings
    if all_issues:
        print(f"\n⚠ Warnings ({len(all_issues)}):")
        for issue in all_issues:
            print(f"  • {issue}")
    
    print(f"\n✓ Bronze validation completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
