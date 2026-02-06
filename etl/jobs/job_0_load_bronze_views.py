"""
Job 0: Load Bronze Views
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0 + Iceberg)

This Glue job creates temporary views from Bronze layer tables:
- Loads Synthea data from Bronze parquet files
- Creates Spark temporary views for downstream ETL jobs
- Validates data availability

Phase 2: ETL Pipeline (Daily/Weekly Batch)
Output: In-memory views for Job 1-6
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

# Bronze tables to load as views
BRONZE_TABLES = [
    'patients',
    'encounters',
    'conditions',
    'medications',
    'procedures',
    'observations',
    'immunizations',
    'allergies',
    'careplans',
    'devices',
    'organizations',
    'providers',
    'payers',
    'payer_transitions'
]


def load_bronze_table(table_name):
    """Load a single Bronze table and register as temp view."""
    table_path = f"{BRONZE_PATH}{table_name}/"
    
    try:
        df = spark.read.parquet(table_path)
        row_count = df.count()
        
        # Register as temporary view
        df.createOrReplaceTempView(f"bronze_{table_name}")
        
        print(f"✓ Loaded {table_name}: {row_count:,} rows")
        return {'table': table_name, 'status': 'success', 'rows': row_count}
        
    except Exception as e:
        print(f"✗ Failed to load {table_name}: {str(e)}")
        return {'table': table_name, 'status': 'error', 'error': str(e)}


def validate_required_tables():
    """Validate that essential tables are loaded."""
    essential_tables = ['patients', 'encounters', 'conditions', 'medications']
    missing = []
    
    for table in essential_tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM bronze_{table}").collect()[0]['cnt']
            if count == 0:
                missing.append(f"{table} (empty)")
        except Exception:
            missing.append(f"{table} (not loaded)")
    
    if missing:
        raise Exception(f"Essential tables missing or empty: {', '.join(missing)}")
    
    print("✓ All essential Bronze tables validated")


def create_source_summary():
    """Create summary view of source data statistics."""
    summary_sql = """
    SELECT 
        'patients' as table_name, COUNT(*) as record_count FROM bronze_patients
    UNION ALL
    SELECT 'encounters', COUNT(*) FROM bronze_encounters
    UNION ALL
    SELECT 'conditions', COUNT(*) FROM bronze_conditions
    UNION ALL
    SELECT 'medications', COUNT(*) FROM bronze_medications
    UNION ALL
    SELECT 'procedures', COUNT(*) FROM bronze_procedures
    UNION ALL
    SELECT 'observations', COUNT(*) FROM bronze_observations
    """
    
    summary_df = spark.sql(summary_sql)
    summary_df.createOrReplaceTempView("bronze_summary")
    
    print("\n📊 Bronze Layer Summary:")
    summary_df.show(truncate=False)


def main():
    """Main ETL process for loading Bronze views."""
    print(f"\n{'#'*60}")
    print(f"# Job 0: Load Bronze Views")
    print(f"# Started at: {datetime.now()}")
    print(f"# Bronze Path: {BRONZE_PATH}")
    print(f"{'#'*60}\n")
    
    results = []
    
    # Load all Bronze tables
    for table_name in BRONZE_TABLES:
        result = load_bronze_table(table_name)
        results.append(result)
    
    # Validate essential tables
    validate_required_tables()
    
    # Create summary view
    create_source_summary()
    
    # Summary
    print(f"\n{'='*60}")
    print("Job 0 Summary")
    print(f"{'='*60}")
    
    success_count = sum(1 for r in results if r['status'] == 'success')
    error_count = sum(1 for r in results if r['status'] == 'error')
    
    print(f"Tables loaded successfully: {success_count}/{len(BRONZE_TABLES)}")
    if error_count > 0:
        print(f"Tables with errors: {error_count}")
    
    print(f"\n✓ Job 0 completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
