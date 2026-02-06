"""
Raw to Bronze ETL Job
Healthcare Lakehouse - OMOP CDM Pipeline

This Glue job ingests raw Synthea CSV data into the Bronze layer:
- Reads CSV files from S3 raw layer
- Performs data quality checks
- Adds metadata columns (ingestion_date, source_file)
- Writes to Parquet format with partitioning
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'raw_path',
    'bronze_path',
    'database_name'
])

job.init(args['JOB_NAME'], args)

# Configuration
RAW_PATH = args['raw_path']
BRONZE_PATH = args['bronze_path']
DATABASE_NAME = args['database_name']

# Synthea table definitions with schema
SYNTHEA_TABLES = {
    'patients': {
        'required_cols': ['Id', 'BIRTHDATE', 'GENDER'],
        'date_cols': ['BIRTHDATE', 'DEATHDATE']
    },
    'encounters': {
        'required_cols': ['Id', 'START', 'STOP', 'PATIENT', 'ENCOUNTERCLASS'],
        'date_cols': ['START', 'STOP']
    },
    'conditions': {
        'required_cols': ['START', 'PATIENT', 'ENCOUNTER', 'CODE'],
        'date_cols': ['START', 'STOP']
    },
    'medications': {
        'required_cols': ['START', 'PATIENT', 'ENCOUNTER', 'CODE'],
        'date_cols': ['START', 'STOP']
    },
    'procedures': {
        'required_cols': ['START', 'PATIENT', 'ENCOUNTER', 'CODE'],
        'date_cols': ['START', 'STOP']
    },
    'observations': {
        'required_cols': ['DATE', 'PATIENT', 'ENCOUNTER', 'CODE'],
        'date_cols': ['DATE']
    },
    'immunizations': {
        'required_cols': ['DATE', 'PATIENT', 'ENCOUNTER', 'CODE'],
        'date_cols': ['DATE']
    },
    'allergies': {
        'required_cols': ['START', 'PATIENT', 'ENCOUNTER', 'CODE'],
        'date_cols': ['START', 'STOP']
    },
    'careplans': {
        'required_cols': ['START', 'PATIENT', 'ENCOUNTER', 'CODE'],
        'date_cols': ['START', 'STOP']
    },
    'devices': {
        'required_cols': ['START', 'PATIENT', 'ENCOUNTER', 'CODE'],
        'date_cols': ['START', 'STOP']
    },
    'imaging_studies': {
        'required_cols': ['DATE', 'PATIENT', 'ENCOUNTER'],
        'date_cols': ['DATE']
    },
    'organizations': {
        'required_cols': ['Id', 'NAME'],
        'date_cols': []
    },
    'providers': {
        'required_cols': ['Id', 'NAME'],
        'date_cols': []
    },
    'payers': {
        'required_cols': ['Id', 'NAME'],
        'date_cols': []
    },
    'payer_transitions': {
        'required_cols': ['PATIENT', 'START_DATE', 'END_DATE', 'PAYER'],
        'date_cols': ['START_DATE', 'END_DATE']
    },
    'supplies': {
        'required_cols': ['DATE', 'PATIENT', 'ENCOUNTER', 'CODE'],
        'date_cols': ['DATE']
    }
}


def validate_required_columns(df, table_name, required_cols):
    """Check that required columns are not all null."""
    issues = []
    for col in required_cols:
        if col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            total_count = df.count()
            if null_count == total_count:
                issues.append(f"{table_name}.{col}: All values are NULL")
    return issues


def remove_duplicates(df, id_col='Id'):
    """Remove exact duplicate rows."""
    if id_col in df.columns:
        return df.dropDuplicates([id_col])
    return df.dropDuplicates()


def add_metadata_columns(df, source_file):
    """Add ingestion metadata columns."""
    return df.withColumn('ingestion_date', F.current_date()) \
             .withColumn('ingestion_timestamp', F.current_timestamp()) \
             .withColumn('source_file', F.lit(source_file))


def process_table(table_name, table_config):
    """Process a single Synthea table."""
    source_path = f"{RAW_PATH}{table_name}.csv"
    target_path = f"{BRONZE_PATH}{table_name}/"
    
    print(f"\n{'='*60}")
    print(f"Processing table: {table_name}")
    print(f"Source: {source_path}")
    print(f"Target: {target_path}")
    print(f"{'='*60}")
    
    try:
        # Read CSV with schema inference
        df = spark.read.csv(
            source_path,
            header=True,
            inferSchema=True,
            timestampFormat='yyyy-MM-dd\'T\'HH:mm:ss\'Z\''
        )
        
        initial_count = df.count()
        print(f"Initial row count: {initial_count}")
        
        if initial_count == 0:
            print(f"WARNING: No data found for {table_name}")
            return {'table': table_name, 'status': 'empty', 'rows': 0}
        
        # Data Quality Checks
        # 1. Remove duplicates
        df = remove_duplicates(df)
        after_dedup = df.count()
        dups_removed = initial_count - after_dedup
        if dups_removed > 0:
            print(f"Removed {dups_removed} duplicate rows")
        
        # 2. Validate required columns
        issues = validate_required_columns(df, table_name, table_config['required_cols'])
        for issue in issues:
            print(f"WARNING: {issue}")
        
        # 3. Parse date columns
        for date_col in table_config['date_cols']:
            if date_col in df.columns:
                # Handle multiple date formats
                df = df.withColumn(
                    date_col,
                    F.coalesce(
                        F.to_timestamp(F.col(date_col), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                        F.to_timestamp(F.col(date_col), "yyyy-MM-dd HH:mm:ss"),
                        F.to_date(F.col(date_col), "yyyy-MM-dd")
                    )
                )
        
        # 4. Add metadata columns
        df = add_metadata_columns(df, source_path)
        
        # 5. Write to Parquet with partitioning
        df.write \
          .mode("overwrite") \
          .partitionBy("ingestion_date") \
          .parquet(target_path)
        
        final_count = df.count()
        print(f"Successfully wrote {final_count} rows to {target_path}")
        
        return {
            'table': table_name,
            'status': 'success',
            'initial_rows': initial_count,
            'final_rows': final_count,
            'duplicates_removed': dups_removed
        }
        
    except Exception as e:
        print(f"ERROR processing {table_name}: {str(e)}")
        return {'table': table_name, 'status': 'error', 'error': str(e)}


def main():
    """Main ETL process."""
    print(f"\n{'#'*60}")
    print(f"# Raw to Bronze ETL Job")
    print(f"# Started at: {datetime.now()}")
    print(f"# Raw Path: {RAW_PATH}")
    print(f"# Bronze Path: {BRONZE_PATH}")
    print(f"{'#'*60}")
    
    results = []
    
    for table_name, table_config in SYNTHEA_TABLES.items():
        result = process_table(table_name, table_config)
        results.append(result)
    
    # Summary
    print(f"\n{'='*60}")
    print("ETL Summary")
    print(f"{'='*60}")
    
    success_count = sum(1 for r in results if r['status'] == 'success')
    error_count = sum(1 for r in results if r['status'] == 'error')
    empty_count = sum(1 for r in results if r['status'] == 'empty')
    
    print(f"Successful: {success_count}")
    print(f"Errors: {error_count}")
    print(f"Empty: {empty_count}")
    
    for result in results:
        status_emoji = "✓" if result['status'] == 'success' else "✗" if result['status'] == 'error' else "⚠"
        print(f"  {status_emoji} {result['table']}: {result['status']}")
    
    print(f"\nCompleted at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
