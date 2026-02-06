"""
Job 3: Visit Occurrence
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0 + Iceberg)

This Glue job creates the visit_occurrence table:
- Reads all_visits from Job 1
- Maps to OMOP visit_occurrence schema
- Links with person_id from Job 2
- Validates >95% mapping rate

Phase 2: ETL Pipeline (Daily/Weekly Batch)
Input: all_visits, person (from Silver)
Output: visit_occurrence (Iceberg) with >95% mapping
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

# OMOP Visit Type Concept IDs
VISIT_TYPE_CONCEPT = 44818518  # Visit derived from EHR


def load_silver_tables():
    """Load all_visits and person from Silver layer."""
    all_visits = spark.read.format("iceberg").load(
        f"glue_catalog.{DATABASE_NAME}.all_visits"
    )
    
    # Load person table
    person = spark.read.format("iceberg").load(
        f"glue_catalog.{DATABASE_NAME}.person"
    )
    
    print(f"✓ Loaded {all_visits.count():,} visits from Silver")
    print(f"✓ Loaded {person.count():,} persons from Silver")
    
    return all_visits, person


def create_visit_occurrence(visits_df, person_df):
    """Create OMOP visit_occurrence table."""
    
    # Join visits with person to get person_id
    person_lookup = person_df.select(
        F.col("person_id"),
        F.col("person_source_value").alias("patient_id")
    )
    
    visit_occurrence = visits_df.join(
        person_lookup,
        visits_df.patient_id == person_lookup.patient_id,
        "left"
    )
    
    # Create visit_occurrence schema
    visit_occurrence = visit_occurrence.select(
        F.col("visit_id").alias("visit_occurrence_id"),
        F.col("person_id"),
        F.col("visit_concept_id"),
        F.to_date(F.col("visit_start_datetime")).alias("visit_start_date"),
        F.col("visit_start_datetime"),
        F.to_date(F.col("visit_end_datetime")).alias("visit_end_date"),
        F.col("visit_end_datetime"),
        F.lit(VISIT_TYPE_CONCEPT).alias("visit_type_concept_id"),
        F.lit(None).cast("long").alias("provider_id"),
        F.lit(None).cast("long").alias("care_site_id"),
        F.col("visit_type").alias("visit_source_value"),
        F.lit(0).alias("visit_source_concept_id"),
        F.lit(0).alias("admitted_from_concept_id"),
        F.lit(None).cast("string").alias("admitted_from_source_value"),
        F.lit(0).alias("discharged_to_concept_id"),
        F.lit(None).cast("string").alias("discharged_to_source_value"),
        F.lit(None).cast("long").alias("preceding_visit_occurrence_id")
    )
    
    return visit_occurrence


def validate_mapping_rate(visit_occurrence_df, total_visits):
    """Validate that >95% of visits are mapped."""
    mapped_count = visit_occurrence_df.filter(F.col("person_id").isNotNull()).count()
    unmapped_count = visit_occurrence_df.filter(F.col("person_id").isNull()).count()
    
    mapping_rate = (mapped_count / total_visits) * 100 if total_visits > 0 else 0
    
    print(f"\n📊 Mapping Statistics:")
    print(f"  - Total visits: {total_visits:,}")
    print(f"  - Mapped: {mapped_count:,} ({mapping_rate:.1f}%)")
    print(f"  - Unmapped: {unmapped_count:,}")
    
    if mapping_rate < 95:
        print(f"⚠ WARNING: Mapping rate {mapping_rate:.1f}% is below 95% threshold")
    else:
        print(f"✓ Mapping rate {mapping_rate:.1f}% meets >95% threshold")
    
    return mapping_rate


def write_to_iceberg(df, table_name):
    """Write DataFrame to Silver layer as Iceberg table.
    
    Uses single database pattern matching Databricks original.
    """
    full_table = f"glue_catalog.{DATABASE_NAME}.{table_name}"
    
    if df.rdd.getNumPartitions() > 200:
        df = df.coalesce(100)
    
    df.writeTo(full_table) \
      .tableProperty("format-version", "2") \
      .createOrReplace()
    
    print(f"✓ Written to Iceberg table: {full_table}")



def main():
    """Main ETL process for visit occurrence."""
    print(f"\n{'#'*60}")
    print(f"# Job 3: Visit Occurrence")
    print(f"# Started at: {datetime.now()}")
    print(f"{'#'*60}\n")
    
    # Load Silver tables
    visits, person = load_silver_tables()
    
    total_visits = visits.count()
    
    # Create visit_occurrence
    visit_occurrence = create_visit_occurrence(visits, person)
    
    # Filter out unmapped visits for final output
    visit_occurrence_mapped = visit_occurrence.filter(F.col("person_id").isNotNull())
    
    # Validate mapping rate
    mapping_rate = validate_mapping_rate(visit_occurrence, total_visits)
    
    # Write to Iceberg
    write_to_iceberg(visit_occurrence_mapped, "visit_occurrence")
    
    # Summary
    print(f"\n{'='*60}")
    print("Job 3 Summary")
    print(f"{'='*60}")
    print(f"Visit occurrences created: {visit_occurrence_mapped.count():,}")
    print(f"Mapping rate: {mapping_rate:.1f}%")
    print(f"\n✓ Job 3 completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
