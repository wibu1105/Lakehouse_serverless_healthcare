"""
Job 2: Person & Observation Period
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0 + Iceberg)

This Glue job creates person and observation_period tables:
- Maps Synthea patients to OMOP person table
- Maps gender/race to OMOP concept IDs
- Calculates observation window from encounters
- Creates location records

Phase 2: ETL Pipeline (Daily/Weekly Batch)
Input: bronze_patients, bronze_encounters
Output: person (Iceberg), observation_period (Iceberg), location (Iceberg)
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
    'bronze_path',
    'silver_path',
    'database_name'
])

job.init(args['JOB_NAME'], args)

# Configuration
BRONZE_PATH = args['bronze_path']
SILVER_PATH = args['silver_path']
DATABASE_NAME = args['database_name']

# Configure Iceberg
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", SILVER_PATH)
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# OMOP Concept Mappings
GENDER_CONCEPT_MAP = {
    'M': 8507,      # Male
    'F': 8532       # Female
}

RACE_CONCEPT_MAP = {
    'white': 8527,
    'black': 8516,
    'asian': 8515,
    'native': 8657,
    'other': 8522
}

ETHNICITY_CONCEPT_MAP = {
    'hispanic': 38003563,
    'nonhispanic': 38003564
}


def load_bronze_tables():
    """Load required tables from Bronze layer."""
    patients = spark.read.parquet(f"{BRONZE_PATH}patients/")
    encounters = spark.read.parquet(f"{BRONZE_PATH}encounters/")
    
    print(f"✓ Loaded {patients.count():,} patients")
    print(f"✓ Loaded {encounters.count():,} encounters")
    
    return patients, encounters


def create_person_table(patients_df):
    """Create OMOP person table from Synthea patients."""
    
    # Generate person_id
    person_df = patients_df.withColumn(
        "person_id", F.monotonically_increasing_id() + 1
    )
    
    # Map gender
    gender_map_expr = F.create_map([F.lit(k) for item in GENDER_CONCEPT_MAP.items() for k in item])
    
    person_df = person_df.select(
        F.col("person_id"),
        F.when(F.col("GENDER") == "M", 8507)
         .when(F.col("GENDER") == "F", 8532)
         .otherwise(0).alias("gender_concept_id"),
        F.year(F.col("BIRTHDATE")).alias("year_of_birth"),
        F.month(F.col("BIRTHDATE")).alias("month_of_birth"),
        F.dayofmonth(F.col("BIRTHDATE")).alias("day_of_birth"),
        F.to_timestamp(F.col("BIRTHDATE")).alias("birth_datetime"),
        F.when(F.lower(F.col("RACE")) == "white", 8527)
         .when(F.lower(F.col("RACE")) == "black", 8516)
         .when(F.lower(F.col("RACE")) == "asian", 8515)
         .when(F.lower(F.col("RACE")) == "native", 8657)
         .otherwise(8522).alias("race_concept_id"),
        F.when(F.lower(F.col("ETHNICITY")).contains("hispanic"), 38003563)
         .otherwise(38003564).alias("ethnicity_concept_id"),
        F.lit(None).cast("long").alias("location_id"),
        F.lit(None).cast("long").alias("provider_id"),
        F.lit(None).cast("long").alias("care_site_id"),
        F.col("Id").alias("person_source_value"),
        F.col("GENDER").alias("gender_source_value"),
        F.lit(0).alias("gender_source_concept_id"),
        F.col("RACE").alias("race_source_value"),
        F.lit(0).alias("race_source_concept_id"),
        F.col("ETHNICITY").alias("ethnicity_source_value"),
        F.lit(0).alias("ethnicity_source_concept_id")
    )
    
    print(f"✓ Created person table: {person_df.count():,} records")
    return person_df


def create_observation_period(patients_df, encounters_df):
    """Create observation_period from encounters."""
    
    # Get min/max encounter dates per patient
    obs_period = encounters_df.groupBy("PATIENT").agg(
        F.min(F.to_date(F.col("START"))).alias("observation_period_start_date"),
        F.max(F.to_date(F.col("STOP"))).alias("observation_period_end_date")
    )
    
    # Join with patients to get person_id mapping
    patient_ids = patients_df.select(
        F.col("Id").alias("PATIENT"),
        F.monotonically_increasing_id().alias("person_id")
    )
    
    obs_period = obs_period.join(patient_ids, "PATIENT")
    
    # Add observation_period_id and concept_id
    obs_period = obs_period.select(
        F.monotonically_increasing_id().alias("observation_period_id"),
        F.col("person_id"),
        F.col("observation_period_start_date"),
        F.col("observation_period_end_date"),
        F.lit(44814724).alias("period_type_concept_id")  # EHR
    )
    
    print(f"✓ Created observation_period: {obs_period.count():,} records")
    return obs_period


def create_location(patients_df):
    """Create location records from patient addresses."""
    
    location = patients_df.select(
        F.monotonically_increasing_id().alias("location_id"),
        F.col("ADDRESS").alias("address_1"),
        F.lit(None).cast("string").alias("address_2"),
        F.col("CITY").alias("city"),
        F.col("STATE").alias("state"),
        F.col("ZIP").alias("zip"),
        F.col("COUNTY").alias("county"),
        F.lit("US").alias("country_concept_id"),
        F.col("LAT").alias("latitude"),
        F.col("LON").alias("longitude"),
        F.concat(F.col("CITY"), F.lit(", "), F.col("STATE")).alias("location_source_value")
    ).dropDuplicates(["address_1", "city", "state", "zip"])
    
    print(f"✓ Created location: {location.count():,} records")
    return location


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
    """Main ETL process for person and observation_period."""
    print(f"\n{'#'*60}")
    print(f"# Job 2: Person & Observation Period")
    print(f"# Started at: {datetime.now()}")
    print(f"{'#'*60}\n")
    
    # Load Bronze tables
    patients, encounters = load_bronze_tables()
    
    # Create OMOP tables
    person = create_person_table(patients)
    observation_period = create_observation_period(patients, encounters)
    location = create_location(patients)
    
    # Write to Iceberg
    write_to_iceberg(person, "person")
    write_to_iceberg(observation_period, "observation_period")
    write_to_iceberg(location, "location")
    
    # Summary
    print(f"\n{'='*60}")
    print("Job 2 Summary")
    print(f"{'='*60}")
    print(f"Person records: {person.count():,}")
    print(f"Observation periods: {observation_period.count():,}")
    print(f"Locations: {location.count():,}")
    print(f"\n✓ Job 2 completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
