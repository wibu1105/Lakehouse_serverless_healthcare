"""
Job 4: Clinical Events
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0 + Iceberg)

This Glue job processes clinical data:
- Conditions → condition_occurrence
- Procedures → procedure_occurrence
- Observations → measurement, observation

Phase 2: ETL Pipeline (Daily/Weekly Batch)
Input: bronze conditions, procedures, observations; silver person, visit_occurrence
Output: condition_occurrence, procedure_occurrence, measurement, observation (Iceberg)
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

# OMOP Type Concept IDs
CONDITION_TYPE_EHR = 32817
PROCEDURE_TYPE_EHR = 32817
MEASUREMENT_TYPE_EHR = 32817
OBSERVATION_TYPE_EHR = 32817


def load_bronze_tables():
    """Load required tables from Bronze layer."""
    conditions = spark.read.parquet(f"{BRONZE_PATH}conditions/")
    procedures = spark.read.parquet(f"{BRONZE_PATH}procedures/")
    observations = spark.read.parquet(f"{BRONZE_PATH}observations/")
    
    print(f"✓ Loaded {conditions.count():,} conditions")
    print(f"✓ Loaded {procedures.count():,} procedures")
    print(f"✓ Loaded {observations.count():,} observations")
    
    return conditions, procedures, observations


def load_silver_mappings():
    """Load person and visit mappings from Silver layer."""
    # Load person lookup
    person = spark.read.format("iceberg").load(
        f"glue_catalog.{DATABASE_NAME}.person"
    )
    
    # Load visit_id mapping
    visit_mapping = spark.read.format("iceberg").load(
        f"glue_catalog.{DATABASE_NAME}.visit_id_mapping"
    )
    
    return person, visit_mapping


def load_concept_lookup():
    """Load concept table for vocabulary mapping.
    
    Maps SNOMED codes to standard concept_id for conditions, procedures, etc.
    """
    concept = spark.read.format("iceberg").load(
        f"glue_catalog.{DATABASE_NAME}.concept"
    )
    
    # Filter for standard concepts from SNOMED vocabulary for conditions
    condition_concepts = concept.filter(
        (F.col("vocabulary_id") == "SNOMED") & 
        (F.col("domain_id") == "Condition") &
        (F.col("standard_concept") == "S")
    ).select(
        F.col("concept_id").alias("mapped_concept_id"),
        F.col("concept_code").alias("source_code")
    )
    
    # Filter for standard concepts for procedures
    procedure_concepts = concept.filter(
        (F.col("vocabulary_id") == "SNOMED") & 
        (F.col("domain_id") == "Procedure") &
        (F.col("standard_concept") == "S")
    ).select(
        F.col("concept_id").alias("mapped_concept_id"),
        F.col("concept_code").alias("source_code")
    )
    
    # Filter for standard concepts for observations/measurements (LOINC)
    observation_concepts = concept.filter(
        (F.col("vocabulary_id").isin("LOINC", "SNOMED")) & 
        (F.col("domain_id").isin("Measurement", "Observation")) &
        (F.col("standard_concept") == "S")
    ).select(
        F.col("concept_id").alias("mapped_concept_id"),
        F.col("concept_code").alias("source_code")
    )
    
    print(f"✓ Loaded {condition_concepts.count():,} condition concepts")
    print(f"✓ Loaded {procedure_concepts.count():,} procedure concepts")
    print(f"✓ Loaded {observation_concepts.count():,} observation concepts")
    
    return condition_concepts, procedure_concepts, observation_concepts


def create_condition_occurrence(conditions_df, person_df, visit_mapping_df, concept_lookup_df):
    """Create condition_occurrence from Synthea conditions.
    
    Maps SNOMED condition codes to standard concept_id via vocabulary lookup.
    """
    
    # Join with person to get person_id
    person_lookup = person_df.select("person_id", "person_source_value")
    
    # Join with concept lookup to map SNOMED code to concept_id
    condition_occurrence = conditions_df.join(
        person_lookup,
        conditions_df.PATIENT == person_lookup.person_source_value,
        "left"
    ).join(
        visit_mapping_df,
        conditions_df.ENCOUNTER == visit_mapping_df.encounter_id,
        "left"
    ).join(
        concept_lookup_df,
        conditions_df.CODE == concept_lookup_df.source_code,
        "left"
    )
    
    condition_occurrence = condition_occurrence.select(
        F.monotonically_increasing_id().alias("condition_occurrence_id"),
        F.col("person_id"),
        F.coalesce(F.col("mapped_concept_id"), F.lit(0)).alias("condition_concept_id"),
        F.to_date(F.col("START")).alias("condition_start_date"),
        F.to_timestamp(F.col("START")).alias("condition_start_datetime"),
        F.to_date(F.col("STOP")).alias("condition_end_date"),
        F.to_timestamp(F.col("STOP")).alias("condition_end_datetime"),
        F.lit(CONDITION_TYPE_EHR).alias("condition_type_concept_id"),
        F.lit(0).alias("condition_status_concept_id"),
        F.lit(None).cast("string").alias("stop_reason"),
        F.lit(None).cast("long").alias("provider_id"),
        F.col("visit_id").alias("visit_occurrence_id"),
        F.lit(None).cast("long").alias("visit_detail_id"),
        F.col("CODE").alias("condition_source_value"),
        F.coalesce(F.col("mapped_concept_id"), F.lit(0)).alias("condition_source_concept_id"),
        F.lit(None).cast("string").alias("condition_status_source_value")
    ).filter(F.col("person_id").isNotNull())
    
    # Log mapping stats
    mapped_count = condition_occurrence.filter(F.col("condition_concept_id") > 0).count()
    total_count = condition_occurrence.count()
    print(f"✓ Created {total_count:,} condition_occurrence records")
    print(f"  → Mapped to concept: {mapped_count:,} ({100*mapped_count/total_count:.1f}%)")
    
    return condition_occurrence


def create_procedure_occurrence(procedures_df, person_df, visit_mapping_df):
    """Create procedure_occurrence from Synthea procedures."""
    
    person_lookup = person_df.select("person_id", "person_source_value")
    
    procedure_occurrence = procedures_df.join(
        person_lookup,
        procedures_df.PATIENT == person_lookup.person_source_value,
        "left"
    ).join(
        visit_mapping_df,
        procedures_df.ENCOUNTER == visit_mapping_df.encounter_id,
        "left"
    )
    
    procedure_occurrence = procedure_occurrence.select(
        F.monotonically_increasing_id().alias("procedure_occurrence_id"),
        F.col("person_id"),
        F.lit(0).alias("procedure_concept_id"),  # To be mapped via vocabulary
        F.to_date(F.col("START")).alias("procedure_date"),
        F.to_timestamp(F.col("START")).alias("procedure_datetime"),
        F.to_date(F.col("STOP")).alias("procedure_end_date"),
        F.to_timestamp(F.col("STOP")).alias("procedure_end_datetime"),
        F.lit(PROCEDURE_TYPE_EHR).alias("procedure_type_concept_id"),
        F.lit(0).alias("modifier_concept_id"),
        F.lit(1).cast("integer").alias("quantity"),
        F.lit(None).cast("long").alias("provider_id"),
        F.col("visit_id").alias("visit_occurrence_id"),
        F.lit(None).cast("long").alias("visit_detail_id"),
        F.col("CODE").alias("procedure_source_value"),
        F.lit(0).alias("procedure_source_concept_id"),
        F.lit(None).cast("string").alias("modifier_source_value")
    ).filter(F.col("person_id").isNotNull())
    
    print(f"✓ Created {procedure_occurrence.count():,} procedure_occurrence records")
    return procedure_occurrence


def create_measurement(observations_df, person_df, visit_mapping_df):
    """Create measurement from Synthea observations (numeric values)."""
    
    person_lookup = person_df.select("person_id", "person_source_value")
    
    # Filter for numeric observations
    numeric_obs = observations_df.filter(
        F.col("TYPE") == "numeric"
    )
    
    measurement = numeric_obs.join(
        person_lookup,
        numeric_obs.PATIENT == person_lookup.person_source_value,
        "left"
    ).join(
        visit_mapping_df,
        numeric_obs.ENCOUNTER == visit_mapping_df.encounter_id,
        "left"
    )
    
    measurement = measurement.select(
        F.monotonically_increasing_id().alias("measurement_id"),
        F.col("person_id"),
        F.lit(0).alias("measurement_concept_id"),
        F.to_date(F.col("DATE")).alias("measurement_date"),
        F.to_timestamp(F.col("DATE")).alias("measurement_datetime"),
        F.lit(None).cast("string").alias("measurement_time"),
        F.lit(MEASUREMENT_TYPE_EHR).alias("measurement_type_concept_id"),
        F.lit(0).alias("operator_concept_id"),
        F.col("VALUE").cast("double").alias("value_as_number"),
        F.lit(None).cast("long").alias("value_as_concept_id"),
        F.lit(0).alias("unit_concept_id"),
        F.lit(None).cast("double").alias("range_low"),
        F.lit(None).cast("double").alias("range_high"),
        F.lit(None).cast("long").alias("provider_id"),
        F.col("visit_id").alias("visit_occurrence_id"),
        F.lit(None).cast("long").alias("visit_detail_id"),
        F.col("CODE").alias("measurement_source_value"),
        F.lit(0).alias("measurement_source_concept_id"),
        F.col("UNITS").alias("unit_source_value"),
        F.lit(None).cast("long").alias("unit_source_concept_id"),
        F.col("VALUE").alias("value_source_value"),
        F.lit(None).cast("long").alias("measurement_event_id"),
        F.lit(0).alias("meas_event_field_concept_id")
    ).filter(F.col("person_id").isNotNull())
    
    print(f"✓ Created {measurement.count():,} measurement records")
    return measurement


def create_observation(observations_df, person_df, visit_mapping_df):
    """Create observation from Synthea observations (non-numeric values)."""
    
    person_lookup = person_df.select("person_id", "person_source_value")
    
    # Filter for non-numeric observations
    non_numeric_obs = observations_df.filter(
        (F.col("TYPE") != "numeric") | (F.col("TYPE").isNull())
    )
    
    observation = non_numeric_obs.join(
        person_lookup,
        non_numeric_obs.PATIENT == person_lookup.person_source_value,
        "left"
    ).join(
        visit_mapping_df,
        non_numeric_obs.ENCOUNTER == visit_mapping_df.encounter_id,
        "left"
    )
    
    observation = observation.select(
        F.monotonically_increasing_id().alias("observation_id"),
        F.col("person_id"),
        F.lit(0).alias("observation_concept_id"),
        F.to_date(F.col("DATE")).alias("observation_date"),
        F.to_timestamp(F.col("DATE")).alias("observation_datetime"),
        F.lit(OBSERVATION_TYPE_EHR).alias("observation_type_concept_id"),
        F.lit(None).cast("double").alias("value_as_number"),
        F.col("VALUE").alias("value_as_string"),
        F.lit(0).alias("value_as_concept_id"),
        F.lit(0).alias("qualifier_concept_id"),
        F.lit(0).alias("unit_concept_id"),
        F.lit(None).cast("long").alias("provider_id"),
        F.col("visit_id").alias("visit_occurrence_id"),
        F.lit(None).cast("long").alias("visit_detail_id"),
        F.col("CODE").alias("observation_source_value"),
        F.lit(0).alias("observation_source_concept_id"),
        F.col("UNITS").alias("unit_source_value"),
        F.lit(None).cast("string").alias("qualifier_source_value"),
        F.lit(None).cast("long").alias("value_source_concept_id"),
        F.lit(None).cast("long").alias("observation_event_id"),
        F.lit(0).alias("obs_event_field_concept_id")
    ).filter(F.col("person_id").isNotNull())
    
    print(f"✓ Created {observation.count():,} observation records")
    return observation


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
    """Main ETL process for clinical events."""
    print(f"\n{'#'*60}")
    print(f"# Job 4: Clinical Events")
    print(f"# Started at: {datetime.now()}")
    print(f"{'#'*60}\n")
    
    # Load data
    conditions, procedures, observations = load_bronze_tables()
    person, visit_mapping = load_silver_mappings()
    
    # Load vocabulary lookups for concept mapping
    condition_concepts, procedure_concepts, observation_concepts = load_concept_lookup()
    
    # Create OMOP tables with concept mapping
    condition_occurrence = create_condition_occurrence(conditions, person, visit_mapping, condition_concepts)
    procedure_occurrence = create_procedure_occurrence(procedures, person, visit_mapping)
    measurement = create_measurement(observations, person, visit_mapping)
    observation = create_observation(observations, person, visit_mapping)
    
    # Write to Iceberg
    write_to_iceberg(condition_occurrence, "condition_occurrence")
    write_to_iceberg(procedure_occurrence, "procedure_occurrence")
    write_to_iceberg(measurement, "measurement")
    write_to_iceberg(observation, "observation")
    
    # Summary
    print(f"\n{'='*60}")
    print("Job 4 Summary")
    print(f"{'='*60}")
    print(f"Condition occurrences: {condition_occurrence.count():,}")
    print(f"Procedure occurrences: {procedure_occurrence.count():,}")
    print(f"Measurements: {measurement.count():,}")
    print(f"Observations: {observation.count():,}")
    print(f"\n✓ Job 4 completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
