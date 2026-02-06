"""
Job 5: Drug Exposure
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0 + Iceberg)

This Glue job processes drug data:
- Medications → drug_exposure
- Immunizations → drug_exposure

Phase 2: ETL Pipeline (Daily/Weekly Batch)
Input: bronze medications, immunizations; silver person, visit_occurrence
Output: drug_exposure (Iceberg)
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
DRUG_TYPE_PRESCRIPTION = 38000177
DRUG_TYPE_IMMUNIZATION = 38000179


def load_bronze_tables():
    """Load required tables from Bronze layer."""
    medications = spark.read.parquet(f"{BRONZE_PATH}medications/")
    immunizations = spark.read.parquet(f"{BRONZE_PATH}immunizations/")
    
    print(f"✓ Loaded {medications.count():,} medications")
    print(f"✓ Loaded {immunizations.count():,} immunizations")
    
    return medications, immunizations


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


def load_drug_concept_lookup():
    """Load concept table for drug vocabulary mapping.
    
    Maps RxNorm and CVX codes to standard drug concept_id.
    """
    concept = spark.read.format("iceberg").load(
        f"glue_catalog.{DATABASE_NAME}.concept"
    )
    
    # RxNorm for medications
    rxnorm_concepts = concept.filter(
        (F.col("vocabulary_id") == "RxNorm") & 
        (F.col("domain_id") == "Drug") &
        (F.col("standard_concept") == "S")
    ).select(
        F.col("concept_id").alias("mapped_concept_id"),
        F.col("concept_code").alias("source_code")
    )
    
    # CVX for immunizations
    cvx_concepts = concept.filter(
        (F.col("vocabulary_id") == "CVX") & 
        (F.col("domain_id") == "Drug") &
        (F.col("standard_concept") == "S")
    ).select(
        F.col("concept_id").alias("mapped_concept_id"),
        F.col("concept_code").alias("source_code")
    )
    
    print(f"✓ Loaded {rxnorm_concepts.count():,} RxNorm drug concepts")
    print(f"✓ Loaded {cvx_concepts.count():,} CVX immunization concepts")
    
    return rxnorm_concepts, cvx_concepts


def create_drug_exposure_medications(medications_df, person_df, visit_mapping_df, drug_concept_df):
    """Create drug_exposure from medications with RxNorm concept mapping."""
    
    person_lookup = person_df.select("person_id", "person_source_value")
    
    # Join with person, visit, and concept lookup
    drug_exposure = medications_df.join(
        person_lookup,
        medications_df.PATIENT == person_lookup.person_source_value,
        "left"
    ).join(
        visit_mapping_df,
        medications_df.ENCOUNTER == visit_mapping_df.encounter_id,
        "left"
    ).join(
        drug_concept_df,
        medications_df.CODE == drug_concept_df.source_code,
        "left"
    )
    
    drug_exposure = drug_exposure.select(
        F.monotonically_increasing_id().alias("drug_exposure_id"),
        F.col("person_id"),
        F.coalesce(F.col("mapped_concept_id"), F.lit(0)).alias("drug_concept_id"),
        F.to_date(F.col("START")).alias("drug_exposure_start_date"),
        F.to_timestamp(F.col("START")).alias("drug_exposure_start_datetime"),
        F.to_date(F.col("STOP")).alias("drug_exposure_end_date"),
        F.to_timestamp(F.col("STOP")).alias("drug_exposure_end_datetime"),
        F.lit(None).cast("date").alias("verbatim_end_date"),
        F.lit(DRUG_TYPE_PRESCRIPTION).alias("drug_type_concept_id"),
        F.lit(None).cast("string").alias("stop_reason"),
        F.lit(None).cast("integer").alias("refills"),
        F.col("DISPENSES").cast("double").alias("quantity"),
        F.lit(None).cast("integer").alias("days_supply"),
        F.lit(None).cast("string").alias("sig"),
        F.lit(0).alias("route_concept_id"),
        F.lit(None).cast("string").alias("lot_number"),
        F.lit(None).cast("long").alias("provider_id"),
        F.col("visit_id").alias("visit_occurrence_id"),
        F.lit(None).cast("long").alias("visit_detail_id"),
        F.col("CODE").alias("drug_source_value"),
        F.coalesce(F.col("mapped_concept_id"), F.lit(0)).alias("drug_source_concept_id"),
        F.lit(None).cast("string").alias("route_source_value"),
        F.lit(None).cast("string").alias("dose_unit_source_value")
    ).filter(F.col("person_id").isNotNull())
    
    return drug_exposure


def create_drug_exposure_immunizations(immunizations_df, person_df, visit_mapping_df, cvx_concept_df):
    """Create drug_exposure from immunizations with CVX concept mapping."""
    
    person_lookup = person_df.select("person_id", "person_source_value")
    
    # Join with person, visit, and CVX concept lookup
    drug_exposure = immunizations_df.join(
        person_lookup,
        immunizations_df.PATIENT == person_lookup.person_source_value,
        "left"
    ).join(
        visit_mapping_df,
        immunizations_df.ENCOUNTER == visit_mapping_df.encounter_id,
        "left"
    ).join(
        cvx_concept_df,
        immunizations_df.CODE == cvx_concept_df.source_code,
        "left"
    )
    
    drug_exposure = drug_exposure.select(
        F.monotonically_increasing_id().alias("drug_exposure_id"),
        F.col("person_id"),
        F.coalesce(F.col("mapped_concept_id"), F.lit(0)).alias("drug_concept_id"),
        F.to_date(F.col("DATE")).alias("drug_exposure_start_date"),
        F.to_timestamp(F.col("DATE")).alias("drug_exposure_start_datetime"),
        F.to_date(F.col("DATE")).alias("drug_exposure_end_date"),
        F.to_timestamp(F.col("DATE")).alias("drug_exposure_end_datetime"),
        F.lit(None).cast("date").alias("verbatim_end_date"),
        F.lit(DRUG_TYPE_IMMUNIZATION).alias("drug_type_concept_id"),
        F.lit(None).cast("string").alias("stop_reason"),
        F.lit(None).cast("integer").alias("refills"),
        F.lit(1.0).cast("double").alias("quantity"),
        F.lit(1).cast("integer").alias("days_supply"),
        F.lit(None).cast("string").alias("sig"),
        F.lit(0).alias("route_concept_id"),
        F.lit(None).cast("string").alias("lot_number"),
        F.lit(None).cast("long").alias("provider_id"),
        F.col("visit_id").alias("visit_occurrence_id"),
        F.lit(None).cast("long").alias("visit_detail_id"),
        F.col("CODE").alias("drug_source_value"),
        F.coalesce(F.col("mapped_concept_id"), F.lit(0)).alias("drug_source_concept_id"),
        F.lit(None).cast("string").alias("route_source_value"),
        F.lit(None).cast("string").alias("dose_unit_source_value")
    ).filter(F.col("person_id").isNotNull())
    
    return drug_exposure


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
    """Main ETL process for drug exposure."""
    print(f"\n{'#'*60}")
    print(f"# Job 5: Drug Exposure")
    print(f"# Started at: {datetime.now()}")
    print(f"{'#'*60}\n")
    
    # Load data
    medications, immunizations = load_bronze_tables()
    person, visit_mapping = load_silver_mappings()
    
    # Load vocabulary lookups for concept mapping
    rxnorm_concepts, cvx_concepts = load_drug_concept_lookup()
    
    # Create drug_exposure from medications and immunizations with concept mapping
    drug_meds = create_drug_exposure_medications(medications, person, visit_mapping, rxnorm_concepts)
    drug_imm = create_drug_exposure_immunizations(immunizations, person, visit_mapping, cvx_concepts)
    
    # Log mapping stats
    meds_mapped = drug_meds.filter(F.col("drug_concept_id") > 0).count()
    meds_total = drug_meds.count()
    imm_mapped = drug_imm.filter(F.col("drug_concept_id") > 0).count()
    imm_total = drug_imm.count()
    
    print(f"✓ Created {meds_total:,} drug exposures from medications")
    print(f"  → Mapped to concept: {meds_mapped:,} ({100*meds_mapped/max(meds_total,1):.1f}%)")
    print(f"✓ Created {imm_total:,} drug exposures from immunizations")
    print(f"  → Mapped to concept: {imm_mapped:,} ({100*imm_mapped/max(imm_total,1):.1f}%)")
    
    # Combine and re-generate IDs
    drug_exposure = drug_meds.unionByName(drug_imm)
    drug_exposure = drug_exposure.drop("drug_exposure_id").withColumn(
        "drug_exposure_id", F.monotonically_increasing_id() + 1
    )
    
    # Write to Iceberg
    write_to_iceberg(drug_exposure, "drug_exposure")
    
    # Summary
    print(f"\n{'='*60}")
    print("Job 5 Summary")
    print(f"{'='*60}")
    print(f"Total drug exposures: {drug_exposure.count():,}")
    print(f"\n✓ Job 5 completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
