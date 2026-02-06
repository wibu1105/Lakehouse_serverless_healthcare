"""
Job 1: Visit Grouping
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0 + Iceberg)

This Glue job groups encounters into visits:
- Groups inpatient encounters by date ranges
- Merges ER visits within 24 hours
- Consolidates outpatient visits by day
- Creates visit_id mapping for downstream jobs

Phase 2: ETL Pipeline (Daily/Weekly Batch)
Input: bronze_encounters
Output: all_visits (Iceberg), visit_id_mapping (Iceberg)
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window

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

# OMOP Visit Concept IDs
VISIT_CONCEPTS = {
    'inpatient': 9201,
    'emergency': 9203,
    'outpatient': 9202,
    'ambulatory': 9202,
    'wellness': 9202,
    'urgentcare': 9203
}


def load_bronze_encounters():
    """Load encounters from Bronze layer."""
    encounters_path = f"{BRONZE_PATH}encounters/"
    df = spark.read.parquet(encounters_path)
    
    # Standardize column names
    df = df.withColumn("patient_id", F.col("PATIENT")) \
           .withColumn("encounter_id", F.col("Id")) \
           .withColumn("encounter_class", F.lower(F.col("ENCOUNTERCLASS"))) \
           .withColumn("start_datetime", F.to_timestamp(F.col("START"))) \
           .withColumn("end_datetime", F.to_timestamp(F.col("STOP"))) \
           .withColumn("start_date", F.to_date(F.col("START")))
    
    print(f"✓ Loaded {df.count():,} encounters from Bronze")
    return df


def group_inpatient_visits(encounters_df):
    """Group inpatient encounters into continuous visits."""
    inpatient = encounters_df.filter(F.col("encounter_class") == "inpatient")
    
    # Window for detecting overlapping/adjacent inpatient stays
    patient_window = Window.partitionBy("patient_id").orderBy("start_datetime")
    
    inpatient = inpatient.withColumn(
        "prev_end", F.lag("end_datetime").over(patient_window)
    ).withColumn(
        "gap_hours", 
        F.when(F.col("prev_end").isNotNull(),
               (F.unix_timestamp("start_datetime") - F.unix_timestamp("prev_end")) / 3600
        ).otherwise(999)
    ).withColumn(
        "new_visit", F.when(F.col("gap_hours") > 24, 1).otherwise(0)
    ).withColumn(
        "visit_group", F.sum("new_visit").over(patient_window)
    )
    
    # Aggregate into visits
    inpatient_visits = inpatient.groupBy("patient_id", "visit_group").agg(
        F.min("start_datetime").alias("visit_start_datetime"),
        F.max("end_datetime").alias("visit_end_datetime"),
        F.collect_list("encounter_id").alias("encounter_ids"),
        F.lit(VISIT_CONCEPTS['inpatient']).alias("visit_concept_id"),
        F.lit("inpatient").alias("visit_type")
    )
    
    print(f"✓ Grouped {inpatient_visits.count():,} inpatient visits")
    return inpatient_visits


def group_er_visits(encounters_df):
    """Merge ER visits within 24 hours."""
    er = encounters_df.filter(
        F.col("encounter_class").isin(["emergency", "urgentcare"])
    )
    
    patient_window = Window.partitionBy("patient_id").orderBy("start_datetime")
    
    er = er.withColumn(
        "prev_end", F.lag("end_datetime").over(patient_window)
    ).withColumn(
        "gap_hours", 
        F.when(F.col("prev_end").isNotNull(),
               (F.unix_timestamp("start_datetime") - F.unix_timestamp("prev_end")) / 3600
        ).otherwise(999)
    ).withColumn(
        "new_visit", F.when(F.col("gap_hours") > 24, 1).otherwise(0)
    ).withColumn(
        "visit_group", F.sum("new_visit").over(patient_window)
    )
    
    # Aggregate into visits
    er_visits = er.groupBy("patient_id", "visit_group").agg(
        F.min("start_datetime").alias("visit_start_datetime"),
        F.max("end_datetime").alias("visit_end_datetime"),
        F.collect_list("encounter_id").alias("encounter_ids"),
        F.lit(VISIT_CONCEPTS['emergency']).alias("visit_concept_id"),
        F.lit("emergency").alias("visit_type")
    )
    
    print(f"✓ Merged {er_visits.count():,} ER visits")
    return er_visits


def consolidate_outpatient(encounters_df):
    """Consolidate outpatient visits by patient and day."""
    outpatient = encounters_df.filter(
        F.col("encounter_class").isin(["outpatient", "ambulatory", "wellness"])
    )
    
    # Group by patient and date
    outpatient_visits = outpatient.groupBy("patient_id", "start_date").agg(
        F.min("start_datetime").alias("visit_start_datetime"),
        F.max("end_datetime").alias("visit_end_datetime"),
        F.collect_list("encounter_id").alias("encounter_ids"),
        F.lit(VISIT_CONCEPTS['outpatient']).alias("visit_concept_id"),
        F.lit("outpatient").alias("visit_type")
    ).withColumn("visit_group", F.lit(0).cast("bigint")).drop("start_date")
    
    print(f"✓ Consolidated {outpatient_visits.count():,} outpatient visits")
    return outpatient_visits


def create_visit_id_mapping(all_visits_df):
    """Create mapping from encounter_id to visit_id."""
    # Explode encounter_ids to create mapping
    mapping = all_visits_df.select(
        F.col("visit_id"),
        F.explode("encounter_ids").alias("encounter_id")
    )
    
    print(f"✓ Created {mapping.count():,} encounter-to-visit mappings")
    return mapping


def write_to_iceberg(df, table_name):
    """Write DataFrame to Silver layer as Iceberg table.
    
    Uses single database pattern matching Databricks original.
    Table names: all_visits, visit_id_mapping, etc.
    """
    full_table = f"glue_catalog.{DATABASE_NAME}.{table_name}"
    
    # Check number of partitions and repartition if needed
    if df.rdd.getNumPartitions() > 200:
        df = df.coalesce(100)
    
    try:
        print(f"Attempting to write to {full_table}...")
        df.writeTo(full_table) \
          .tableProperty("format-version", "2") \
          .createOrReplace()
        print(f"✓ Written to Iceberg table: {full_table}")
        
    except Exception as e:
        # Catch S3 404 'Key does not exist' or Iceberg 'NotFound'
        error_msg = str(e)
        if "does not exist" in error_msg or "Key does not exist" in error_msg or "NotFound" in error_msg:
            print(f"⚠ WARNING: Table {full_table} seems corrupted (Metadata/S3 mismatch).")
            print(f"⚠ ACTION: Dropping table {full_table} to force recreate...")
            
            try:
                # Force drop table from Catalog
                spark.sql(f"DROP TABLE IF EXISTS {full_table}")
                print(f"✓ Dropped table {full_table}")
                
                # Retry write
                df.writeTo(full_table) \
                  .tableProperty("format-version", "2") \
                  .createOrReplace()
                print(f"✓ Successfully recreated and written to {full_table}")
                return
            except Exception as e2:
                print(f"ERROR: Failed to recover table {full_table}: {str(e2)}")
                raise e2
        else:
            raise e



def main():
    """Main ETL process for visit grouping."""
    print(f"\n{'#'*60}")
    print(f"# Job 1: Visit Grouping")
    print(f"# Started at: {datetime.now()}")
    print(f"{'#'*60}\n")
    
    # Load encounters
    encounters_df = load_bronze_encounters()
    
    # Group by visit type
    inpatient_visits = group_inpatient_visits(encounters_df)
    er_visits = group_er_visits(encounters_df)
    outpatient_visits = consolidate_outpatient(encounters_df)
    
    # Union all visits
    all_visits = inpatient_visits.unionByName(er_visits).unionByName(outpatient_visits)
    
    # Generate visit_id
    all_visits = all_visits.withColumn(
        "visit_id", 
        F.monotonically_increasing_id() + 1
    )
    
    # Create visit_id mapping
    visit_mapping = create_visit_id_mapping(all_visits)
    
    # Write to Iceberg
    write_to_iceberg(all_visits.drop("encounter_ids"), "all_visits")
    write_to_iceberg(visit_mapping, "visit_id_mapping")
    
    # Summary
    print(f"\n{'='*60}")
    print("Job 1 Summary")
    print(f"{'='*60}")
    print(f"Total visits created: {all_visits.count():,}")
    print(f"Visit mappings created: {visit_mapping.count():,}")
    print(f"\n✓ Job 1 completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
