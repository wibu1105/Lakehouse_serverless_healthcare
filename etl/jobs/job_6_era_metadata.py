"""
Job 6: Era & Metadata
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0 + Iceberg)

This Glue job calculates era tables and CDM metadata:
- Condition era (≤30 day gap)
- Drug era (≤30 day gap)
- CDM source metadata

Phase 2: ETL Pipeline (Daily/Weekly Batch)
Input: condition_occurrence, drug_exposure from Silver
Output: condition_era, drug_era, cdm_source (Iceberg)
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
    'database_name',
    'gap_days'
])

job.init(args['JOB_NAME'], args)

# Configuration
SILVER_PATH = args['silver_path']
DATABASE_NAME = args['database_name']
GAP_DAYS = int(args.get('gap_days', '30'))

# Configure Iceberg
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", SILVER_PATH)
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")


def load_silver_tables():
    """Load required tables from Silver layer."""
    condition_occurrence = spark.read.format("iceberg").load(
        f"glue_catalog.{DATABASE_NAME}.condition_occurrence"
    )
    
    drug_exposure = spark.read.format("iceberg").load(
        f"glue_catalog.{DATABASE_NAME}.drug_exposure"
    )
    
    print(f"✓ Loaded {condition_occurrence.count():,} condition occurrences")
    print(f"✓ Loaded {drug_exposure.count():,} drug exposures")
    
    return condition_occurrence, drug_exposure


def calculate_condition_era(condition_occurrence_df, gap_days=30):
    """Calculate condition_era with gap handling."""
    
    # Detect column names (handle both OMOP standard and simplified schema)
    cols = condition_occurrence_df.columns
    if "condition_start_date" in cols:
        start_col = "condition_start_date"
        end_col = "condition_end_date" if "condition_end_date" in cols else "condition_start_date"
    else:
        start_col = "start_date" if "start_date" in cols else "START"
        end_col = "end_date" if "end_date" in cols else start_col
    
    concept_col = "condition_concept_id" if "condition_concept_id" in cols else "concept_id"
    
    print(f"  Using columns: {start_col}, {end_col}, {concept_col}")
    
    # First rename columns to standardized names, then use Window
    # This avoids issues with dynamic column names in Window specs
    conditions = condition_occurrence_df.select(
        "person_id",
        F.col(concept_col).alias("condition_concept_id"),
        F.col(start_col).alias("start_date"),
        F.coalesce(
            F.col(end_col),
            F.col(start_col)
        ).alias("end_date")
    )
    
    # Window for detecting gaps (now using standardized column names)
    person_condition_window = Window.partitionBy(
        "person_id", "condition_concept_id"
    ).orderBy("start_date")
    
    # Identify era boundaries based on gap
    conditions = conditions.withColumn(
        "prev_end", F.lag("end_date").over(person_condition_window)
    ).withColumn(
        "gap_days", F.datediff(F.col("start_date"), F.col("prev_end"))
    ).withColumn(
        "new_era", F.when(
            (F.col("gap_days").isNull()) | (F.col("gap_days") > gap_days), 1
        ).otherwise(0)
    ).withColumn(
        "era_group", F.sum("new_era").over(person_condition_window)
    )
    
    # Aggregate into eras
    condition_era = conditions.groupBy(
        "person_id", "condition_concept_id", "era_group"
    ).agg(
        F.min("start_date").alias("condition_era_start_date"),
        F.max("end_date").alias("condition_era_end_date"),
        F.count("*").alias("condition_occurrence_count")
    )
    
    # Add era_id
    condition_era = condition_era.select(
        F.monotonically_increasing_id().alias("condition_era_id"),
        F.col("person_id"),
        F.col("condition_concept_id"),
        F.col("condition_era_start_date"),
        F.col("condition_era_end_date"),
        F.col("condition_occurrence_count").cast("integer")
    )
    
    print(f"✓ Created {condition_era.count():,} condition eras (gap: {gap_days} days)")
    return condition_era


def calculate_drug_era(drug_exposure_df, gap_days=30):
    """Calculate drug_era with gap handling."""
    
    # Detect column names (handle both OMOP standard and simplified schema)
    cols = drug_exposure_df.columns
    if "drug_exposure_start_date" in cols:
        start_col = "drug_exposure_start_date"
        end_col = "drug_exposure_end_date" if "drug_exposure_end_date" in cols else start_col
    else:
        start_col = "start_date" if "start_date" in cols else "START"
        end_col = "end_date" if "end_date" in cols else start_col
    
    concept_col = "drug_concept_id" if "drug_concept_id" in cols else "concept_id"
    
    print(f"  Using columns: {start_col}, {end_col}, {concept_col}")
    
    # First rename columns to standardized names, then use Window
    # This avoids issues with dynamic column names in Window specs
    drugs = drug_exposure_df.select(
        "person_id",
        F.col(concept_col).alias("drug_concept_id"),
        F.col(start_col).alias("start_date"),
        F.coalesce(
            F.col(end_col),
            F.col(start_col)
        ).alias("end_date")
    )
    
    # Window for detecting gaps (now using standardized column names)
    person_drug_window = Window.partitionBy(
        "person_id", "drug_concept_id"
    ).orderBy("start_date")
    
    # Identify era boundaries based on gap
    drugs = drugs.withColumn(
        "prev_end", F.lag("end_date").over(person_drug_window)
    ).withColumn(
        "gap_days", F.datediff(F.col("start_date"), F.col("prev_end"))
    ).withColumn(
        "new_era", F.when(
            (F.col("gap_days").isNull()) | (F.col("gap_days") > gap_days), 1
        ).otherwise(0)
    ).withColumn(
        "era_group", F.sum("new_era").over(person_drug_window)
    )
    
    # Aggregate into eras
    drug_era = drugs.groupBy(
        "person_id", "drug_concept_id", "era_group"
    ).agg(
        F.min("start_date").alias("drug_era_start_date"),
        F.max("end_date").alias("drug_era_end_date"),
        F.count("*").alias("drug_exposure_count")
    )
    
    # Calculate gap days (simplified - actual implementation would track total gap)
    drug_era = drug_era.select(
        F.monotonically_increasing_id().alias("drug_era_id"),
        F.col("person_id"),
        F.col("drug_concept_id"),
        F.col("drug_era_start_date"),
        F.col("drug_era_end_date"),
        F.col("drug_exposure_count").cast("integer"),
        F.lit(0).cast("integer").alias("gap_days")
    )
    
    print(f"✓ Created {drug_era.count():,} drug eras (gap: {gap_days} days)")
    return drug_era


def create_cdm_source():
    """Create CDM source metadata table."""
    
    cdm_source_data = [(
        "OMOP CDM Lakehouse",                    # cdm_source_name
        "OMOP CDM",                              # cdm_source_abbreviation
        "Healthcare Lakehouse AWS",              # cdm_holder
        "https://github.com/OHDSI/CommonDataModel",  # source_description
        None,                                     # source_documentation_reference
        "OMOP CDM v5.4",                         # cdm_etl_reference
        datetime.now().date(),                   # source_release_date
        datetime.now().date(),                   # cdm_release_date
        "CDM v5.4",                              # cdm_version
        None,                                     # cdm_version_concept_id
        "OMOP Standardized Vocabularies v5.0"   # vocabulary_version
    )]
    
    cdm_source_schema = StructType([
        StructField("cdm_source_name", StringType(), True),
        StructField("cdm_source_abbreviation", StringType(), True),
        StructField("cdm_holder", StringType(), True),
        StructField("source_description", StringType(), True),
        StructField("source_documentation_reference", StringType(), True),
        StructField("cdm_etl_reference", StringType(), True),
        StructField("source_release_date", DateType(), True),
        StructField("cdm_release_date", DateType(), True),
        StructField("cdm_version", StringType(), True),
        StructField("cdm_version_concept_id", LongType(), True),
        StructField("vocabulary_version", StringType(), True)
    ])
    
    cdm_source = spark.createDataFrame(cdm_source_data, cdm_source_schema)
    
    print("✓ Created CDM source metadata")
    return cdm_source


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
    """Main ETL process for era calculation and metadata."""
    print(f"\n{'#'*60}")
    print(f"# Job 6: Era & Metadata")
    print(f"# Started at: {datetime.now()}")
    print(f"# Gap Days: {GAP_DAYS}")
    print(f"{'#'*60}\n")
    
    # Load Silver tables
    condition_occurrence, drug_exposure = load_silver_tables()
    
    # Calculate eras
    condition_era = calculate_condition_era(condition_occurrence, GAP_DAYS)
    drug_era = calculate_drug_era(drug_exposure, GAP_DAYS)
    
    # Create CDM source metadata
    cdm_source = create_cdm_source()
    
    # Write to Iceberg
    write_to_iceberg(condition_era, "condition_era")
    write_to_iceberg(drug_era, "drug_era")
    write_to_iceberg(cdm_source, "cdm_source")
    
    # Summary
    print(f"\n{'='*60}")
    print("Job 6 Summary")
    print(f"{'='*60}")
    print(f"Condition eras: {condition_era.count():,}")
    print(f"Drug eras: {drug_era.count():,}")
    print(f"CDM source metadata: Created")
    print(f"\n✓ Job 6 completed at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
