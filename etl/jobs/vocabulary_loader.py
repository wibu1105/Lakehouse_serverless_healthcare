"""
Vocabulary Loader ETL Job
Healthcare Lakehouse - OMOP CDM Pipeline (AWS Glue 4.0 + Iceberg)

This Glue job loads OMOP vocabulary tables from CSV files:
- Loads standard vocabulary CSV files from S3
- Creates Iceberg tables in Glue Data Catalog
- Supports incremental updates using Iceberg MERGE
"""

import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Initialize Glue context with Iceberg support
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'vocabulary_path',
    'silver_path',
    'database_name'
])

job.init(args['JOB_NAME'], args)

VOCABULARY_PATH = args['vocabulary_path']
SILVER_PATH = args['silver_path']
DATABASE_NAME = args['database_name']

# Configure Iceberg with Glue Catalog
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", SILVER_PATH)
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Vocabulary table configurations
VOCAB_TABLES = {
    'concept': {
        'primary_key': 'concept_id',
        'schema': StructType([
            StructField("concept_id", LongType(), False),
            StructField("concept_name", StringType(), True),
            StructField("domain_id", StringType(), True),
            StructField("vocabulary_id", StringType(), True),
            StructField("concept_class_id", StringType(), True),
            StructField("standard_concept", StringType(), True),
            StructField("concept_code", StringType(), True),
            StructField("valid_start_date", DateType(), True),
            StructField("valid_end_date", DateType(), True),
            StructField("invalid_reason", StringType(), True)
        ])
    },
    'concept_relationship': {
        'primary_key': ['concept_id_1', 'concept_id_2', 'relationship_id'],
        'schema': StructType([
            StructField("concept_id_1", LongType(), False),
            StructField("concept_id_2", LongType(), False),
            StructField("relationship_id", StringType(), False),
            StructField("valid_start_date", DateType(), True),
            StructField("valid_end_date", DateType(), True),
            StructField("invalid_reason", StringType(), True)
        ])
    },
    'concept_ancestor': {
        'primary_key': ['ancestor_concept_id', 'descendant_concept_id'],
        'schema': StructType([
            StructField("ancestor_concept_id", LongType(), False),
            StructField("descendant_concept_id", LongType(), False),
            StructField("min_levels_of_separation", IntegerType(), True),
            StructField("max_levels_of_separation", IntegerType(), True)
        ])
    },
    'concept_synonym': {
        'primary_key': ['concept_id', 'concept_synonym_name'],
        'schema': StructType([
            StructField("concept_id", LongType(), False),
            StructField("concept_synonym_name", StringType(), False),
            StructField("language_concept_id", LongType(), True)
        ])
    },
    'vocabulary': {
        'primary_key': 'vocabulary_id',
        'schema': StructType([
            StructField("vocabulary_id", StringType(), False),
            StructField("vocabulary_name", StringType(), True),
            StructField("vocabulary_reference", StringType(), True),
            StructField("vocabulary_version", StringType(), True),
            StructField("vocabulary_concept_id", LongType(), True)
        ])
    },
    'domain': {
        'primary_key': 'domain_id',
        'schema': StructType([
            StructField("domain_id", StringType(), False),
            StructField("domain_name", StringType(), True),
            StructField("domain_concept_id", LongType(), True)
        ])
    },
    'concept_class': {
        'primary_key': 'concept_class_id',
        'schema': StructType([
            StructField("concept_class_id", StringType(), False),
            StructField("concept_class_name", StringType(), True),
            StructField("concept_class_concept_id", LongType(), True)
        ])
    },
    'relationship': {
        'primary_key': 'relationship_id',
        'schema': StructType([
            StructField("relationship_id", StringType(), False),
            StructField("relationship_name", StringType(), True),
            StructField("is_hierarchical", StringType(), True),
            StructField("defines_ancestry", StringType(), True),
            StructField("reverse_relationship_id", StringType(), True),
            StructField("relationship_concept_id", LongType(), True)
        ])
    },
    'drug_strength': {
        'primary_key': 'drug_concept_id',
        'schema': StructType([
            StructField("drug_concept_id", LongType(), False),
            StructField("ingredient_concept_id", LongType(), True),
            StructField("amount_value", DoubleType(), True),
            StructField("amount_unit_concept_id", LongType(), True),
            StructField("numerator_value", DoubleType(), True),
            StructField("numerator_unit_concept_id", LongType(), True),
            StructField("denominator_value", DoubleType(), True),
            StructField("denominator_unit_concept_id", LongType(), True),
            StructField("box_size", IntegerType(), True),
            StructField("valid_start_date", DateType(), True),
            StructField("valid_end_date", DateType(), True),
            StructField("invalid_reason", StringType(), True)
        ])
    }
}


def load_vocabulary_csv(table_name, schema):
    """Load vocabulary CSV file with specified schema."""
    file_path = f"{VOCABULARY_PATH}{table_name.upper()}.csv"
    
    try:
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("delimiter", "\t") \
            .option("quote", '"') \
            .option("escape", '"') \
            .schema(schema) \
            .load(file_path)
        
        print(f"Loaded {df.count()} rows from {table_name.upper()}.csv")
        return df
    except Exception as e:
        print(f"ERROR loading {table_name}: {str(e)}")
        return None


def write_vocab_to_iceberg(df, table_name):
    """Write vocabulary DataFrame to Iceberg table using Glue Catalog."""
    full_table_name = f"glue_catalog.{DATABASE_NAME}.{table_name}"
    df.createOrReplaceTempView(f"temp_{table_name}")
    
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        table_exists = True
    except:
        table_exists = False
    
    location = f"{SILVER_PATH}vocabulary/{table_name}/"
    
    if not table_exists:
        create_sql = f"""
        CREATE TABLE {full_table_name}
        USING iceberg
        LOCATION '{location}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'snappy'
        )
        AS SELECT * FROM temp_{table_name}
        """
        spark.sql(create_sql)
        print(f"Created Iceberg table {full_table_name} with {df.count()} rows")
    else:
        # For vocabulary, we do a full overwrite
        spark.sql(f"INSERT OVERWRITE {full_table_name} SELECT * FROM temp_{table_name}")
        print(f"Overwrote Iceberg table {full_table_name} with {df.count()} rows")


def create_vocabulary_mapping_views():
    """Create source-to-standard and source-to-source mapping views."""
    # Source to Standard mapping
    s2s_sql = f"""
    CREATE OR REPLACE VIEW glue_catalog.{DATABASE_NAME}.source_to_standard_vocab_map AS
    SELECT
        c.concept_code AS source_code,
        c.concept_id AS source_concept_id,
        c.concept_name AS source_code_description,
        c.vocabulary_id AS source_vocabulary_id,
        c.domain_id AS source_domain_id,
        c.concept_class_id AS source_concept_class_id,
        c.valid_start_date AS source_valid_start_date,
        c.valid_end_date AS source_valid_end_date,
        c.invalid_reason AS source_invalid_reason,
        c2.concept_id AS target_concept_id,
        c2.concept_name AS target_concept_name,
        c2.vocabulary_id AS target_vocabulary_id,
        c2.domain_id AS target_domain_id,
        c2.concept_class_id AS target_concept_class_id,
        c2.invalid_reason AS target_invalid_reason,
        c2.standard_concept AS target_standard_concept
    FROM glue_catalog.{DATABASE_NAME}.concept c
    JOIN glue_catalog.{DATABASE_NAME}.concept_relationship cr 
        ON c.concept_id = cr.concept_id_1
        AND cr.relationship_id = 'Maps to'
        AND cr.invalid_reason IS NULL
    JOIN glue_catalog.{DATABASE_NAME}.concept c2 
        ON cr.concept_id_2 = c2.concept_id
        AND c2.invalid_reason IS NULL
        AND c2.standard_concept = 'S'
    """
    
    try:
        spark.sql(s2s_sql)
        print("Created source_to_standard_vocab_map view")
    except Exception as e:
        print(f"WARNING: Could not create mapping view: {e}")


def main():
    """Main ETL process for vocabulary loading."""
    print(f"\n{'#'*60}")
    print(f"# Vocabulary Loader (Glue + Iceberg)")
    print(f"# Started at: {datetime.now()}")
    print(f"# Database: {DATABASE_NAME}")
    print(f"# Vocabulary path: {VOCABULARY_PATH}")
    print(f"{'#'*60}")
    
    loaded_tables = []
    failed_tables = []
    
    for table_name, config in VOCAB_TABLES.items():
        print(f"\nProcessing {table_name}...")
        
        df = load_vocabulary_csv(table_name, config['schema'])
        
        if df is not None and df.count() > 0:
            write_vocab_to_iceberg(df, table_name)
            loaded_tables.append(table_name)
        else:
            failed_tables.append(table_name)
    
    # Create mapping views if core tables loaded
    if 'concept' in loaded_tables and 'concept_relationship' in loaded_tables:
        print("\nCreating vocabulary mapping views...")
        create_vocabulary_mapping_views()
    
    # Summary
    print(f"\n{'='*60}")
    print(f"VOCABULARY LOADING SUMMARY")
    print(f"{'='*60}")
    print(f"Successfully loaded: {len(loaded_tables)} tables")
    for t in loaded_tables:
        print(f"  ✓ {t}")
    if failed_tables:
        print(f"\nFailed/Skipped: {len(failed_tables)} tables")
        for t in failed_tables:
            print(f"  ✗ {t}")
    
    print(f"\nCompleted at: {datetime.now()}")


if __name__ == "__main__":
    main()
    job.commit()
