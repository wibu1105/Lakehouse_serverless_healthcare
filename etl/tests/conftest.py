"""
Pytest fixtures for OMOP CDM ETL tests.
Provides SparkSession, sample data, and temporary paths.
"""

import pytest
import tempfile
import shutil
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    TimestampType, DoubleType, IntegerType, LongType
)


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("OMOP_CDM_Tests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=" + tempfile.mkdtemp()) \
        .getOrCreate()
    
    yield spark
    spark.stop()


@pytest.fixture
def temp_paths():
    """Create temporary directories for input/output."""
    base_dir = tempfile.mkdtemp()
    paths = {
        'raw': f"{base_dir}/raw/",
        'bronze': f"{base_dir}/bronze/",
        'silver': f"{base_dir}/silver/"
    }
    for path in paths.values():
        import os
        os.makedirs(path, exist_ok=True)
    
    yield paths
    shutil.rmtree(base_dir, ignore_errors=True)


@pytest.fixture
def sample_patients(spark):
    """Sample patients data matching Synthea schema."""
    schema = StructType([
        StructField("Id", StringType(), False),
        StructField("BIRTHDATE", DateType(), True),
        StructField("DEATHDATE", DateType(), True),
        StructField("GENDER", StringType(), True),
        StructField("RACE", StringType(), True),
        StructField("ETHNICITY", StringType(), True),
        StructField("ADDRESS", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("COUNTY", StringType(), True),
        StructField("ZIP", StringType(), True),
        StructField("LAT", DoubleType(), True),
        StructField("LON", DoubleType(), True),
    ])
    
    data = [
        ("patient-001", date(1980, 5, 15), None, "M", "white", "nonhispanic", 
         "123 Main St", "Boston", "MA", "Suffolk", "02101", 42.36, -71.05),
        ("patient-002", date(1992, 8, 22), None, "F", "black", "nonhispanic",
         "456 Oak Ave", "Boston", "MA", "Suffolk", "02102", 42.35, -71.06),
        ("patient-003", date(1975, 3, 10), date(2023, 1, 5), "M", "asian", "hispanic",
         "789 Pine Rd", "Cambridge", "MA", "Middlesex", "02139", 42.37, -71.08),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_encounters(spark):
    """Sample encounters data matching Synthea schema."""
    schema = StructType([
        StructField("Id", StringType(), False),
        StructField("START", TimestampType(), True),
        StructField("STOP", TimestampType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTERCLASS", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
    ])
    
    data = [
        # Inpatient encounters for patient-001 (should merge - less than 24h gap)
        ("enc-001", datetime(2023, 6, 1, 8, 0), datetime(2023, 6, 3, 16, 0), 
         "patient-001", "inpatient", "185347001", "Inpatient stay"),
        ("enc-002", datetime(2023, 6, 4, 10, 0), datetime(2023, 6, 5, 12, 0),
         "patient-001", "inpatient", "185347001", "Inpatient stay"),
        # Inpatient with gap (should NOT merge - more than 24h)
        ("enc-003", datetime(2023, 6, 10, 8, 0), datetime(2023, 6, 12, 10, 0),
         "patient-001", "inpatient", "185347001", "Inpatient stay"),
        # ER visits for patient-002
        ("enc-004", datetime(2023, 7, 15, 14, 30), datetime(2023, 7, 15, 18, 0),
         "patient-002", "emergency", "50849002", "ER visit"),
        # Outpatient visits for patient-003
        ("enc-005", datetime(2023, 8, 1, 9, 0), datetime(2023, 8, 1, 9, 30),
         "patient-003", "outpatient", "185349003", "Outpatient visit"),
        ("enc-006", datetime(2023, 8, 1, 14, 0), datetime(2023, 8, 1, 14, 45),
         "patient-003", "outpatient", "185349003", "Outpatient visit"),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_conditions(spark):
    """Sample conditions data matching Synthea schema."""
    schema = StructType([
        StructField("START", TimestampType(), True),
        StructField("STOP", TimestampType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
    ])
    
    data = [
        (datetime(2023, 6, 1), datetime(2023, 6, 5), "patient-001", "enc-001", 
         "38341003", "Hypertensive disorder"),
        (datetime(2023, 6, 1), None, "patient-001", "enc-001",
         "44054006", "Type 2 diabetes mellitus"),
        (datetime(2023, 7, 15), datetime(2023, 7, 15), "patient-002", "enc-004",
         "195662009", "Acute viral pharyngitis"),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_medications(spark):
    """Sample medications data matching Synthea schema."""
    schema = StructType([
        StructField("START", TimestampType(), True),
        StructField("STOP", TimestampType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("DISPENSES", IntegerType(), True),
    ])
    
    data = [
        (datetime(2023, 6, 1), datetime(2023, 6, 30), "patient-001", "enc-001",
         "316049", "Metformin", 30),
        (datetime(2023, 6, 1), datetime(2023, 12, 31), "patient-001", "enc-001",
         "314076", "Lisinopril", 180),
        (datetime(2023, 7, 15), datetime(2023, 7, 22), "patient-002", "enc-004",
         "1049529", "Acetaminophen", 7),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_observations(spark):
    """Sample observations data matching Synthea schema."""
    schema = StructType([
        StructField("DATE", TimestampType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("VALUE", StringType(), True),
        StructField("UNITS", StringType(), True),
        StructField("TYPE", StringType(), True),
    ])
    
    data = [
        (datetime(2023, 6, 1), "patient-001", "enc-001", "8480-6",
         "Systolic Blood Pressure", "140", "mm[Hg]", "numeric"),
        (datetime(2023, 6, 1), "patient-001", "enc-001", "8462-4", 
         "Diastolic Blood Pressure", "90", "mm[Hg]", "numeric"),
        (datetime(2023, 6, 1), "patient-001", "enc-001", "72166-2",
         "Tobacco smoking status", "Never smoker", None, "text"),
    ]
    
    return spark.createDataFrame(data, schema)


# Helper functions for tests
def assert_columns_exist(df, expected_columns):
    """Assert that all expected columns exist in DataFrame."""
    missing = set(expected_columns) - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


def assert_no_nulls(df, columns):
    """Assert that specified columns have no null values."""
    from pyspark.sql import functions as F
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        assert null_count == 0, f"Column {col} has {null_count} null values"
