"""
Idempotency Utilities for ETL Jobs
Healthcare Lakehouse - OMOP CDM Pipeline

Provides utilities for idempotent ETL execution:
- Source file checksum tracking
- ETL run logging
- Change detection for incremental processing
"""

import hashlib
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType
)


class IdempotencyManager:
    """
    Manages idempotent ETL job execution.
    
    Tracks source file checksums and job run metadata to:
    - Skip processing if source data hasn't changed
    - Record successful runs for audit/debugging
    - Enable incremental processing
    """
    
    def __init__(self, spark: SparkSession, log_path: str, database_name: str):
        """
        Initialize IdempotencyManager.
        
        Args:
            spark: Active SparkSession
            log_path: S3 path for ETL run logs (e.g., s3://bucket/etl_logs/)
            database_name: Glue database name
        """
        self.spark = spark
        self.log_path = log_path
        self.database_name = database_name
        self._init_log_table()
    
    def _init_log_table(self):
        """Initialize ETL run log table if needed."""
        self.log_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("job_name", StringType(), False),
            StructField("run_timestamp", TimestampType(), False),
            StructField("status", StringType(), False),
            StructField("source_checksum", StringType(), True),
            StructField("input_row_count", LongType(), True),
            StructField("output_row_count", LongType(), True),
            StructField("duration_seconds", LongType(), True),
            StructField("error_message", StringType(), True),
            StructField("metadata", StringType(), True),  # JSON
        ])
    
    def generate_run_id(self, job_name: str) -> str:
        """Generate unique run ID."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{job_name}_{timestamp}"
    
    def compute_dataframe_checksum(self, df: DataFrame, sample_size: int = 10000) -> str:
        """
        Compute a checksum for a DataFrame.
        
        For large DataFrames, samples rows to compute a representative checksum.
        Suitable for change detection, not cryptographic purposes.
        
        Args:
            df: DataFrame to compute checksum for
            sample_size: Number of rows to sample for large DataFrames
        """
        row_count = df.count()
        
        if row_count == 0:
            return "empty_df"
        
        # For small DataFrames, use all rows; otherwise sample
        if row_count <= sample_size:
            sample_df = df
        else:
            fraction = sample_size / row_count
            sample_df = df.sample(fraction=fraction, seed=42)
        
        # Convert to string representation and hash
        rows_str = str(sample_df.collect())
        checksum = hashlib.md5(rows_str.encode()).hexdigest()
        
        return f"{checksum}_{row_count}"
    
    def compute_path_checksum(self, path: str) -> str:
        """
        Compute checksum based on file metadata (path listing).
        
        Uses file names and sizes rather than content for efficiency.
        
        Args:
            path: S3 path to compute checksum for
        """
        try:
            # Read file listing
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jvm.java.net.URI(path), hadoop_conf
            )
            
            path_obj = self.spark._jvm.org.apache.hadoop.fs.Path(path)
            
            if not fs.exists(path_obj):
                return "path_not_found"
            
            # Get file statuses
            statuses = fs.listStatus(path_obj)
            file_info = []
            
            for status in statuses:
                file_info.append({
                    'name': status.getPath().getName(),
                    'size': status.getLen(),
                    'modified': status.getModificationTime()
                })
            
            # Sort and hash
            file_info.sort(key=lambda x: x['name'])
            info_str = json.dumps(file_info)
            checksum = hashlib.md5(info_str.encode()).hexdigest()
            
            return checksum
            
        except Exception as e:
            # If we can't compute checksum, return a unique value to force processing
            return f"error_{datetime.now().timestamp()}"
    
    def get_last_successful_run(self, job_name: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata from last successful run of a job.
        
        Args:
            job_name: Name of the ETL job
            
        Returns:
            Dict with run metadata, or None if no successful runs found
        """
        try:
            log_df = self.spark.read.parquet(f"{self.log_path}etl_run_log/")
            
            last_run = log_df.filter(
                (F.col("job_name") == job_name) & 
                (F.col("status") == "SUCCESS")
            ).orderBy(F.col("run_timestamp").desc()).first()
            
            if last_run:
                return last_run.asDict()
            return None
            
        except Exception:
            # Log table doesn't exist yet
            return None
    
    def should_process(self, job_name: str, current_checksum: str) -> bool:
        """
        Determine if job should run based on source changes.
        
        Args:
            job_name: Name of the ETL job
            current_checksum: Checksum of current source data
            
        Returns:
            True if job should run, False if source unchanged
        """
        last_run = self.get_last_successful_run(job_name)
        
        if last_run is None:
            # No previous run, should process
            return True
        
        last_checksum = last_run.get('source_checksum')
        
        if last_checksum is None or last_checksum != current_checksum:
            # Checksum changed or missing, should process
            return True
        
        # Source unchanged, skip processing
        print(f"⏭ Skipping {job_name}: source unchanged (checksum: {current_checksum[:8]}...)")
        return False
    
    def record_run_start(self, job_name: str, source_checksum: str = None) -> str:
        """
        Record the start of an ETL run.
        
        Args:
            job_name: Name of the ETL job
            source_checksum: Optional source checksum
            
        Returns:
            Run ID for this execution
        """
        run_id = self.generate_run_id(job_name)
        
        # Store run context (will be updated on completion)
        self._current_run = {
            'run_id': run_id,
            'job_name': job_name,
            'start_time': datetime.now(),
            'source_checksum': source_checksum
        }
        
        print(f"📋 Starting run: {run_id}")
        return run_id
    
    def record_run_complete(
        self, 
        status: str,
        input_row_count: int = None,
        output_row_count: int = None,
        error_message: str = None,
        metadata: Dict = None
    ):
        """
        Record completion of an ETL run.
        
        Args:
            status: 'SUCCESS' or 'FAILED'
            input_row_count: Number of input rows processed
            output_row_count: Number of output rows written
            error_message: Error message if failed
            metadata: Additional metadata dict
        """
        if not hasattr(self, '_current_run'):
            print("⚠ No active run to record")
            return
        
        run = self._current_run
        end_time = datetime.now()
        duration = int((end_time - run['start_time']).total_seconds())
        
        run_record = [(
            run['run_id'],
            run['job_name'],
            run['start_time'],
            status,
            run.get('source_checksum'),
            input_row_count,
            output_row_count,
            duration,
            error_message,
            json.dumps(metadata) if metadata else None
        )]
        
        log_df = self.spark.createDataFrame(run_record, self.log_schema)
        
        # Append to log table
        log_df.write \
            .mode("append") \
            .partitionBy("job_name") \
            .parquet(f"{self.log_path}etl_run_log/")
        
        status_emoji = "✓" if status == "SUCCESS" else "✗"
        print(f"{status_emoji} Run {run['run_id']} completed: {status} ({duration}s)")
        
        del self._current_run


def add_batch_tracking_columns(df: DataFrame, batch_id: str) -> DataFrame:
    """
    Add batch tracking columns to a DataFrame.
    
    Args:
        df: Input DataFrame
        batch_id: Unique batch identifier
        
    Returns:
        DataFrame with added columns
    """
    return df.withColumn("_etl_batch_id", F.lit(batch_id)) \
             .withColumn("_etl_loaded_at", F.current_timestamp())


def get_batch_id() -> str:
    """Generate a unique batch ID for this ETL run."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")
