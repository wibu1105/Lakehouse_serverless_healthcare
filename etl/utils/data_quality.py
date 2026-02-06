"""
Data Quality Checks Module
Healthcare Lakehouse - OMOP CDM Pipeline

Provides data quality validation functions for OMOP CDM tables.
Used by ETL jobs and for post-load validation.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Tuple
from dataclasses import dataclass


@dataclass
class DQCheckResult:
    """Result of a data quality check."""
    check_name: str
    table_name: str
    status: str  # PASS, FAIL, WARN
    metric_value: float
    threshold: float
    message: str


class OMOPDataQuality:
    """Data quality checks for OMOP CDM tables."""
    
    def __init__(self, spark: SparkSession, silver_path: str):
        self.spark = spark
        self.silver_path = silver_path
        self.results: List[DQCheckResult] = []
    
    def load_table(self, table_name: str, layer: str = "clinical") -> DataFrame:
        """Load a table from Silver layer."""
        path = f"{self.silver_path}{layer}/{table_name}/"
        return self.spark.read.parquet(path)
    
    # ========================================================================
    # Completeness Checks
    # ========================================================================
    
    def check_table_populated(self, table_name: str, layer: str = "clinical", 
                               min_rows: int = 0) -> DQCheckResult:
        """Check that table has minimum number of rows."""
        df = self.load_table(table_name, layer)
        row_count = df.count()
        
        status = "PASS" if row_count > min_rows else "FAIL"
        return DQCheckResult(
            check_name="table_populated",
            table_name=table_name,
            status=status,
            metric_value=row_count,
            threshold=min_rows,
            message=f"Table has {row_count} rows (min: {min_rows})"
        )
    
    def check_column_completeness(self, table_name: str, column: str, 
                                   layer: str = "clinical",
                                   threshold: float = 0.95) -> DQCheckResult:
        """Check that column has at least threshold% non-null values."""
        df = self.load_table(table_name, layer)
        total = df.count()
        
        if total == 0:
            return DQCheckResult(
                check_name="column_completeness",
                table_name=f"{table_name}.{column}",
                status="FAIL",
                metric_value=0.0,
                threshold=threshold,
                message="Table is empty"
            )
        
        non_null = df.filter(F.col(column).isNotNull()).count()
        completeness = non_null / total
        
        status = "PASS" if completeness >= threshold else "FAIL"
        return DQCheckResult(
            check_name="column_completeness",
            table_name=f"{table_name}.{column}",
            status=status,
            metric_value=completeness,
            threshold=threshold,
            message=f"Completeness: {completeness:.2%} (threshold: {threshold:.2%})"
        )
    
    # ========================================================================
    # Conformance Checks
    # ========================================================================
    
    def check_concept_mapping_rate(self, table_name: str, concept_column: str,
                                    layer: str = "clinical",
                                    threshold: float = 0.80) -> DQCheckResult:
        """Check that concept_id column has acceptable mapping rate (>0)."""
        df = self.load_table(table_name, layer)
        total = df.count()
        
        if total == 0:
            return DQCheckResult(
                check_name="concept_mapping_rate",
                table_name=f"{table_name}.{concept_column}",
                status="FAIL",
                metric_value=0.0,
                threshold=threshold,
                message="Table is empty"
            )
        
        mapped = df.filter(F.col(concept_column) > 0).count()
        mapping_rate = mapped / total
        
        if mapping_rate >= threshold:
            status = "PASS"
        elif mapping_rate >= threshold * 0.8:
            status = "WARN"
        else:
            status = "FAIL"
        
        return DQCheckResult(
            check_name="concept_mapping_rate",
            table_name=f"{table_name}.{concept_column}",
            status=status,
            metric_value=mapping_rate,
            threshold=threshold,
            message=f"Mapping rate: {mapping_rate:.2%} (threshold: {threshold:.2%})"
        )
    
    def check_date_range(self, table_name: str, date_column: str,
                          layer: str = "clinical",
                          min_date: str = "1900-01-01",
                          max_date: str = "2100-01-01") -> DQCheckResult:
        """Check that dates are within valid range."""
        df = self.load_table(table_name, layer)
        
        invalid_count = df.filter(
            (F.col(date_column) < F.lit(min_date)) |
            (F.col(date_column) > F.lit(max_date))
        ).count()
        
        total = df.count()
        valid_rate = 1 - (invalid_count / total) if total > 0 else 0
        
        status = "PASS" if invalid_count == 0 else "FAIL"
        return DQCheckResult(
            check_name="date_range",
            table_name=f"{table_name}.{date_column}",
            status=status,
            metric_value=valid_rate,
            threshold=1.0,
            message=f"{invalid_count} dates outside valid range"
        )
    
    # ========================================================================
    # Plausibility Checks
    # ========================================================================
    
    def check_start_before_end(self, table_name: str, 
                                start_col: str, end_col: str,
                                layer: str = "clinical") -> DQCheckResult:
        """Check that start dates are before end dates."""
        df = self.load_table(table_name, layer)
        
        # Only check where both dates are non-null
        df_with_both = df.filter(
            F.col(start_col).isNotNull() & F.col(end_col).isNotNull()
        )
        total = df_with_both.count()
        
        if total == 0:
            return DQCheckResult(
                check_name="start_before_end",
                table_name=f"{table_name}",
                status="PASS",
                metric_value=1.0,
                threshold=1.0,
                message="No records with both dates to check"
            )
        
        invalid = df_with_both.filter(F.col(start_col) > F.col(end_col)).count()
        valid_rate = 1 - (invalid / total)
        
        status = "PASS" if invalid == 0 else "FAIL" if invalid > total * 0.01 else "WARN"
        return DQCheckResult(
            check_name="start_before_end",
            table_name=f"{table_name}",
            status=status,
            metric_value=valid_rate,
            threshold=0.99,
            message=f"{invalid} records have start > end date"
        )
    
    def check_foreign_key(self, table_name: str, fk_column: str,
                           ref_table: str, ref_column: str,
                           layer: str = "clinical",
                           ref_layer: str = "clinical") -> DQCheckResult:
        """Check that foreign keys reference existing records."""
        df = self.load_table(table_name, layer)
        ref_df = self.load_table(ref_table, ref_layer)
        
        # Get non-null FK values
        fk_df = df.filter(F.col(fk_column).isNotNull()).select(fk_column).distinct()
        fk_count = fk_df.count()
        
        if fk_count == 0:
            return DQCheckResult(
                check_name="foreign_key",
                table_name=f"{table_name}.{fk_column} -> {ref_table}.{ref_column}",
                status="PASS",
                metric_value=1.0,
                threshold=1.0,
                message="No foreign key values to check"
            )
        
        # Check which FKs exist in reference table
        ref_ids = ref_df.select(ref_column).distinct()
        orphans = fk_df.join(ref_ids, fk_df[fk_column] == ref_ids[ref_column], "left_anti")
        orphan_count = orphans.count()
        
        valid_rate = 1 - (orphan_count / fk_count)
        
        status = "PASS" if orphan_count == 0 else "FAIL" if valid_rate < 0.95 else "WARN"
        return DQCheckResult(
            check_name="foreign_key",
            table_name=f"{table_name}.{fk_column} -> {ref_table}.{ref_column}",
            status=status,
            metric_value=valid_rate,
            threshold=0.95,
            message=f"{orphan_count} orphan foreign keys"
        )
    
    # ========================================================================
    # Run All Checks
    # ========================================================================
    
    def run_person_checks(self) -> List[DQCheckResult]:
        """Run all data quality checks for person table."""
        checks = [
            self.check_table_populated("person", min_rows=1),
            self.check_column_completeness("person", "person_id"),
            self.check_column_completeness("person", "gender_concept_id"),
            self.check_column_completeness("person", "year_of_birth"),
            self.check_concept_mapping_rate("person", "gender_concept_id"),
            self.check_concept_mapping_rate("person", "race_concept_id", threshold=0.50),
            self.check_date_range("person", "year_of_birth", min_date="1900", max_date="2100"),
        ]
        return checks
    
    def run_condition_checks(self) -> List[DQCheckResult]:
        """Run all data quality checks for condition_occurrence table."""
        checks = [
            self.check_table_populated("condition_occurrence"),
            self.check_column_completeness("condition_occurrence", "person_id"),
            self.check_concept_mapping_rate("condition_occurrence", "condition_concept_id"),
            self.check_start_before_end("condition_occurrence", 
                                         "condition_start_date", "condition_end_date"),
            self.check_foreign_key("condition_occurrence", "person_id", "person", "person_id"),
        ]
        return checks
    
    def run_drug_exposure_checks(self) -> List[DQCheckResult]:
        """Run all data quality checks for drug_exposure table."""
        checks = [
            self.check_table_populated("drug_exposure"),
            self.check_column_completeness("drug_exposure", "person_id"),
            self.check_concept_mapping_rate("drug_exposure", "drug_concept_id"),
            self.check_start_before_end("drug_exposure", 
                                         "drug_exposure_start_date", "drug_exposure_end_date"),
            self.check_foreign_key("drug_exposure", "person_id", "person", "person_id"),
        ]
        return checks
    
    def run_all_checks(self) -> List[DQCheckResult]:
        """Run all data quality checks."""
        all_checks = []
        all_checks.extend(self.run_person_checks())
        all_checks.extend(self.run_condition_checks())
        all_checks.extend(self.run_drug_exposure_checks())
        
        self.results = all_checks
        return all_checks
    
    def print_summary(self):
        """Print summary of all check results."""
        print("\n" + "="*80)
        print("DATA QUALITY CHECK SUMMARY")
        print("="*80)
        
        pass_count = sum(1 for r in self.results if r.status == "PASS")
        warn_count = sum(1 for r in self.results if r.status == "WARN")
        fail_count = sum(1 for r in self.results if r.status == "FAIL")
        
        print(f"\nPASS: {pass_count} | WARN: {warn_count} | FAIL: {fail_count}")
        print("-"*80)
        
        for result in self.results:
            status_symbol = "✓" if result.status == "PASS" else "⚠" if result.status == "WARN" else "✗"
            print(f"{status_symbol} [{result.status}] {result.check_name} - {result.table_name}")
            print(f"   {result.message}")
        
        print("="*80)
