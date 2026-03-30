"""
monitoring.py
=============
Data quality, pipeline observability, and alerting utilities.

Author  : Lead Data Engineer
Version : 2.0.0
Runtime : Databricks 13.3 LTS+

Production philosophy: every pipeline should emit metrics, every table
should have quality checks, and every anomaly should have an owner.
Blind pipelines are the #1 cause of undetected data corruption.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, isnan, isnull, lit,
    max as spark_max, mean, min as spark_min,
    stddev, sum as spark_sum, when,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Data Quality Checks
# ─────────────────────────────────────────────

class Severity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass
class QualityCheck:
    name: str
    column: Optional[str]
    severity: Severity
    condition: str         # Human-readable description
    passed: bool = False
    actual_value: Optional[float] = None
    threshold: Optional[float] = None
    message: str = ""


@dataclass
class QualityReport:
    table: str
    run_timestamp: str
    total_rows: int
    checks: List[QualityCheck] = field(default_factory=list)
    overall_passed: bool = True

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str, indent=2)

    def failed_checks(self) -> List[QualityCheck]:
        return [c for c in self.checks if not c.passed]

    def critical_failures(self) -> List[QualityCheck]:
        return [c for c in self.checks if not c.passed and c.severity == Severity.CRITICAL]


class DataQualityRunner:
    """
    Fluent interface for building and running data quality checks on a DataFrame.

    Usage:
        report = (
            DataQualityRunner(spark, df, "prod.customers")
            .check_not_null("customer_id", severity=Severity.CRITICAL)
            .check_not_null("email", severity=Severity.WARNING)
            .check_uniqueness("customer_id")
            .check_value_range("age", min_val=0, max_val=150)
            .check_regex("email", r"^[\\w.-]+@[\\w.-]+\\.\\w+$")
            .check_referential_integrity("tier", ["bronze", "silver", "gold", "platinum"])
            .run()
        )
        if report.critical_failures():
            raise DataQualityError(report)
    """

    def __init__(self, spark: SparkSession, df: DataFrame, table_name: str):
        self.spark = spark
        self.df = df
        self.table_name = table_name
        self._checks: List[Callable] = []

    def check_not_null(self, column: str, severity: Severity = Severity.CRITICAL,
                       threshold_pct: float = 0.0) -> "DataQualityRunner":
        """Fail if null rate exceeds threshold_pct (0.0 = zero nulls allowed)."""
        def _check(df: DataFrame, total: int) -> QualityCheck:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_pct = null_count / total if total > 0 else 0
            passed = null_pct <= threshold_pct
            return QualityCheck(
                name=f"not_null_{column}",
                column=column,
                severity=severity,
                condition=f"NULL rate <= {threshold_pct:.1%}",
                passed=passed,
                actual_value=round(null_pct, 6),
                threshold=threshold_pct,
                message=f"{null_count:,} nulls ({null_pct:.2%}) in {column}" if not passed else "OK",
            )
        self._checks.append(_check)
        return self

    def check_uniqueness(self, column: str, severity: Severity = Severity.CRITICAL) -> "DataQualityRunner":
        """Fail if column has duplicate values."""
        def _check(df: DataFrame, total: int) -> QualityCheck:
            distinct = df.select(countDistinct(col(column))).collect()[0][0]
            passed = distinct == total
            dupe_count = total - distinct
            return QualityCheck(
                name=f"unique_{column}",
                column=column,
                severity=severity,
                condition="No duplicate values",
                passed=passed,
                actual_value=float(dupe_count),
                threshold=0.0,
                message=f"{dupe_count:,} duplicate values in {column}" if not passed else "OK",
            )
        self._checks.append(_check)
        return self

    def check_value_range(self, column: str, min_val: float, max_val: float,
                          severity: Severity = Severity.WARNING) -> "DataQualityRunner":
        """Fail if any value falls outside [min_val, max_val]."""
        def _check(df: DataFrame, total: int) -> QualityCheck:
            out_of_range = df.filter(
                (col(column) < min_val) | (col(column) > max_val)
            ).count()
            passed = out_of_range == 0
            return QualityCheck(
                name=f"range_{column}",
                column=column,
                severity=severity,
                condition=f"{min_val} <= {column} <= {max_val}",
                passed=passed,
                actual_value=float(out_of_range),
                threshold=0.0,
                message=f"{out_of_range:,} values outside [{min_val}, {max_val}]" if not passed else "OK",
            )
        self._checks.append(_check)
        return self

    def check_referential_integrity(self, column: str, allowed_values: List,
                                     severity: Severity = Severity.CRITICAL) -> "DataQualityRunner":
        """Fail if column contains values not in allowed_values set."""
        def _check(df: DataFrame, total: int) -> QualityCheck:
            invalid = df.filter(~col(column).isin(allowed_values)).count()
            passed = invalid == 0
            return QualityCheck(
                name=f"ref_integrity_{column}",
                column=column,
                severity=severity,
                condition=f"{column} IN {allowed_values}",
                passed=passed,
                actual_value=float(invalid),
                threshold=0.0,
                message=f"{invalid:,} rows with invalid {column} values" if not passed else "OK",
            )
        self._checks.append(_check)
        return self

    def check_row_count(self, min_rows: int, max_rows: Optional[int] = None,
                        severity: Severity = Severity.CRITICAL) -> "DataQualityRunner":
        """Fail if row count is outside expected range."""
        def _check(df: DataFrame, total: int) -> QualityCheck:
            passed = total >= min_rows and (max_rows is None or total <= max_rows)
            threshold_str = f">= {min_rows}" + (f" and <= {max_rows}" if max_rows else "")
            return QualityCheck(
                name="row_count",
                column=None,
                severity=severity,
                condition=f"Row count {threshold_str}",
                passed=passed,
                actual_value=float(total),
                threshold=float(min_rows),
                message=f"Got {total:,} rows, expected {threshold_str}" if not passed else "OK",
            )
        self._checks.append(_check)
        return self

    def run(self) -> QualityReport:
        """Execute all registered checks and return a report."""
        total = self.df.count()
        results: List[QualityCheck] = []

        for check_fn in self._checks:
            result = check_fn(self.df, total)
            results.append(result)
            log_level = logging.ERROR if not result.passed and result.severity == Severity.CRITICAL \
                else logging.WARNING if not result.passed else logging.INFO
            logger.log(log_level, "[DQ] %s | %s | value=%s | %s",
                       result.severity.value, result.name, result.actual_value, result.message)

        overall_passed = all(
            c.passed or c.severity != Severity.CRITICAL
            for c in results
        )

        return QualityReport(
            table=self.table_name,
            run_timestamp=datetime.utcnow().isoformat(),
            total_rows=total,
            checks=results,
            overall_passed=overall_passed,
        )


# ─────────────────────────────────────────────
# Pipeline Metrics
# ─────────────────────────────────────────────

@dataclass
class PipelineMetrics:
    """Emitted at the end of every pipeline stage."""
    stage: str
    table: str
    run_id: str
    start_ts: str
    end_ts: str
    rows_read: int = 0
    rows_written: int = 0
    rows_skipped: int = 0
    bytes_written: int = 0
    duration_seconds: float = 0.0
    status: str = "UNKNOWN"
    error_message: Optional[str] = None

    def log(self) -> None:
        logger.info(
            "PIPELINE METRICS | stage=%s | table=%s | rows_in=%d | rows_out=%d | "
            "duration=%.1fs | status=%s",
            self.stage, self.table, self.rows_read, self.rows_written,
            self.duration_seconds, self.status,
        )

    def to_dict(self) -> Dict:
        return asdict(self)


def compute_profile(df: DataFrame, sample_fraction: float = 1.0) -> DataFrame:
    """
    Compute a statistical profile of a DataFrame for data validation.

    Returns a summary DataFrame with null counts, distinct counts,
    min/max/mean/stddev for each column. Useful for catching data drift
    between pipeline runs.

    Args:
        df              : DataFrame to profile
        sample_fraction : Use sampling for large datasets (0.1 = 10% sample)
    """
    if sample_fraction < 1.0:
        df = df.sample(sample_fraction)

    aggs = []
    for col_name in df.columns:
        aggs.extend([
            count(when(col(col_name).isNull(), col_name)).alias(f"{col_name}__nulls"),
            countDistinct(col(col_name)).alias(f"{col_name}__distinct"),
        ])
        # Numeric stats for numeric columns
        try:
            df.select(col(col_name).cast("double"))  # Test if numeric
            aggs.extend([
                spark_min(col(col_name).cast("double")).alias(f"{col_name}__min"),
                spark_max(col(col_name).cast("double")).alias(f"{col_name}__max"),
                mean(col(col_name).cast("double")).alias(f"{col_name}__mean"),
                stddev(col(col_name).cast("double")).alias(f"{col_name}__stddev"),
            ])
        except Exception:
            pass

    return df.agg(*aggs)


def write_metrics_to_delta(
    spark: SparkSession,
    metrics: PipelineMetrics,
    metrics_table: str = "ops.pipeline_metrics",
) -> None:
    """
    Persist pipeline metrics to a Delta audit table.

    Use this to build operational dashboards, SLA monitoring, and
    runbook-driven incident detection in Databricks SQL.
    """
    row = spark.createDataFrame([metrics.to_dict()])
    row.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .saveAsTable(metrics_table)


# ─────────────────────────────────────────────
# Table Health Monitoring
# ─────────────────────────────────────────────

def monitor_table_health(spark: SparkSession, tables: List[str]) -> DataFrame:
    """
    Scan a list of Delta tables and return a health report DataFrame.

    Designed to be called by a scheduled Databricks job and results
    written to a monitoring table for alerting.

    Alert thresholds (customize per your SLA):
        - CRITICAL: avg_file_mb < 16 or num_files > 100k
        - WARNING:  avg_file_mb < 64 or num_files > 50k or last_optimize_days > 14
    """
    from src.utils.delta_utils import get_table_health

    rows = []
    for table in tables:
        try:
            health = get_table_health(spark, table)
            rows.append(health)
        except Exception as exc:
            logger.error("Health check failed for %s: %s", table, exc)
            rows.append({
                "table": table, "health_status": "ERROR",
                "num_files": -1, "size_gb": -1, "avg_file_size_mb": -1,
            })

    return spark.createDataFrame(rows)


# ─────────────────────────────────────────────
# Custom Exceptions
# ─────────────────────────────────────────────

class DataQualityError(Exception):
    """Raised when critical data quality checks fail."""
    def __init__(self, report: QualityReport):
        self.report = report
        failures = "\n".join(f"  - {c.name}: {c.message}" for c in report.critical_failures())
        super().__init__(
            f"Critical DQ failures on {report.table} ({report.run_timestamp}):\n{failures}"
        )
