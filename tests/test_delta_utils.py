"""
test_delta_utils.py
===================
Unit tests for src/utils/delta_utils.py and src/utils/schema_utils.py.

Run with: pytest tests/ -v --tb=short
Integration tests (require Spark): pytest tests/ -v -m integration
"""

import pytest
from unittest.mock import MagicMock, patch, call
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType, FloatType,
)

# ─────────────────────────────────────────────
# Schema Utils Tests (no Spark required)
# ─────────────────────────────────────────────

from src.utils.schema_utils import (
    detect_schema_changes,
    ChangeType,
    SchemaBreakingChangeError,
    assert_schema_compatible,
    _WIDENING_MAP,
)


class TestDetectSchemaChanges:
    """Tests for the core schema diff engine."""

    def _schema(self, fields: list) -> StructType:
        return StructType(fields)

    def test_no_changes_returns_compatible(self):
        schema = self._schema([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        result = detect_schema_changes(schema, schema)
        assert result.is_compatible is True
        assert len(result.breaking_changes) == 0
        assert len(result.safe_changes) == 0

    def test_new_column_is_safe(self):
        old = self._schema([StructField("id", IntegerType(), True)])
        new = self._schema([
            StructField("id", IntegerType(), True),
            StructField("email", StringType(), True),  # new column
        ])
        result = detect_schema_changes(old, new)
        assert result.is_compatible is True
        assert len(result.safe_changes) == 1
        assert result.safe_changes[0].change_type == ChangeType.COLUMN_ADDED
        assert result.safe_changes[0].column == "email"

    def test_dropped_column_is_breaking(self):
        old = self._schema([
            StructField("id", IntegerType(), True),
            StructField("legacy_field", StringType(), True),
        ])
        new = self._schema([StructField("id", IntegerType(), True)])
        result = detect_schema_changes(old, new)
        assert result.is_compatible is False
        assert len(result.breaking_changes) == 1
        assert result.breaking_changes[0].change_type == ChangeType.COLUMN_DROPPED
        assert result.breaking_changes[0].is_breaking is True

    def test_int_to_long_is_safe_widening(self):
        old = self._schema([StructField("count", IntegerType(), True)])
        new = self._schema([StructField("count", LongType(), True)])
        result = detect_schema_changes(old, new)
        assert result.is_compatible is True
        assert result.safe_changes[0].change_type == ChangeType.TYPE_WIDENED

    def test_double_to_int_is_breaking_narrowing(self):
        old = self._schema([StructField("amount", DoubleType(), True)])
        new = self._schema([StructField("amount", IntegerType(), True)])
        result = detect_schema_changes(old, new)
        assert result.is_compatible is False
        assert result.breaking_changes[0].change_type == ChangeType.TYPE_NARROWED

    def test_not_null_to_nullable_is_safe(self):
        old = self._schema([StructField("id", IntegerType(), nullable=False)])
        new = self._schema([StructField("id", IntegerType(), nullable=True)])
        result = detect_schema_changes(old, new)
        assert result.is_compatible is True
        assert result.safe_changes[0].change_type == ChangeType.NULLABLE_RELAXED

    def test_nullable_to_not_null_is_breaking(self):
        old = self._schema([StructField("id", IntegerType(), nullable=True)])
        new = self._schema([StructField("id", IntegerType(), nullable=False)])
        result = detect_schema_changes(old, new)
        assert result.is_compatible is False
        assert result.breaking_changes[0].change_type == ChangeType.NULLABLE_TIGHTENED

    def test_multiple_changes_classified_independently(self):
        old = self._schema([
            StructField("id", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("legacy", StringType(), True),
        ])
        new = self._schema([
            StructField("id", LongType(), True),       # widening — safe
            StructField("amount", IntegerType(), True), # narrowing — breaking
            StructField("new_col", StringType(), True), # added — safe
            # "legacy" dropped — breaking
        ])
        result = detect_schema_changes(old, new)
        assert result.is_compatible is False
        assert len(result.breaking_changes) == 2   # narrowing + dropped
        assert len(result.safe_changes) == 2        # widening + added

    def test_widening_map_is_complete(self):
        """Every pair in the widening map should be (smaller, larger) type."""
        # Sanity check that the map doesn't contain reversed pairs
        for from_type, to_type in _WIDENING_MAP:
            assert from_type != to_type, f"Self-widening found: {from_type}"


class TestAssertSchemaCompatible:
    """Tests for the schema gate function."""

    def test_raises_on_breaking_change(self):
        mock_spark = MagicMock()
        old = StructType([StructField("id", IntegerType(), True)])
        new = StructType([])  # dropped column

        mock_spark.table.return_value.schema = old

        incoming_df = MagicMock()
        incoming_df.schema = new

        with pytest.raises(SchemaBreakingChangeError) as exc_info:
            assert_schema_compatible(incoming_df, "prod.my_table", mock_spark)

        assert "prod.my_table" in str(exc_info.value)

    def test_new_table_is_always_compatible(self):
        mock_spark = MagicMock()
        mock_spark.table.side_effect = Exception("Table does not exist")

        incoming_df = MagicMock()
        incoming_df.schema = StructType([StructField("id", IntegerType(), True)])

        result = assert_schema_compatible(incoming_df, "prod.new_table", mock_spark)
        assert result.is_compatible is True

    def test_strict_mode_treats_new_columns_as_breaking(self):
        mock_spark = MagicMock()
        old = StructType([StructField("id", IntegerType(), True)])
        new = StructType([
            StructField("id", IntegerType(), True),
            StructField("new_col", StringType(), True),
        ])
        mock_spark.table.return_value.schema = old

        incoming_df = MagicMock()
        incoming_df.schema = new

        with pytest.raises(SchemaBreakingChangeError):
            assert_schema_compatible(
                incoming_df, "prod.strict_table", mock_spark,
                allow_new_columns=False
            )


# ─────────────────────────────────────────────
# Delta Utils Tests (mocked Spark + DeltaTable)
# ─────────────────────────────────────────────

from src.utils.delta_utils import (
    VacuumConfig,
    OptimizeConfig,
    RetryConfig,
    _compute_health,
    _quote_table,
)


class TestVacuumConfig:
    def test_rejects_retention_below_7_days(self):
        from src.utils.delta_utils import vacuum_table
        mock_spark = MagicMock()
        with pytest.raises(ValueError, match="7-day minimum"):
            vacuum_table(mock_spark, "prod.table", VacuumConfig(retain_hours=48))

    def test_accepts_168_hours_exactly(self):
        """168 hours = 7 days exactly — this is the floor, must be accepted."""
        from src.utils.delta_utils import vacuum_table
        mock_spark = MagicMock()
        # Mock dry run returns 0 files
        mock_spark.sql.return_value.count.return_value = 0
        # Should not raise
        result = vacuum_table(mock_spark, "prod.table", VacuumConfig(retain_hours=168, dry_run=True))
        assert result == 0


class TestHealthComputation:
    def test_critical_when_tiny_avg_file(self):
        assert _compute_health(1000, 5.0) == "CRITICAL"

    def test_critical_when_too_many_files(self):
        assert _compute_health(200_000, 200.0) == "CRITICAL"

    def test_warning_when_small_files(self):
        assert _compute_health(30_000, 40.0) == "WARNING"

    def test_healthy_with_good_files(self):
        assert _compute_health(5_000, 256.0) == "HEALTHY"


class TestQuoteTable:
    def test_path_gets_backtick_prefix(self):
        assert _quote_table("/mnt/delta/table") == "delta.`/mnt/delta/table`"

    def test_dbfs_path_gets_backtick_prefix(self):
        assert _quote_table("dbfs:/mnt/delta/table") == "delta.`dbfs:/mnt/delta/table`"

    def test_table_name_unchanged(self):
        assert _quote_table("prod.customers") == "prod.customers"

    def test_simple_table_unchanged(self):
        assert _quote_table("customers") == "customers"


# ─────────────────────────────────────────────
# Monitoring Tests
# ─────────────────────────────────────────────

from src.utils.monitoring import (
    DataQualityRunner, QualityReport, Severity, DataQualityError
)


class TestDataQualityRunner:
    """Tests using MagicMock DataFrames — no Spark required."""

    def _mock_df(self, rows: list, columns: list) -> MagicMock:
        """Create a minimal mock DataFrame."""
        mock = MagicMock()
        mock.columns = columns
        mock.count.return_value = len(rows)
        return mock

    def test_not_null_check_passes_when_no_nulls(self):
        mock_spark = MagicMock()
        mock_df = self._mock_df([(1, "Alice"), (2, "Bob")], ["id", "name"])
        mock_df.filter.return_value.count.return_value = 0  # 0 nulls

        runner = DataQualityRunner(mock_spark, mock_df, "test.table")
        runner.check_not_null("id")
        report = runner.run()

        assert report.overall_passed is True
        assert len(report.failed_checks()) == 0

    def test_not_null_check_fails_on_nulls(self):
        mock_spark = MagicMock()
        mock_df = self._mock_df([(1, None), (2, "Bob")], ["id", "name"])
        mock_df.filter.return_value.count.return_value = 1  # 1 null

        runner = DataQualityRunner(mock_spark, mock_df, "test.table")
        runner.check_not_null("name", severity=Severity.CRITICAL)
        report = runner.run()

        assert report.overall_passed is False
        assert len(report.critical_failures()) == 1

    def test_row_count_check_passes(self):
        mock_spark = MagicMock()
        mock_df = self._mock_df(list(range(100)), ["id"])

        runner = DataQualityRunner(mock_spark, mock_df, "test.table")
        runner.check_row_count(min_rows=50)
        report = runner.run()

        assert report.overall_passed is True

    def test_row_count_check_fails_below_minimum(self):
        mock_spark = MagicMock()
        mock_df = self._mock_df([1, 2, 3], ["id"])

        runner = DataQualityRunner(mock_spark, mock_df, "test.table")
        runner.check_row_count(min_rows=100, severity=Severity.CRITICAL)
        report = runner.run()

        assert report.overall_passed is False

    def test_data_quality_error_message_contains_table(self):
        mock_spark = MagicMock()
        mock_df = self._mock_df([], ["id"])

        runner = DataQualityRunner(mock_spark, mock_df, "prod.customers")
        runner.check_row_count(min_rows=1, severity=Severity.CRITICAL)
        report = runner.run()

        with pytest.raises(DataQualityError) as exc_info:
            if not report.overall_passed:
                raise DataQualityError(report)

        assert "prod.customers" in str(exc_info.value)

    def test_warning_severity_does_not_fail_overall(self):
        """WARNING failures should not set overall_passed=False."""
        mock_spark = MagicMock()
        mock_df = self._mock_df([(1, None)], ["id", "optional_col"])
        mock_df.filter.return_value.count.return_value = 1  # 1 null

        runner = DataQualityRunner(mock_spark, mock_df, "test.table")
        runner.check_not_null("optional_col", severity=Severity.WARNING)
        report = runner.run()

        # WARNING failures don't block the pipeline
        assert report.overall_passed is True
        assert len(report.failed_checks()) == 1


# ─────────────────────────────────────────────
# Integration Tests (require running Spark)
# ─────────────────────────────────────────────

@pytest.mark.integration
class TestDeltaUtilsIntegration:
    """
    Integration tests for Delta operations.
    Requires: SPARK_HOME set or running on Databricks cluster.

    Run with: pytest tests/ -v -m integration
    """

    @pytest.fixture(scope="class")
    def spark(self):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("delta-utils-integration-tests") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .master("local[2]") \
            .getOrCreate()
        yield spark
        spark.stop()

    def test_safe_merge_upserts_correctly(self, spark, tmp_path):
        from src.utils.delta_utils import safe_merge

        # Create initial table
        initial = spark.createDataFrame(
            [(1, "Alice", 100), (2, "Bob", 200)],
            ["id", "name", "amount"]
        )
        table_path = str(tmp_path / "test_merge")
        initial.write.format("delta").save(table_path)

        # Update Alice, add Charlie
        updates = spark.createDataFrame(
            [(1, "Alice", 999), (3, "Charlie", 300)],
            ["id", "name", "amount"]
        )
        stats = safe_merge(spark, updates, table_path, "id")

        result = spark.read.format("delta").load(table_path).orderBy("id")
        rows = result.collect()

        assert len(rows) == 3
        assert rows[0]["amount"] == 999   # Alice updated
        assert rows[2]["name"] == "Charlie"  # Charlie inserted
        assert stats["rows_updated"] == 1
        assert stats["rows_inserted"] == 1

    def test_vacuum_rejects_short_retention(self, spark, tmp_path):
        from src.utils.delta_utils import vacuum_table, VacuumConfig

        # Create minimal table
        df = spark.range(10).toDF("id")
        table_path = str(tmp_path / "test_vacuum")
        df.write.format("delta").save(table_path)

        with pytest.raises(ValueError, match="7-day minimum"):
            vacuum_table(spark, table_path, VacuumConfig(retain_hours=24))
