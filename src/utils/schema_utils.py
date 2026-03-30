"""
schema_utils.py
===============
Schema evolution, enforcement, and contract management for Delta tables.

Author  : Lead Data Engineer
Version : 2.0.0
Runtime : Databricks 13.3 LTS+ (Spark 3.4+, Delta 3.0+)

Production rule: schema changes should be PLANNED, not discovered at 3am.
This module makes schema drift visible before it breaks pipelines.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DataType, StructField, StructType

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Schema Change Classification
# ─────────────────────────────────────────────

class ChangeType(Enum):
    COLUMN_ADDED = "column_added"
    COLUMN_DROPPED = "column_dropped"
    TYPE_WIDENED = "type_widened"     # safe: int → long, float → double
    TYPE_NARROWED = "type_narrowed"   # unsafe: double → int (data loss)
    TYPE_INCOMPATIBLE = "type_incompatible"  # unsafe: string → int
    NULLABLE_RELAXED = "nullable_relaxed"    # not_null → nullable (safe)
    NULLABLE_TIGHTENED = "nullable_tightened" # nullable → not_null (unsafe)


@dataclass
class SchemaChange:
    column: str
    change_type: ChangeType
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    is_breaking: bool = False
    recommended_action: str = ""


@dataclass
class SchemaCompatibilityResult:
    is_compatible: bool
    breaking_changes: List[SchemaChange] = field(default_factory=list)
    safe_changes: List[SchemaChange] = field(default_factory=list)
    summary: str = ""


# ─────────────────────────────────────────────
# Widening type pairs (safe to auto-evolve)
# ─────────────────────────────────────────────
_WIDENING_MAP = {
    ("ByteType", "ShortType"),
    ("ByteType", "IntegerType"),
    ("ByteType", "LongType"),
    ("ByteType", "FloatType"),
    ("ByteType", "DoubleType"),
    ("ShortType", "IntegerType"),
    ("ShortType", "LongType"),
    ("ShortType", "FloatType"),
    ("ShortType", "DoubleType"),
    ("IntegerType", "LongType"),
    ("IntegerType", "FloatType"),
    ("IntegerType", "DoubleType"),
    ("FloatType", "DoubleType"),
    ("DecimalType", "DecimalType"),  # precision/scale increase only
}


# ─────────────────────────────────────────────
# Core Schema Analysis
# ─────────────────────────────────────────────

def detect_schema_changes(
    current_schema: StructType,
    incoming_schema: StructType,
) -> SchemaCompatibilityResult:
    """
    Compare two StructTypes and classify all differences.

    This is the single source of truth for schema compatibility decisions.
    Call this BEFORE any write to a managed Delta table.

    Returns a SchemaCompatibilityResult with all changes classified
    into breaking vs safe, and recommended actions for each.
    """
    current_fields: Dict[str, StructField] = {f.name: f for f in current_schema.fields}
    incoming_fields: Dict[str, StructField] = {f.name: f for f in incoming_schema.fields}

    breaking: List[SchemaChange] = []
    safe: List[SchemaChange] = []

    current_names: Set[str] = set(current_fields)
    incoming_names: Set[str] = set(incoming_fields)

    # Columns added in incoming (generally safe)
    for col in incoming_names - current_names:
        f = incoming_fields[col]
        change = SchemaChange(
            column=col,
            change_type=ChangeType.COLUMN_ADDED,
            new_value=str(f.dataType),
            is_breaking=False,
            recommended_action="Add via ALTER TABLE or use mergeSchema=true",
        )
        safe.append(change)

    # Columns dropped from incoming (always breaking for downstream consumers)
    for col in current_names - incoming_names:
        change = SchemaChange(
            column=col,
            change_type=ChangeType.COLUMN_DROPPED,
            old_value=str(current_fields[col].dataType),
            is_breaking=True,
            recommended_action=(
                "Version the table contract. Add a compatibility view that "
                "exposes the dropped column as NULL with the original type."
            ),
        )
        breaking.append(change)

    # Columns present in both — check type & nullability changes
    for col in current_names & incoming_names:
        cur = current_fields[col]
        inc = incoming_fields[col]
        _classify_type_change(col, cur, inc, breaking, safe)
        _classify_nullability_change(col, cur, inc, breaking, safe)

    is_compatible = len(breaking) == 0
    summary_parts = []
    if safe:
        summary_parts.append(f"{len(safe)} safe change(s): {[c.column for c in safe]}")
    if breaking:
        summary_parts.append(f"{len(breaking)} BREAKING change(s): {[c.column for c in breaking]}")

    return SchemaCompatibilityResult(
        is_compatible=is_compatible,
        breaking_changes=breaking,
        safe_changes=safe,
        summary=" | ".join(summary_parts) if summary_parts else "No schema changes detected",
    )


def assert_schema_compatible(
    incoming_df: DataFrame,
    target_table: str,
    spark: SparkSession,
    allow_new_columns: bool = True,
) -> SchemaCompatibilityResult:
    """
    Gate function: raise if incoming data has breaking schema changes.

    Use at the START of every pipeline stage that writes to a managed table.
    This is your early-warning system — catch schema drift in Bronze before
    it corrupts Silver or Gold.

    Args:
        incoming_df      : DataFrame about to be written
        target_table     : Fully qualified table name
        spark            : SparkSession
        allow_new_columns: If False, treat new columns as breaking (strict mode)

    Raises:
        SchemaBreakingChangeError if breaking changes detected
    """
    try:
        current_schema = spark.table(target_table).schema
    except Exception:
        logger.info("Table %s does not exist yet — no schema to compare.", target_table)
        return SchemaCompatibilityResult(is_compatible=True, summary="New table")

    result = detect_schema_changes(current_schema, incoming_df.schema)

    if not allow_new_columns and result.safe_changes:
        new_cols = [c for c in result.safe_changes if c.change_type == ChangeType.COLUMN_ADDED]
        if new_cols:
            result.breaking_changes.extend(new_cols)
            result.safe_changes = [c for c in result.safe_changes if c not in new_cols]
            result.is_compatible = False

    if not result.is_compatible:
        for change in result.breaking_changes:
            logger.error(
                "BREAKING schema change | column=%s | type=%s | action=%s",
                change.column, change.change_type.value, change.recommended_action
            )
        raise SchemaBreakingChangeError(
            f"Schema breaking changes detected for {target_table}:\n" +
            "\n".join(f"  - {c.column}: {c.change_type.value}" for c in result.breaking_changes)
        )

    if result.safe_changes:
        for change in result.safe_changes:
            logger.info(
                "Safe schema change | column=%s | type=%s",
                change.column, change.change_type.value
            )

    return result


def evolve_schema(
    spark: SparkSession,
    table: str,
    changes: SchemaCompatibilityResult,
) -> None:
    """
    Apply safe schema changes to a Delta table via ALTER TABLE.

    Preferred over mergeSchema=true because it's explicit, auditable,
    and does not silently add columns on every write.
    """
    for change in changes.safe_changes:
        if change.change_type == ChangeType.COLUMN_ADDED:
            # Infer a safe SQL type string
            sql = f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS `{change.column}` STRING"
            logger.info("Evolving schema: %s", sql)
            spark.sql(sql)

        elif change.change_type == ChangeType.NULLABLE_RELAXED:
            logger.info(
                "Column %s relaxed to nullable — no DDL needed, Delta handles automatically.",
                change.column
            )

    logger.info("Schema evolution complete on %s", table)


def enable_column_mapping(spark: SparkSession, table: str) -> None:
    """
    Enable column mapping on an existing Delta table.

    Required before you can:
        - Rename columns (ALTER TABLE ... RENAME COLUMN)
        - Drop columns (ALTER TABLE ... DROP COLUMN)

    Note: This is a one-way operation. Once enabled, the table requires
    reader version 2 and writer version 5.
    """
    spark.sql(f"""
        ALTER TABLE {table} SET TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.minReaderVersion'   = '2',
            'delta.minWriterVersion'   = '5'
        )
    """)
    logger.info("Column mapping enabled on %s", table)


def get_schema_evolution_history(spark: SparkSession, table: str) -> DataFrame:
    """
    Return a DataFrame of all schema changes in the table's history.
    Useful for audit trails and understanding schema drift over time.
    """
    return spark.sql(f"""
        SELECT
            version,
            timestamp,
            operationParameters.schema AS schema_snapshot,
            operationParameters.predicate
        FROM (DESCRIBE HISTORY {table})
        WHERE operationParameters.schema IS NOT NULL
        ORDER BY version DESC
    """)


# ─────────────────────────────────────────────
# Auto Loader Schema Configuration
# ─────────────────────────────────────────────

def build_autoloader_stream(
    spark: SparkSession,
    source_path: str,
    schema_location: str,
    file_format: str = "json",
    evolution_mode: str = "addNewColumns",
) -> DataFrame:
    """
    Configure an Auto Loader stream with best-practice schema handling.

    evolution_mode options:
        - "addNewColumns"  : safe default — new columns added, no failures
        - "rescue"         : unexpected columns go to _rescued_data column
        - "failOnNewColumns": strict mode — new columns raise an error
        - "none"           : no evolution — unexpected columns dropped

    The schema_location persists inferred schemas across job restarts,
    preventing re-inference costs on every run.
    """
    return spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", file_format) \
        .option("cloudFiles.schemaLocation", schema_location) \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaEvolutionMode", evolution_mode) \
        .option("cloudFiles.includeExistingFiles", "true") \
        .load(source_path)


# ─────────────────────────────────────────────
# Custom Exceptions
# ─────────────────────────────────────────────

class SchemaBreakingChangeError(Exception):
    """Raised when a schema change would break downstream consumers."""


# ─────────────────────────────────────────────
# Internal Helpers
# ─────────────────────────────────────────────

def _classify_type_change(
    col: str,
    cur: StructField,
    inc: StructField,
    breaking: List[SchemaChange],
    safe: List[SchemaChange],
) -> None:
    cur_type = type(cur.dataType).__name__
    inc_type = type(inc.dataType).__name__

    if cur_type == inc_type:
        return  # No type change

    if (cur_type, inc_type) in _WIDENING_MAP:
        safe.append(SchemaChange(
            column=col,
            change_type=ChangeType.TYPE_WIDENED,
            old_value=cur_type,
            new_value=inc_type,
            is_breaking=False,
            recommended_action="Widening type change — safe to auto-evolve.",
        ))
    else:
        # Check if it's a narrowing or truly incompatible
        is_narrowing = (inc_type, cur_type) in _WIDENING_MAP
        change_type = ChangeType.TYPE_NARROWED if is_narrowing else ChangeType.TYPE_INCOMPATIBLE
        breaking.append(SchemaChange(
            column=col,
            change_type=change_type,
            old_value=cur_type,
            new_value=inc_type,
            is_breaking=True,
            recommended_action=(
                f"Cast {col} back to {cur_type} in your transform, or version the table."
            ),
        ))


def _classify_nullability_change(
    col: str,
    cur: StructField,
    inc: StructField,
    breaking: List[SchemaChange],
    safe: List[SchemaChange],
) -> None:
    if cur.nullable == inc.nullable:
        return

    if not cur.nullable and inc.nullable:
        # NOT NULL → nullable: safe (relaxed constraint)
        safe.append(SchemaChange(
            column=col,
            change_type=ChangeType.NULLABLE_RELAXED,
            old_value="NOT NULL",
            new_value="NULLABLE",
            is_breaking=False,
            recommended_action="Nullability relaxed — monitor for unexpected NULLs downstream.",
        ))
    else:
        # nullable → NOT NULL: breaking (source may have NULLs)
        breaking.append(SchemaChange(
            column=col,
            change_type=ChangeType.NULLABLE_TIGHTENED,
            old_value="NULLABLE",
            new_value="NOT NULL",
            is_breaking=True,
            recommended_action=(
                f"Tightening nullability on {col} will fail if source contains NULLs. "
                "Add a NOT NULL constraint after validating there are no NULLs in the data."
            ),
        ))
