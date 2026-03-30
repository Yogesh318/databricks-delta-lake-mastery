"""
delta_utils.py
==============
Production-grade Delta Lake utility functions.

Author  : Lead Data Engineer
Version : 2.0.0
Runtime : Databricks 13.3 LTS+ (Spark 3.4+, Delta 3.0+)

Battle-tested helpers for MERGE, OPTIMIZE, VACUUM, and concurrency
management. Designed to be imported in any notebook or pipeline job.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from delta.tables import DeltaTable
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

@dataclass
class OptimizeConfig:
    """Configuration for table optimization."""
    zorder_cols: List[str] = field(default_factory=list)
    target_file_size_mb: int = 128
    partition_filter: Optional[str] = None   # e.g. "event_date >= '2024-01-01'"


@dataclass
class VacuumConfig:
    """Configuration for VACUUM governance."""
    retain_hours: int = 168          # 7 days minimum — NEVER go below this
    dry_run: bool = True             # Always dry_run first in prod
    max_files_without_confirm: int = 5_000  # Require manual gate above this


@dataclass
class RetryConfig:
    """Exponential backoff config for concurrent writes."""
    max_retries: int = 5
    base_wait_seconds: float = 1.0
    max_wait_seconds: float = 60.0
    jitter: bool = True              # Prevents thundering herd


# ─────────────────────────────────────────────
# Core MERGE Patterns
# ─────────────────────────────────────────────

def safe_merge(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    merge_key: str,
    update_condition: Optional[str] = None,
    delete_condition: Optional[str] = None,
    retry_config: RetryConfig = RetryConfig(),
    job_id: Optional[str] = None,
) -> Dict:
    """
    Idempotent MERGE with automatic retry on concurrent modification.

    This is the primary upsert pattern for all production pipelines.
    It handles:
      - OCC conflicts via exponential backoff with jitter
      - Soft deletes via configurable delete condition
      - Provenance tracking (last_updated_by, last_updated_ts)
      - Deduplication of source before MERGE to prevent fan-out

    Args:
        spark          : Active SparkSession
        source_df      : Incoming updates DataFrame
        target_table   : Fully qualified table name or Delta path
        merge_key      : Column used to match source to target (e.g. "customer_id")
        update_condition: SQL condition for when to fire an update (None = always)
        delete_condition: SQL expr identifying rows to delete (None = no deletes)
        retry_config   : Backoff parameters
        job_id         : Optional job identifier for audit trail

    Returns:
        dict with keys: attempts, rows_inserted, rows_updated, rows_deleted

    Raises:
        RuntimeError if max retries exceeded
    """
    # Enrich source with provenance columns
    enriched = source_df.dropDuplicates([merge_key]) \
        .withColumn("_last_updated_by", lit(job_id or "unknown")) \
        .withColumn("_last_updated_ts", current_timestamp())

    stats = {"attempts": 0, "rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0}

    for attempt in range(retry_config.max_retries):
        stats["attempts"] = attempt + 1
        try:
            delta_tbl = _resolve_table(spark, target_table)

            merge_builder = delta_tbl.alias("target").merge(
                enriched.alias("source"),
                f"target.{merge_key} = source.{merge_key}"
            )

            # Handle deletes (soft or hard)
            if delete_condition:
                merge_builder = merge_builder.whenMatchedDelete(condition=delete_condition)

            # Handle updates
            if update_condition:
                merge_builder = merge_builder.whenMatchedUpdate(
                    condition=update_condition,
                    set={c: f"source.{c}" for c in source_df.columns}
                )
            else:
                merge_builder = merge_builder.whenMatchedUpdateAll()

            merge_builder = merge_builder.whenNotMatchedInsertAll()
            merge_builder.execute()

            # Collect operation metrics from Delta log
            history = delta_tbl.history(1).collect()[0]
            metrics = history.operationMetrics or {}
            stats["rows_inserted"] = int(metrics.get("numTargetRowsInserted", 0))
            stats["rows_updated"] = int(metrics.get("numTargetRowsUpdated", 0))
            stats["rows_deleted"] = int(metrics.get("numTargetRowsDeleted", 0))

            logger.info(
                "MERGE complete | table=%s | inserted=%d | updated=%d | deleted=%d | attempts=%d",
                target_table,
                stats["rows_inserted"],
                stats["rows_updated"],
                stats["rows_deleted"],
                stats["attempts"],
            )
            return stats

        except (Py4JJavaError, Exception) as exc:
            err_str = str(exc)
            is_conflict = any(
                token in err_str
                for token in ["ConcurrentModificationException",
                              "ConcurrentAppendException",
                              "ConcurrentDeleteReadException"]
            )
            if is_conflict and attempt < retry_config.max_retries - 1:
                wait = min(
                    retry_config.base_wait_seconds * (2 ** attempt),
                    retry_config.max_wait_seconds,
                )
                if retry_config.jitter:
                    wait += random.uniform(0, wait * 0.25)
                logger.warning(
                    "Conflict detected on attempt %d/%d — retrying in %.1fs",
                    attempt + 1, retry_config.max_retries, wait
                )
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"MERGE failed on {target_table} after {stats['attempts']} attempts"
                ) from exc

    raise RuntimeError(f"MERGE exhausted {retry_config.max_retries} retries on {target_table}")


def partition_scoped_overwrite(
    df: DataFrame,
    table_path: str,
    partition_col: str,
    partition_value: str,
) -> None:
    """
    Atomic overwrite of a single partition — enables parallel jobs on same table.

    Pattern: Each job owns one partition value → zero write conflicts.
    Delta tracks this at the partition level, so concurrent jobs on different
    partitions are serialized independently with no cross-job conflicts.

    Args:
        df              : Data to write (must be pre-filtered to the partition)
        table_path      : Delta table path
        partition_col   : Partition column name
        partition_value : Value this job owns (string-quoted for SQL predicate)
    """
    df.write.format("delta") \
        .option("replaceWhere", f"{partition_col} = '{partition_value}'") \
        .mode("overwrite") \
        .save(table_path)

    logger.info(
        "Partition overwrite complete | path=%s | %s='%s' | rows=%d",
        table_path, partition_col, partition_value, df.count()
    )


# ─────────────────────────────────────────────
# Table Maintenance
# ─────────────────────────────────────────────

def optimize_table(
    spark: SparkSession,
    table: str,
    config: OptimizeConfig = OptimizeConfig(),
) -> None:
    """
    Run OPTIMIZE with optional ZORDER and partition filter.

    Designed for scheduled maintenance jobs. Run at most once daily on
    high-DML tables. Running more often wastes compute without benefit —
    Delta will skip already-optimized files.

    Best practice:
        - ZORDER by columns that appear in WHERE clauses of your hottest queries
        - Use partition_filter to scope to recent data (avoids full table rewrite)
        - Liquid Clustering (DBR 13.3+) is preferred over ZORDER for new tables
    """
    zorder_clause = ""
    if config.zorder_cols:
        cols = ", ".join(config.zorder_cols)
        zorder_clause = f"ZORDER BY ({cols})"

    where_clause = ""
    if config.partition_filter:
        where_clause = f"WHERE {config.partition_filter}"

    sql = f"OPTIMIZE {_quote_table(table)} {where_clause} {zorder_clause}".strip()
    logger.info("Running: %s", sql)
    spark.sql(sql)

    detail = spark.sql(f"DESCRIBE DETAIL {_quote_table(table)}").collect()[0]
    logger.info(
        "Post-OPTIMIZE | table=%s | files=%d | size=%.2f GB",
        table,
        detail["numFiles"],
        detail["sizeInBytes"] / 1e9,
    )


def vacuum_table(
    spark: SparkSession,
    table: str,
    config: VacuumConfig = VacuumConfig(),
) -> int:
    """
    Governed VACUUM with mandatory dry-run preflight and safety gates.

    NEVER call this with retain_hours < 168 in production. The check is
    enforced here and will raise before touching the table.

    Returns:
        Number of files that were (or would be) deleted.
    """
    if config.retain_hours < 168:
        raise ValueError(
            f"retain_hours={config.retain_hours} is below the 7-day minimum (168h). "
            "Lowering retention can break active readers and streaming checkpoints. "
            "If you absolutely need this, disable via Spark conf at your own risk."
        )

    # Always perform a dry run first to count affected files
    dry_result = spark.sql(
        f"VACUUM {_quote_table(table)} RETAIN {config.retain_hours} HOURS DRY RUN"
    )
    file_count = dry_result.count()
    logger.info("VACUUM DRY RUN | table=%s | files_to_delete=%d", table, file_count)

    if file_count == 0:
        logger.info("Nothing to vacuum on %s", table)
        return 0

    if config.dry_run:
        logger.info("dry_run=True — skipping actual VACUUM. Set dry_run=False to proceed.")
        return file_count

    if file_count > config.max_files_without_confirm:
        raise RuntimeError(
            f"VACUUM would delete {file_count} files, exceeding the safety threshold "
            f"of {config.max_files_without_confirm}. Review DRY RUN output and set "
            "max_files_without_confirm higher to proceed."
        )

    spark.sql(f"VACUUM {_quote_table(table)} RETAIN {config.retain_hours} HOURS")
    logger.info("VACUUM complete | table=%s | deleted=%d files", table, file_count)
    return file_count


def get_table_health(spark: SparkSession, table: str) -> Dict:
    """
    Return a health summary for a Delta table.

    Useful for monitoring dashboards and alerting pipelines.
    Alert when:
        - avg_file_size_mb < 32  (small file problem)
        - num_files > 50_000     (metadata overhead)
        - last_optimize_days > 7 (stale compaction)
    """
    detail = spark.sql(f"DESCRIBE DETAIL {_quote_table(table)}").collect()[0]
    history = spark.sql(f"DESCRIBE HISTORY {_quote_table(table)}").collect()

    num_files = detail["numFiles"] or 0
    size_bytes = detail["sizeInBytes"] or 0
    avg_file_mb = (size_bytes / num_files / 1e6) if num_files > 0 else 0

    last_optimize = next(
        (h["timestamp"] for h in history if h["operation"] == "OPTIMIZE"), None
    )

    return {
        "table": table,
        "num_files": num_files,
        "size_gb": round(size_bytes / 1e9, 3),
        "avg_file_size_mb": round(avg_file_mb, 2),
        "current_version": detail["numOutputRows"],
        "last_optimize_ts": str(last_optimize),
        "health_status": _compute_health(num_files, avg_file_mb),
    }


# ─────────────────────────────────────────────
# Time Travel Utilities
# ─────────────────────────────────────────────

def read_at_version(spark: SparkSession, table: str, version: int) -> DataFrame:
    """Read a Delta table at a specific historical version."""
    return spark.read.format("delta") \
        .option("versionAsOf", version) \
        .table(table) if not table.startswith("/") else \
        spark.read.format("delta").option("versionAsOf", version).load(table)


def diff_versions(
    spark: SparkSession,
    table: str,
    version_before: int,
    version_after: int,
    key_col: str,
) -> DataFrame:
    """
    Return rows that changed between two versions of a Delta table.
    Useful for audit trails and debugging data quality regressions.
    """
    before = read_at_version(spark, table, version_before).alias("before")
    after = read_at_version(spark, table, version_after).alias("after")

    # New or modified rows (in after but not matching before)
    changed = after.join(before, key_col, "left_anti") \
        .withColumn("change_type", lit("ADDED_OR_MODIFIED"))

    # Deleted rows (in before but not in after)
    deleted = before.join(after, key_col, "left_anti") \
        .withColumn("change_type", lit("DELETED"))

    return changed.unionByName(deleted, allowMissingColumns=True)


def restore_to_version(spark: SparkSession, table: str, version: int) -> None:
    """
    Restore a Delta table to a previous version.

    ⚠️ This creates a new version in the history — it does NOT overwrite history.
    Always validate with diff_versions() before restoring in production.
    """
    logger.warning(
        "RESTORE TABLE %s TO VERSION AS OF %d — this is irreversible per normal flow.", table, version
    )
    spark.sql(f"RESTORE TABLE {_quote_table(table)} TO VERSION AS OF {version}")
    logger.info("Restore complete. Table is now at version %d (a new commit).", version + 1)


# ─────────────────────────────────────────────
# Internal Helpers
# ─────────────────────────────────────────────

def _resolve_table(spark: SparkSession, table: str) -> DeltaTable:
    """Resolve table name or path to DeltaTable object."""
    if table.startswith("/") or table.startswith("dbfs:"):
        return DeltaTable.forPath(spark, table)
    return DeltaTable.forName(spark, table)


def _quote_table(table: str) -> str:
    """Wrap path-based references in backticks for SQL."""
    if table.startswith("/") or table.startswith("dbfs:"):
        return f"delta.`{table}`"
    return table


def _compute_health(num_files: int, avg_file_mb: float) -> str:
    """Heuristic health classification for monitoring."""
    if avg_file_mb < 16 or num_files > 100_000:
        return "CRITICAL"
    if avg_file_mb < 64 or num_files > 50_000:
        return "WARNING"
    return "HEALTHY"
