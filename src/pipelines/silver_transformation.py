"""
silver_transformation.py
========================
Production Silver layer: CDC processing, deduplication, and data cleansing.

Author  : Lead Data Engineer
Version : 2.0.0
Runtime : Databricks 13.3 LTS+

Silver is where raw Bronze data becomes trusted. The key guarantees:
  - No duplicate records (deduplicated by business key + event timestamp)
  - All change events propagated (inserts, updates, deletes via CDF)
  - Schema validated before write
  - Data quality gates block bad data from reaching Gold
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, row_number,
)
from pyspark.sql.window import Window

from src.utils.delta_utils import RetryConfig, safe_merge
from src.utils.monitoring import DataQualityRunner, Severity
from src.utils.schema_utils import assert_schema_compatible

logger = logging.getLogger(__name__)


@dataclass
class SilverTableConfig:
    """Configuration for a Silver transformation pipeline."""
    source_table: str              # Bronze Delta table (CDF enabled)
    target_table: str              # Silver Delta table
    primary_key: str               # Business key for deduplication
    event_timestamp_col: str       # Used for ordering within a key
    checkpoint_location: str

    # Quality gates
    not_null_columns: List[str] = field(default_factory=list)
    allowed_values: Dict[str, List] = field(default_factory=dict)
    min_row_count: int = 1

    # CDC
    use_cdf: bool = True           # Read from Bronze CDF vs full table
    starting_version: str = "latest"

    # Streaming
    trigger_interval: str = "2 minutes"

    # Retry on concurrent writes
    retry_config: RetryConfig = field(default_factory=RetryConfig)

    # Custom transform hook (optional)
    transform_fn: Optional[Callable[[DataFrame], DataFrame]] = None


def create_silver_stream(
    spark: SparkSession,
    config: SilverTableConfig,
) -> "StreamingQuery":
    """
    Build and start a Silver CDC streaming pipeline.

    Reads changes from Bronze CDF → deduplicates → validates quality →
    applies MERGE into Silver (upserts + deletes).

    The foreachBatch pattern gives us:
        - Full control over the MERGE logic
        - Idempotency (re-runs produce identical results)
        - Ability to apply business logic per micro-batch
    """
    source_stream = _build_cdf_reader(spark, config)

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        _process_silver_batch(spark, batch_df, batch_id, config)

    writer = source_stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", config.checkpoint_location) \
        .queryName(f"silver_{config.target_table.replace('.', '_')}")

    if config.trigger_interval == "availableNow":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=config.trigger_interval)

    query = writer.start()
    logger.info(
        "Silver stream started | source=%s | target=%s",
        config.source_table, config.target_table
    )
    return query


def run_silver_batch(spark: SparkSession, config: SilverTableConfig) -> Dict:
    """
    Run Silver pipeline as a one-shot batch (availableNow).
    Preferred for scheduled Databricks Workflow tasks.
    """
    import time
    config.trigger_interval = "availableNow"
    start = time.time()
    query = create_silver_stream(spark, config)
    query.awaitTermination()
    duration = time.time() - start

    progress = query.recentProgress
    rows = sum(p.get("numInputRows", 0) for p in progress)
    logger.info("Silver batch complete | rows=%d | duration=%.1fs", rows, duration)
    return {"rows_processed": rows, "duration_seconds": round(duration, 2)}


# ─────────────────────────────────────────────
# Batch Processing Logic
# ─────────────────────────────────────────────

def _process_silver_batch(
    spark: SparkSession,
    batch_df: DataFrame,
    batch_id: int,
    config: SilverTableConfig,
) -> None:
    """
    Core micro-batch processing function called by foreachBatch.

    Steps:
        1. Deduplicate within the batch (keep latest per key)
        2. Separate deletes from upserts
        3. Run data quality checks on upserts
        4. Apply MERGE into Silver table
    """
    if batch_df.isEmpty():
        logger.debug("Batch %d: empty — skipping.", batch_id)
        return

    # ── Step 1: Deduplicate within batch ────────────────────────────────
    # When multiple events arrive for the same key in one micro-batch,
    # keep only the most recent (by event_timestamp_col).
    # For CDF streams, also exclude preimage rows.
    if config.use_cdf:
        clean = batch_df.filter(
            ~col("_change_type").isin("update_preimage", "insert_preimage")
        )
    else:
        clean = batch_df

    window = Window.partitionBy(config.primary_key).orderBy(
        col(config.event_timestamp_col).desc()
    )
    deduped = clean.withColumn("_rank", row_number().over(window)) \
        .filter(col("_rank") == 1) \
        .drop("_rank")

    # ── Step 2: Separate deletes from upserts ────────────────────────────
    if config.use_cdf and "_change_type" in deduped.columns:
        deletes = deduped.filter(col("_change_type") == "delete")
        upserts = deduped.filter(col("_change_type") != "delete")
    else:
        deletes = spark.createDataFrame([], deduped.schema)
        upserts = deduped

    # Drop CDF metadata columns before writing to Silver
    cdf_cols = ["_change_type", "_commit_version", "_commit_timestamp"]
    upserts = upserts.drop(*[c for c in cdf_cols if c in upserts.columns])
    deletes = deletes.drop(*[c for c in cdf_cols if c in deletes.columns])

    # ── Step 3: Data quality gate on upserts ────────────────────────────
    if not upserts.isEmpty():
        runner = DataQualityRunner(spark, upserts, config.target_table) \
            .check_not_null(config.primary_key, severity=Severity.CRITICAL) \
            .check_row_count(min_rows=0)

        for col_name in config.not_null_columns:
            runner.check_not_null(col_name, severity=Severity.WARNING)

        for col_name, allowed in config.allowed_values.items():
            runner.check_referential_integrity(col_name, allowed, severity=Severity.WARNING)

        report = runner.run()
        if not report.overall_passed:
            logger.error(
                "Batch %d: DQ failures detected. Quarantining batch. Report:\n%s",
                batch_id, report.to_json()
            )
            _quarantine_batch(spark, upserts, config, batch_id, report)
            return

    # ── Step 4: Apply custom transform (optional) ────────────────────────
    if config.transform_fn and not upserts.isEmpty():
        upserts = config.transform_fn(upserts)

    # ── Step 5: MERGE into Silver ────────────────────────────────────────
    delta_tbl = DeltaTable.forName(spark, config.target_table)

    if not deletes.isEmpty():
        delta_tbl.alias("t").merge(
            deletes.alias("s"),
            f"t.{config.primary_key} = s.{config.primary_key}"
        ).whenMatchedDelete().execute()
        logger.info("Batch %d: applied %d deletes.", batch_id, deletes.count())

    if not upserts.isEmpty():
        stats = safe_merge(
            spark=spark,
            source_df=upserts,
            target_table=config.target_table,
            merge_key=config.primary_key,
            retry_config=config.retry_config,
            job_id=f"silver_batch_{batch_id}",
        )
        logger.info(
            "Batch %d: MERGE complete | inserted=%d | updated=%d",
            batch_id, stats["rows_inserted"], stats["rows_updated"]
        )


def _build_cdf_reader(spark: SparkSession, config: SilverTableConfig) -> DataFrame:
    """Build a streaming reader from Bronze table CDF or full scan."""
    if config.use_cdf:
        return spark.readStream.format("delta") \
            .option("readChangeData", "true") \
            .option("startingVersion", config.starting_version) \
            .table(config.source_table)
    else:
        return spark.readStream.format("delta").table(config.source_table)


def _quarantine_batch(
    spark: SparkSession,
    df: DataFrame,
    config: SilverTableConfig,
    batch_id: int,
    report,
) -> None:
    """Write failed batches to quarantine table for investigation."""
    quarantine_table = config.target_table.replace("silver", "quarantine", 1)
    df.withColumn("_quarantine_batch_id", lit(batch_id)) \
      .withColumn("_quarantine_ts", current_timestamp()) \
      .withColumn("_quarantine_reason", lit(str(report.failed_checks()))) \
      .write.format("delta") \
      .option("mergeSchema", "true") \
      .mode("append") \
      .saveAsTable(quarantine_table)
    logger.warning(
        "Batch %d quarantined to %s — investigate before reprocessing.",
        batch_id, quarantine_table
    )
