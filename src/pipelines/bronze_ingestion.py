"""
bronze_ingestion.py
===================
Production Bronze layer ingestion using Databricks Auto Loader.

Author  : Lead Data Engineer
Version : 2.0.0
Runtime : Databricks 13.3 LTS+

Design principles:
  - Append-only: Bronze is a raw historical record — never update, never delete
  - Exactly-once: checkpoint location + idempotent writes prevent duplicates
  - Self-healing: schema evolution handled automatically via CDF
  - Observable: every batch emits metrics, every anomaly is logged
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit, md5, concat_ws,
)

logger = logging.getLogger(__name__)


@dataclass
class BronzeTableConfig:
    """Complete configuration for a Bronze ingestion stream."""

    # Source
    source_path: str
    source_format: str = "json"          # json | parquet | csv | avro | orc

    # Target
    target_table: str = ""
    target_path: str = ""
    partition_cols: List[str] = field(default_factory=list)

    # Auto Loader schema management
    schema_location: str = ""
    schema_evolution_mode: str = "addNewColumns"  # addNewColumns | rescue | failOnNewColumns
    infer_column_types: bool = True

    # Streaming
    checkpoint_location: str = ""
    trigger_interval: str = "5 minutes"   # or "availableNow" for cost-efficient batch
    max_files_per_trigger: int = 1000      # Rate-limit ingestion

    # Audit columns (added to every Bronze row)
    add_audit_cols: bool = True
    source_system: str = "unknown"


def create_bronze_stream(
    spark: SparkSession,
    config: BronzeTableConfig,
) -> "StreamingQuery":
    """
    Build and start a production Auto Loader stream into a Bronze Delta table.

    Auto Loader handles:
        - Incremental file discovery (no full S3 listing on restart)
        - Schema inference and evolution
        - Exactly-once file processing via internal checkpointing
        - Automatic backfill of files that arrived while stream was down

    Returns the active StreamingQuery.
    """
    # ── Read ────────────────────────────────────────────────────────────
    raw_stream = _build_autoloader_reader(spark, config)

    # ── Transform ────────────────────────────────────────────────────────
    bronze_stream = _add_bronze_metadata(raw_stream, config)

    # ── Write ────────────────────────────────────────────────────────────
    writer = bronze_stream.writeStream \
        .format("delta") \
        .option("checkpointLocation", config.checkpoint_location) \
        .option("mergeSchema", "true") \
        .outputMode("append")

    # Determine trigger type
    if config.trigger_interval == "availableNow":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=config.trigger_interval)

    if config.partition_cols:
        writer = writer.partitionBy(*config.partition_cols)

    if config.target_table:
        query = writer.toTable(config.target_table)
    elif config.target_path:
        query = writer.start(config.target_path)
    else:
        raise ValueError("Either target_table or target_path must be set in BronzeTableConfig.")

    logger.info(
        "Bronze stream started | source=%s | target=%s | trigger=%s",
        config.source_path,
        config.target_table or config.target_path,
        config.trigger_interval,
    )
    return query


def run_bronze_batch(
    spark: SparkSession,
    config: BronzeTableConfig,
) -> Dict:
    """
    Run Bronze ingestion as a triggered batch (availableNow mode).

    Preferred for cost-sensitive workloads. Processes all available files
    and exits cleanly. Use in Databricks Workflow tasks.

    Returns:
        dict with rows_written, files_processed, duration_seconds
    """
    import time
    start = time.time()

    config.trigger_interval = "availableNow"
    query = create_bronze_stream(spark, config)
    query.awaitTermination()

    duration = time.time() - start
    progress = query.recentProgress

    rows = sum(p.get("numInputRows", 0) for p in progress)
    files = sum(p.get("sources", [{}])[0].get("numFilesOutstanding", 0) for p in progress)

    logger.info(
        "Bronze batch complete | rows=%d | duration=%.1fs", rows, duration
    )
    return {
        "rows_written": rows,
        "files_processed": len(progress),
        "duration_seconds": round(duration, 2),
    }


# ─────────────────────────────────────────────
# Internal Helpers
# ─────────────────────────────────────────────

def _build_autoloader_reader(spark: SparkSession, config: BronzeTableConfig) -> DataFrame:
    """Configure Auto Loader with production-grade options."""
    reader = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", config.source_format) \
        .option("cloudFiles.schemaLocation", config.schema_location) \
        .option("cloudFiles.inferColumnTypes", str(config.infer_column_types).lower()) \
        .option("cloudFiles.schemaEvolutionMode", config.schema_evolution_mode) \
        .option("cloudFiles.includeExistingFiles", "true") \
        .option("cloudFiles.maxFilesPerTrigger", str(config.max_files_per_trigger)) \
        .option("cloudFiles.useNotifications", "true")   # SNS/SQS for near-real-time file detection

    # Format-specific options
    if config.source_format == "csv":
        reader = reader \
            .option("cloudFiles.format", "csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiLine", "true") \
            .option("escape", '"')

    elif config.source_format == "json":
        reader = reader \
            .option("multiLine", "false") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record")

    return reader.load(config.source_path)


def _add_bronze_metadata(df: DataFrame, config: BronzeTableConfig) -> DataFrame:
    """
    Enrich every Bronze row with audit metadata.

    These columns are critical for:
        - Lineage: knowing exactly which file a row came from
        - Deduplication: _bronze_row_id can detect cross-file duplicates
        - Incident response: _ingest_ts narrows down when bad data arrived
        - Cost tracking: _source_system maps rows to upstream teams
    """
    if not config.add_audit_cols:
        return df

    return df \
        .withColumn("_ingest_ts", current_timestamp()) \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_source_system", lit(config.source_system)) \
        .withColumn(
            "_bronze_row_id",
            md5(concat_ws("||", *[col(c) for c in df.columns]))
        )
