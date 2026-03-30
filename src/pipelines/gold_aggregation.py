"""
gold_aggregation.py
===================
Production Gold layer: business aggregations for BI, dashboards, and ML.

Author  : Lead Data Engineer
Version : 2.0.0
Runtime : Databricks 13.3 LTS+

Gold tables are read-mostly, query-optimized, and serve two masters:
  - Batch analytics:  scheduled daily aggregations for BI tools
  - Near real-time:   5-min streaming aggregations for live dashboards

Both are exposed through a unified UNION view so consumers never need
to know which path their data came from.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    approx_count_distinct, avg, col, count, current_date, current_timestamp,
    date_trunc, lit, max as spark_max, min as spark_min,
    percentile_approx, sum as spark_sum, window,
)

from src.utils.delta_utils import OptimizeConfig, optimize_table

logger = logging.getLogger(__name__)


@dataclass
class GoldTableConfig:
    """Configuration for a Gold aggregation pipeline."""
    source_silver_table: str
    target_realtime_table: str
    target_daily_table: str
    unified_view_name: str
    checkpoint_location: str

    # Streaming (real-time path)
    window_duration: str = "5 minutes"
    watermark_delay: str = "10 minutes"
    trigger_interval: str = "1 minute"

    # Batch (daily path)
    lookback_days: int = 1           # Recompute last N days
    partition_col: str = "event_date"

    # Optimization
    zorder_cols: List[str] = field(default_factory=list)


def create_realtime_gold_stream(
    spark: SparkSession,
    config: GoldTableConfig,
) -> "StreamingQuery":
    """
    Build a streaming Gold aggregation for near-real-time dashboards.

    Uses tumbling windows over Silver CDC stream.
    Output mode is 'complete' — the entire result table is rewritten
    on each trigger. This is safe for window aggregations.

    ⚠️ Size your Gold cluster for 'complete' mode — it holds the entire
       aggregated state in memory. Use Spark UI to monitor state store size.
    """
    silver_stream = spark.readStream.format("delta").table(config.source_silver_table)

    agg_stream = silver_stream \
        .withWatermark("event_ts", config.watermark_delay) \
        .groupBy(
            window(col("event_ts"), config.window_duration),
            "region",
            "product_category",
        ).agg(
            spark_sum("amount_usd").alias("revenue"),
            count("event_id").alias("transaction_count"),
            approx_count_distinct("customer_id").alias("unique_customers"),
            avg("amount_usd").alias("avg_order_value"),
            percentile_approx("amount_usd", 0.95).alias("p95_order_value"),
            spark_max("amount_usd").alias("max_order_value"),
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "region",
            "product_category",
            "revenue",
            "transaction_count",
            "unique_customers",
            "avg_order_value",
            "p95_order_value",
            "max_order_value",
            current_timestamp().alias("_aggregated_ts"),
            lit("realtime").alias("_agg_source"),
        )

    query = agg_stream.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{config.checkpoint_location}/realtime") \
        .option("mergeSchema", "true") \
        .outputMode("complete") \
        .trigger(processingTime=config.trigger_interval) \
        .toTable(config.target_realtime_table)

    logger.info(
        "Real-time Gold stream started | source=%s | target=%s | window=%s",
        config.source_silver_table, config.target_realtime_table, config.window_duration,
    )
    return query


def run_daily_gold_batch(
    spark: SparkSession,
    config: GoldTableConfig,
) -> Dict:
    """
    Run the daily Gold batch aggregation.

    Recomputes the last N days atomically — partition-scoped overwrite
    means re-runs are idempotent and concurrent-safe.

    Scheduled as a Databricks Workflow task after Silver completes.
    """
    import time
    start = time.time()

    spark.sql(f"""
        INSERT OVERWRITE TABLE {config.target_daily_table}
        PARTITION ({config.partition_col})
        SELECT
            DATE(event_ts)                          AS event_date,
            region,
            product_category,
            SUM(amount_usd)                         AS revenue,
            COUNT(event_id)                         AS transaction_count,
            COUNT(DISTINCT customer_id)             AS unique_customers,
            AVG(amount_usd)                         AS avg_order_value,
            PERCENTILE(amount_usd, 0.5)             AS median_order_value,
            PERCENTILE(amount_usd, 0.95)            AS p95_order_value,
            MAX(amount_usd)                         AS max_order_value,
            MIN(amount_usd)                         AS min_order_value,
            current_timestamp()                     AS _aggregated_ts,
            'batch'                                 AS _agg_source
        FROM {config.source_silver_table}
        WHERE DATE(event_ts) >= current_date() - INTERVAL {config.lookback_days} DAYS
          AND amount_usd > 0
        GROUP BY 1, 2, 3
    """)

    duration = time.time() - start
    rows = spark.table(config.target_daily_table) \
        .filter(f"event_date >= current_date() - INTERVAL {config.lookback_days} DAYS") \
        .count()

    # Optimize Gold daily for BI query patterns
    optimize_table(
        spark,
        config.target_daily_table,
        OptimizeConfig(
            zorder_cols=config.zorder_cols or ["region", "product_category"],
            partition_filter=f"event_date >= current_date() - INTERVAL {config.lookback_days + 1} DAYS",
        ),
    )

    logger.info(
        "Daily Gold batch complete | rows=%d | duration=%.1fs", rows, duration
    )
    return {"rows_written": rows, "duration_seconds": round(duration, 2)}


def create_unified_view(spark: SparkSession, config: GoldTableConfig) -> None:
    """
    Create a UNION view that blends real-time and batch Gold tables.

    Consumers (BI tools, dashboards, data scientists) always query this
    view — they never need to know whether they're reading real-time or
    batch data. The view handles the seam.

    Pattern:
        - Last 1 hour from real-time table (sub-minute freshness)
        - Everything older from batch table (full aggregation quality)
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {config.unified_view_name} AS
        SELECT
            window_start                    AS period_start,
            window_end                      AS period_end,
            region,
            product_category,
            revenue,
            transaction_count,
            unique_customers,
            avg_order_value,
            p95_order_value,
            max_order_value,
            _agg_source,
            _aggregated_ts
        FROM {config.target_realtime_table}
        WHERE window_start >= current_timestamp() - INTERVAL 2 HOURS

        UNION ALL

        SELECT
            event_date                      AS period_start,
            event_date + INTERVAL 1 DAY     AS period_end,
            region,
            product_category,
            revenue,
            transaction_count,
            unique_customers,
            avg_order_value,
            p95_order_value,
            max_order_value,
            _agg_source,
            _aggregated_ts
        FROM {config.target_daily_table}
        WHERE event_date < current_date()
    """)

    logger.info(
        "Unified Gold view created: %s", config.unified_view_name
    )


def materialize_top_n_cache(
    spark: SparkSession,
    source_view: str,
    cache_table: str,
    n: int = 100,
) -> None:
    """
    Materialize the top-N revenue segments to a tiny cached table.

    Some dashboards only need the top 100 rows. Writing this to a separate
    tiny table reduces BI query latency from seconds to milliseconds.
    """
    spark.sql(f"""
        CREATE OR REPLACE TABLE {cache_table} AS
        SELECT *
        FROM {source_view}
        ORDER BY revenue DESC
        LIMIT {n}
    """)
    # Cache in Spark memory for immediate queries
    spark.catalog.cacheTable(cache_table)
    logger.info("Top-%d cache materialized and cached: %s", n, cache_table)
