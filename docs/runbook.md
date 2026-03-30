# Production Runbook — Delta Lake Operations

**Owner:** Lead Data Engineer  
**Last Updated:** 2024  
**On-call rotation:** data-platform-oncall@yourcompany.com

This runbook covers the most common production incidents on our Delta Lake platform.
Each section includes: symptoms → diagnosis → fix → prevention.

---

## Table of Contents

1. [Query Performance Sudden Degradation](#1-query-performance-sudden-degradation)
2. [Streaming Pipeline Lag Spike](#2-streaming-pipeline-lag-spike)
3. [ConcurrentModificationException in Job Logs](#3-concurrentmodificationexception-in-job-logs)
4. [Data Quality Failure — Quarantine Table Growing](#4-data-quality-failure--quarantine-table-growing)
5. [Schema Mismatch Pipeline Failure](#5-schema-mismatch-pipeline-failure)
6. [VACUUM Job Failure](#6-vacuum-job-failure)
7. [Gold Table Freshness SLA Breach](#7-gold-table-freshness-sla-breach)
8. [Incorrect Row Count After Merge](#8-incorrect-row-count-after-merge)
9. [Delta Log Corruption](#9-delta-log-corruption)

---

## 1. Query Performance Sudden Degradation

**Symptoms:** Query time increased 5–10x with no code changes deployed.

**Diagnosis:**
```sql
-- Check file health
DESCRIBE DETAIL your_catalog.your_schema.your_table;
-- Alert if: numFiles > 50,000 OR sizeInBytes/numFiles < 32MB

-- Check transaction log size
SELECT COUNT(*) FROM (DESCRIBE HISTORY your_catalog.your_schema.your_table);
-- Alert if: > 10,000 versions without recent OPTIMIZE

-- Look for OPTIMIZE in recent history
SELECT version, timestamp, operation
FROM (DESCRIBE HISTORY your_catalog.your_schema.your_table)
WHERE operation = 'OPTIMIZE'
LIMIT 5;
```

**Fix:**
```python
# Run targeted OPTIMIZE on affected partitions
spark.sql("""
    OPTIMIZE your_catalog.your_schema.your_table
    WHERE event_date >= current_date() - INTERVAL 7 DAYS
    ZORDER BY (user_id, event_ts)
""")
```

**Prevention:** Ensure `OPTIMIZE` is scheduled in your Databricks Workflow. Frequency: daily for high-DML tables, weekly for append-only tables.

---

## 2. Streaming Pipeline Lag Spike

**Symptoms:** Databricks streaming dashboard shows trigger duration > 2x normal. Input rows per second drops. Consumer tables fall behind SLA.

**Diagnosis:**

1. Open Databricks UI → Compute → Streaming tab
2. Check `inputRowsPerSecond` and `processedRowsPerSecond` in recent progress
3. Check `durationMs.triggerExecution` — if > trigger interval, you have a backpressure problem

```python
# In notebook: inspect recent streaming progress
for p in query.recentProgress[-5:]:
    print(f"Batch {p['batchId']}: "
          f"input={p['numInputRows']}, "
          f"duration={p['durationMs'].get('triggerExecution', '?')}ms")
```

**Common causes and fixes:**

| Cause | Fix |
|---|---|
| Upstream Kafka partition imbalance | Re-partition source topic |
| State store growth (stateful aggregation) | Reduce watermark window or add state TTL |
| Executor OOM | Increase executor memory or add more nodes |
| Too many small files on Silver | Run OPTIMIZE before stream reads Silver |
| Target MERGE taking longer (table grew) | Add partition scoping to MERGE condition |

**Prevention:** Alert on `processedRowsPerSecond < 0.8 * inputRowsPerSecond` sustained for > 5 minutes.

---

## 3. ConcurrentModificationException in Job Logs

**Symptoms:** Job fails with `io.delta.exceptions.ConcurrentModificationException`. Data not written.

**Important:** This is NOT data corruption. Delta's OCC detected a conflict and aborted cleanly. No rows were lost.

**Diagnosis:**
```python
# Check which operations conflicted
spark.sql("""
    SELECT version, timestamp, operation, isolationLevel
    FROM (DESCRIBE HISTORY your_catalog.your_schema.your_table)
    ORDER BY version DESC
    LIMIT 20
""").show(truncate=False)
```

**Fix options (choose based on your write pattern):**

1. **Retry** (simplest): The job has built-in retry with exponential backoff. Check if it self-healed.

2. **Partition isolation** (best throughput): Assign each concurrent job to its own `replaceWhere` partition:
   ```python
   df.write.option("replaceWhere", "region = 'US'").mode("overwrite").save(path)
   ```

3. **Job sequencing** (correctness > throughput): Add task dependencies in Databricks Workflow to serialize conflicting writes.

**Prevention:** Review job schedule. Overlapping job windows on the same hot table will produce frequent conflicts. Either widen trigger intervals or implement partition isolation.

---

## 4. Data Quality Failure — Quarantine Table Growing

**Symptoms:** Alert fires: `prod.quarantine.silver_customers` row count > 0.

**Diagnosis:**
```sql
-- Inspect quarantined records
SELECT
    _quarantine_reason,
    _quarantine_batch_id,
    COUNT(*) AS row_count,
    MIN(_quarantine_ts) AS first_seen,
    MAX(_quarantine_ts) AS last_seen
FROM prod.quarantine.silver_customers
GROUP BY 1, 2
ORDER BY first_seen DESC
LIMIT 20;
```

**Common root causes:**

- Upstream team added NULLs to a NOT NULL column → fix upstream, backfill
- New enum value not in allowed_values list → update DQ config + reprocess
- Bulk delete in source sent empty batch → check row_count threshold settings

**Fix:**
```python
# After investigating, reprocess quarantined records
quarantine_df = spark.table("prod.quarantine.silver_customers") \
    .drop("_quarantine_batch_id", "_quarantine_ts", "_quarantine_reason")

# Fix the data quality issue first, then re-run silver pipeline on this data
from src.utils.delta_utils import safe_merge
safe_merge(spark, quarantine_df, "prod.silver.customers", "customer_id")

# Clear quarantine after successful reprocessing
spark.sql("DELETE FROM prod.quarantine.silver_customers WHERE _quarantine_batch_id = X")
```

---

## 5. Schema Mismatch Pipeline Failure

**Symptoms:** Pipeline fails with `AnalysisException: A schema mismatch detected`.

**Diagnosis:**
```python
# Compare current schema to incoming
current = spark.table("prod.silver.customers").schema
incoming = spark.read.format("delta").load("/mnt/bronze/customers").schema

from src.utils.schema_utils import detect_schema_changes
result = detect_schema_changes(current, incoming)
print(result.summary)
for change in result.breaking_changes:
    print(f"BREAKING: {change.column} — {change.recommended_action}")
```

**Fix based on change type:**

```python
# CASE 1: New column added (safe)
spark.sql("ALTER TABLE prod.silver.customers ADD COLUMN IF NOT EXISTS new_col STRING")

# CASE 2: Column renamed (requires column mapping)
from src.utils.schema_utils import enable_column_mapping
enable_column_mapping(spark, "prod.silver.customers")
spark.sql("ALTER TABLE prod.silver.customers RENAME COLUMN old_name TO new_name")

# CASE 3: Type narrowing (data quality issue upstream)
# → Fix at source, do NOT cast in pipeline (data loss)
```

---

## 6. VACUUM Job Failure

**Symptoms:** VACUUM job fails or was cancelled. Storage not being reclaimed.

**CRITICAL:** Never re-run VACUUM with a shorter retention to "catch up." This can delete files still referenced by active queries.

**Diagnosis:**
```sql
-- Check last successful VACUUM
SELECT version, timestamp, operation
FROM (DESCRIBE HISTORY your_catalog.your_schema.your_table)
WHERE operation = 'VACUUM END'
ORDER BY version DESC
LIMIT 5;
```

**Safe recovery:**
```python
from src.utils.delta_utils import vacuum_table, VacuumConfig

# Always dry_run first to see impact
vacuum_table(
    spark,
    "your_catalog.your_schema.your_table",
    VacuumConfig(retain_hours=168, dry_run=True)  # 7 days minimum
)
```

**If VACUUM is blocked by active streaming:**
1. Check streaming checkpoint version: look for the oldest `startingVersion` in checkpoint dirs
2. Ensure retention_hours > (current_version - checkpoint_version) * avg_minutes_per_version / 60

---

## 7. Gold Table Freshness SLA Breach

**Symptoms:** Dashboard shows stale data. Gold table last updated > SLA threshold.

**Diagnosis:**
```sql
-- Check Gold table last write
SELECT MAX(_aggregated_ts) AS last_updated, current_timestamp() AS now,
       DATEDIFF(MINUTE, MAX(_aggregated_ts), current_timestamp()) AS lag_minutes
FROM prod.gold.sales_daily;

-- Check if Silver pipeline is running
-- (check Databricks Workflows for last run status)
```

**Fix triage:**
1. Is Silver pipeline running? → Check Workflows, restart if failed
2. Is Silver table stale? → Check Bronze pipeline
3. Is Bronze pipeline running? → Check Auto Loader metrics and source files
4. Manual trigger for Gold batch:
   ```python
   from src.pipelines.gold_aggregation import run_daily_gold_batch, GoldTableConfig
   run_daily_gold_batch(spark, config)
   ```

---

## 8. Incorrect Row Count After Merge

**Symptoms:** Row count in Silver is higher than expected. Possible duplicates.

**Diagnosis:**
```sql
-- Check for duplicates on primary key
SELECT customer_id, COUNT(*) AS cnt
FROM prod.silver.customers
GROUP BY customer_id
HAVING cnt > 1
ORDER BY cnt DESC
LIMIT 20;

-- Time travel to see when duplicates appeared
SELECT version, timestamp, operation, operationMetrics
FROM (DESCRIBE HISTORY prod.silver.customers)
ORDER BY version DESC
LIMIT 20;
```

**Fix:**
```python
from src.utils.delta_utils import diff_versions
# Find which version introduced duplicates
changes = diff_versions(spark, "prod.silver.customers",
                         version_before=N-1, version_after=N,
                         key_col="customer_id")
changes.show()

# If needed, restore to clean version
spark.sql("RESTORE TABLE prod.silver.customers TO VERSION AS OF N-1")
```

**Root cause (most common):** Source DataFrame was not deduplicated before MERGE. Always call `.dropDuplicates([primary_key])` on the source.

---

## 9. Delta Log Corruption

**Symptoms:** `DeltaAnalysisException: The transaction log is corrupted`.

**This is rare.** Usually caused by direct object storage manipulation (never do this).

**Recovery steps:**
1. Do NOT write to the table
2. Contact your cloud storage team to check for recent object-level operations
3. Try reading the last known good version:
   ```python
   df = spark.read.format("delta").option("versionAsOf", LAST_GOOD_VERSION).load(path)
   ```
4. If reading fails: restore from backup or re-ingest from Bronze

**Prevention:** Never delete `_delta_log/` files directly. Always use `VACUUM` with proper retention.

---

## Escalation Path

| Severity | Response Time | Contact |
|---|---|---|
| P1 — Data loss, all pipelines down | 15 min | PagerDuty → data-platform-oncall |
| P2 — Single pipeline down, SLA at risk | 1 hour | Slack #data-incidents |
| P3 — Degraded performance, no SLA breach | Next business day | Jira ticket |
| P4 — Optimization opportunity | Sprint planning | Backlog |
