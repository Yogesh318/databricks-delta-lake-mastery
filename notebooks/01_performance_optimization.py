# Databricks notebook source
# MAGIC %md
# MAGIC # Scenario 1: Performance Degradation with Frequent Updates & Deletes
# MAGIC
# MAGIC **Problem:** A Delta table with frequent updates/deletes accumulates small files and stale data, causing read slowdowns over time.
# MAGIC
# MAGIC **Solution:** OPTIMIZE + ZORDER + Auto Optimize properties + scheduled VACUUM.
# MAGIC
# MAGIC ---
# MAGIC **Author:** Lead Data Engineer  
# MAGIC **Runtime:** Databricks 13.3 LTS+

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *
import sys
sys.path.insert(0, "/Workspace/Repos/databricks-delta-lake-mastery")

from src.utils.delta_utils import (
    optimize_table, vacuum_table, get_table_health,
    OptimizeConfig, VacuumConfig,
)

TABLE_PATH = "/tmp/delta_playbook/transactions"
TABLE_NAME = f"delta.`{TABLE_PATH}`"

# COMMAND ----------

# MAGIC %md ## Step 1 — Create and populate a hot Delta table

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")  # Off for demo
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")    # Off for demo

# Generate 1M rows simulating a transaction table
df = spark.range(1_000_000).toDF("id") \
    .withColumn("user_id", (rand() * 10000).cast("int")) \
    .withColumn("amount", round(rand() * 1000, 2)) \
    .withColumn("status", when(rand() > 0.5, lit("active")).otherwise(lit("inactive"))) \
    .withColumn("region", when(rand() > 0.5, lit("US")).otherwise(lit("EU"))) \
    .withColumn("updated_at", current_timestamp())

df.repartition(200).write.format("delta").mode("overwrite").save(TABLE_PATH)
print(f"✅ Table created with {df.count():,} rows across 200 small files")

# COMMAND ----------

# MAGIC %md ## Step 2 — Diagnose: view the small file problem

# COMMAND ----------

health_before = get_table_health(spark, TABLE_PATH)
print("=== TABLE HEALTH BEFORE OPTIMIZE ===")
for k, v in health_before.items():
    print(f"  {k:30s}: {v}")

# Anything below 32MB average is a small file problem
# CRITICAL < 16MB | WARNING < 64MB | HEALTHY >= 64MB

# COMMAND ----------

# MAGIC %md ## Step 3 — Simulate frequent DML (updates + deletes)

# COMMAND ----------

delta_tbl = DeltaTable.forPath(spark, TABLE_PATH)

# Simulate production pattern: 10% upsert every pipeline run
updates = df.sample(0.1) \
    .withColumn("status", lit("processed")) \
    .withColumn("updated_at", current_timestamp())

delta_tbl.alias("t").merge(
    updates.alias("s"),
    "t.id = s.id"
).whenMatchedUpdate(set={
    "status": "s.status",
    "updated_at": "s.updated_at"
}).whenNotMatchedInsertAll().execute()

print(f"✅ MERGE applied on 10% of rows — file fragmentation increased")

# COMMAND ----------

# MAGIC %md ## Step 4 — Fix with OPTIMIZE + ZORDER

# COMMAND ----------

# ZORDER on columns used in WHERE clauses of hottest queries
# Rule: only ZORDER on columns you actually filter by — more columns = diminishing returns
optimize_table(
    spark,
    TABLE_PATH,
    OptimizeConfig(
        zorder_cols=["user_id", "updated_at"],
        # Scope to recent data to avoid full rewrite in prod
        partition_filter=None,  # No partitions in this demo table
    )
)

health_after = get_table_health(spark, TABLE_PATH)
print("\n=== TABLE HEALTH AFTER OPTIMIZE ===")
for k, v in health_after.items():
    print(f"  {k:30s}: {v}")

# COMMAND ----------

# MAGIC %md ## Step 5 — Enable Auto-Optimize for future writes

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {TABLE_NAME} SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true',
        'delta.targetFileSize'             = '134217728',
        'delta.logRetentionDuration'       = 'interval 30 days',
        'delta.deletedFileRetentionDuration' = 'interval 7 days'
    )
""")
print("✅ Auto-optimize enabled. Future writes will produce larger, fewer files.")

# COMMAND ----------

# MAGIC %md ## Step 6 — Governed VACUUM

# COMMAND ----------

# Always dry_run=True first in production!
files_to_delete = vacuum_table(
    spark, TABLE_PATH,
    VacuumConfig(retain_hours=168, dry_run=True)  # 7 days — never go below
)
print(f"DRY RUN: Would delete {files_to_delete} files")

# Set dry_run=False when you're confident
# vacuum_table(spark, TABLE_PATH, VacuumConfig(retain_hours=168, dry_run=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC | Pattern | When to Use |
# MAGIC |---|---|
# MAGIC | `OPTIMIZE` (manual) | Scheduled job: daily for high-DML, weekly for append-only |
# MAGIC | `autoOptimize` | Continuous streaming tables — set on table properties |
# MAGIC | `ZORDER` | Only on columns that appear in WHERE filters of slow queries |
# MAGIC | `Liquid Clustering` | New tables on DBR 13.3+ — preferred over ZORDER |
# MAGIC | `VACUUM` | Monthly with dry_run first — never below 168 hours |
