# Databricks notebook source
# MAGIC %md
# MAGIC # Dataset Setup — SaaS Product Analytics (10M Events)
# MAGIC
# MAGIC **Run this notebook first.** It generates and loads the full dataset into
# MAGIC Bronze Delta tables, ready for all 9 production scenarios.
# MAGIC
# MAGIC **Dataset:** SaaS Product Analytics  
# MAGIC **Schema inspired by:** Kaggle eCommerce Behavior Data (Mihail Zabkov, CC BY 4.0)  
# MAGIC **Scale:** 10M events · 500K users · 6 months  
# MAGIC **Estimated runtime:** ~12 minutes on a 4-node m5d.2xlarge cluster
# MAGIC
# MAGIC ---
# MAGIC ### What gets created
# MAGIC
# MAGIC | Table | Rows | Purpose |
# MAGIC |---|---|---|
# MAGIC | `bronze.raw_events` | 10,000,000 | Feature usage, API calls, lifecycle events |
# MAGIC | `bronze.raw_users` | 500,000 | User profiles with plan tier, health score |
# MAGIC | `bronze.raw_subscriptions` | ~225,000 | Paid subscriptions (MRR, churn dates) |
# MAGIC | `mutations.user_updates` | ~50,000 | 10% of users updated (for CDC/concurrent write demos) |
# MAGIC | `mutations.users_v2_schema` | ~150,000 | Users with 3 new columns (for schema evolution demo) |

# COMMAND ----------

# MAGIC %md ## 0. Install dependencies

# COMMAND ----------

# MAGIC %pip install pyarrow faker --quiet

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/YOUR_USERNAME/databricks-delta-lake-mastery")

from data.generators.generate_dataset import (
    generate_user_pool, generate_subscriptions, generate_events_stream,
    generate_user_updates, generate_schema_v2_users,
    SCALE_CONFIG,
)
from data.schemas.schema_definitions import (
    EVENT_TYPES, PLAN_TIERS, PRODUCT_AREAS, REGIONS,
    PLAN_MRR, SCHEMA_V2_COLUMNS,
)

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

print("✅ Imports OK")

# COMMAND ----------

# MAGIC %md ## 1. Configuration

# COMMAND ----------

# ── Configure these for your environment ──────────────────────────────
CATALOG      = "main"          # Your Unity Catalog name
SCHEMA       = "saas_analytics" # Will be created if not exists
SCALE        = "medium"        # small | medium | large
SEED         = 42              # Reproducible generation
# ─────────────────────────────────────────────────────────────────────

CFG = SCALE_CONFIG[SCALE]

print(f"""
Dataset Configuration
─────────────────────────────────────────────
Catalog      : {CATALOG}
Schema       : {SCHEMA}
Scale        : {SCALE}
Users        : {CFG['n_users']:,}
Events       : {CFG['n_events']:,}
Date range   : {CFG['start_date'].date()} → +{CFG['n_months']} months
""")

# COMMAND ----------

# MAGIC %md ## 2. Create catalog and schemas

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.mutations")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.quarantine")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ops")

print(f"✅ Schemas created under {CATALOG}")

# COMMAND ----------

# MAGIC %md ## 3. Generate & load users (500K)

# COMMAND ----------

print("Generating user pool...")
users = generate_user_pool(
    n_users=CFG["n_users"],
    n_companies=CFG["n_companies"],
    start_date=CFG["start_date"],
    n_months=CFG["n_months"],
    seed=SEED,
)

users_df = spark.createDataFrame(users) \
    .withColumn("signup_date",    to_date(col("signup_date").cast("string"))) \
    .withColumn("trial_ends_at",  to_timestamp(col("trial_ends_at"))) \
    .withColumn("churned_at",     to_timestamp(col("churned_at"))) \
    .withColumn("last_active_ts", to_timestamp(col("last_active_ts"))) \
    .withColumn("updated_at",     to_timestamp(col("updated_at")))

users_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.bronze.raw_users")

print(f"✅ bronze.raw_users: {users_df.count():,} rows")
users_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ## 4. Generate & load subscriptions (~225K)

# COMMAND ----------

import random
subs = generate_subscriptions(users, rng_seed=SEED + 1)

subs_df = spark.createDataFrame(subs) \
    .withColumn("started_at",   to_timestamp(col("started_at"))) \
    .withColumn("renewed_at",   to_timestamp(col("renewed_at"))) \
    .withColumn("cancelled_at", to_timestamp(col("cancelled_at"))) \
    .withColumn("updated_at",   to_timestamp(col("updated_at")))

subs_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.bronze.raw_subscriptions")

print(f"✅ bronze.raw_subscriptions: {subs_df.count():,} rows")

plan_breakdown = subs_df.groupBy("plan_tier").agg(
    count("*").alias("count"),
    round(sum("mrr_usd"), 2).alias("total_mrr")
).orderBy("total_mrr", ascending=False)
plan_breakdown.show()

# COMMAND ----------

# MAGIC %md ## 5. Generate & load events (10M rows, partitioned by event_date)
# MAGIC This is the largest step — ~8–10 minutes for 10M rows.

# COMMAND ----------

from data.generators.generate_dataset import BATCH_SIZE

print(f"Generating {CFG['n_events']:,} events in batches of {BATCH_SIZE:,}...")
event_iter = generate_events_stream(
    users=users,
    n_events=CFG["n_events"],
    start_date=CFG["start_date"],
    n_months=CFG["n_months"],
    seed=SEED,
)

first_batch = True
total_written = 0

for batch in event_iter:
    batch_df = spark.createDataFrame(batch) \
        .withColumn("event_ts",  to_timestamp(col("event_ts"))) \
        .withColumn("ingest_ts", to_timestamp(col("ingest_ts"))) \
        .withColumn("event_date", to_date(col("event_date")))

    mode = "overwrite" if first_batch else "append"
    batch_df.write.format("delta") \
        .partitionBy("event_date") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", str(first_batch).lower()) \
        .mode(mode) \
        .saveAsTable(f"{CATALOG}.bronze.raw_events")

    total_written += len(batch)
    first_batch = False

print(f"✅ bronze.raw_events: {total_written:,} rows")

# COMMAND ----------

# MAGIC %md ## 6. Set production table properties (Bronze tier)

# COMMAND ----------

bronze_tables = [
    f"{CATALOG}.bronze.raw_events",
    f"{CATALOG}.bronze.raw_users",
    f"{CATALOG}.bronze.raw_subscriptions",
]

for tbl in bronze_tables:
    spark.sql(f"""
        ALTER TABLE {tbl} SET TBLPROPERTIES (
            'delta.enableChangeDataFeed'             = 'true',
            'delta.autoOptimize.optimizeWrite'       = 'true',
            'delta.autoOptimize.autoCompact'         = 'true',
            'delta.logRetentionDuration'             = 'interval 14 days',
            'delta.deletedFileRetentionDuration'     = 'interval 7 days'
        )
    """)
    print(f"  ✅ Properties set on {tbl}")

# COMMAND ----------

# MAGIC %md ## 7. Generate mutation datasets (for scenario demos)

# COMMAND ----------

# Scenario 2 & 3 & 8: 10% of users have been updated (plan upgrades, health score, churn)
print("Generating user updates (Scenario 2, 3, 8 demos)...")
updates = generate_user_updates(users, update_fraction=0.10, seed=SEED + 10)
updates_df = spark.createDataFrame(updates) \
    .withColumn("signup_date",    to_date(col("signup_date").cast("string"))) \
    .withColumn("trial_ends_at",  to_timestamp(col("trial_ends_at"))) \
    .withColumn("churned_at",     to_timestamp(col("churned_at"))) \
    .withColumn("last_active_ts", to_timestamp(col("last_active_ts"))) \
    .withColumn("updated_at",     to_timestamp(col("updated_at")))

updates_df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.mutations.user_updates")

print(f"  ✅ mutations.user_updates: {updates_df.count():,} rows")

# Scenario 5: Users with 3 new columns (lifecycle_stage, csm_owner, nps_score)
print("\nGenerating V2 schema users (Scenario 5 — schema evolution demo)...")
v2_users = generate_schema_v2_users(users, fraction=0.30, seed=SEED + 20)
v2_df = spark.createDataFrame(v2_users) \
    .withColumn("signup_date",    to_date(col("signup_date").cast("string"))) \
    .withColumn("trial_ends_at",  to_timestamp(col("trial_ends_at"))) \
    .withColumn("churned_at",     to_timestamp(col("churned_at"))) \
    .withColumn("updated_at",     to_timestamp(col("updated_at")))

v2_df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.mutations.users_v2_schema")

print(f"  ✅ mutations.users_v2_schema: {v2_df.count():,} rows")
print(f"     New columns: {list(SCHEMA_V2_COLUMNS.keys())}")

# COMMAND ----------

# MAGIC %md ## 8. Dataset validation & summary

# COMMAND ----------

print("=" * 60)
print("DATASET SUMMARY")
print("=" * 60)

tables = {
    "bronze.raw_events":               f"{CATALOG}.bronze.raw_events",
    "bronze.raw_users":                f"{CATALOG}.bronze.raw_users",
    "bronze.raw_subscriptions":        f"{CATALOG}.bronze.raw_subscriptions",
    "mutations.user_updates":          f"{CATALOG}.mutations.user_updates",
    "mutations.users_v2_schema":       f"{CATALOG}.mutations.users_v2_schema",
}

for label, full_name in tables.items():
    try:
        n = spark.table(full_name).count()
        detail = spark.sql(f"DESCRIBE DETAIL {full_name}").collect()[0]
        size_mb = detail["sizeInBytes"] / 1e6
        print(f"  {label:<40s} {n:>12,} rows  {size_mb:>8.1f} MB")
    except Exception as e:
        print(f"  {label:<40s} ERROR: {e}")

# COMMAND ----------

# MAGIC %md ## 9. Quick data quality checks

# COMMAND ----------

events = spark.table(f"{CATALOG}.bronze.raw_events")

print("Event type distribution:")
events.groupBy("event_type").count() \
    .orderBy(col("count").desc()) \
    .show(10, truncate=False)

print("\nRegion distribution:")
events.groupBy("region").count() \
    .orderBy(col("count").desc()) \
    .show()

print("\nPlan tier distribution in events:")
events.groupBy("plan_tier").count() \
    .orderBy(col("count").desc()) \
    .show()

print("\nDate range of events:")
events.agg(
    min("event_date").alias("earliest_event"),
    max("event_date").alias("latest_event"),
    countDistinct("user_id").alias("unique_users"),
    countDistinct("session_id").alias("unique_sessions"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Dataset Ready!
# MAGIC
# MAGIC Your tables are loaded and ready for all 9 scenarios:
# MAGIC
# MAGIC | Notebook | Uses |
# MAGIC |---|---|
# MAGIC | `01_performance_optimization` | `bronze.raw_events` (10M rows, many small files) |
# MAGIC | `02_concurrent_writes_debug` | `bronze.raw_users` + `mutations.user_updates` |
# MAGIC | `03_cdc_pipeline` | `bronze.raw_users` (CDF enabled) |
# MAGIC | `04_small_files_fix` | `bronze.raw_events` (partitioned, fragmented) |
# MAGIC | `05_schema_evolution` | `mutations.users_v2_schema` (3 new columns) |
# MAGIC | `06_medallion_architecture` | All bronze tables |
# MAGIC | `07_delta_vs_parquet` | `bronze.raw_users` (upserts) |
# MAGIC | `08_concurrent_jobs` | `bronze.raw_events` (region-partitioned parallel writes) |
# MAGIC | `09_vacuum_governance` | `bronze.raw_events` (large table, needs VACUUM) |
# MAGIC
# MAGIC **Next:** Open `notebooks/01_performance_optimization.py`
