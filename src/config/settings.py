"""
settings.py
===========
Centralized configuration for the Delta Lake pipeline.
All environment-specific values live here — nothing hardcoded in notebooks.
"""
import os

# ── Catalog & Schema ─────────────────────────────────────────────────
CATALOG      = os.getenv("DELTA_CATALOG", "main")
BRONZE_DB    = f"{CATALOG}.bronze"
SILVER_DB    = f"{CATALOG}.silver"
GOLD_DB      = f"{CATALOG}.gold"
OPS_DB       = f"{CATALOG}.ops"

# ── Table Names ──────────────────────────────────────────────────────
BRONZE_EVENTS         = f"{BRONZE_DB}.raw_events"
BRONZE_USERS          = f"{BRONZE_DB}.raw_users"
BRONZE_SUBSCRIPTIONS  = f"{BRONZE_DB}.raw_subscriptions"

SILVER_EVENTS         = f"{SILVER_DB}.events"
SILVER_USERS          = f"{SILVER_DB}.users"

GOLD_DAILY_METRICS    = f"{GOLD_DB}.daily_metrics"
GOLD_REALTIME_METRICS = f"{GOLD_DB}.realtime_metrics"
GOLD_UNIFIED_VIEW     = f"{GOLD_DB}.metrics_unified"

# ── Storage Paths ────────────────────────────────────────────────────
STORAGE_ROOT     = os.getenv("STORAGE_ROOT", "/mnt/delta-playbook")
CHECKPOINT_ROOT  = f"{STORAGE_ROOT}/checkpoints"
SCHEMA_ROOT      = f"{STORAGE_ROOT}/schemas"

# ── Streaming ────────────────────────────────────────────────────────
BRONZE_TRIGGER    = os.getenv("BRONZE_TRIGGER", "5 minutes")
SILVER_TRIGGER    = os.getenv("SILVER_TRIGGER", "2 minutes")
GOLD_RT_TRIGGER   = os.getenv("GOLD_RT_TRIGGER", "1 minute")

# ── Optimization ─────────────────────────────────────────────────────
TARGET_FILE_SIZE_BYTES = 134_217_728  # 128 MB

# ── Vacuum retention (hours) — NEVER below 168 ──────────────────────
BRONZE_VACUUM_RETAIN_H  = int(os.getenv("BRONZE_VACUUM_RETAIN_H", "168"))   # 7 days
SILVER_VACUUM_RETAIN_H  = int(os.getenv("SILVER_VACUUM_RETAIN_H", "336"))   # 14 days
GOLD_VACUUM_RETAIN_H    = int(os.getenv("GOLD_VACUUM_RETAIN_H",   "720"))   # 30 days
