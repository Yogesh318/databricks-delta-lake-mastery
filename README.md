# databricks-delta-lake-mastery
=======
# 🔷 Databricks Delta Lake — Production Engineering Playbook

> **Built by a Lead Data Engineer with 11+ years of experience**  
> Solving the hardest Delta Lake challenges encountered across Fortune 500 data platforms, financial services pipelines, and high-scale e-commerce lakehouses.

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.4%2B-orange?logo=apachespark)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0%2B-003366)](https://delta.io)
[![Databricks](https://img.shields.io/badge/Databricks-Runtime%2013.3%2B-FF3621?logo=databricks)](https://databricks.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## 📌 Why This Project Exists

After a decade of building enterprise data platforms, I've seen the same hard problems appear again and again — just dressed differently. Small files killing SLA. Silent schema drift at 2am. VACUUM wiping time-travel history before an audit. CDC pipelines that look correct but lose events under load.

This repository is my **production playbook** — real patterns, real anti-patterns, runbook-ready code, and the architectural thinking behind every decision. It's not documentation. It's what I'd commit to your repo.

---

## 🏗️ Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                     MEDALLION ARCHITECTURE                       │
│                                                                  │
│  Kafka / S3 / JDBC                                               │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────┐    Auto Loader     ┌─────────┐    DQ Rules         │
│  │ BRONZE  │ ─────────────────▶ │ SILVER  │ ──────────────┐     │
│  │  Raw    │   Structured       │ Cleansed│               │     │
│  │ Append  │   Streaming        │ Deduped │               ▼     │
│  └─────────┘                   └─────────┘          ┌─────────┐ │
│                                     │                │  GOLD   │ │
│                                     │   MERGE/AGG    │ Serving │ │
│                                     └──────────────▶ │ Layer   │ │
│                                                      └─────────┘ │
│                                                           │      │
│  Delta CDF ──▶ CDC Consumers          BI / ML / APIs ◀───┘      │
└──────────────────────────────────────────────────────────────────┘
```

---

## 📂 Project Structure

```
databricks-delta-lake-mastery/
│
├── 📁 notebooks/                    # Databricks-ready notebooks (% magic cells)
│   ├── 01_performance_optimization.py
│   ├── 02_concurrent_writes_debug.py
│   ├── 03_cdc_pipeline.py
│   ├── 04_small_files_fix.py
│   ├── 05_schema_evolution.py
│   ├── 06_medallion_architecture.py
│   ├── 07_delta_vs_parquet.py
│   ├── 08_concurrent_jobs.py
│   └── 09_vacuum_governance.py
│
├── 📁 src/                          # Production-grade Python library
│   ├── utils/
│   │   ├── delta_utils.py           # Reusable Delta helpers (MERGE, VACUUM, etc.)
│   │   ├── schema_utils.py          # Schema evolution & validation
│   │   └── monitoring.py            # Data quality & observability
│   ├── pipelines/
│   │   ├── bronze_ingestion.py      # Auto Loader streaming ingestion
│   │   ├── silver_transformation.py # CDC, dedup, cleansing
│   │   └── gold_aggregation.py      # Business aggregations
│   └── config/
│       └── settings.py              # Centralized config management
│
├── 📁 configs/                      # Databricks job & cluster configs
│   ├── cluster_config.json
│   ├── job_config.json
│   └── table_properties.yaml
│
├── 📁 tests/                        # Unit + integration tests (pytest)
│   ├── test_delta_utils.py
│   └── test_pipelines.py
│
├── 📁 docs/
│   ├── architecture.md              # Deep-dive architecture decisions
│   └── runbook.md                   # On-call runbook for production issues
│
├── 📁 .github/workflows/
│   └── ci.yml                       # CI: lint + test on every PR
│
├── requirements.txt
├── setup.py
└── README.md
```

---

## 🚨 The 9 Production Scenarios

Each scenario maps to a notebook + reusable `src/` module. Click any to jump to details.

| # | Scenario | Root Cause | Key Solution |
|---|---|---|---|
| [1](#1-performance-degradation) | Performance Degradation with Frequent DML | File fragmentation | `OPTIMIZE` + `ZORDER` + Auto Compact |
| [2](#2-inconsistent-concurrent-writes) | Inconsistent Data After Concurrent Writes | OCC conflicts | Isolation levels + idempotent MERGE |
| [3](#3-cdc-pipeline) | Production CDC Pipeline | Missing change events | Delta Change Data Feed + Structured Streaming |
| [4](#4-millions-of-small-files) | Millions of Small Files | Streaming micro-batches | Liquid Clustering + `optimizeWrite` |
| [5](#5-schema-evolution) | Schema Evolution Mid-Pipeline | Source schema drift | `mergeSchema` + Auto Loader CDF |
| [6](#6-dual-mode-serving) | Batch Analytics + Near-Real-Time Reporting | Different SLAs | Medallion Architecture + Gold UNION view |
| [7](#7-delta-vs-parquet) | When to Use Delta vs Parquet | Wrong tool choice | Capability matrix + concrete comparison |
| [8](#8-concurrent-jobs) | Concurrent Writes from Multiple Jobs | Hot table contention | Partition isolation + retry with backoff |
| [9](#9-vacuum-governance) | VACUUM Risks in Production | Accidental data loss | Dry-run governance + retention tiers |

---

## ⚡ Quick Start

### Prerequisites

- Databricks Runtime **13.3 LTS** or higher (Spark 3.4+)
- Python **3.10+**
- Unity Catalog enabled (recommended) or Hive Metastore

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/databricks-delta-lake-mastery.git
cd databricks-delta-lake-mastery
```

### 2. Install Dependencies (local dev / CI)

```bash
pip install -r requirements.txt
```

### 3. Import Notebooks into Databricks

**Option A — Databricks CLI:**
```bash
pip install databricks-cli
databricks configure --token

# Import all notebooks
databricks workspace import_dir notebooks/ /Users/you@company.com/delta-playbook
```

**Option B — Databricks Repos (recommended):**  
1. In Databricks UI → Repos → Add Repo → paste your GitHub URL  
2. All notebooks auto-sync on push

### 4. Run Tests Locally

```bash
# Unit tests (no Spark required — uses MagicMock)
pytest tests/ -v --tb=short

# Integration tests (requires running Spark)
pytest tests/ -v -m integration
```

---

## 🔬 Scenario Deep-Dives

### 1. Performance Degradation

**Symptom:** Queries that took 30s now take 8 minutes. No code changes deployed.

**Diagnosis checklist:**
```python
# Check file health
spark.sql("DESCRIBE DETAIL my_table").select("numFiles", "sizeInBytes").show()

# Check transaction log size
spark.sql("DESCRIBE HISTORY my_table").count()  # Should not exceed 10k versions without OPTIMIZE
```

**Solution architecture:**
```
Write path:  autoOptimize + binSize=128MB
Read path:   ZORDER on predicate columns
Scheduled:   OPTIMIZE weekly + VACUUM monthly
```

📓 Notebook: [`notebooks/01_performance_optimization.py`](notebooks/01_performance_optimization.py)  
🔧 Module: [`src/utils/delta_utils.py#optimize_table`](src/utils/delta_utils.py)

---

### 2. Inconsistent Concurrent Writes

**Symptom:** Duplicate rows appear after two jobs ran at the same time.

**Root cause:** Using `mode("append")` without dedup = fan-out on retry.

**Production pattern:**
```python
# Never do this for upserts:
df.write.mode("append").save(path)  # ❌ duplicates on retry

# Always do this:
delta_table.alias("t").merge(df.alias("s"), "t.id = s.id") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()  # ✅ idempotent
```

📓 Notebook: [`notebooks/02_concurrent_writes_debug.py`](notebooks/02_concurrent_writes_debug.py)

---

### 3. CDC Pipeline

**Symptom:** Downstream tables are hours behind source. Or: deletes never propagate.

**Architecture:** Delta CDF → Streaming MERGE → Silver/Gold

```
Source Table (CDF enabled)
    │  _change_type ∈ {insert, update_preimage, update_postimage, delete}
    ▼
Streaming Reader (readChangeData=true)
    │  Filter preimages, dedup by key + max(_commit_version)
    ▼
foreachBatch MERGE into Target
    │  Upserts → whenMatchedUpdateAll / whenNotMatchedInsertAll
    │  Deletes → whenMatchedDelete
    ▼
Target Silver Table (exactly-once via checkpoint)
```

📓 Notebook: [`notebooks/03_cdc_pipeline.py`](notebooks/03_cdc_pipeline.py)  
🔧 Module: [`src/pipelines/silver_transformation.py`](src/pipelines/silver_transformation.py)

---

### 4. Small Files

**Warning signs:**
- `numFiles > 10,000` on a table under 100GB
- Average file size < 32MB
- Auto Loader streaming with `processingTime='30 seconds'`

**Fix matrix:**

| Cause | Fix |
|---|---|
| Streaming micro-batches | Coarser trigger (`5 min` or `availableNow`) |
| Over-partitioned table | Remove low-cardinality partition columns |
| Missing autoOptimize | Enable `delta.autoOptimize.optimizeWrite=true` |
| Historical accumulation | One-time `OPTIMIZE` + enable Liquid Clustering |

📓 Notebook: [`notebooks/04_small_files_fix.py`](notebooks/04_small_files_fix.py)

---

### 5. Schema Evolution

**Symptom:** Pipeline fails at 3am after upstream team adds a nullable column.

**Decision tree:**
```
New column added?
  → additive: use mergeSchema=true ✅
  → rename: enable columnMapping first ✅

Column type changed?
  → widening (int→long): usually safe ✅
  → narrowing (double→int): reject + alert ❌

Column dropped?
  → version the table contract
  → use views to shield consumers ✅
```

📓 Notebook: [`notebooks/05_schema_evolution.py`](notebooks/05_schema_evolution.py)  
🔧 Module: [`src/utils/schema_utils.py`](src/utils/schema_utils.py)

---

### 6. Dual-Mode Serving (Batch + Real-Time)

**Architecture:** Medallion with two Gold paths

```
Silver (streaming, 2-min lag)
    ├──▶ Gold Realtime  (5-min windows, complete mode, dashboard)
    └──▶ Gold Daily     (full day aggregation, scheduled job, BI)

UNION VIEW combines both → single endpoint for consumers
```

📓 Notebook: [`notebooks/06_medallion_architecture.py`](notebooks/06_medallion_architecture.py)  
🔧 Modules: [`src/pipelines/`](src/pipelines/)

---

### 7. Delta vs Parquet Decision Framework

| Feature | Parquet | Delta Lake |
|---|---|---|
| ACID transactions | ❌ | ✅ |
| Upserts / Deletes | Read-modify-write | Native MERGE |
| Schema enforcement | None | Strict |
| Time travel | ❌ | ✅ 30-day default |
| Streaming + batch | Read-only | Full read/write |
| Scalable metadata | List API (slow) | Transaction log (O(1)) |
| GDPR right-to-erase | Partition drop hack | `DELETE` + `VACUUM` |

**Use Parquet when:** Write-once archival, pure ML training sets, no DML ever needed.  
**Use Delta when:** Any DML, streaming, schema changes, audit requirements.

📓 Notebook: [`notebooks/07_delta_vs_parquet.py`](notebooks/07_delta_vs_parquet.py)

---

### 8. Concurrent Multi-Job Writes

**Pattern: Partition isolation (best throughput)**
```python
# Job A: owns region=US — zero conflict with Job B
df.write.option("replaceWhere", "region='US'").mode("overwrite").save(path)

# Job B: owns region=EU — runs truly in parallel
df.write.option("replaceWhere", "region='EU'").mode("overwrite").save(path)
```

**Pattern: Retry with jitter (hot tables)**
```python
for attempt in range(max_retries):
    try:
        delta_table.merge(...).execute()
        break
    except ConcurrentModificationException:
        time.sleep((2 ** attempt) + random.uniform(0, 1))
```

📓 Notebook: [`notebooks/08_concurrent_jobs.py`](notebooks/08_concurrent_jobs.py)  
🔧 Module: [`src/utils/delta_utils.py#safe_merge`](src/utils/delta_utils.py)

---

### 9. VACUUM Governance

**The three rules of production VACUUM:**

1. **Never go below 168 hours** (7 days) — this is a hard floor, not a guideline
2. **Always dry run first** — `VACUUM table RETAIN X HOURS DRY RUN`
3. **Coordinate with streaming** — checkpoint retention must be ≤ table retention

**Retention tiers:**

| Layer | Log Retention | File Retention | Rationale |
|---|---|---|---|
| Bronze | 14 days | 7 days | Re-ingest from source if needed |
| Silver | 30 days | 14 days | ETL bug rollback window |
| Gold | 90 days | 30 days | Audit + regulatory buffer |
| Regulatory | 365 days | 90 days | Check GDPR/HIPAA requirements |

📓 Notebook: [`notebooks/09_vacuum_governance.py`](notebooks/09_vacuum_governance.py)

---

## 🧠 Key Design Principles

1. **Idempotency everywhere** — every pipeline should produce identical results on re-run
2. **Schema contracts** — define and enforce before consumers depend on them
3. **Observability first** — instrument writes, track file health, alert on drift
4. **Retention = recovery window** — tune VACUUM to your rollback SLA, not your storage bill
5. **Partition for pruning, cluster for skipping** — understand both, misuse neither

---

## 🔗 Resources

- [Delta Lake Official Docs](https://docs.delta.io/latest/index.html)
- [Databricks Best Practices](https://docs.databricks.com/delta/best-practices.html)
- [The Delta Lake Paper (VLDB 2020)](https://databricks.com/research/delta-lake-high-performance-acid-table-storage-under-cloud-object-stores)
- [Liquid Clustering Deep Dive](https://www.databricks.com/blog/2023/10/04/delta-lake-liquid-clustering.html)

---

## 🤝 Contributing

PRs welcome. Please:
1. Add/update the relevant test in `tests/`
2. Update the runbook in `docs/runbook.md` if adding operational patterns
3. Keep notebook cells self-contained (runnable independently)

---

## 📄 License

MIT — use freely, attribution appreciated.

---

*Built with production scars. Every pattern in here has a war story behind it.*
>>>>>>> 77de8f7 (initial project setup — Delta Lake production playbook)
