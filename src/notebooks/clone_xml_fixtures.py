# Databricks notebook source
# MAGIC %md
# MAGIC # Clone XML Fixtures (EMIR + MiFIR)
# MAGIC
# MAGIC Fans out parallel copies of the existing sample XML files in the landing volumes so that the
# MAGIC bronze pipelines have more inputs to ingest. The copy work runs on the driver via a
# MAGIC `ThreadPoolExecutor`. Each task is a single `shutil.copyfile` between two UC Volume paths;
# MAGIC the actual copy releases the GIL inside `sendfile`/`copy_file_range`, so a pool of N threads
# MAGIC gives near-linear scaling on I/O-bound writes. Serverless Spark Connect doesn't expose
# MAGIC `sc.parallelize` (the legacy RDD API), which is why we use threads instead of a Spark RDD
# MAGIC fan-out.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC For each enabled regulation it:
# MAGIC 1. Discovers the source XML in the landing folder (or uses the explicit override).
# MAGIC 2. Optionally backs up the existing landing/*.xml files server-side to a sibling
# MAGIC    `landing_backup_YYYYMMDD/` directory before cloning.
# MAGIC 3. Builds `num_clones` target paths under a fresh sibling subdirectory (e.g. `landing/clones/`)
# MAGIC    so the ESMA filename of each clone is **identical** to the original — the bronze loader's
# MAGIC    filename-regex still matches.
# MAGIC 4. Parallelizes the (src, dst) tuples across `num_partitions` threads and runs
# MAGIC    `shutil.copyfile` on each.
# MAGIC 5. Prints a summary (count, total bytes, elapsed seconds, MiB/s).
# MAGIC
# MAGIC ## Why this layout instead of varying filenames?
# MAGIC
# MAGIC The bronze pipeline keys files by **full path**, so duplicating filename under different
# MAGIC subdirectories produces distinct submission_file rows. Keeping the filename intact preserves
# MAGIC the ESMA naming-convention regex that extracts `file_batch_index` / `file_batch_size` /
# MAGIC `file_version` / `esma_date` into the silver tables. If you'd rather flatten everything into
# MAGIC the same directory and append a numeric suffix, set `flat_layout` to `true`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text(
    "emir_landing_path",
    "/Volumes/esma_dev/default/regulatory_data/emir/landing/",
    "EMIR landing root (must end with /)",
)
dbutils.widgets.text(
    "mifir_landing_path",
    "/Volumes/esma_dev/default/regulatory_data/mifir/landing/",
    "MiFIR landing root (must end with /)",
)
dbutils.widgets.text(
    "emir_source_file",
    "",
    "EMIR source override (blank = auto-pick first .xml in landing)",
)
dbutils.widgets.text(
    "mifir_source_file",
    "",
    "MiFIR source override (blank = auto-pick first .xml in landing)",
)
dbutils.widgets.text("num_clones", "200", "Clones per regulation")
dbutils.widgets.text(
    "num_partitions",
    "64",
    "Spark partitions (parallelism cap) — bump up for more concurrent copies",
)
dbutils.widgets.dropdown("clone_emir", "true", ["true", "false"], "Clone EMIR")
dbutils.widgets.dropdown("clone_mifir", "true", ["true", "false"], "Clone MiFIR")
dbutils.widgets.dropdown(
    "flat_layout",
    "false",
    ["true", "false"],
    "Flat layout (same dir + numeric suffix). False = clones/NNN/ subdirs (preserves filename regex).",
)
dbutils.widgets.dropdown(
    "subdir_name",
    "clones",
    ["clones", "synthetic", "load_test"],
    "Sibling subdir under landing/ when flat_layout=false",
)
dbutils.widgets.dropdown(
    "backup_sources",
    "true",
    ["true", "false"],
    "Server-side copy each landing/*.xml to landing_backup_YYYYMMDD/ before cloning",
)

emir_landing = dbutils.widgets.get("emir_landing_path")
mifir_landing = dbutils.widgets.get("mifir_landing_path")
emir_source_override = dbutils.widgets.get("emir_source_file").strip() or None
mifir_source_override = dbutils.widgets.get("mifir_source_file").strip() or None
num_clones = int(dbutils.widgets.get("num_clones"))
num_partitions = int(dbutils.widgets.get("num_partitions"))
clone_emir = dbutils.widgets.get("clone_emir") == "true"
clone_mifir = dbutils.widgets.get("clone_mifir") == "true"
flat_layout = dbutils.widgets.get("flat_layout") == "true"
subdir_name = dbutils.widgets.get("subdir_name")
backup_sources = dbutils.widgets.get("backup_sources") == "true"

print(f"emir_landing       = {emir_landing}")
print(f"mifir_landing      = {mifir_landing}")
print(f"num_clones         = {num_clones}")
print(f"num_partitions     = {num_partitions}")
print(f"clone_emir         = {clone_emir}")
print(f"clone_mifir        = {clone_mifir}")
print(f"flat_layout        = {flat_layout}")
print(f"subdir_name        = {subdir_name}")
print(f"backup_sources     = {backup_sources}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source resolution

# COMMAND ----------

import os


def _resolve_source(landing_dir: str, override: str | None) -> tuple[str, str]:
    """Return (source_path, source_filename). Uses override if given, else first .xml under landing_dir."""
    if override:
        if not os.path.isfile(override):
            raise FileNotFoundError(f"Source override not found: {override}")
        return override, os.path.basename(override)
    entries = sorted(
        e for e in os.listdir(landing_dir) if e.lower().endswith(".xml") and os.path.isfile(os.path.join(landing_dir, e))
    )
    if not entries:
        raise FileNotFoundError(f"No .xml file found in {landing_dir}")
    src = os.path.join(landing_dir, entries[0])
    return src, entries[0]


targets: list[tuple[str, str, str]] = []  # (label, source_path, dest_root_dir)

if clone_emir:
    emir_src, emir_name = _resolve_source(emir_landing, emir_source_override)
    emir_dest_root = emir_landing if flat_layout else os.path.join(emir_landing, subdir_name) + "/"
    print(f"EMIR  source: {emir_src} ({os.path.getsize(emir_src):,} bytes)")
    print(f"EMIR  dest root: {emir_dest_root}")
    targets.append(("emir", emir_src, emir_dest_root))

if clone_mifir:
    mifir_src, mifir_name = _resolve_source(mifir_landing, mifir_source_override)
    mifir_dest_root = mifir_landing if flat_layout else os.path.join(mifir_landing, subdir_name) + "/"
    print(f"MiFIR source: {mifir_src} ({os.path.getsize(mifir_src):,} bytes)")
    print(f"MiFIR dest root: {mifir_dest_root}")
    targets.append(("mifir", mifir_src, mifir_dest_root))

if not targets:
    dbutils.notebook.exit("Both clone_emir and clone_mifir set to false — nothing to do.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Server-side source backup (optional)
# MAGIC
# MAGIC Before cloning, copy each landing root's existing XMLs to a sibling `landing_backup_YYYYMMDD/`.
# MAGIC Runs on the cluster (POSIX `shutil.copyfile` between UC volume paths) — much faster than a
# MAGIC client-side `databricks fs cp` for multi-GB inputs.

# COMMAND ----------

if backup_sources:
    import datetime
    import shutil
    import time

    backup_stamp = datetime.datetime.utcnow().strftime("%Y%m%d")
    print(f"Backup stamp: {backup_stamp}")
    for label, _src, _dest_root in targets:
        landing = emir_landing if label == "emir" else mifir_landing
        # Pick up only direct-child *.xml files in landing/ (skip subdirs like processed/, clones/, backups themselves)
        candidates = sorted(
            os.path.join(landing, f)
            for f in os.listdir(landing)
            if f.lower().endswith(".xml") and os.path.isfile(os.path.join(landing, f))
        )
        # Strip trailing slash for parent computation
        parent = os.path.dirname(landing.rstrip("/"))
        backup_dir = os.path.join(parent, f"landing_backup_{backup_stamp}") + "/"
        os.makedirs(backup_dir, exist_ok=True)
        print(f"{label}: backing up {len(candidates)} file(s) → {backup_dir}")
        for c in candidates:
            dst = os.path.join(backup_dir, os.path.basename(c))
            t0 = time.time()
            size = os.path.getsize(c)
            shutil.copyfile(c, dst)
            elapsed = time.time() - t0
            mbs = size / max(elapsed, 0.001) / (1024 * 1024)
            print(f"  {os.path.basename(c):60s}  {size:>14,} bytes  in {elapsed:6.2f}s  ({mbs:6.1f} MiB/s)")
else:
    print("backup_sources=false — skipping backup step.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build (source, destination) tuples
# MAGIC
# MAGIC Destination layout:
# MAGIC - `flat_layout=false`: `{landing}/{subdir_name}/{NNN}/{original_filename}` — preserves filename
# MAGIC   regex, NNN is zero-padded to width `len(str(num_clones))`.
# MAGIC - `flat_layout=true`:  `{landing}/{stem}__clone_{NNN}{ext}` — same dir, regex won't match.

# COMMAND ----------

def _build_pairs(source_path: str, dest_root: str, n: int, flat: bool) -> list[tuple[str, str]]:
    width = max(3, len(str(n)))
    filename = os.path.basename(source_path)
    stem, ext = os.path.splitext(filename)
    pairs: list[tuple[str, str]] = []
    for i in range(1, n + 1):
        if flat:
            dst = f"{dest_root}{stem}__clone_{i:0{width}d}{ext}"
        else:
            dst = f"{dest_root}{i:0{width}d}/{filename}"
        pairs.append((source_path, dst))
    return pairs


all_pairs: list[tuple[str, str]] = []
per_label_counts: dict[str, int] = {}
for label, src, dest_root in targets:
    pairs = _build_pairs(src, dest_root, num_clones, flat_layout)
    all_pairs.extend(pairs)
    per_label_counts[label] = len(pairs)
    print(f"{label}: {len(pairs)} pairs queued; first dest = {pairs[0][1]}")

print(f"\nTotal copies to perform: {len(all_pairs)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parallel copy via ThreadPoolExecutor
# MAGIC
# MAGIC Each task is a self-contained `(src, dst) → shutil.copyfile` with `os.makedirs(parent, exist_ok=True)`
# MAGIC so multiple tasks creating the same NNN subdirectory race-safely. Errors are returned per task
# MAGIC and summarized at the end — one failed copy does not abort the rest of the job.
# MAGIC
# MAGIC `shutil.copyfile` on UC Volume paths uses `sendfile`/`copy_file_range` under the hood; the actual
# MAGIC copy releases the GIL, so a thread pool of `num_partitions` workers gets near-linear scaling on
# MAGIC I/O-bound writes.

# COMMAND ----------

import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def _copy_task(pair: tuple[str, str]) -> tuple[str, str, str, int, str | None]:
    """Returns (src, dst, status, bytes, error). status ∈ {'OK', 'ERROR'}."""
    src, dst = pair
    try:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(src, dst)
        return (src, dst, "OK", os.path.getsize(dst), None)
    except Exception as exc:  # noqa: BLE001
        return (src, dst, "ERROR", 0, repr(exc))


workers = max(1, min(num_partitions, len(all_pairs)))
print(f"Dispatching {len(all_pairs)} copies across {workers} threads...")

t0 = time.time()
results: list[tuple[str, str, str, int, str | None]] = []
last_print = t0
with ThreadPoolExecutor(max_workers=workers) as ex:
    futures = [ex.submit(_copy_task, p) for p in all_pairs]
    for fut in as_completed(futures):
        results.append(fut.result())
        # Progress every 20 copies so the driver log shows liveness.
        if len(results) % 20 == 0 or len(results) == len(futures):
            now = time.time()
            print(
                f"  progress: {len(results):>4} / {len(futures)}  "
                f"(elapsed {now - t0:6.1f}s, last interval {now - last_print:5.1f}s)"
            )
            last_print = now
elapsed = time.time() - t0

ok = [r for r in results if r[2] == "OK"]
errors = [r for r in results if r[2] == "ERROR"]
total_bytes = sum(r[3] for r in ok)

print(
    f"\nDone in {elapsed:.1f}s — {len(ok)} OK, {len(errors)} ERROR, "
    f"{total_bytes:,} bytes written, "
    f"throughput ≈ {total_bytes / max(elapsed, 0.001) / (1024 * 1024):.1f} MiB/s"
)

if errors:
    print("\nFirst 10 errors:")
    for r in errors[:10]:
        print(f"  {r[1]}  →  {r[4]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification — list landing trees and counts

# COMMAND ----------

from pyspark.sql import functions as F


def _file_summary(landing_dir: str, label: str) -> None:
    files_df = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.xml")
        .option("recursiveFileLookup", "true")
        .load(landing_dir)
        .select(
            F.col("path"),
            F.col("length").alias("bytes"),
            F.col("modificationTime"),
        )
    )
    total = files_df.count()
    total_bytes = files_df.agg(F.sum("bytes")).first()[0] or 0
    print(f"\n=== {label} ({landing_dir}) ===")
    print(f"XML files (recursive): {total} — total bytes: {total_bytes:,}")
    files_df.orderBy(F.desc("modificationTime")).limit(5).show(truncate=False)


for label, _src, dest_root in targets:
    landing = emir_landing if label == "emir" else mifir_landing
    _file_summary(landing, label.upper())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC After running this notebook, trigger a fresh refresh on the bronze pipelines so Auto Loader
# MAGIC picks up the new files:
# MAGIC
# MAGIC ```bash
# MAGIC databricks --profile azure pipelines start-update <emir_bronze_pipeline_id>
# MAGIC databricks --profile azure pipelines start-update <mifir_bronze_pipeline_id>
# MAGIC ```
# MAGIC
# MAGIC No `--full-refresh` needed — the new files are incrementally picked up. To get rid of the
# MAGIC clones later:
# MAGIC
# MAGIC ```python
# MAGIC dbutils.fs.rm("/Volumes/esma_dev/default/regulatory_data/emir/landing/clones/",  True)
# MAGIC dbutils.fs.rm("/Volumes/esma_dev/default/regulatory_data/mifir/landing/clones/", True)
# MAGIC ```
