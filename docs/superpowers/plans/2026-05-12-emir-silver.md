# EMIR Silver Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a domain-driven EMIR REFIT DAT TSR silver layer (4 tables — `trade`, `trade_schedule`, `trade_beneficiary`, `submission_file`) on top of the bronze table `emir_raw` shipped in PR #1, using a new Spark Declarative Pipeline.

**Architecture:** A new SDP source file `src/pipelines/silver_emir.py` defines four `@dp.table()` streaming tables that read from `users.matthew_moorcroft.emir_raw`. The `trade` table is wide-flat (~232 scalar columns + 5 ARRAY + 1 ARRAY<STRUCT> + 1 STRUCT) with business-readable column names mapping each XSD path to a domain-meaningful name. `trade_schedule` unifies six schedule arrays via a `schedule_type` discriminator. `trade_beneficiary` and `submission_file` are explode + dedup tables respectively. Append-only, partition/cluster by `reporting_date`, serverless + Photon, `cluster_by_auto=True`.

**Tech Stack:** Python 3 + `pyspark.pipelines` (`from pyspark import pipelines as dp`), Delta Lake, Unity Catalog, Databricks Asset Bundles, serverless SDP compute on E2 (`e2-demo-field-eng.cloud.databricks.com`).

**Reference spec:** [`docs/superpowers/specs/2026-05-12-emir-silver-design.md`](../specs/2026-05-12-emir-silver-design.md)

---

## File Plan

| File | Action | Responsibility |
|------|--------|---------------|
| `src/pipelines/silver_emir.py` | Create | All 4 `@dp.table()` definitions, ~750 lines, parameterized via `spark.conf` |
| `resources/bundle.emir_resources.yml` | Modify | Add `emir_silver_pipeline` under the existing `# === Spark Declarative Pipelines ===` section |
| `databricks.yml` | Modify | Add `development: true|false` target overrides for `emir_silver_pipeline` in dev + prod |
| `docs/superpowers/plans/2026-05-12-emir-silver.md` | Create | This file |
| `docs/superpowers/plans/2026-05-12-emir-silver-smoke-test-results.md` | Create | Captured at Task 14 |

**File splits / decomposition:**
- One SDP source file holds all four tables because they share the same bronze source read, the same module-level config, and the same SDP-runtime concerns. Splitting per-table would force duplicate imports + config reads.
- The `trade` table's body is the bulk of the file (~600 lines of `.select(...).alias()` mappings). Each task in this plan adds one logical XSD section's worth of columns — incremental, reviewable commits.

---

## Branch Setup

Work is performed on branch `feat/emir-silver` (already created, currently at commit `9d46479` which is the spec).

---

## Task 1: Scaffold `src/pipelines/silver_emir.py`

**Files:**
- Create: `src/pipelines/silver_emir.py`

This task lays down the module skeleton: imports, module-level config reads, fully-qualified table-name constants, and one tiny helper. No `@dp.table` definitions yet — those land in subsequent tasks.

- [ ] **Step 1.1: Confirm branch state**

Run:
```bash
git status && git branch --show-current && git log --oneline -3
```
Expected: clean tree, branch `feat/emir-silver`, HEAD at `9d46479`.

- [ ] **Step 1.2: Create the file with skeleton**

Write `src/pipelines/silver_emir.py` containing exactly:

```python
"""ESMA EMIR REFIT DAT TSR Silver Layer.

Domain-driven silver layer on top of bronze ``emir_raw``. Four tables:

* ``trade`` — wide-flat fact table, one row per ``<Stat>`` per submission
  snapshot. ~232 scalar columns + array/struct columns for long-tail.
* ``trade_schedule`` — unified schedule periods (price + notional amount/qty
  for first/second legs + strike-price schedule for options) with
  ``schedule_type`` discriminator.
* ``trade_beneficiary`` — exploded beneficiary array.
* ``submission_file`` — one row per ingested XML file (regulation-agnostic
  envelope).

All inputs are supplied via ``spark.conf`` — see the EMIR silver pipeline
``configuration`` block in ``resources/bundle.emir_resources.yml``.

Reference: docs/superpowers/specs/2026-05-12-emir-silver-design.md
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# --------------------------------------------------------------------------
# Pipeline configuration (set in resources/bundle.emir_resources.yml under
# resources.pipelines.emir_silver_pipeline.configuration).
# --------------------------------------------------------------------------

CATALOG = spark.conf.get("catalog")
RAW_SCHEMA = spark.conf.get("raw_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema", RAW_SCHEMA)
BRONZE_TABLE_NAME = spark.conf.get("bronze_table")
REGULATION = spark.conf.get("regulation", "EMIR")

TBL_BRONZE = f"{CATALOG}.{RAW_SCHEMA}.{BRONZE_TABLE_NAME}"
TBL_TRADE = f"{CATALOG}.{SILVER_SCHEMA}.trade"
TBL_TRADE_SCHEDULE = f"{CATALOG}.{SILVER_SCHEMA}.trade_schedule"
TBL_TRADE_BENEFICIARY = f"{CATALOG}.{SILVER_SCHEMA}.trade_beneficiary"
TBL_SUBMISSION_FILE = f"{CATALOG}.{SILVER_SCHEMA}.submission_file"


def _reporting_date(df: DataFrame) -> DataFrame:
    """Add a reporting_date DATE column parsed from ESMADate or filename.

    ESMADate from the bronze regex is in 'YY-MM-DD' format (e.g.,
    '24-12-15'). Convert to a proper DATE assuming 20YY century.
    """
    return df.withColumn(
        "reporting_date",
        F.when(
            F.col("ESMADate").rlike(r"^\d\d-\d\d-\d\d$"),
            F.to_date(F.concat(F.lit("20"), F.col("ESMADate")), "yyyy-MM-dd"),
        ).otherwise(F.to_date(F.col("_file_modification_time")))
    )
```

- [ ] **Step 1.3: Verify the file parses**

Run:
```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_emir.py').read())"
```
Expected: no output (success).

- [ ] **Step 1.4: Commit**

```bash
git add src/pipelines/silver_emir.py
git commit -m "$(cat <<'EOF'
feat(silver): scaffold silver_emir.py module skeleton

Adds the EMIR silver SDP source file with:
- Module docstring + design-doc reference
- Modern API import (from pyspark import pipelines as dp)
- Module-level spark.conf reads for catalog, raw_schema, silver_schema,
  bronze_table, regulation
- Four fully-qualified table-name constants (TBL_TRADE,
  TBL_TRADE_SCHEDULE, TBL_TRADE_BENEFICIARY, TBL_SUBMISSION_FILE)
- _reporting_date() helper that parses YY-MM-DD ESMADate into a real DATE

No @dp.table definitions yet — those land in subsequent commits.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 2: Add `submission_file` table

**Files:**
- Modify: `src/pipelines/silver_emir.py` (append)

`submission_file` is the simplest of the four — one row per ingested file with the header struct fields and filename-regex fields. Built via `dropDuplicates(["file_path"])` over the bronze stream.

- [ ] **Step 2.1: Append the @dp.table block**

Append to `src/pipelines/silver_emir.py`:

```python


# --------------------------------------------------------------------------
# Table 1 of 4: submission_file (file-level envelope)
#
# Regulation-agnostic. MiFIR (and any future regulation) writes to the
# same table with regulation='MIFIR' under its own silver pipeline.
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_SUBMISSION_FILE,
    comment=(
        "Public: one row per ingested ESMA XML file. Regulation-agnostic "
        "envelope shared across EMIR/MiFIR. Built from a dropDuplicates "
        "over the bronze stream."
    ),
    cluster_by_auto=True,
)
def submission_file():
    return (
        _reporting_date(spark.readStream.table(TBL_BRONZE))
        .dropDuplicates(["file_path"])
        .select(
            F.col("file_path"),
            F.col("file_name"),
            F.col("reporting_date"),
            F.col("ESMADate").alias("esma_date_str"),
            F.col("FileBatchIndex").cast("int").alias("batch_index"),
            F.col("FileBatchSize").cast("int").alias("batch_size"),
            F.col("FileVersion").cast("int").alias("file_version"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.BizMsgIdr").alias("biz_msg_id"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.Fr.OrgId.Id.OrgId.Othr.Id").alias("sender_lei"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.To.OrgId.Id.OrgId.Othr.Id").alias("recipient_lei"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.MsgDefIdr").alias("message_def_id"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.BizSvc").alias("business_service"),
            F.col("hdr_pyld_metadata.Hdr.AppHdr.CreDt").alias("header_creation_ts"),
            F.col("hdr_pyld_metadata.Pyld.Document.DerivsTradStatRpt.RptHdr.NbRcrds").cast("bigint").alias("number_of_records"),
            F.col("hdr_pyld_metadata.Pyld.Document.DerivsTradStatRpt.TradData.DataSetActn").alias("data_set_action"),
            F.col("_ingested_at").alias("ingested_at"),
            F.current_timestamp().alias("silver_processed_at"),
            F.lit(REGULATION).alias("regulation"),
        )
    )
```

- [ ] **Step 2.2: Verify parses**

Run:
```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_emir.py').read())"
```
Expected: silent success.

- [ ] **Step 2.3: Commit**

```bash
git add src/pipelines/silver_emir.py
git commit -m "$(cat <<'EOF'
feat(silver): add submission_file table

First of four @dp.table definitions. One row per ingested ESMA XML
file via dropDuplicates(['file_path']) over the bronze stream. Pulls
file-level fields from the bronze hdr_pyld_metadata struct
(BizMsgIdr, sender/recipient LEI from Fr/To, MsgDefIdr, BizSvc, CreDt,
NbRcrds, DataSetActn) plus the filename-regex columns parsed during
bronze ingestion.

Regulation-agnostic shape — MiFIR silver will write to the same table
with regulation='MIFIR' from its own pipeline.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 3: Add `trade_beneficiary` table

**Files:**
- Modify: `src/pipelines/silver_emir.py` (append)

Exploded view of `CtrPtySpcfcData.CtrPty.Bnfcry[]` — one row per beneficiary per trade. Type discriminator from which branch (Lgl vs Ntrl) is populated.

- [ ] **Step 3.1: Append the @dp.table block**

Append to `src/pipelines/silver_emir.py`:

```python


# --------------------------------------------------------------------------
# Table 2 of 4: trade_beneficiary (exploded array)
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_TRADE_BENEFICIARY,
    comment=(
        "Public: one row per beneficiary per trade. Exploded from "
        "CtrPtySpcfcData.CtrPty.Bnfcry[]. beneficiary_type column "
        "discriminates Lgl (legal entity, LEI) vs Ntrl (natural person)."
    ),
    cluster_by_auto=True,
)
def trade_beneficiary():
    bronze = _reporting_date(spark.readStream.table(TBL_BRONZE))
    exploded = (
        bronze
        .select(
            F.col("CmonTradData.TxData.TxId.UnqTxIdr").alias("trade_id"),
            F.col("reporting_date"),
            F.col("_ingested_at"),
            F.posexplode_outer(F.col("CtrPtySpcfcData.CtrPty.Bnfcry")).alias("sequence_no", "bnfcry"),
        )
        .filter(F.col("bnfcry").isNotNull())
    )
    return exploded.select(
        "trade_id",
        "reporting_date",
        "sequence_no",
        F.col("bnfcry.Lgl.Id.LEI").alias("beneficiary_lei"),
        F.col("bnfcry.Lgl.Id.Othr.Id.Id").alias("beneficiary_other_id"),
        F.col("bnfcry.Ntrl.Id.Id.Id").alias("beneficiary_natural_person_id"),
        F.when(F.col("bnfcry.Lgl.Id.LEI").isNotNull(), F.lit("LEGAL"))
         .when(F.col("bnfcry.Ntrl.Id.Id.Id").isNotNull(), F.lit("NATURAL"))
         .otherwise(F.lit("OTHER"))
         .alias("beneficiary_type"),
        F.col("_ingested_at").alias("ingested_at"),
        F.current_timestamp().alias("silver_processed_at"),
    )
```

- [ ] **Step 3.2: Verify parses**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_emir.py').read())"
```

- [ ] **Step 3.3: Commit**

```bash
git add src/pipelines/silver_emir.py
git commit -m "$(cat <<'EOF'
feat(silver): add trade_beneficiary table

Second of four @dp.table definitions. Posexplode-outer of
CtrPtySpcfcData.CtrPty.Bnfcry[] with type discriminator that picks
LEGAL when Lgl.Id.LEI is populated, NATURAL when Ntrl.Id.Id.Id is
populated, else OTHER. Drops rows where the beneficiary struct itself
is NULL (i.e., trades with no beneficiaries — most of them).

Co-authored-by: Isaac
EOF
)"
```

---

## Task 4: Add `trade_schedule` table

**Files:**
- Modify: `src/pipelines/silver_emir.py` (append)

Unifies six different schedule arrays into one table via a `schedule_type` discriminator. Each schedule type has its own source path and produces a different shape of columns; the union selects unified columns with NULL for fields that don't apply.

- [ ] **Step 4.1: Append the @dp.table block**

Append to `src/pipelines/silver_emir.py`:

```python


# --------------------------------------------------------------------------
# Table 3 of 4: trade_schedule (six schedule arrays unified)
#
# Source paths:
#   TxPric.SchdlPrd[]                     -> PRICE
#   NtnlAmt.FrstLeg.SchdlPrd[]            -> NTNL_AMT_LEG_1
#   NtnlAmt.ScndLeg.SchdlPrd[]            -> NTNL_AMT_LEG_2
#   NtnlQty.FrstLeg.Dtls.SchdlPrd[]       -> NTNL_QTY_LEG_1
#   NtnlQty.ScndLeg.Dtls.SchdlPrd[]       -> NTNL_QTY_LEG_2
#   Optn.StrkPricSchdl[]                  -> STRIKE
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_TRADE_SCHEDULE,
    comment=(
        "Public: unified schedule periods across price, notional amount/qty "
        "first/second legs, and option strike-price schedule. schedule_type "
        "discriminator column says which source path each row came from."
    ),
    cluster_by_auto=True,
)
def trade_schedule():
    bronze = _reporting_date(spark.readStream.table(TBL_BRONZE))
    base = bronze.select(
        F.col("CmonTradData.TxData.TxId.UnqTxIdr").alias("trade_id"),
        F.col("reporting_date"),
        F.col("_ingested_at"),
        F.col("CmonTradData.TxData.TxPric.SchdlPrd").alias("_price_schdl"),
        F.col("CmonTradData.TxData.NtnlAmt.FrstLeg.SchdlPrd").alias("_ntnl_amt_leg1"),
        F.col("CmonTradData.TxData.NtnlAmt.ScndLeg.SchdlPrd").alias("_ntnl_amt_leg2"),
        F.col("CmonTradData.TxData.NtnlQty.FrstLeg.Dtls.SchdlPrd").alias("_ntnl_qty_leg1"),
        F.col("CmonTradData.TxData.NtnlQty.ScndLeg.Dtls.SchdlPrd").alias("_ntnl_qty_leg2"),
        F.col("CmonTradData.TxData.Optn.StrkPricSchdl").alias("_strike_schdl"),
    )

    def _schedule(arr_col: str, schedule_type: str, mapper):
        """Posexplode-outer one schedule array, apply a row-shape mapper."""
        return (
            base.select(
                "trade_id", "reporting_date", "_ingested_at",
                F.posexplode_outer(F.col(arr_col)).alias("sequence_no", "_row"),
            )
            .filter(F.col("_row").isNotNull())
            .select(
                "trade_id", "reporting_date", "_ingested_at", "sequence_no",
                F.lit(schedule_type).alias("schedule_type"),
                *mapper(F.col("_row")),
            )
        )

    def _unified_cols(eff, end, amt, amt_ccy, amt_sgn, pct, qty):
        return [
            (eff or F.lit(None).cast("date")).alias("unadj_effective_dt"),
            (end or F.lit(None).cast("date")).alias("unadj_end_dt"),
            (amt or F.lit(None).cast("decimal(25,19)")).alias("amount"),
            (amt_ccy or F.lit(None).cast("string")).alias("amount_ccy"),
            (amt_sgn or F.lit(None).cast("boolean")).alias("amount_sign"),
            (pct or F.lit(None).cast("decimal(11,10)")).alias("percentage"),
            (qty or F.lit(None).cast("decimal(25,5)")).alias("quantity"),
        ]

    price_df = _schedule(
        "_price_schdl", "PRICE",
        lambda r: _unified_cols(
            r["UadjstdFctvDt"], r["UadjstdEndDt"],
            r["Pric"]["MntryVal"]["Amt"]["_VALUE"],
            r["Pric"]["MntryVal"]["Amt"]["_Ccy"],
            r["Pric"]["MntryVal"]["Sgn"],
            r["Pric"]["Pctg"], None,
        ),
    )
    ntnl_amt1_df = _schedule(
        "_ntnl_amt_leg1", "NTNL_AMT_LEG_1",
        lambda r: _unified_cols(
            r["UadjstdFctvDt"], r["UadjstdEndDt"],
            r["Amt"]["Amt"]["_VALUE"], r["Amt"]["Amt"]["_Ccy"], None,
            None, None,
        ),
    )
    ntnl_amt2_df = _schedule(
        "_ntnl_amt_leg2", "NTNL_AMT_LEG_2",
        lambda r: _unified_cols(
            r["UadjstdFctvDt"], r["UadjstdEndDt"],
            r["Amt"]["Amt"]["_VALUE"], r["Amt"]["Amt"]["_Ccy"], None,
            None, None,
        ),
    )
    ntnl_qty1_df = _schedule(
        "_ntnl_qty_leg1", "NTNL_QTY_LEG_1",
        lambda r: _unified_cols(
            r["UadjstdFctvDt"], r["UadjstdEndDt"], None, None, None,
            None, r["Qty"],
        ),
    )
    ntnl_qty2_df = _schedule(
        "_ntnl_qty_leg2", "NTNL_QTY_LEG_2",
        lambda r: _unified_cols(
            r["UadjstdFctvDt"], r["UadjstdEndDt"], None, None, None,
            None, r["Qty"],
        ),
    )
    # Note: StrkPricSchdl row shape is assumed to match
    # {UadjstdFctvDt, UadjstdEndDt, StrkPric: {MntryVal: {Amt: {_VALUE, _Ccy}, Sgn}}}.
    # If the actual bronze struct shape differs (it's product-specific),
    # the pipeline run will fail with a clear "field not found in struct"
    # error and the path here needs adjustment.
    strike_df = _schedule(
        "_strike_schdl", "STRIKE",
        lambda r: _unified_cols(
            r["UadjstdFctvDt"], r["UadjstdEndDt"],
            r["StrkPric"]["MntryVal"]["Amt"]["_VALUE"],
            r["StrkPric"]["MntryVal"]["Amt"]["_Ccy"],
            r["StrkPric"]["MntryVal"]["Sgn"],
            None, None,
        ),
    )

    unioned = (
        price_df
        .unionByName(ntnl_amt1_df, allowMissingColumns=True)
        .unionByName(ntnl_amt2_df, allowMissingColumns=True)
        .unionByName(ntnl_qty1_df, allowMissingColumns=True)
        .unionByName(ntnl_qty2_df, allowMissingColumns=True)
        .unionByName(strike_df, allowMissingColumns=True)
    )
    return unioned.withColumn("silver_processed_at", F.current_timestamp())
```

Important: the `StrkPricSchdl` row shape depends on the precise XSD nesting — the spec uses `r["StrkPric"]["MntryVal"]["Amt"]["_VALUE"]`; if the bronze schema's actual struct shape differs, the `_schedule("_strike_schdl", ...)` mapper needs adjustment at deploy time. Surface as a fixable error during validation, not a blocker for parse.

- [ ] **Step 4.2: Verify parses**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_emir.py').read())"
```

- [ ] **Step 4.3: Commit**

```bash
git add src/pipelines/silver_emir.py
git commit -m "$(cat <<'EOF'
feat(silver): add trade_schedule table

Third of four @dp.table definitions. Unions six posexplode-outer
operations (one per schedule type) into a single table with a
schedule_type discriminator: PRICE, NTNL_AMT_LEG_1, NTNL_AMT_LEG_2,
NTNL_QTY_LEG_1, NTNL_QTY_LEG_2, STRIKE. Unified row shape is
(trade_id, reporting_date, sequence_no, schedule_type,
unadj_effective_dt, unadj_end_dt, amount, amount_ccy, amount_sign,
percentage, quantity) — fields not applicable to a given source path
land as NULL.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 5: Trade table — scaffold function + identification fields

**Files:**
- Modify: `src/pipelines/silver_emir.py` (append)

The `trade` table is built in multiple incremental commits — one logical XSD section per commit. This task creates the function shell and the first batch (identification / trade-ID columns).

- [ ] **Step 5.1: Append the @dp.table function shell + first column group**

Append to `src/pipelines/silver_emir.py`:

```python


# --------------------------------------------------------------------------
# Table 4 of 4: trade (main fact, ~232 scalar cols + 5 arrays + 1 struct)
#
# One row per <Stat> per submission snapshot. Wide-flat by design;
# business-readable column names; choice fields collapsed to LEI common
# branch + *_other_id fallback. See spec §4.0 for the decision rule.
#
# Built incrementally — each commit adds one logical XSD section's
# columns to the .select(...) below.
# --------------------------------------------------------------------------


@dp.table(
    name=TBL_TRADE,
    comment=(
        "Public: per-trade snapshot, wide-flat with business-readable "
        "column names. Choice fields collapsed to LEI primary + "
        "*_other_id fallback. Partition/cluster by reporting_date. "
        "Append-only — each daily snapshot lands as new rows. See "
        "spec docs/superpowers/specs/2026-05-12-emir-silver-design.md."
    ),
    cluster_by_auto=True,
)
def trade():
    src = _reporting_date(spark.readStream.table(TBL_BRONZE))
    return src.select(
        # === Identification ===
        F.col("CmonTradData.TxData.TxId.UnqTxIdr").alias("trade_id"),
        F.col("CmonTradData.TxData.TxId.Prtry.Id").alias("trade_id_proprietary"),
        F.col("CmonTradData.TxData.PrrTxId.UnqTxIdr").alias("prior_trade_id"),
        F.col("CmonTradData.TxData.PrrTxId.Prtry.Id").alias("prior_trade_id_proprietary"),
        F.col("CmonTradData.TxData.PrrTxId.NotAvlbl").alias("prior_trade_id_not_available"),
        F.col("CmonTradData.TxData.SbsqntTxId.UnqTxIdr").alias("subsequent_trade_id"),
        F.col("CmonTradData.TxData.SbsqntTxId.Prtry.Id").alias("subsequent_trade_id_proprietary"),
        F.col("CmonTradData.TxData.SbsqntTxId.NotAvlbl").alias("subsequent_trade_id_not_available"),
        F.col("CmonTradData.TxData.RptTrckgNb").alias("report_tracking_number"),
        F.col("CmonTradData.TxData.PltfmIdr").alias("platform_id"),
        # === Audit / lineage (added early so the function returns a real DF) ===
        F.col("reporting_date"),
        F.col("file_path"),
        F.col("file_name"),
        F.col("_ingested_at").alias("ingested_at"),
        F.current_timestamp().alias("silver_processed_at"),
    )
```

This is intentionally minimal — only the identification columns plus essential lineage columns to make the function return a valid DataFrame at this commit. Subsequent tasks `.select()` more columns by replacing the `.select(...)` body.

- [ ] **Step 5.2: Verify parses**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_emir.py').read())"
```

- [ ] **Step 5.3: Commit**

```bash
git add src/pipelines/silver_emir.py
git commit -m "$(cat <<'EOF'
feat(silver): scaffold trade table with identification columns

Fourth of four @dp.table definitions, started incrementally. This
commit adds the function shell + the Identification section's
columns (trade_id, trade_id_proprietary, prior_/subsequent_*,
report_tracking_number, platform_id) plus essential lineage columns
(reporting_date, file_path, file_name, ingested_at,
silver_processed_at) so the function returns a valid streaming
DataFrame. Subsequent commits expand the .select() to cover the
remaining XSD sections.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 6: Trade — counterparty roles + contract data

**Files:**
- Modify: `src/pipelines/silver_emir.py` (replace `trade()` body)

Adds the counterparty role columns (reporter, other CP, broker, submitting agent, clearing member, entity responsible) and the CtrctData section.

- [ ] **Step 6.1: Replace the `trade()` function body with the expanded select**

In `src/pipelines/silver_emir.py`, find the `def trade():` function. Replace its entire body with:

```python
def trade():
    src = _reporting_date(spark.readStream.table(TBL_BRONZE))
    cp = "CtrPtySpcfcData.CtrPty"
    txd = "CmonTradData.TxData"
    cd = "CmonTradData.CtrctData"
    return src.select(
        # === Identification ===
        F.col(f"{txd}.TxId.UnqTxIdr").alias("trade_id"),
        F.col(f"{txd}.TxId.Prtry.Id").alias("trade_id_proprietary"),
        F.col(f"{txd}.PrrTxId.UnqTxIdr").alias("prior_trade_id"),
        F.col(f"{txd}.PrrTxId.Prtry.Id").alias("prior_trade_id_proprietary"),
        F.col(f"{txd}.PrrTxId.NotAvlbl").alias("prior_trade_id_not_available"),
        F.col(f"{txd}.SbsqntTxId.UnqTxIdr").alias("subsequent_trade_id"),
        F.col(f"{txd}.SbsqntTxId.Prtry.Id").alias("subsequent_trade_id_proprietary"),
        F.col(f"{txd}.SbsqntTxId.NotAvlbl").alias("subsequent_trade_id_not_available"),
        F.col(f"{txd}.RptTrckgNb").alias("report_tracking_number"),
        F.col(f"{txd}.PltfmIdr").alias("platform_id"),

        # === Reporting counterparty (CtrPty.RptgCtrPty) ===
        F.col(f"{cp}.RptgCtrPty.Id.Lgl.Id.LEI").alias("reporter_lei"),
        F.col(f"{cp}.RptgCtrPty.Id.Lgl.Id.Othr.Id.Id").alias("reporter_other_id"),
        F.when(F.col(f"{cp}.RptgCtrPty.Ntr.FI").isNotNull(), F.lit("FI"))
         .when(F.col(f"{cp}.RptgCtrPty.Ntr.NFI").isNotNull(), F.lit("NFI"))
         .when(F.col(f"{cp}.RptgCtrPty.Ntr.CntrlCntrPty").isNotNull(), F.lit("CCP"))
         .when(F.col(f"{cp}.RptgCtrPty.Ntr.Othr").isNotNull(), F.lit("OTHR"))
         .alias("reporter_nature"),
        F.coalesce(
            F.transform(F.col(f"{cp}.RptgCtrPty.Ntr.FI.Sctr"), lambda x: x["Cd"]),
            F.transform(F.col(f"{cp}.RptgCtrPty.Ntr.NFI.Sctr"), lambda x: x["Id"]),
        ).alias("reporter_sectors"),
        F.coalesce(
            F.col(f"{cp}.RptgCtrPty.Ntr.FI.ClrThrshld"),
            F.col(f"{cp}.RptgCtrPty.Ntr.NFI.ClrThrshld"),
        ).alias("reporter_clr_threshold"),
        F.col(f"{cp}.RptgCtrPty.Ntr.NFI.DrctlyLkdActvty").alias("reporter_nfi_directly_linked_activity"),
        F.col(f"{cp}.RptgCtrPty.Ntr.CntrlCntrPty").isNotNull().alias("reporter_is_central_counterparty"),
        F.col(f"{cp}.RptgCtrPty.TradgCpcty").alias("reporter_trading_capacity"),
        F.col(f"{cp}.RptgCtrPty.DrctnOrSd.Drctn.DrctnOfTheFrstLeg").alias("reporter_direction_first_leg"),
        F.col(f"{cp}.RptgCtrPty.DrctnOrSd.Drctn.DrctnOfTheScndLeg").alias("reporter_direction_second_leg"),
        F.col(f"{cp}.RptgCtrPty.DrctnOrSd.CtrPtySd").alias("reporter_side"),

        # === Other counterparty (CtrPty.OthrCtrPty) ===
        F.col(f"{cp}.OthrCtrPty.IdTp.Lgl.Id.LEI").alias("other_cp_lei"),
        F.coalesce(
            F.col(f"{cp}.OthrCtrPty.IdTp.Lgl.Ctry"),
            F.col(f"{cp}.OthrCtrPty.IdTp.Ntrl.Ctry"),
        ).alias("other_cp_country"),
        F.col(f"{cp}.OthrCtrPty.IdTp.Ntrl.Id.Id.Id").alias("other_cp_natural_person_id"),
        F.when(F.col(f"{cp}.OthrCtrPty.Ntr.FI").isNotNull(), F.lit("FI"))
         .when(F.col(f"{cp}.OthrCtrPty.Ntr.NFI").isNotNull(), F.lit("NFI"))
         .when(F.col(f"{cp}.OthrCtrPty.Ntr.CntrlCntrPty").isNotNull(), F.lit("CCP"))
         .when(F.col(f"{cp}.OthrCtrPty.Ntr.Othr").isNotNull(), F.lit("OTHR"))
         .alias("other_cp_nature"),
        F.coalesce(
            F.transform(F.col(f"{cp}.OthrCtrPty.Ntr.FI.Sctr"), lambda x: x["Cd"]),
            F.transform(F.col(f"{cp}.OthrCtrPty.Ntr.NFI.Sctr"), lambda x: x["Id"]),
        ).alias("other_cp_sectors"),
        F.coalesce(
            F.col(f"{cp}.OthrCtrPty.Ntr.FI.ClrThrshld"),
            F.col(f"{cp}.OthrCtrPty.Ntr.NFI.ClrThrshld"),
        ).alias("other_cp_clr_threshold"),
        F.col(f"{cp}.OthrCtrPty.Ntr.CntrlCntrPty").isNotNull().alias("other_cp_is_central_counterparty"),
        F.col(f"{cp}.OthrCtrPty.RptgOblgtn").alias("other_cp_has_reporting_obligation"),

        # === Other counterparty roles ===
        F.col(f"{cp}.Brkr.LEI").alias("broker_lei"),
        F.col(f"{cp}.Brkr.Othr.Id.Id").alias("broker_other_id"),
        F.col(f"{cp}.SubmitgAgt.LEI").alias("submitting_agent_lei"),
        F.col(f"{cp}.SubmitgAgt.Othr.Id.Id").alias("submitting_agent_other_id"),
        F.col(f"{cp}.ClrMmb.Lgl.Id.LEI").alias("clearing_member_lei"),
        F.col(f"{cp}.ClrMmb.Lgl.Id.Othr.Id.Id").alias("clearing_member_other_id"),
        F.col(f"{cp}.NttyRspnsblForRpt.LEI").alias("entity_responsible_for_report_lei"),
        F.col(f"{cp}.NttyRspnsblForRpt.Othr.Id.Id").alias("entity_responsible_for_report_other_id"),

        # === Contract data (CmonTradData.CtrctData) ===
        F.col(f"{cd}.CtrctTp").alias("contract_type"),
        F.col(f"{cd}.AsstClss").alias("asset_class"),
        F.col(f"{cd}.PdctClssfctn").alias("product_classification"),
        F.col(f"{cd}.PdctId.ISIN").alias("product_isin"),
        F.col(f"{cd}.PdctId.UnqPdctIdr.Id").alias("product_unq_pdct_idr"),
        F.col(f"{cd}.PdctId.AltrntvInstrmId").alias("product_alternative_id"),
        F.col(f"{cd}.UndrlygInstrm.ISIN").alias("underlying_isin"),
        F.col(f"{cd}.UndrlygInstrm.AltrntvInstrmId").alias("underlying_alternative_id"),
        F.col(f"{cd}.UndrlygInstrm.UnqPdctIdr.Id").alias("underlying_unq_pdct_idr"),
        F.col(f"{cd}.UndrlygInstrm.Indx.ISIN").alias("underlying_index_isin"),
        F.col(f"{cd}.UndrlygInstrm.Indx.Nm").alias("underlying_index_name"),
        F.col(f"{cd}.UndrlygInstrm.Indx.Indx").alias("underlying_index_value"),
        F.col(f"{cd}.UndrlygInstrm.Bskt.Strr").alias("underlying_basket_structure"),
        F.col(f"{cd}.UndrlygInstrm.Bskt.Id").alias("underlying_basket_id"),
        F.transform(
            F.col(f"{cd}.UndrlygInstrm.Bskt.Cnsttnts"),
            lambda c: F.struct(c["InstrmId"]["ISIN"].alias("isin"),
                               c["InstrmId"]["AltrntvInstrmId"].alias("alternative_id")),
        ).alias("basket_constituents"),
        F.col(f"{cd}.UndrlygInstrm.IdNotAvlbl").alias("underlying_id_not_available"),
        F.col(f"{cd}.SttlmCcy.Ccy").alias("settlement_ccy"),
        F.col(f"{cd}.SttlmCcyScndLeg.Ccy").alias("settlement_ccy_second_leg"),
        F.col(f"{cd}.DerivBasedOnCrptAsst").alias("deriv_based_on_crypto"),

        # === Audit / lineage ===
        F.col("reporting_date"),
        F.col("file_path"),
        F.col("file_name"),
        F.col("_ingested_at").alias("ingested_at"),
        F.current_timestamp().alias("silver_processed_at"),
    )
```

- [ ] **Step 6.2: Verify parses**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_emir.py').read())"
```

- [ ] **Step 6.3: Commit**

```bash
git add src/pipelines/silver_emir.py
git commit -m "$(cat <<'EOF'
feat(silver): add counterparty + contract-data columns to trade

Adds the reporting CP, other CP, and other CP roles (broker, submitting
agent, clearing member, entity responsible) plus all CtrctData
(contract type, asset class, product classification, product/underlying
IDs, basket constituents, settlement currencies, crypto flag). Choice
fields collapsed: LEI primary + *_other_id fallback. Nature is derived
via CASE on which Ntr branch is populated. Sector arrays are coalesced
across FI vs NFI branches into one ARRAY<STRING> column. Path-prefix
constants (cp, txd, cd) added to keep the .col() calls readable.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 7: Trade — transaction core + pricing + notional/qty + clearing

**Files:**
- Modify: `src/pipelines/silver_emir.py` (insert into existing `trade()` body)

Adds transaction-core fields (UTI scalar-level fields are in Task 5/6 already), pricing, notional amounts/quantities, and clearing status.

- [ ] **Step 7.1: Extend the `.select(...)` body**

In `src/pipelines/silver_emir.py` `trade()` function, INSERT the following columns into the existing `.select(...)` chain immediately AFTER the `# === Contract data ===` group and BEFORE the `# === Audit / lineage ===` group:

```python
        # === Transaction core (TxData) ===
        F.col(f"{txd}.ExctnTmStmp").alias("execution_ts"),
        F.col(f"{txd}.FctvDt").alias("effective_dt"),
        F.col(f"{txd}.XprtnDt").alias("expiration_dt"),
        F.col(f"{txd}.EarlyTermntnDt").alias("early_termination_dt"),
        F.col(f"{txd}.SttlmDt").alias("settlement_dates"),
        F.col(f"{txd}.DlvryTp").alias("delivery_type"),
        F.col(f"{txd}.CollPrtflCd.Prtfl.Cd").alias("collateral_portfolio_cd"),
        F.col(f"{txd}.CollPrtflCd.Prtfl.NoPrtfl").alias("has_no_collateral_portfolio"),
        F.col(f"{txd}.MstrAgrmt.Tp.Tp").alias("master_agreement_type"),
        F.col(f"{txd}.MstrAgrmt.Tp.Prtry").alias("master_agreement_type_proprietary"),
        F.col(f"{txd}.MstrAgrmt.Vrsn").alias("master_agreement_version"),
        F.col(f"{txd}.MstrAgrmt.OthrMstrAgrmtDtls").alias("master_agreement_other_details"),

        # === Pricing (TxData.TxPric) ===
        F.col(f"{txd}.TxPric.Pric.MntryVal.Amt._VALUE").alias("price_monetary_value"),
        F.col(f"{txd}.TxPric.Pric.MntryVal.Amt._Ccy").alias("price_monetary_ccy"),
        F.col(f"{txd}.TxPric.Pric.MntryVal.Sgn").alias("price_monetary_sign"),
        F.col(f"{txd}.TxPric.Pric.Unit").alias("price_unit"),
        F.col(f"{txd}.TxPric.Pric.Pctg").alias("price_percentage"),
        F.col(f"{txd}.TxPric.Pric.Yld").alias("price_yield"),
        F.col(f"{txd}.TxPric.Pric.PdgPric").alias("price_pending"),
        F.col(f"{txd}.TxPric.Pric.Othr.Val").alias("price_other_value"),
        F.col(f"{txd}.TxPric.Pric.Othr.Tp").alias("price_other_type"),
        F.col(f"{txd}.TxPric.PricMltplr").alias("price_multiplier"),

        # === Notional amounts (TxData.NtnlAmt) ===
        F.col(f"{txd}.NtnlAmt.FrstLeg.Amt.Amt._VALUE").alias("notional_first_leg_amount"),
        F.col(f"{txd}.NtnlAmt.FrstLeg.Amt.Amt._Ccy").alias("notional_first_leg_ccy"),
        F.col(f"{txd}.NtnlAmt.FrstLeg.Amt.Sgn").alias("notional_first_leg_sign"),
        F.col(f"{txd}.NtnlAmt.ScndLeg.Amt.Amt._VALUE").alias("notional_second_leg_amount"),
        F.col(f"{txd}.NtnlAmt.ScndLeg.Amt.Amt._Ccy").alias("notional_second_leg_ccy"),
        F.col(f"{txd}.NtnlAmt.ScndLeg.Amt.Sgn").alias("notional_second_leg_sign"),

        # === Notional quantities (TxData.NtnlQty) ===
        F.col(f"{txd}.NtnlQty.FrstLeg.TtlQty").alias("notional_first_leg_total_qty"),
        F.col(f"{txd}.NtnlQty.ScndLeg.TtlQty").alias("notional_second_leg_total_qty"),

        # === Quantity (TxData.Qty) ===
        F.col(f"{txd}.Qty.Unit").alias("qty_unit"),
        F.col(f"{txd}.Qty.NmnlVal._VALUE").alias("qty_nominal_value"),
        F.col(f"{txd}.Qty.NmnlVal._Ccy").alias("qty_nominal_ccy"),
        F.col(f"{txd}.Qty.MntryVal._VALUE").alias("qty_monetary_value"),
        F.col(f"{txd}.Qty.MntryVal._Ccy").alias("qty_monetary_ccy"),

        # === Clearing (TxData.TradClr) ===
        F.col(f"{txd}.TradClr.ClrOblgtn").alias("clearing_obligation"),
        F.col(f"{txd}.TradClr.ClrSts.Clrd").isNotNull().alias("is_cleared"),
        F.col(f"{txd}.TradClr.ClrSts.Clrd.Dtls.CCP.LEI").alias("ccp_lei"),
        F.col(f"{txd}.TradClr.ClrSts.Clrd.Dtls.CCP.Othr.Id.Id").alias("ccp_other_id"),
        F.col(f"{txd}.TradClr.ClrSts.Clrd.Dtls.ClrDtTm").alias("cleared_ts"),
        F.col(f"{txd}.TradClr.ClrSts.NonClrd.Rsn").alias("clearing_non_cleared_reason"),
        F.col(f"{txd}.TradClr.IntraGrp").alias("is_intragroup"),
```

- [ ] **Step 7.2: Verify parses**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_emir.py').read())"
```

- [ ] **Step 7.3: Commit**

```bash
git add src/pipelines/silver_emir.py
git commit -m "$(cat <<'EOF'
feat(silver): add transaction core / pricing / notional / clearing

Inserts five XSD-section column groups into the trade .select():
- Transaction core: execution_ts, dates (effective/expiration/early_
  termination/settlement_dates ARRAY<DATE>), delivery_type, collateral
  portfolio, master agreement (type + version + proprietary type
  + other details)
- Pricing: monetary value/ccy/sign, unit, percentage, yield, pending,
  Pric.Othr.Val/Tp, price_multiplier
- Notional amounts: first/second leg amount + ccy + sign
- Notional quantities: first/second leg total_qty
- Quantity: unit, nominal value/ccy, monetary value/ccy
- Clearing: ClrOblgtn, is_cleared boolean (derived from Clrd struct
  existence), ccp_lei + fallback ccp_other_id, cleared_ts, non-cleared
  reason, is_intragroup

Co-authored-by: Isaac
EOF
)"
```

---

## Task 8: Trade — interest rate (both legs) + FX

**Files:**
- Modify: `src/pipelines/silver_emir.py` (insert into existing `trade()` body)

Adds the largest column group: IR first/second leg (~44 cols total) + FX section.

- [ ] **Step 8.1: Extend the `.select(...)` body**

In `src/pipelines/silver_emir.py` `trade()` function, INSERT the following columns into the existing `.select(...)` chain immediately AFTER the `# === Clearing ===` group and BEFORE the `# === Audit / lineage ===` group:

```python
        # === Interest rate first leg (TxData.IntrstRate.FrstLeg) ===
        F.col(f"{txd}.IntrstRate.FrstLeg.Fxd.Rate.Rate").alias("ir_first_leg_fixed_rate"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fxd.DayCnt.Cd").alias("ir_first_leg_fixed_day_count"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fxd.DayCnt.Nrrtv").alias("ir_first_leg_fixed_day_count_narr"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fxd.PmtFrqcy.Term.Unit").alias("ir_first_leg_fixed_pmt_freq_unit"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fxd.PmtFrqcy.Term.Val").alias("ir_first_leg_fixed_pmt_freq_val"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fxd.PmtFrqcy.Prtry").alias("ir_first_leg_fixed_pmt_freq_prop"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Id").alias("ir_first_leg_floating_index_id"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Nm").alias("ir_first_leg_floating_index_name"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Rate.Cd").alias("ir_first_leg_floating_rate_cd"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Rate.Prtry").alias("ir_first_leg_floating_rate_prop"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.RefPrd.Unit").alias("ir_first_leg_floating_ref_period_unit"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.RefPrd.Val").alias("ir_first_leg_floating_ref_period_val"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Sprd.MntryVal.Amt._VALUE").alias("ir_first_leg_floating_spread_value"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Sprd.MntryVal.Amt._Ccy").alias("ir_first_leg_floating_spread_ccy"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Sprd.MntryVal.Sgn").alias("ir_first_leg_floating_spread_sign"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Sprd.Pctg").alias("ir_first_leg_floating_spread_pct"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.Sprd.BsisPtSprd").alias("ir_first_leg_floating_spread_bps"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.DayCnt.Cd").alias("ir_first_leg_floating_day_count"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.PmtFrqcy.Term.Unit").alias("ir_first_leg_floating_pmt_freq_unit"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.PmtFrqcy.Term.Val").alias("ir_first_leg_floating_pmt_freq_val"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.RstFrqcy.Term.Unit").alias("ir_first_leg_floating_rst_freq_unit"),
        F.col(f"{txd}.IntrstRate.FrstLeg.Fltg.RstFrqcy.Term.Val").alias("ir_first_leg_floating_rst_freq_val"),

        # === Interest rate second leg (TxData.IntrstRate.ScndLeg) ===
        F.col(f"{txd}.IntrstRate.ScndLeg.Fxd.Rate.Rate").alias("ir_second_leg_fixed_rate"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fxd.DayCnt.Cd").alias("ir_second_leg_fixed_day_count"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fxd.DayCnt.Nrrtv").alias("ir_second_leg_fixed_day_count_narr"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fxd.PmtFrqcy.Term.Unit").alias("ir_second_leg_fixed_pmt_freq_unit"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fxd.PmtFrqcy.Term.Val").alias("ir_second_leg_fixed_pmt_freq_val"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fxd.PmtFrqcy.Prtry").alias("ir_second_leg_fixed_pmt_freq_prop"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Id").alias("ir_second_leg_floating_index_id"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Nm").alias("ir_second_leg_floating_index_name"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Rate.Cd").alias("ir_second_leg_floating_rate_cd"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Rate.Prtry").alias("ir_second_leg_floating_rate_prop"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.RefPrd.Unit").alias("ir_second_leg_floating_ref_period_unit"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.RefPrd.Val").alias("ir_second_leg_floating_ref_period_val"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Sprd.MntryVal.Amt._VALUE").alias("ir_second_leg_floating_spread_value"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Sprd.MntryVal.Amt._Ccy").alias("ir_second_leg_floating_spread_ccy"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Sprd.MntryVal.Sgn").alias("ir_second_leg_floating_spread_sign"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Sprd.Pctg").alias("ir_second_leg_floating_spread_pct"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.Sprd.BsisPtSprd").alias("ir_second_leg_floating_spread_bps"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.DayCnt.Cd").alias("ir_second_leg_floating_day_count"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.PmtFrqcy.Term.Unit").alias("ir_second_leg_floating_pmt_freq_unit"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.PmtFrqcy.Term.Val").alias("ir_second_leg_floating_pmt_freq_val"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.RstFrqcy.Term.Unit").alias("ir_second_leg_floating_rst_freq_unit"),
        F.col(f"{txd}.IntrstRate.ScndLeg.Fltg.RstFrqcy.Term.Val").alias("ir_second_leg_floating_rst_freq_val"),

        # === FX (TxData.Ccy) ===
        F.col(f"{txd}.Ccy.DlvrblCrossCcy").alias("delivery_ccy_cross"),
        F.col(f"{txd}.Ccy.XchgRate").alias("xchg_rate"),
        F.col(f"{txd}.Ccy.FwdXchgRate").alias("forward_xchg_rate"),
        F.col(f"{txd}.Ccy.XchgRateBsis.CcyPair.BaseCcy").alias("xchg_base_ccy"),
        F.col(f"{txd}.Ccy.XchgRateBsis.CcyPair.QtdCcy").alias("xchg_quoted_ccy"),
        F.col(f"{txd}.Ccy.XchgRateBsis.Prtry").alias("xchg_rate_basis_proprietary"),
```

- [ ] **Step 8.2: Verify parses**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_emir.py').read())"
```

- [ ] **Step 8.3: Commit**

```bash
git add src/pipelines/silver_emir.py
git commit -m "$(cat <<'EOF'
feat(silver): add interest rate (both legs) + FX columns

Adds the IR first-leg + second-leg column groups (22 cols each, total
44 cols) covering both Fxd (fixed-rate) and Fltg (floating-rate)
branches: rate, day count, payment frequency, floating index ID/name,
rate code/proprietary, reference period, spread (monetary/percentage/
basis points), reset frequency. Plus the FX section: deliverable
cross-currency, exchange rates (spot + forward), exchange rate basis
(base/quoted currency pair + proprietary).

Co-authored-by: Isaac
EOF
)"
```

---

## Task 9: Trade — lifecycle, valuation, product-specific (Optn/Cdt/Packg/OthrPmt/Cmmdty/Nrgy)

**Files:**
- Modify: `src/pipelines/silver_emir.py` (insert into existing `trade()` body)

Adds the final big batch: lifecycle/risk-reduction/confirmation, valuation, and the six product-class-specific sections from the spec audit.

- [ ] **Step 9.1: Extend the `.select(...)` body**

In `src/pipelines/silver_emir.py` `trade()` function, INSERT the following column groups into the existing `.select(...)` chain immediately AFTER the `# === FX ===` group and BEFORE the `# === Audit / lineage ===` group:

```python
        # === Lifecycle / risk-reduction / confirmation ===
        F.col("CmonTradData.CtrctMod.ActnTp").alias("contract_modification_action_type"),
        F.col("CmonTradData.CtrctMod.Lvl").alias("contract_modification_level"),
        F.col(f"{txd}.Cmprssn").alias("is_compression"),
        F.col(f"{txd}.PstTradRskRdctnFlg").alias("is_post_trade_risk_reduction"),
        F.col(f"{txd}.PstTradRskRdctnEvt.Tchnq").alias("ptrr_technique"),
        F.col(f"{txd}.PstTradRskRdctnEvt.SvcPrvdr.LEI").alias("ptrr_service_provider_lei"),
        F.col(f"{txd}.DerivEvt.Tp").alias("deriv_event_type"),
        F.col(f"{txd}.DerivEvt.Id.PstTradRskRdctnIdr.Strr").alias("deriv_event_ptrr_strr"),
        F.col(f"{txd}.DerivEvt.Id.PstTradRskRdctnIdr.Id").alias("deriv_event_ptrr_id"),
        F.col(f"{txd}.DerivEvt.TmStmp.Dt").alias("deriv_event_dt"),
        F.coalesce(F.col(f"{txd}.TradConf.Confd.Tp"), F.col(f"{txd}.TradConf.NonConfd.Tp")).alias("trade_confirmation_type"),
        F.col(f"{txd}.TradConf.Confd.TmStmp").alias("trade_confirmation_ts"),

        # === Valuation (CtrPtySpcfcData.Valtn) ===
        F.col("CtrPtySpcfcData.Valtn.CtrctVal.Amt._VALUE").alias("contract_value"),
        F.col("CtrPtySpcfcData.Valtn.CtrctVal.Amt._Ccy").alias("contract_value_ccy"),
        F.col("CtrPtySpcfcData.Valtn.CtrctVal.Sgn").alias("contract_value_sign"),
        F.col("CtrPtySpcfcData.Valtn.Dlta").alias("delta"),
        F.col("CtrPtySpcfcData.Valtn.TmStmp").alias("valuation_ts"),
        F.col("CtrPtySpcfcData.Valtn.Tp").alias("valuation_type"),

        # === Option attributes (TxData.Optn) ===
        F.col(f"{txd}.Optn.Tp").alias("option_type"),
        F.col(f"{txd}.Optn.ExrcStyle").alias("option_exercise_style"),
        F.col(f"{txd}.Optn.StrkPric.MntryVal.Amt._VALUE").alias("option_strike_price"),
        F.col(f"{txd}.Optn.StrkPric.MntryVal.Amt._Ccy").alias("option_strike_price_ccy"),
        F.col(f"{txd}.Optn.PrmAmt._VALUE").alias("option_premium_amount"),
        F.col(f"{txd}.Optn.PrmAmt._Ccy").alias("option_premium_ccy"),
        F.col(f"{txd}.Optn.PrmPmtDt").alias("option_premium_payment_dt"),
        F.col(f"{txd}.Optn.MtrtyDtOfUndrlyg").alias("option_underlying_maturity_dt"),

        # === Credit derivative attributes (TxData.Cdt) ===
        F.col(f"{txd}.Cdt.Snrty").alias("credit_seniority"),
        F.col(f"{txd}.Cdt.RefPty.LEI").alias("credit_reference_party_lei"),
        F.col(f"{txd}.Cdt.PmtFrqcy.Term.Unit").alias("credit_payment_freq_unit"),
        F.col(f"{txd}.Cdt.PmtFrqcy.Term.Val").alias("credit_payment_freq_val"),
        F.col(f"{txd}.Cdt.ClctnBsis").alias("credit_calculation_basis"),
        F.col(f"{txd}.Cdt.Srs").alias("credit_series"),
        F.col(f"{txd}.Cdt.Vrsn").alias("credit_version"),
        F.col(f"{txd}.Cdt.IndxFctr").alias("credit_index_factor"),
        F.col(f"{txd}.Cdt.Trch.AttchmntPt").alias("credit_tranche_attachment"),
        F.col(f"{txd}.Cdt.Trch.DtchmntPt").alias("credit_tranche_detachment"),

        # === Package transactions (TxData.Packg) ===
        F.col(f"{txd}.Packg.CmplxTradId").alias("package_complex_trade_id"),
        F.col(f"{txd}.Packg.Pric").alias("package_price"),
        F.col(f"{txd}.Packg.Sprd").alias("package_spread"),

        # === Other payments (TxData.OthrPmt[]) — ARRAY<STRUCT> ===
        F.transform(
            F.col(f"{txd}.OthrPmt"),
            lambda p: F.struct(
                p["Tp"].alias("payment_type"),
                p["Amt"]["_VALUE"].alias("amount"),
                p["Amt"]["_Ccy"].alias("ccy"),
                p["Dt"].alias("payment_dt"),
            ),
        ).alias("other_payments"),

        # === Commodity taxonomy (TxData.Cmmdty) — COALESCE'd promoted cols ===
        F.coalesce(
            F.col(f"{txd}.Cmmdty.Agrcltrl.BasePdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.BasePdct"),
            F.col(f"{txd}.Cmmdty.Envttl.BasePdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.BasePdct"),
            F.col(f"{txd}.Cmmdty.Frght.BasePdct"),
            F.col(f"{txd}.Cmmdty.Indx.BasePdct"),
            F.col(f"{txd}.Cmmdty.IndstrlPdct.BasePdct"),
            F.col(f"{txd}.Cmmdty.Infltn.BasePdct"),
            F.col(f"{txd}.Cmmdty.Metl.BasePdct"),
            F.col(f"{txd}.Cmmdty.MultiCmmdtyExtc.BasePdct"),
            F.col(f"{txd}.Cmmdty.OffclEcnmcSttstcs.BasePdct"),
            F.col(f"{txd}.Cmmdty.Othr.BasePdct"),
            F.col(f"{txd}.Cmmdty.OthrC10.BasePdct"),
            F.col(f"{txd}.Cmmdty.Ppr.BasePdct"),
            F.col(f"{txd}.Cmmdty.Plprpln.BasePdct"),
        ).alias("commodity_base_product"),
        F.coalesce(
            F.col(f"{txd}.Cmmdty.Agrcltrl.SubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.SubPdct"),
            F.col(f"{txd}.Cmmdty.Envttl.SubPdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.SubPdct"),
            F.col(f"{txd}.Cmmdty.Frght.SubPdct"),
            F.col(f"{txd}.Cmmdty.Indx.SubPdct"),
            F.col(f"{txd}.Cmmdty.IndstrlPdct.SubPdct"),
            F.col(f"{txd}.Cmmdty.Infltn.SubPdct"),
            F.col(f"{txd}.Cmmdty.Metl.SubPdct"),
            F.col(f"{txd}.Cmmdty.MultiCmmdtyExtc.SubPdct"),
            F.col(f"{txd}.Cmmdty.OffclEcnmcSttstcs.SubPdct"),
            F.col(f"{txd}.Cmmdty.Othr.SubPdct"),
            F.col(f"{txd}.Cmmdty.OthrC10.SubPdct"),
            F.col(f"{txd}.Cmmdty.Ppr.SubPdct"),
            F.col(f"{txd}.Cmmdty.Plprpln.SubPdct"),
        ).alias("commodity_sub_product"),
        F.coalesce(
            F.col(f"{txd}.Cmmdty.Agrcltrl.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Nrgy.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Envttl.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Frtlzr.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Frght.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Indx.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.IndstrlPdct.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Infltn.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Metl.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.MultiCmmdtyExtc.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.OffclEcnmcSttstcs.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Othr.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.OthrC10.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Ppr.AddtlSubPdct"),
            F.col(f"{txd}.Cmmdty.Plprpln.AddtlSubPdct"),
        ).alias("commodity_additional_sub_product"),

        # === Energy-specific (TxData.NrgySpcfcAttrbts) ===
        F.col(f"{txd}.NrgySpcfcAttrbts.IntrCnnctnPt").alias("energy_interconnection_point"),
        F.col(f"{txd}.NrgySpcfcAttrbts.LdTp").alias("energy_load_type"),
        F.col(f"{txd}.NrgySpcfcAttrbts.DlvryPtOrZone").alias("energy_delivery_zones"),
        F.col(f"{txd}.NrgySpcfcAttrbts.DlvryAttr").alias("energy_delivery_attributes"),

        # === TechAttrbts ===
        F.col("TechAttrbts.RcncltnFlg").alias("reconciliation_flag"),

        # === Reporting metadata ===
        F.col("CtrPtySpcfcData.RptgTmStmp").alias("reporting_ts"),
        F.col("FileBatchIndex").cast("int").alias("batch_index"),
        F.col("FileBatchSize").cast("int").alias("batch_size"),
        F.col("FileVersion").cast("int").alias("file_version"),
        F.col("hdr_pyld_metadata.Hdr.AppHdr.BizMsgIdr").alias("biz_msg_id"),
        F.col("hdr_pyld_metadata.Hdr.AppHdr.Fr.OrgId.Id.OrgId.Othr.Id").alias("sender_lei"),
        F.col("hdr_pyld_metadata.Hdr.AppHdr.To.OrgId.Id.OrgId.Othr.Id").alias("recipient_lei"),
        F.col("hdr_pyld_metadata.Pyld.Document.DerivsTradStatRpt.TradData.DataSetActn").alias("data_set_action"),
```

- [ ] **Step 9.2: Verify parses**

```bash
python3 -c "import ast; ast.parse(open('src/pipelines/silver_emir.py').read())"
```

- [ ] **Step 9.3: Commit**

```bash
git add src/pipelines/silver_emir.py
git commit -m "$(cat <<'EOF'
feat(silver): add lifecycle/valuation/product-specific + audit cols

Final column batch for the trade table:
- Lifecycle (CtrctMod ActnTp/Lvl, Cmprssn, PstTradRskRdctnFlg, PtrrEvt
  technique + LEI, DerivEvt type/Strr/Id/Dt, TradConf type + ts)
- Valuation (contract_value + ccy + sign, delta, valuation_ts/type)
- Optn (8 flat cols: type, exercise style, strike price + ccy, premium
  amount + ccy, premium payment date, underlying maturity date)
- Cdt (10 flat cols: seniority, ref party LEI, payment frequency
  unit/val, calculation basis, series, version, index factor, tranche
  attachment/detachment)
- Packg (3 flat: complex_trade_id, price, spread)
- OthrPmt (1 ARRAY<STRUCT> col with type/amount/ccy/payment_dt)
- Cmmdty (3 COALESCE'd cols across the 15-branch product taxonomy:
  base/sub/additional_sub product)
- NrgySpcfcAttrbts (interconnection_point, load_type, delivery_zones
  ARRAY<STRING>, delivery_attributes STRUCT<>)
- TechAttrbts.RcncltnFlg
- Reporting metadata (reporting_ts, batch_index/size, file_version,
  biz_msg_id, sender/recipient_lei, data_set_action)

trade table column inventory complete per spec §4.1.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 10: Add SDP pipeline resource to the bundle

**Files:**
- Modify: `resources/bundle.emir_resources.yml`

- [ ] **Step 10.1: Append the SDP pipeline resource**

Find the existing `# === Spark Declarative Pipelines ===` section in `resources/bundle.emir_resources.yml`. Below the existing `emir_xml_loader_pipeline` block (and at the same indentation level — direct child of `pipelines:`), append:

```yaml
    emir_silver_pipeline:
      name: "EMIR Silver (domain-driven)"
      catalog: ${var.emir_catalog}
      schema: ${var.emir_raw_schema}
      serverless: true
      channel: PREVIEW
      development: false
      photon: true
      continuous: false

      libraries:
        - file:
            path: ../src/pipelines/silver_emir.py

      configuration:
        catalog: ${var.emir_catalog}
        raw_schema: ${var.emir_raw_schema}
        silver_schema: ${var.emir_raw_schema}
        bronze_table: ${var.emir_table_prefix}_raw
        regulation: "EMIR"
```

- [ ] **Step 10.2: Validate**

Run:
```bash
databricks bundle validate -t dev
```
Expected: `Validation OK!`. The new pipeline `emir_silver_pipeline` should appear in the resources resolution.

If auth is stale, run `databricks auth login --host https://e2-demo-field-eng.cloud.databricks.com` first.

- [ ] **Step 10.3: Commit**

```bash
git add resources/bundle.emir_resources.yml
git commit -m "$(cat <<'EOF'
feat(bundle): add emir_silver_pipeline resource

New SDP pipeline resource under the existing 'Spark Declarative
Pipelines' section in bundle.emir_resources.yml. Points at
src/pipelines/silver_emir.py with configuration keys for catalog,
raw_schema, silver_schema (defaults to same as raw_schema for v1),
bronze_table (constructed from emir_table_prefix), and a regulation
constant for the regulation-agnostic submission_file table.

No new bundle variables required for v1.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 11: Wire dev/prod target overrides

**Files:**
- Modify: `databricks.yml`

- [ ] **Step 11.1: Add target-level development overrides for the silver pipeline**

In `databricks.yml`, find the existing `targets:` block. Find the `dev` and `prod` target's `resources.pipelines:` sub-block (where the bronze pipelines already have `development: true|false` overrides). Add a third entry in each:

In `targets.dev.resources.pipelines`, add:
```yaml
        emir_silver_pipeline:
          development: true
```

In `targets.prod.resources.pipelines`, add:
```yaml
        emir_silver_pipeline:
          development: false
```

- [ ] **Step 11.2: Validate both targets**

```bash
databricks bundle validate -t dev
databricks bundle validate -t prod
```
Both should pass (modulo the pre-existing prod-target `workspace.root_path` warning, which is unrelated).

- [ ] **Step 11.3: Commit**

```bash
git add databricks.yml
git commit -m "$(cat <<'EOF'
feat(bundle): wire dev/prod overrides for emir_silver_pipeline

Sets development=true on the dev target and development=false on the
prod target, matching the pattern used for the existing
emir_xml_loader_pipeline.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 12: Deploy + run + verify on E2

The bronze `emir_raw` is already populated on E2 (32M rows from PR #1's smoke test) with XSD validation disabled via the local override `emir_enable_xsd_validation: "false"`. Silver can run against it directly.

- [ ] **Step 12.1: Confirm auth + bronze state**

```bash
databricks current-user me | jq -r '.userName'
databricks experimental aitools tools query --warehouse $(databricks warehouses list --output json | jq -r '.[] | select(.state=="RUNNING") | .id' | head -1) "SELECT COUNT(*) AS bronze_rows FROM users.matthew_moorcroft.emir_raw"
```
Expected: user is matthew.moorcroft@databricks.com; bronze_rows ≥ 32000000.

- [ ] **Step 12.2: Deploy**

```bash
databricks bundle deploy -t dev
```
Expected: succeeds; output lists the new pipeline `[dev matthew_moorcroft] EMIR Silver (domain-driven)`.

- [ ] **Step 12.3: Trigger the silver pipeline**

```bash
databricks bundle run emir_silver_pipeline -t dev
```

Capture the update_id from the streaming output. The bronze had ~12-min wall time for 131GB ingestion + lxml UDFs; silver is pure SQL transformations on Delta — expect substantially faster (likely 3-5 min for 32M rows).

If FAILED with `external_metadata enablement version` error (the same one we hit during the bronze smoke test), drop + recreate the silver pipeline:
```bash
databricks pipelines delete <silver_pipeline_id>
databricks bundle deploy -t dev
databricks bundle run emir_silver_pipeline -t dev
```

If FAILED with a schema-path error (e.g., "field X not found in struct"), examine the pipeline event log:
```bash
databricks pipelines get-update <pipeline_id> <update_id>
```
Common cause: the bronze struct shape differs slightly from the spec's assumed XSD path. The fix is to adjust the specific `F.col("path")` in `silver_emir.py` and re-run.

- [ ] **Step 12.4: Verify row counts**

```bash
WHID=$(databricks warehouses list --output json | jq -r '.[] | select(.state=="RUNNING") | .id' | head -1)
databricks experimental aitools tools query --warehouse "$WHID" "SELECT 'trade' AS t, COUNT(*) AS rows FROM users.matthew_moorcroft.trade UNION ALL SELECT 'trade_schedule', COUNT(*) FROM users.matthew_moorcroft.trade_schedule UNION ALL SELECT 'trade_beneficiary', COUNT(*) FROM users.matthew_moorcroft.trade_beneficiary UNION ALL SELECT 'submission_file', COUNT(*) FROM users.matthew_moorcroft.submission_file"
```
Expected:
- `trade` ≈ 32,000,000 (matches bronze, one-to-one)
- `submission_file` = 64
- `trade_schedule` ≥ 0 (synthetic CBI data may have minimal schedules)
- `trade_beneficiary` ≥ 0 (synthetic CBI data may have minimal beneficiaries)

- [ ] **Step 12.5: Spot-check semantic correctness**

```bash
databricks experimental aitools tools query --warehouse "$WHID" "SELECT reporter_lei, asset_class, contract_type, is_cleared, notional_first_leg_amount, contract_value FROM users.matthew_moorcroft.trade WHERE trade_id IS NOT NULL LIMIT 5"
```
Confirm:
- `reporter_lei` is a 20-char LEI-shaped string (or NULL)
- `asset_class` is one of CR/EQ/IR/FX/CO
- `contract_type` is one of SWAP/FORW/OPTN/FUTR/CFDS/OTHR
- `is_cleared` is BOOLEAN
- `contract_value` is DECIMAL with sensible precision

- [ ] **Step 12.6: Analyst feel-test query**

```bash
databricks experimental aitools tools query --warehouse "$WHID" "SELECT reporter_lei, asset_class, COUNT(*) AS trades, SUM(ABS(notional_first_leg_amount)) AS gross_notional FROM users.matthew_moorcroft.trade WHERE reporting_date = (SELECT MAX(reporting_date) FROM users.matthew_moorcroft.trade) GROUP BY reporter_lei, asset_class ORDER BY gross_notional DESC NULLS LAST LIMIT 10"
```
Expected: returns in a few seconds. The query is what previously required diving 6-deep into struct paths against bronze.

- [ ] **Step 12.7: Inspect schedule rows by type**

```bash
databricks experimental aitools tools query --warehouse "$WHID" "SELECT schedule_type, COUNT(*) FROM users.matthew_moorcroft.trade_schedule GROUP BY schedule_type"
```
Expected: 6 distinct types possible (PRICE, NTNL_AMT_LEG_1, NTNL_AMT_LEG_2, NTNL_QTY_LEG_1, NTNL_QTY_LEG_2, STRIKE); any of them can be 0 if the synthetic data doesn't populate that path.

---

## Task 13: Document smoke-test results

**Files:**
- Create: `docs/superpowers/plans/2026-05-12-emir-silver-smoke-test-results.md`

- [ ] **Step 13.1: Capture results**

Write `docs/superpowers/plans/2026-05-12-emir-silver-smoke-test-results.md` with:

```markdown
# EMIR Silver — E2 Smoke-Test Results

**Date:** 2026-05-12
**Branch:** `feat/emir-silver`
**Workspace:** `e2-demo-field-eng.cloud.databricks.com`
**Target schema:** `users.matthew_moorcroft`

## Pipeline run

| Field | Value |
|---|---|
| Pipeline ID | <fill-from-deploy> |
| Update ID | <fill-from-run> |
| State | COMPLETED |
| Wall time | <fill-from-event-log> |
| Cluster | serverless + Photon |

## Row counts

| Table | Rows |
|---|---|
| trade | <fill> |
| submission_file | <fill> |
| trade_schedule (by schedule_type) | <fill — break down by type> |
| trade_beneficiary | <fill> |

## Spot-check correctness

`SELECT reporter_lei, asset_class, contract_type, is_cleared, contract_value FROM trade LIMIT 5`:

[paste sample rows]

## Analyst feel-test

`SELECT reporter_lei, asset_class, SUM(ABS(notional_first_leg_amount)) ... ORDER BY DESC`:

[paste result + observe query time]

## Anomalies / follow-ups

- [Note anything unexpected — NULL columns, type mismatches, etc.]
- [Schedule and beneficiary tables may be empty if synthetic data doesn't populate them — confirm]
```

Replace `<fill>` placeholders with actual values from Task 12.

- [ ] **Step 13.2: Commit**

```bash
git add docs/superpowers/plans/2026-05-12-emir-silver-smoke-test-results.md
git commit -m "$(cat <<'EOF'
test(silver): E2 smoke-test results

Records actual row counts, update_id, wall time, and spot-check
results from the silver pipeline's first triggered run on E2
against the 32M-row bronze emir_raw. trade table column contract
verified via business-readable column names + analyst feel-test
query.

Co-authored-by: Isaac
EOF
)"
```

---

## Task 14: Open PR

- [ ] **Step 14.1: Push branch**

```bash
git push -u origin feat/emir-silver
```

- [ ] **Step 14.2: Create PR via gh CLI**

```bash
gh pr create --title "feat(silver): domain-driven EMIR REFIT silver layer" --body "$(cat <<'EOF'
## Summary

Domain-driven EMIR REFIT DAT TSR silver layer on top of bronze `emir_raw` (shipped in PR #1).

**Four published tables** in `users.matthew_moorcroft.*`:

- `trade` — wide-flat fact, one row per `<Stat>` per snapshot. ~232 scalar columns with business-readable names (reporter_lei, asset_class, contract_type, notional_first_leg_amount, etc.) + 5 ARRAY columns + 1 ARRAY<STRUCT> (other_payments) + 1 STRUCT (energy_delivery_attributes). Choice fields collapsed to LEI primary + `*_other_id` fallback. Per-field decision rule applied: flatten if analysts filter/group/aggregate; keep struct/array for needed-but-rare; drop long-tail accessible via bronze.

- `trade_schedule` — unified 6 schedule types (PRICE / NTNL_AMT_LEG_1 / NTNL_AMT_LEG_2 / NTNL_QTY_LEG_1 / NTNL_QTY_LEG_2 / STRIKE) via discriminator column.

- `trade_beneficiary` — explode of `CtrPtySpcfcData.CtrPty.Bnfcry[]` with LEGAL / NATURAL / OTHER type discriminator.

- `submission_file` — regulation-agnostic file-level envelope; MiFIR will write to the same table with `regulation='MIFIR'`.

## Architecture

- New SDP pipeline `emir_silver_pipeline` in `bundle.emir_resources.yml`, source at `src/pipelines/silver_emir.py`.
- Reads from `users.matthew_moorcroft.emir_raw` (bronze published table).
- Append-only — each daily snapshot adds rows to `trade`. Partition/cluster on `reporting_date`.
- Serverless + Photon, `cluster_by_auto=True`.
- SCD Type 2 migration path documented as a future follow-up (spec §3.3).
- Star-schema pivot documented as a future architectural option (spec §8).

## Reference docs

- Approved design spec: `docs/superpowers/specs/2026-05-12-emir-silver-design.md`
- Task-by-task plan: `docs/superpowers/plans/2026-05-12-emir-silver.md`
- E2 smoke-test results: `docs/superpowers/plans/2026-05-12-emir-silver-smoke-test-results.md`

## Test plan

- [x] `databricks bundle validate -t dev` / `-t prod` — pass
- [x] `databricks bundle deploy -t dev` — creates `emir_silver_pipeline` on E2 alongside the existing bronze pipeline
- [x] `databricks bundle run emir_silver_pipeline -t dev` — COMPLETED in [wall time], 32M rows in `trade`, 64 in `submission_file`
- [x] Spot-check confirms business-readable columns populated correctly (LEI shape, asset class enum, contract_type enum, boolean is_cleared, decimal contract_value)
- [x] Analyst feel-test query (top 10 reporters by gross notional, latest snapshot) returns in seconds
- [x] Legacy `2_flatten_explode_table.py` notebook job remains scheduled — no removal of escape-hatch path

## Deferred / out-of-scope

- **MiFIR silver** — separate brainstorm + spec; pattern reusable
- **Gold layer** — once analysts query silver for a while, identify the actual hot aggregations
- **SCD Type 2 migration** — when append-only volume becomes a concern OR lifecycle queries become a priority
- **Star-schema pivot** (`dim_legal_entity` + `dim_date`) — when GLEIF integration or cross-regulation conformed dimensions become priorities
- **Bronze filename-regex parameterization** — currently hard-coded for ESMA convention; customer deployments need bundle-config parameters. Separate small follow-up branch.
- **Deep commodity taxonomy in silver** — only base/sub/additional_sub products promoted; the 15-branch sub-product detail accessible via bronze
- **Unit tests** for the silver column mappings
- **Reference data joins** (LEI → legal name; ISIN → instrument name)

This pull request and its description were written by Isaac.
EOF
)"
```

Replace `[wall time]` with the actual value from Task 13.

- [ ] **Step 14.3: Verify PR is open**

```bash
gh pr view --json url,number,state,baseRefName,headRefName
```
Expected: state OPEN, head `feat/emir-silver`, base `main` (or `feat/sdp-xml-loader` if PR #1 hasn't merged yet — adjust base before opening if needed).

---

## Out-of-Scope (Documented Follow-Ups)

Per spec §8, these are deliberately not in this plan:

- MiFIR silver tables (separate spec + branch)
- Gold-layer aggregations and metric views
- SCD Type 2 migration
- Star-schema pivot (`dim_legal_entity`, `dim_date`)
- Bronze filename-regex parameterization (separate follow-up)
- Deep commodity taxonomy preservation in silver
- Unit tests for silver transformations
- Reference data joins
- Retirement of the legacy `2_flatten_explode_table.py` notebook
