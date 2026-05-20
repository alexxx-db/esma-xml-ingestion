# Plan: Convert legacy flatten/explode notebook to an SDP

**Date:** 2026-05-19
**Status:** Draft — capturing intent; implementation TBD
**Owner:** matthew.moorcroft@databricks.com

---

## 1. Context

The original notebook-based architecture has three stages:

1. **`0_1_xml_schema_xsd.py`** — XSD → JSON schemas (Schema Prep). *Still active* in the SDP architecture as a one-time prep step.
2. **`1_xml_file_loader_body.py`** — Auto Loader XML ingest with row-tag scoping, LXML header extraction, write to `{prefix}_raw`. *Superseded* by the SDP loader at `src/pipelines/xml_loader.py`.
3. **`2_flatten_explode_table.py`** — generic recursive flatten of the raw table into named child tables with surrogate keys and parent FKs. *Currently legacy* — the per-regime silver pipelines (`silver_emir.py`, `silver_mifir.py`) replace it for EMIR and MiFIR by projecting the regulator-defined columns directly into named, queryable tables.

The per-regime silver approach is the right pattern for regulators we're actively supporting because:

- It produces meaningful, named columns (`trade.notional_ccy`, `transaction.transaction_id`) rather than auto-generated names from path-based flattening
- It enforces domain semantics (e.g. EMIR's `trade_schedule` unifies five different schedule-period shapes via a `schedule_type` discriminator)
- It composes well with downstream silver-layer quality checks

But it costs effort to build for each new regulation: the silver pipeline is ~770–895 lines per regime. For partners who need to ingest a new ESMA regime as a **first cut** (before investing in domain modeling), having a **generic flatten step as an SDP** is valuable.

This doc captures the plan to convert `2_flatten_explode_table.py` into an SDP-equivalent.

## 2. Goals

- A new module under `src/pipelines/` (working name: `flatten_explode.py`) that takes the bronze `{prefix}_raw` table and emits a dynamic set of flattened child tables via `@dp.table(...)`.
- Configurable per regime via DAB variables (the same pattern as the loader + silver pipelines).
- Coexists with the per-regime silver: a customer can choose either or both.
- Drops in for any ESMA regime (or any deeply nested XML) without code changes — just point it at a bronze table and a target schema.

## 3. Non-goals

- Not removing the per-regime silver pipelines. Those continue to be the recommended production path for EMIR and MiFIR.
- Not removing the legacy notebook (`2_flatten_explode_table.py`) until the SDP version is proven on real data. The notebook remains as the documented legacy reference path.
- Not changing the bronze loader. The SDP flatten reads from `{prefix}_raw` as input.

## 4. Design challenges

### 4.1 SDP and dynamically named tables

The notebook produces a variable number of tables based on the bronze schema. Each nested array becomes a child table; depth is unbounded. The `@dp.table(...)` decorator is normally applied per-function at module-import time — this doesn't naturally express "emit N tables where N is determined by inspecting an upstream schema."

**Options:**

1. **Generate `@dp.table` definitions in a loop at module-import time.** Inspect the bronze schema before any pipeline function runs (`spark.read.table` is fine at import in SDP), build the flatten plan, then dynamically register each child table via `@dp.table(name=...)`. This is *possible* but a bit unusual; need to confirm SDP supports decorator application in a loop.
2. **Pre-compute the flatten plan in Schema Prep and persist it.** The Schema Prep step (or a new step) inspects the bronze schema, writes a YAML or JSON plan describing the table-and-column structure, and the SDP module reads that plan at import time and emits one `@dp.table` per planned table.
3. **Emit a single SDP "flatten" entry point that returns a struct, then materialize child tables via downstream `@dp.materialized_view`.** Less natural; might lose the parent-FK ergonomics.

**Likely choice:** Option 2 — Schema Prep step gains a "flatten plan" output. SDP module is simple, predictable, and the plan is auditable.

### 4.2 Schema evolution

When the bronze schema evolves (new field added inside a struct, new optional array), the flatten plan needs to evolve too. With Option 2, this means Schema Prep regenerates the plan whenever the XSD changes — which is already the cadence for the existing JSON schemas. Good.

With Option 1, the SDP would re-inspect the schema on every pipeline start. Simpler but slower-feedback if the bronze schema drifts.

### 4.3 Surrogate keys and parent FKs

The notebook generates surrogate keys via MD5 hash of content + position. This needs to be preserved in the SDP version so:
- `_sk` column on each table is the table-local PK
- `_parent_fk_{parent_table}` on each child table is the FK to its parent's `_sk`
- `array_pos` preserves array order

These should be computed once per table in the SDP, identically to the notebook.

### 4.4 Streaming semantics

The notebook uses streaming reads + writes with `mergeSchema`. The SDP equivalent should:
- Use `spark.readStream.table(TBL_BRONZE)` (matches the silver pattern)
- Each `@dp.table` is a streaming materialized view that handles append-only growth
- For corrected/late-arriving data, use `dp.create_streaming_table` + `dp.apply_changes` if needed (mirror `apply_changes` patterns from the regulator's CDC events)

## 5. Proposed structure

```
src/pipelines/flatten_explode.py
├── Pipeline config (spark.conf reads)
├── Load flatten plan from {prefix}_flatten_plan.json (produced by Schema Prep)
├── Generate @dp.table definitions in a loop, one per planned table
│   ├── base table: F.col path-based flat fields + struct flattens + _sk
│   └── child tables: array explode + array_pos + _parent_fk_{parent} + _sk
└── (Optional) emit a "flatten_plan" debug table containing the plan itself
```

DAB variables to add per regime (when adopted):

```yaml
emir_enable_generic_flatten:
  description: "Enable the SDP-based generic flatten (in addition to/instead of silver_emir)."
  default: "false"
emir_flatten_schema:
  description: "Target schema for the generic-flatten tables. Distinct from emir_silver if both are enabled."
  default: "emir_bronze"
emir_flatten_plan_path:
  description: "Path to the JSON flatten plan produced by Schema Prep."
  default: "${var.emir_volume_path}/emir/schemas/flatten_plan.json"
```

## 6. Open questions (to resolve before implementing)

- **Does SDP allow `@dp.table` decorators applied in a loop with dynamic names?** Need to verify against current `pyspark.pipelines` docs. If not, Option 2 above is the only viable path.
- **Plan format.** YAML, JSON, or generated Python? JSON is easiest to produce/consume.
- **Naming collisions.** When the generic flatten runs alongside the per-regime silver in the same schema, will table names collide? Likely yes (`emir_trade` vs `emir_<bronze-tree>`-style auto names). Either use a distinct schema (recommended) or a column-mangling prefix (ugly).
- **Backwards compatibility.** The legacy `2_flatten_explode_table.py` produces a specific table-naming convention (e.g. `emir_Pyld_TxRpt_OthrPty`). Should the SDP version match that exactly so consumers can swap in place? Or take this chance to clean up names?

## 7. Out of scope

- Replacing the per-regime silver pipelines. They stay.
- Changes to bronze. Bronze SDP is the input.
- Changes to Schema Prep beyond optionally emitting the flatten plan.

## 8. Suggested first slice (when work begins)

1. Confirm whether `@dp.table` decorators can be applied dynamically (Option 1).
2. If yes — prototype against a sample bronze table, ~50 lines.
3. If no — extend `0_1_xml_schema_xsd.py` to emit `flatten_plan.json`. Then prototype the SDP that consumes it.
4. Validate against a small EMIR sample. Compare the SDP output to the notebook output (same surrogate keys, same parent FKs, same column counts).
5. Wire into DAB as a *separate, opt-in pipeline* (do not modify existing `emir_xml_loader_pipeline` or `emir_silver_pipeline`).
6. Document in README under "Key Components". Add to the new-regulation template as an optional pipeline.

## 9. Decision: hold for now

Per the operating decision (2026-05-19), the legacy notebook is marked as legacy in-place
and this plan is captured for future work. No code changes to `2_flatten_explode_table.py`
yet beyond the legacy banner; no new SDP file added.

---

## See also

- `src/notebooks/2_flatten_explode_table.py` — the legacy notebook this plan replaces
- `src/pipelines/silver_emir.py`, `silver_mifir.py` — the domain-driven silver alternative
- `src/notebooks/0_1_xml_schema_xsd.py` — Schema Prep (potential extension point)
- `README.md` — current architecture overview
