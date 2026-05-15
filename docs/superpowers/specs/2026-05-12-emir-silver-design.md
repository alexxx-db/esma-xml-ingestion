# EMIR REFIT Silver Layer — Design

**Status:** Approved (interactive review complete)
**Date:** 2026-05-12
**Author:** Matthew Moorcroft
**Branch (target):** `feat/emir-silver` (off `main` after PR #1 merges, or off `feat/sdp-xml-loader`)
**Reference bronze:** `users.matthew_moorcroft.emir_raw` (32M rows, 64 files, EMIR REFIT DAT TSR ingested via the SDP loader from PR #1)

---

## 1. Problem & Motivation

PR #1 produced `emir_raw`, a public streaming Delta table containing one row per `<Stat>` (derivative trade state) element from EMIR REFIT submissions, with the full nested ISO 20022 payload preserved as struct columns (`CtrPtySpcfcData`, `CmonTradData`, `TechAttrbts`) plus file/header metadata.

`emir_raw` is structurally correct but analytically painful:

- Counterparty LEI lookup requires `WHERE CtrPtySpcfcData.CtrPty.RptgCtrPty.Id.Lgl.LEI = '...'` — verbose, brittle to XSD path changes, hostile to BI tools.
- Most BI tools handle 6-level-deep struct nesting poorly.
- "Choice" structures (Lgl / Ntrl / Othr identification options) coexist as sibling fields, most NULL per row — confusing to analysts who don't know which branch to query.
- Critical multi-valued arrays (schedule periods, beneficiaries) are buried inside struct trees, hard to filter or aggregate.

The legacy `src/notebooks/2_flatten_explode_table.py` (still scheduled as the `EMIR_XML_Processing.emir_xml_flatten` task) does generic recursive flatten — it works mechanically but produces a maze of tables with cryptic names (`emir_CtrPtySpcfcData_CtrPty_RptgCtrPty_Ntr_FI_Sctr`) and MD5 surrogate keys.

This branch designs a **domain-driven, opinionated EMIR silver layer** that turns the bronze trade-state snapshots into business-meaningful tables analysts can actually query.

## 2. Goals & Non-Goals

### Goals

- Four-table silver model, all in `{catalog}.{raw_schema}` (or a sibling `_silver` schema — see §3.2):
  - `trade` — the EMIR trade-state fact, one row per `<Stat>` per snapshot, fully flat with business-named columns
  - `trade_schedule` — unified schedule periods (price + notional amount/qty across legs)
  - `trade_beneficiary` — beneficiaries (array fan-out from `CtrPtySpcfcData.CtrPty.Bnfcry`)
  - `submission_file` — file-level envelope, regulation-agnostic shape
- Domain-meaningful column names: `reporter_lei` not `ctr_pty_spcfc_data_ctr_pty_rptg_ctr_pty_id_lgl_lei`
- Choice fields collapsed to the LEI common branch with a fallback `*_other_id` column; long-tail "Othr" name/scheme/domicile fields accessible via bronze
- Append-only (snapshot per `reporting_date`); SCD Type 2 migration path documented
- Spark-native, incremental via Spark Declarative Pipelines (SDP), serverless + Photon
- Forward-compatible with MiFIR via the regulation-agnostic `submission_file` envelope
- The legacy recursive-flatten notebook stays as the long-tail escape hatch

### Non-Goals

- MiFIR silver tables — separate follow-up
- Gold layer (aggregations, counterparty exposures, daily volume metrics) — separate follow-up after analysts confirm queries
- `trade_latest` view — analysts apply the `WHERE reporting_date = (SELECT MAX(...))` filter themselves
- Reference data joins (LEI-to-legal-entity-name, ISIN-to-instrument-name) — out of scope; LEI/ISIN stay as opaque codes
- Trade Activity Reports (auth.030 — TARTAR) — different XSD, different design, not on this branch
- SCD Type 2 — documented migration path, not implemented now (32M rows × daily snapshots in append-only Delta is fine for ≥1 year of history without overhead)
- Unit tests for the silver transformations — same posture as the bronze pipeline

## 3. Architecture

### 3.1 High-level

```
                   src/pipelines/xml_loader.py            (PR #1, already shipped)
                                │
                                ▼
              users.matthew_moorcroft.emir_raw            (bronze streaming Delta)
                                │  spark.readStream.table(...)
                                ▼
                 src/pipelines/silver_emir.py             (NEW — this spec)
                                │
       ┌────────────────────────┼─────────────────────────────┐
       ▼                        ▼                             ▼
     trade                trade_schedule              trade_beneficiary
       │                        │                             │
       └────────────┬───────────┴───────────────┬─────────────┘
                    ▼                           ▼
              submission_file             (file-level envelope,
              (NEW)                        regulation-agnostic shape)
```

Four `@dp.table()` streaming tables in one SDP source file. All read from `users.matthew_moorcroft.emir_raw` (the bronze). Each writes to `{catalog}.{raw_schema}` (or `{raw_schema}_silver` — see §3.2).

### 3.2 Schema layout

Two viable layouts. **Decision: same schema as bronze** for this branch — simpler bundle config, no new schema to create. If/when proliferation becomes a concern, splitting to `{raw_schema}_silver` is a one-line bundle change.

| Table | Fully qualified name |
|---|---|
| Bronze | `users.matthew_moorcroft.emir_raw` |
| Silver | `users.matthew_moorcroft.trade` |
| Silver | `users.matthew_moorcroft.trade_schedule` |
| Silver | `users.matthew_moorcroft.trade_beneficiary` |
| Silver | `users.matthew_moorcroft.submission_file` |

`{table_prefix}` from the bundle is NOT applied to silver tables — silver names are global ("the EMIR trade table" not "the emir_trade table"). Rationale: a developer working in the silver layer thinks in EMIR/MiFIR domain terms, not in regulation prefixes. The MiFIR follow-up would write `mifir_transaction` (or sit in a `mifir_silver` schema), unambiguous either way.

### 3.3 SCD strategy — append-only, partition by `reporting_date`

Each daily ESMA submission is a SNAPSHOT — the same trade reported every day with updated valuation, clearing status, etc. The silver `trade` table records EACH snapshot as its own row. Estimated growth: 32M rows × ~250 trading days/year ≈ 8B rows after a year. Delta partitioned/clustered on `reporting_date` handles this comfortably.

Analysts get "today's state" via:

```sql
SELECT * FROM users.matthew_moorcroft.trade
WHERE reporting_date = (SELECT MAX(reporting_date) FROM users.matthew_moorcroft.trade)
  AND reporter_lei = '...'
```

**Future migration to SCD Type 2** (documented, not implemented now):
1. Add `dp.create_streaming_table("trade_history")` and `dp.create_auto_cdc_flow(target="trade_history", source="trade", keys=["trade_id"], sequence_by=col("reporting_ts"), stored_as_scd_type=2)`
2. Repoint downstream queries to use `WHERE __END_AT IS NULL` instead of `WHERE reporting_date = MAX(...)` for the latest view
3. The append-only `trade` becomes the change feed; `trade_history` becomes the canonical "latest state" + lifecycle queries

This migration costs one new table and a SQL pattern shift — design is forward-compatible.

## 4. Table Definitions

> **Erratum (post-implementation):** During smoke test on real bronze data,
> five schema-path assumptions in §4.1 below turned out to differ from
> the actual `emir_raw` struct shape. The implementation reflects the
> corrections; this section reads literally per the original design.
> See `docs/superpowers/plans/2026-05-12-emir-silver-smoke-test-results.md`
> for the corrections — affected fields: `credit_payment_freq` (single
> STRING, not `*_unit` + `*_val`), `credit_tranche_attachment` /
> `_detachment` (under `Cdt.Trch.Trnchd`, plus new `credit_tranche_untranched`),
> `other_payments` ARRAY<STRUCT> (richer shape — adds `sign`, `payer_lei`,
> `receiver_lei`), STRIKE schedule rows (shared `Pric` shape with PRICE),
> and `commodity_*` COALESCE blocks (two-level taxonomy for 9 of 15
> categories). The contract (analyst-facing column names + business
> meanings) is unchanged.

### 4.0 Decision principle for which XSD fields become which kind of column

For every XSD leaf or sub-tree, the rule is:

| Will an analyst filter, group, or aggregate on this? | Outcome |
|---|---|
| **Yes** | Flat scalar column with a business-readable name. Always, no exceptions. Choice fields collapsed to the common branch (LEI) + a `*_other_id` fallback. |
| **No, but the data is still occasionally needed** | `ARRAY<…>` or `STRUCT<…>` column on `trade` — preserves fidelity without polluting the top level. Used when the field is multi-valued (settlement dates, other payments) OR a deep choice taxonomy where ≥95% of leaves would be NULL per row (commodity sub-product taxonomy, energy delivery attributes). |
| **No, and the field is genuinely long-tail** | Drop from silver. Accessible via bronze's `emir_raw` for the rare analyst who needs it. |

This rule applies field-by-field, not section-by-section. A given XSD struct may have some leaves promoted to flat columns and others kept nested — driven by analytical hotness, not XSD structure.

The wide-flat design is the right call for THIS branch because:
- It gives analysts business-readable column names at top level — the renaming + promotion alone is a substantive win over querying bronze's deep dot-notation paths.
- Spark/Delta + Photon handle wide tables fine; column pruning means SELECT cost scales with referenced columns, not table width.
- It minimises pipeline complexity (no surrogate-key generation, no dimension upsert logic).

A **star-schema migration path** is reserved as a documented future option (see §8 follow-ups): once GLEIF reference data integration becomes a priority OR cross-regulation analytics (EMIR + MiFIR sharing counterparty/instrument dims) becomes material, splitting out `dim_legal_entity` + `dim_date` is the natural next step. The wide-flat columns and clear column-to-XSD-path mapping make that pivot straightforward — every `reporter_lei`-style column becomes a natural surrogate-key target.

### 4.1 `trade` — main fact table

**Grain:** one row per `<Stat>` element per submission snapshot.
**Approx column count:** ~210 scalars + 3 array columns.
**Clustering:** `cluster_by=["AUTO"]`. Spark/Delta will likely pick `reporting_date`, `trade_id`, `reporter_lei`, `asset_class`.
**Naming conventions:**
- `*_lei` for LEI fields (legal entity identifier)
- `*_other_id` for the "Othr.Id" fallback when LEI isn't used
- `*_dt` for date columns, `*_ts` for timestamp columns
- `is_*` for boolean flags
- Currency columns paired: `*_amount` + `*_ccy` + `*_sign`
- Snake_case throughout
- Choice fields: pick the LEI/common branch; rare alternative branches accessible via bronze

The complete column list (organized by domain section — actual column count is the union; some columns appear in multiple conceptual groups for documentation):

**Identification:**
```
trade_id                            STRING       -- TxData.TxId.UnqTxIdr
trade_id_proprietary                STRING       -- TxData.TxId.Prtry.Id (UTI fallback)
prior_trade_id                      STRING
prior_trade_id_proprietary          STRING
prior_trade_id_not_available        STRING       -- 'NORE' / 'NOAP' marker
subsequent_trade_id                 STRING
subsequent_trade_id_proprietary     STRING
subsequent_trade_id_not_available   STRING
report_tracking_number              STRING       -- TxData.RptTrckgNb
platform_id                         STRING       -- TxData.PltfmIdr
```

**Reporting counterparty (CtrPty.RptgCtrPty):**
```
reporter_lei                        STRING       -- Id.Lgl.LEI
reporter_other_id                   STRING       -- Id.Lgl.Othr.Id.Id (fallback)
reporter_nature                     STRING       -- 'FI' | 'NFI' | 'CCP' | 'OTHR'
reporter_sectors                    ARRAY<STRING> -- Ntr.FI.Sctr[].Cd OR Ntr.NFI.Sctr[].Id (faithful array)
reporter_clr_threshold              BOOLEAN      -- Ntr.FI.ClrThrshld OR Ntr.NFI.ClrThrshld
reporter_nfi_directly_linked_activity BOOLEAN    -- Ntr.NFI.DrctlyLkdActvty
reporter_is_central_counterparty    BOOLEAN      -- Ntr.CntrlCntrPty IS NOT NULL
reporter_trading_capacity           STRING       -- 'PRIN' | 'AGEN'
reporter_direction_first_leg        STRING       -- 'BUYR' | 'SELR' | ...
reporter_direction_second_leg       STRING
reporter_side                       STRING       -- DrctnOrSd.CtrPtySd (fallback to direction)
```

**Other counterparty (CtrPty.OthrCtrPty):**
```
other_cp_lei                        STRING       -- IdTp.Lgl.Id.LEI
other_cp_country                    STRING       -- IdTp.Lgl.Ctry OR IdTp.Ntrl.Ctry
other_cp_natural_person_id          STRING       -- IdTp.Ntrl.Id.Id.Id (when applicable)
other_cp_nature                     STRING
other_cp_sectors                    ARRAY<STRING>
other_cp_clr_threshold              BOOLEAN
other_cp_is_central_counterparty    BOOLEAN
other_cp_has_reporting_obligation   BOOLEAN      -- OthrCtrPty.RptgOblgtn
```

**Other counterparty roles (CtrPty):**
```
broker_lei                          STRING
broker_other_id                     STRING
submitting_agent_lei                STRING
submitting_agent_other_id           STRING
clearing_member_lei                 STRING
clearing_member_other_id            STRING
entity_responsible_for_report_lei   STRING
entity_responsible_for_report_other_id STRING
```

**Contract data (CmonTradData.CtrctData):**
```
contract_type                       STRING       -- CtrctTp (SWAP, FORW, OPTN, FUTR, CFDS, ...)
asset_class                         STRING       -- AsstClss (CR, EQ, IR, FX, CO)
product_classification              STRING       -- PdctClssfctn (CFI code)
product_isin                        STRING
product_unq_pdct_idr                STRING
product_alternative_id              STRING
underlying_isin                     STRING
underlying_alternative_id           STRING
underlying_unq_pdct_idr             STRING
underlying_index_isin               STRING
underlying_index_name               STRING
underlying_index_value              STRING
underlying_basket_structure         STRING       -- Bskt.Strr (ISIN/Strr code)
underlying_basket_id                STRING
basket_constituents                 ARRAY<STRUCT<isin STRING, alternative_id STRING>>
underlying_id_not_available         STRING       -- 'IdNotAvlbl' code
settlement_ccy                      STRING
settlement_ccy_second_leg           STRING
deriv_based_on_crypto               BOOLEAN
```

**Transaction core (CmonTradData.TxData):**
```
execution_ts                        TIMESTAMP    -- ExctnTmStmp
effective_dt                        DATE         -- FctvDt
expiration_dt                       DATE         -- XprtnDt
early_termination_dt                DATE         -- EarlyTermntnDt
settlement_dates                    ARRAY<DATE>  -- SttlmDt[]
delivery_type                       STRING       -- DlvryTp ('CASH' | 'PHYS' | 'OPTL')
collateral_portfolio_cd             STRING       -- CollPrtflCd.Prtfl.Cd
has_no_collateral_portfolio         STRING       -- CollPrtflCd.Prtfl.NoPrtfl
master_agreement_type               STRING
master_agreement_type_proprietary   STRING
master_agreement_version            STRING
master_agreement_other_details      STRING
```

**Pricing (TxData.TxPric):**
```
price_monetary_value                DECIMAL(25,19)
price_monetary_ccy                  STRING
price_monetary_sign                 BOOLEAN
price_unit                          DECIMAL(25,19)
price_percentage                    DECIMAL(11,10)
price_yield                         DECIMAL(11,10)
price_pending                       STRING       -- PdgPric code
price_other_value                   DECIMAL(25,19)
price_other_type                    STRING
price_multiplier                    DECIMAL(25,19)
```

**Notional amounts (TxData.NtnlAmt):**
```
notional_first_leg_amount           DECIMAL(25,19)
notional_first_leg_ccy              STRING
notional_first_leg_sign             BOOLEAN
notional_second_leg_amount          DECIMAL(25,5)
notional_second_leg_ccy             STRING
notional_second_leg_sign            BOOLEAN
```

**Notional quantities (TxData.NtnlQty):**
```
notional_first_leg_total_qty        DECIMAL(25,19)
notional_second_leg_total_qty       DECIMAL(25,5)
notional_second_leg_qty_ccy         STRING
```

**Quantity (TxData.Qty):**
```
qty_unit                            DECIMAL(25,19)
qty_nominal_value                   DECIMAL(25,19)
qty_nominal_ccy                     STRING
qty_monetary_value                  DECIMAL(25,19)
qty_monetary_ccy                    STRING
```

**Clearing (TxData.TradClr):**
```
clearing_obligation                 STRING       -- ClrOblgtn
is_cleared                          BOOLEAN      -- ClrSts.Clrd IS NOT NULL
ccp_lei                             STRING       -- ClrSts.Clrd.Dtls.CCP.LEI
ccp_other_id                        STRING       -- ClrSts.Clrd.Dtls.CCP.Othr.Id.Id
cleared_ts                          TIMESTAMP    -- ClrSts.Clrd.Dtls.ClrDtTm
clearing_non_cleared_reason         STRING       -- ClrSts.NonClrd.Rsn
is_intragroup                       BOOLEAN      -- TradClr.IntraGrp
```

**Interest rate first leg (TxData.IntrstRate.FrstLeg):**
```
ir_first_leg_fixed_rate             DECIMAL(11,10)
ir_first_leg_fixed_day_count        STRING       -- DayCnt.Cd
ir_first_leg_fixed_day_count_narr   STRING       -- DayCnt.Nrrtv
ir_first_leg_fixed_pmt_freq_unit    STRING       -- PmtFrqcy.Term.Unit
ir_first_leg_fixed_pmt_freq_val     DECIMAL(3,0) -- PmtFrqcy.Term.Val
ir_first_leg_fixed_pmt_freq_prop    STRING       -- PmtFrqcy.Prtry
ir_first_leg_floating_index_id      STRING       -- Fltg.Id
ir_first_leg_floating_index_name    STRING       -- Fltg.Nm
ir_first_leg_floating_rate_cd       STRING       -- Fltg.Rate.Cd
ir_first_leg_floating_rate_prop     STRING       -- Fltg.Rate.Prtry
ir_first_leg_floating_ref_period_unit STRING     -- Fltg.RefPrd.Unit
ir_first_leg_floating_ref_period_val DECIMAL(3,0)
ir_first_leg_floating_spread_value  DECIMAL(18,13)
ir_first_leg_floating_spread_ccy    STRING
ir_first_leg_floating_spread_sign   BOOLEAN
ir_first_leg_floating_spread_pct    DECIMAL(11,10)
ir_first_leg_floating_spread_bps    DECIMAL(5,0)
ir_first_leg_floating_day_count     STRING
ir_first_leg_floating_pmt_freq_unit STRING
ir_first_leg_floating_pmt_freq_val  DECIMAL(3,0)
ir_first_leg_floating_rst_freq_unit STRING
ir_first_leg_floating_rst_freq_val  DECIMAL(3,0)
```

**Interest rate second leg (TxData.IntrstRate.ScndLeg):** Mirror of first leg — same column shape with `ir_second_leg_*` prefix. ~22 columns.

**FX (TxData.Ccy):**
```
delivery_ccy_cross                  STRING
xchg_rate                           DECIMAL(18,13)
forward_xchg_rate                   DECIMAL(18,13)
xchg_base_ccy                       STRING
xchg_quoted_ccy                     STRING
xchg_rate_basis_proprietary         STRING       -- Ccy.XchgRateBsis.Prtry
```

**Option attributes (TxData.Optn) — for any optional / option-like derivative:**
```
option_type                         STRING       -- Tp (CALL/PUT/OTHR)
option_exercise_style               STRING       -- ExrcStyle (AMER/EURO/BERM/ASIA)
option_strike_price                 DECIMAL(25,19) -- StrkPric (scalar; schedule goes to trade_schedule)
option_strike_price_ccy             STRING
option_premium_amount               DECIMAL(25,19) -- PrmAmt
option_premium_ccy                  STRING
option_premium_payment_dt           DATE         -- PrmPmtDt
option_underlying_maturity_dt       DATE         -- MtrtyDtOfUndrlyg
```
Note: `Optn.StrkPricSchdl[]` (strike-price schedule, multi-valued) becomes rows in `trade_schedule` with `schedule_type='STRIKE'` — see §4.2.

**Credit derivative attributes (TxData.Cdt) — for CDS, CDSX, credit indices:**
```
credit_seniority                    STRING       -- Cdt.Snrty (SNDB/SBOD/MZZD/...)
credit_reference_party_lei          STRING       -- Cdt.RefPty.LEI
credit_payment_freq_unit            STRING       -- Cdt.PmtFrqcy.Term.Unit
credit_payment_freq_val             DECIMAL(3,0) -- Cdt.PmtFrqcy.Term.Val
credit_calculation_basis            STRING       -- Cdt.ClctnBsis
credit_series                       STRING       -- Cdt.Srs
credit_version                      STRING       -- Cdt.Vrsn
credit_index_factor                 DECIMAL(11,10) -- Cdt.IndxFctr
credit_tranche_attachment           DECIMAL(11,10) -- Cdt.Trch.AttchmntPt (if present)
credit_tranche_detachment           DECIMAL(11,10) -- Cdt.Trch.DtchmntPt
```

**Package transaction attributes (TxData.Packg):**
```
package_complex_trade_id            STRING       -- Packg.CmplxTradId
package_price                       DECIMAL(25,19) -- Packg.Pric
package_spread                      DECIMAL(25,19) -- Packg.Sprd
```

**Other payments (TxData.OthrPmt[]):**
```
other_payments                      ARRAY<STRUCT<
                                       payment_type STRING,   -- e.g., UFRO, PRYM
                                       amount       DECIMAL(25,19),
                                       ccy          STRING,
                                       payment_dt   DATE
                                     >>
```
Multi-valued, rarely individually queried; analysts who need per-payment detail explode this on demand.

**Commodity product taxonomy (TxData.Cmmdty) — for commodity derivatives only:**
Three COALESCE'd promoted columns at top level (analyst answer to "what kind of commodity?"):
```
commodity_base_product              STRING       -- COALESCE across Agrcltrl|Nrgy|Envttl|Frtlzr|Frght|Indx|IndstrlPdct|Infltn|Metl|MultiCmmdtyExtc|OffclEcnmcSttstcs|Othr|OthrC10|Ppr|Plprpln base
commodity_sub_product               STRING       -- COALESCE across the SubPdct fields
commodity_additional_sub_product    STRING       -- COALESCE across the AddtlSubPdct fields
```
The deep 15-branch taxonomy detail (specific to e.g., agricultural→dairy→grade-A vs agricultural→grain→wheat-type) is NOT carried in silver — accessible via bronze for the rare deep-commodity-analytics use case. This keeps the trade table from gaining ~50 mostly-NULL columns for the ~5–10% of trades that are commodity-class.

**Energy-specific attributes (TxData.NrgySpcfcAttrbts) — for energy commodity sub-class:**
```
energy_interconnection_point        STRING       -- IntrCnnctnPt
energy_load_type                    STRING       -- LdTp
energy_delivery_zones               ARRAY<STRING> -- DlvryPtOrZone[]
energy_delivery_attributes          STRUCT<
                                       frequency STRING,
                                       time_interval STRING,
                                       time_zone STRING,
                                       delivery_capacity STRING,
                                       quantity_unit STRING,
                                       price_per_unit DECIMAL(25,19),
                                       price_ccy STRING
                                     >    -- DlvryAttr — kept as struct (composite, rarely scalar-queried)
```

**Lifecycle / risk-reduction / confirmation:**
```
contract_modification_action_type   STRING       -- CtrctMod.ActnTp (NEWT/MODI/CORR/TERM/REVI/CANC/EROR/POSC)
contract_modification_level         STRING       -- CtrctMod.Lvl ('TCTN' / 'PSTN')
is_compression                      BOOLEAN      -- Cmprssn
is_post_trade_risk_reduction        BOOLEAN      -- PstTradRskRdctnFlg
ptrr_technique                      STRING       -- PstTradRskRdctnEvt.Tchnq
ptrr_service_provider_lei           STRING       -- PstTradRskRdctnEvt.SvcPrvdr.LEI
deriv_event_type                    STRING       -- DerivEvt.Tp
deriv_event_ptrr_strr               STRING       -- DerivEvt.Id.PstTradRskRdctnIdr.Strr
deriv_event_ptrr_id                 STRING       -- DerivEvt.Id.PstTradRskRdctnIdr.Id
deriv_event_dt                      DATE         -- DerivEvt.TmStmp.Dt
trade_confirmation_type             STRING       -- TradConf.Confd.Tp OR TradConf.NonConfd.Tp
trade_confirmation_ts               TIMESTAMP    -- TradConf.Confd.TmStmp
```

**Valuation (CtrPtySpcfcData.Valtn):**
```
contract_value                      DECIMAL(25,19)
contract_value_ccy                  STRING
contract_value_sign                 BOOLEAN
delta                               DECIMAL(25,5)
valuation_ts                        TIMESTAMP
valuation_type                      STRING       -- MTM / MTMR / ...
```

**Reporting metadata + audit / lineage:**
```
reporting_ts                        TIMESTAMP    -- CtrPtySpcfcData.RptgTmStmp
reconciliation_flag                 STRING       -- TechAttrbts.RcncltnFlg
data_set_action                     STRING       -- File-level TradData.DataSetActn (denormalized for filter convenience)
file_path                           STRING
file_name                           STRING
reporting_date                      DATE         -- parsed from filename or ESMADate; the partition / cluster key
batch_index                         INT
batch_size                          INT
file_version                        INT
biz_msg_id                          STRING       -- from header
sender_lei                          STRING       -- from header (Fr.OrgId)
recipient_lei                       STRING       -- from header (To.OrgId)
ingested_at                         TIMESTAMP
silver_processed_at                 TIMESTAMP    -- current_timestamp() at silver write
```

**Total `trade` columns: ~232 scalars + 5 array columns + 1 struct column**:
- Array columns: `reporter_sectors`, `other_cp_sectors`, `settlement_dates`, `basket_constituents`, `energy_delivery_zones`
- Plus 1 ARRAY<STRUCT> column: `other_payments`
- Plus 1 STRUCT column: `energy_delivery_attributes` (preserved as composite — rarely scalar-queried)

### 4.2 `trade_schedule`

**Grain:** one row per schedule period across all schedule types.

```
trade_id                  STRING       -- FK to trade.trade_id (NOT enforced, by convention)
reporting_date            DATE         -- denormalized for partition pruning
schedule_type             STRING       -- 'PRICE' | 'NTNL_AMT_LEG_1' | 'NTNL_AMT_LEG_2' | 'NTNL_QTY_LEG_1' | 'NTNL_QTY_LEG_2'
sequence_no               INT          -- position within the schedule array (posexplode)
unadj_effective_dt        DATE
unadj_end_dt              DATE
amount                    DECIMAL(25,19)
amount_ccy                STRING
amount_sign               BOOLEAN
percentage                DECIMAL(11,10)
quantity                  DECIMAL(25,5)
ingested_at               TIMESTAMP
silver_processed_at       TIMESTAMP
```

Clustering: `AUTO` (likely picks `trade_id` + `unadj_effective_dt`).

The schedule discriminator is filled per source path:
- `TxPric.SchdlPrd[]` → `PRICE`
- `NtnlAmt.FrstLeg.SchdlPrd[]` → `NTNL_AMT_LEG_1`
- `NtnlAmt.ScndLeg.SchdlPrd[]` → `NTNL_AMT_LEG_2`
- `NtnlQty.FrstLeg.Dtls.SchdlPrd[]` → `NTNL_QTY_LEG_1`
- `NtnlQty.ScndLeg.Dtls.SchdlPrd[]` → `NTNL_QTY_LEG_2`
- `Optn.StrkPricSchdl[]` → `STRIKE`

Source-specific fields populate the union: `PRICE` rows populate `amount`/`percentage`; `NTNL_AMT_*` populate `amount`/`amount_ccy`/`amount_sign`; `NTNL_QTY_*` populate `quantity`. Unused fields stay NULL.

### 4.3 `trade_beneficiary`

**Grain:** one row per beneficiary per trade.

```
trade_id                       STRING       -- FK to trade.trade_id
reporting_date                 DATE         -- denormalized for partition pruning
sequence_no                    INT
beneficiary_lei                STRING       -- Bnfcry.Lgl.Id.LEI (most common)
beneficiary_other_id           STRING       -- Bnfcry.Lgl.Id.Othr.Id.Id
beneficiary_natural_person_id  STRING       -- Bnfcry.Ntrl.Id.Id.Id
beneficiary_type               STRING       -- 'LEGAL' | 'NATURAL' | 'OTHER'
ingested_at                    TIMESTAMP
silver_processed_at            TIMESTAMP
```

### 4.4 `submission_file`

**Grain:** one row per ESMA XML file ingested. Regulation-agnostic — MiFIR will write to the same table later with `regulation='MIFIR'`.

```
file_path                      STRING       -- PK
file_name                      STRING
reporting_date                 DATE         -- partition / cluster key
esma_date_str                  STRING       -- raw filename regex (kept for debug)
batch_index                    INT
batch_size                     INT
file_version                   INT
biz_msg_id                     STRING
sender_lei                     STRING
recipient_lei                  STRING
message_def_id                 STRING       -- 'auth.107.001.01_ESMAUG_DATTSR_1.1.0'
business_service               STRING
header_creation_ts             TIMESTAMP
number_of_records              BIGINT       -- DerivsTradStatRpt.RptHdr.NbRcrds
data_set_action                STRING       -- TradData.DataSetActn
ingested_at                    TIMESTAMP    -- min(ingested_at) over rows
silver_processed_at            TIMESTAMP
regulation                     STRING       -- 'EMIR' (constant for this branch; 'MIFIR' added by the follow-up)
```

Built via `dropDuplicates(["file_path"])` over the bronze stream — one published row per file. Same pattern as the (now removed) bronze `file_hdr_metadata` intermediate.

## 5. Implementation Details

### 5.1 Pipeline source structure

New file: `src/pipelines/silver_emir.py`. Layout mirrors `xml_loader.py`:
- Module docstring + design-doc reference
- `from pyspark import pipelines as dp` + `pyspark.sql.functions as F` + types
- Module-level `spark.conf.get(...)` reads for `catalog`, `raw_schema`, `silver_schema` (defaults to `raw_schema`), `bronze_table` (defaults to `{prefix}_raw`)
- Fully qualified output table-name constants (`TBL_TRADE`, `TBL_TRADE_SCHEDULE`, `TBL_TRADE_BENEFICIARY`, `TBL_SUBMISSION_FILE`)
- Helpers:
  - `_lei_or_null(struct_col)` — extract LEI from `Id.Lgl.LEI`; null when path missing
  - `_other_id_or_null(struct_col)` — extract `Id.Lgl.Othr.Id.Id`
  - `_safe(path, type)` — wrap nested dot-access with try/except None to handle missing branches gracefully
- Four `@dp.table(name=..., comment=..., cluster_by_auto=True)` functions

### 5.2 Mapping convention

For each silver column the SDP source has one line of the form:
```python
F.col("CmonTradData.CtrctData.AsstClss").alias("asset_class")
```

Long `select()` chains with explicit `.alias()` calls. Verbose but grep-able and easy to update when XSD versions shift.

For choice fields:
```python
F.coalesce(
    F.col("CtrPtySpcfcData.CtrPty.RptgCtrPty.Id.Lgl.LEI"),
    F.lit(None).cast("string"),
).alias("reporter_lei")
```

Or for the "first non-null branch" pattern:
```python
F.coalesce(
    F.col("...Id.Lgl.LEI"),
    F.col("...Id.Lgl.Othr.Id.Id"),
).alias("reporter_id")  # used only where we don't separate LEI vs Other
```

### 5.3 Reading from bronze

`spark.readStream.table(TBL_BRONZE)` for streaming-incremental processing. Each silver table is a `@dp.table()` returning a streaming DataFrame.

`trade` reads bronze, performs the wide `.select(...)` mapping, and parses `reporting_date` from `ESMADate` or filename regex. NOT `dropDuplicates` — bronze is already de-duplicated upstream and snapshots ARE supposed to repeat per day.

`trade_schedule` reads bronze, performs six `posexplode(...)` operations (one per schedule type: PRICE, NTNL_AMT_LEG_1, NTNL_AMT_LEG_2, NTNL_QTY_LEG_1, NTNL_QTY_LEG_2, STRIKE), unions the six sub-DataFrames, and selects unified columns. Implementation note: use `F.posexplode_outer` so trades with NULL schedule arrays don't drop from the join graph (zero-row output is fine, but we want no SparkSession warnings).

`trade_beneficiary` reads bronze, `posexplode(F.col("CtrPtySpcfcData.CtrPty.Bnfcry"))`, derives `beneficiary_type` from which branch is populated.

`submission_file` reads bronze, `dropDuplicates(["file_path"])`, extracts the small set of file-level fields. This one re-creates the per-file table pattern we removed from the bronze loader, but at the silver grain it's a published table with a documented audit purpose.

### 5.4 Bundle resources

New SDP pipeline resource added to `resources/bundle.emir_resources.yml` under the existing `# === Spark Declarative Pipelines ===` section:

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
        silver_schema: ${var.emir_raw_schema}          # same schema for v1
        bronze_table: ${var.emir_table_prefix}_raw
        regulation: "EMIR"
```

No new bundle variables required for v1 (defaults derived from existing `emir_*`). The `regulation` constant is set in pipeline config so the same file could in principle drive MiFIR silver too — but the follow-up will likely diverge enough that a separate `silver_mifir.py` is cleaner.

Targets get the standard `development: true|false` override (same pattern as the bronze pipeline).

### 5.5 Legacy flatten notebook

`src/notebooks/2_flatten_explode_table.py` stays in place AND stays scheduled as the second task in `EMIR_XML_Processing`. The silver SDP pipeline runs in parallel — not as a replacement for the legacy notebook, but as an additive domain-driven alternative.

When silver is proven in production, the legacy notebook can be retired in a follow-up branch. For now: both exist. Documented in README.

## 6. Validation Plan

### 6.1 Target environment

Same E2 setup as PR #1: `e2-demo-field-eng.cloud.databricks.com`, `users.matthew_moorcroft`, central_bank_ireland volume. Bronze `emir_raw` populated (32M rows, XSD validation OFF for the test data — production deployments would re-enable). Local override file already in place from PR #1.

### 6.2 Deploy and run

1. `databricks bundle validate -t dev` — passes
2. `databricks bundle deploy -t dev` — creates `[dev <user>] EMIR Silver (domain-driven)` alongside the existing bronze pipeline
3. `databricks bundle run emir_silver_pipeline -t dev` — completes

### 6.3 Row-count invariants

After the silver pipeline completes:

```sql
-- trade should equal bronze (one-to-one per Stat row)
SELECT COUNT(*) FROM users.matthew_moorcroft.trade
  -- expected: 32000000 (matches emir_raw)

-- submission_file: one per ingested file
SELECT COUNT(*) FROM users.matthew_moorcroft.submission_file
  -- expected: 64

-- trade_schedule: 0 or more per trade (depends on whether the synthetic data has schedules)
SELECT schedule_type, COUNT(*) FROM users.matthew_moorcroft.trade_schedule GROUP BY 1

-- trade_beneficiary: 0 or more per trade
SELECT COUNT(*) FROM users.matthew_moorcroft.trade_beneficiary
```

### 6.4 Spot-check semantic correctness

```sql
-- Most-traded LEIs
SELECT reporter_lei, COUNT(*) AS trades
FROM users.matthew_moorcroft.trade
GROUP BY reporter_lei
ORDER BY trades DESC LIMIT 10

-- Asset class breakdown
SELECT asset_class, COUNT(*) FROM users.matthew_moorcroft.trade GROUP BY 1

-- Cleared vs uncleared
SELECT is_cleared, COUNT(*) FROM users.matthew_moorcroft.trade GROUP BY 1
```

Confirm that:
- `reporter_lei` is a 20-character LEI-shaped string (or NULL if `*_other_id` is populated instead — should not be both NULL)
- `asset_class` is one of CR/EQ/IR/FX/CO
- `execution_ts` is a real timestamp
- `is_cleared` is BOOLEAN, not the string "true"
- `contract_value` is DECIMAL with sensible precision

### 6.5 BI / analyst feel test

Run a query that would have been painful against bronze:

```sql
-- Top 10 reporting counterparties by gross notional, current snapshot
SELECT reporter_lei,
       SUM(ABS(notional_first_leg_amount)) AS gross_notional_leg_1
FROM users.matthew_moorcroft.trade
WHERE reporting_date = (SELECT MAX(reporting_date) FROM users.matthew_moorcroft.trade)
  AND notional_first_leg_amount IS NOT NULL
GROUP BY reporter_lei
ORDER BY gross_notional_leg_1 DESC
LIMIT 10
```

Acceptance: the query is readable, returns sensible-looking results in under a few seconds against 32M rows on serverless + Photon.

### 6.6 Performance baseline

Capture wall time + cluster sizing for the first silver pipeline run on 32M bronze rows. Compare to bronze's ~11.5 min baseline. Silver should be substantially faster (no XML parsing, no lxml UDFs, pure SQL transformations on Delta).

Document results in `docs/superpowers/plans/2026-05-12-emir-silver-smoke-test-results.md` after the implementation lands.

## 7. Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Wide `trade` table (~210 cols) hard to maintain across XSD version bumps | Maintenance burden when SWIFT publishes auth.107.001.02 | Each column has one `.alias()` line; XSD diffs translate to targeted edits. Documented in `silver_emir.py` |
| Synthetic CBI data may not populate all 210 columns | False sense of correctness | Validation §6.4 spot-checks. Some columns will be 100% NULL in synthetic data — that's expected; assert non-null only on the fields the data is supposed to fill |
| Schedule discriminator confusion (analysts forget to filter by `schedule_type`) | Wrong aggregations | Documented in `trade_schedule` table COMMENT. `schedule_type` is a partition-friendly cluster key |
| Choice-field policy drops natural-person counterparties | Lossy for retail-derivative edge cases | Bronze remains as escape hatch. Documented in §2 non-goals |
| `reporter_sectors ARRAY<STRING>` is BI-tool-hostile | Some downstream tools can't filter on arrays | If proven painful, derive a `reporter_primary_sector_cd STRING` column in a follow-up (first array element) |
| Append-only storage grows unbounded (8B+ rows/year) | Cost at scale | Documented SCD2 migration path. Partition-by-`reporting_date` keeps query cost flat |
| Legacy notebook + silver pipeline both running might double bronze read | Slight cost duplication | Acceptable for v1; legacy notebook is removed in the follow-up that retires it |
| Decision: same schema as bronze (no `_silver` schema) | Some analysts get confused when listing schema | Tables clearly distinguishable by name (`trade` vs `emir_raw`). Easy to split later if pain emerges |

## 8. Open Follow-Ups

- **MiFIR silver** — separate brainstorm + spec; pattern from this design (domain entities + envelope) is reusable
- **Gold layer** — once analysts have queried silver for a few weeks, identify the actual hot aggregations and design `daily_*` gold tables. Could include metric views.
- **SCD Type 2 migration** — when the append-only volume becomes unmanageable OR when analysts ask for "trade lifecycle" queries
- **Star-schema pivot — `dim_legal_entity` + `dim_date`** — When cross-regulation analytics (EMIR + MiFIR sharing counterparty dim) becomes a priority OR GLEIF reference data integration starts. Migration is mechanical: the current `*_lei` columns become surrogate-key targets; the entity dimension absorbs sector/nature/country attributes that currently appear as separate columns in `trade`. Wide-flat design was chosen for v1 because the renaming + promotion alone is a substantive analytics win and the pipeline complexity is lower.
- **Bronze filename-regex parameterization** — `xml_loader.py` currently hard-codes `_FILE_INDEX_PATTERN = r"\d\d\d\d\d\d-\d"` and `_ESMA_DATE_PATTERN = r"-\d\d\d\d\d\d_"`. These are ESMA-specific. Customer deployments with different naming conventions need bundle-config parameters. Out of scope for this silver spec (a bronze concern), queued as a separate small follow-up branch.
- **Retire legacy `2_flatten_explode_table.py`** — once silver is production-proven, remove the notebook and its job task. Update `bundle.new-type_resources.yml.template` to scaffold without the flatten step.
- **`reporter_primary_sector_cd`** (and other array-to-scalar derivations) — add if BI-tool friction proves real
- **Deep commodity taxonomy in silver** — current spec keeps only the COALESCE'd base/sub/additional-sub product codes. If commodity-derivatives analytics becomes a focus, flatten the 15-branch product taxonomy or add a `commodity STRUCT<>` column with the full sub-tree preserved.
- **Unit tests for the silver column-mapping** — currently we trust the SDP runtime; UDF or pure-SQL transforms should be unit-testable in isolation
- **Reference data joins** — eventually join LEI to legal-entity name / country; out of scope for v1 (becomes a natural `dim_legal_entity` enrichment under the star-schema pivot)

## 9. Approval

All sections (scope, architecture, table definitions, implementation, validation) reviewed and approved interactively before this document was written. Decisions captured:
- 4-table cut: `trade`, `trade_schedule`, `trade_beneficiary`, `submission_file`
- Domain-driven, business-readable column names. Wide-flat scalars in `trade` (~232 cols), per-field decision rule (flatten what analysts query; STRUCT/ARRAY where data is needed but rarely queried; drop long-tail) — see §4.0
- ARRAY/STRUCT columns retained for the long-tail composites: `reporter_sectors`, `other_cp_sectors`, `settlement_dates`, `basket_constituents`, `energy_delivery_zones`, `other_payments ARRAY<STRUCT>`, `energy_delivery_attributes STRUCT<>`
- 6 product-class-specific TxData sections covered: `Optn`, `Cdt`, `Packg` fully flat; `OthrPmt` as ARRAY<STRUCT>; `Cmmdty` deep taxonomy collapsed to 3 COALESCE'd promoted columns (`commodity_base_product` etc.) with deep tree NOT in silver; `NrgySpcfcAttrbts` partially flat with one STRUCT for delivery attributes
- Choice fields: LEI primary + `*_other_id` fallback; rare branches accessible via bronze
- SCD: append-only, partition/cluster on `reporting_date`. SCD2 migration documented (§3.3)
- Star-schema migration path documented as a follow-up (§8). Wide-flat chosen for v1 because the renaming + promotion alone is a substantive win and pipeline complexity is lower
- No `trade_latest` view — analysts handle filtering
- No gold layer in this spec
- Legacy `2_flatten_explode_table.py` kept as escape hatch; same-schema layout for silver
- Bronze filename-regex parameterization queued as a separate follow-up branch (§8); out of scope for silver
