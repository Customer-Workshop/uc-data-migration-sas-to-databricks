# SAS → dbt/Databricks — End-to-End Demo Runbook

A step-by-step script for showcasing, **live against a real Databricks
workspace**, how Devin converts a SAS estate into runnable dbt/Databricks code —
with **verifiable confidence** at every step. The headline is not the finished
artifact; it is Devin doing the migration: reasoning over unfamiliar legacy code,
converting it off a playbook, proving parity with the source through programmatic
reconciliation, catching a real divergence, fixing it, and opening a PR — and
doing it in parallel across many programs.

This demo is designed to be run **many times** without ever changing the repo's
`main` (before) state or the durable raw source data.

> The demo doc `demos/data-engineering/sas-to-databricks-demo.md` in
> `workshop-metadata` mirrors this runbook. The commands and prompts are kept
> identical between the two — if you change one, change the other.

## Table of Contents

- [What this demo proves](#what-it-proves)
- [The mental model: before, after, and the verification loop](#mental-model)
- [Prerequisites & one-time setup](#setup)
- [Part 1 — Devin does the migration (the headline)](#part-1)
  - [Act 1 — Orient in seconds (Ask Devin / DeepWiki)](#act-1)
  - [Act 2 — Convert one program live, with verification](#act-2)
  - [Act 3 — Fan out in parallel](#act-3)
  - [Act 4 — Confidence = programmatic verification](#act-4)
- [Part 2 — Run the produced artifact live (before/after)](#part-2)
  - [Build the "after" + reconcile](#build)
  - [Query before vs after](#query)
  - [The PySpark alternative](#pyspark)
  - [IaC + CD: deploy, run, revert](#iac-cd)
- [Concurrent runs (isolated DB spaces)](#concurrent)
- [Warehouse sizing note](#warehouse)
- [Cheat sheet](#cheat-sheet)

---

<a id="what-it-proves"></a>
## What this demo proves

1. **Acceleration** — Devin reads an unfamiliar SAS estate and converts programs
   to runnable dbt/PySpark in minutes, off a reusable playbook so every run is
   consistent.
2. **Confidence through verification** — every conversion is gated by
   reconciliation controls (completeness, control totals, source-parity mappings)
   that fail loudly when the conversion drifts from the SAS source. The demo
   shows a **real divergence being caught and fixed**.
3. **Scale** — conversions are independent, so multiple Devin sessions convert
   multiple programs in parallel, each producing its own verified PR.

> **On "parity":** there is no live SAS runtime in this environment, so parity
> means *source → target reconciliation* against the SAS code as the source of
> truth (control totals, row counts, mapping parity, referential integrity) plus
> dbt tests — a deterministic contract, not a byte-for-byte SAS-vs-Databricks
> output diff. This is stated plainly so the confidence story is honest.

---

<a id="mental-model"></a>
## The mental model: before, after, and the verification loop

| | Code | Data |
|---|---|---|
| **Before** | `main` branch: the **banking** domain already migrated (Phase 1), plus the tooling, reconciliation harness, seeder, and the [`docs/CONVERSION_PLAYBOOK.md`](CONVERSION_PLAYBOOK.md). The SAS estate lives in [`ts-sas-legacy-analytics`](https://github.com/Cognition-Partner-Workshops/ts-sas-legacy-analytics). | `banking_analytics.raw.*` — raw source tables (mirror the SAS LIBNAME inputs). **Durable; never overwritten.** |
| **After** | a PR branch with the **regulatory + insurance** programs converted live (dbt models + their reconciliation controls + the PySpark job + IaC/CD) | `banking_analytics.<NS>_staging / _intermediate / _marts / _curated` — built per run, disposable |

The **before** state is deliberately a *partial* migration: the banking programs
(`load_customer_accounts`, `daily_transaction_processing`, `credit_risk_scoring`)
are already on `main` with a working reconciliation control, so the harness and
playbook are in place. What Devin converts **live** in the demo is the next
wave — the regulatory and insurance programs that are *not* yet on `main`. That
is why `main` stays a faithful before-state you can return to every run.

The **verification loop** sits between before and after: every converted model is
built into a namespace and checked by reconciliation controls before it is
trusted. The **before** state (raw data + `main`) is durable; the **after** state
lives in per-namespace schemas you can create and destroy at will, so multiple
runs never collide.

---

<a id="setup"></a>
## Prerequisites & one-time setup

- A Databricks workspace with Unity Catalog and a (serverless) SQL warehouse.
- A workspace PAT for a user/SP that can use the warehouse, create
  catalogs/schemas/tables, and create/run jobs.
- Python 3.9+ and the Databricks CLI (the one that supports `bundle`).

```bash
export DATABRICKS_HOST="https://dbc-248ae8cf-2d5d.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/890be8990bf134aa"
export DATABRICKS_TOKEN="dapi..."   # PAT generated in THIS workspace

# Part 1 (Devin converts live): start from `main` — it already has the harness,
#   seeder, and playbook; Devin produces the "after" on a fresh branch + PR.
# Part 2 (run the finished artifact): check out the "after" reference branch.
git checkout main
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # dbt-core, dbt-databricks, sqlfluff
pip install -r seed/requirements.txt     # faker, databricks-sql-connector
pip install -r verify/requirements.txt   # databricks-sql-connector (reconcile report)
cd dbt_project && dbt deps && cd ..

databricks current-user me               # confirm connectivity
make seed                                # seed before-state raw data (idempotent)
```

---

<a id="part-1"></a>
# Part 1 — Devin does the migration (the headline)

<a id="act-1"></a>
## Act 1 — Orient in seconds (Ask Devin / DeepWiki)

Open the SAS estate and ask Devin to explain it. The point: Devin reasons over
unfamiliar legacy code immediately — no week of ramp-up.

```
Using the ts-sas-legacy-analytics repo, give me a map of the SAS estate:
the banking and insurance programs, what each one reads and writes, the
LIBNAMEs, the macros and PROC FORMATs they depend on, and which programs are
set-based (good for dbt) vs procedural/multi-output (better as PySpark).
```

Expected: a tour of `Programs/Banking/*`, `Programs/Insurance/*`, the
`Macro/` and `Formats/` dependencies, and the Control-M-style `BatchJobs/`
wrappers — with a dbt-vs-PySpark recommendation per program.

<a id="act-2"></a>
## Act 2 — Convert one program live, with verification

This is the core beat. Paste the playbook prompt for one program. Devin reads the
SAS, writes the dbt model + reconciliation controls, builds against the live
workspace, runs the controls, **catches a divergence, fixes it**, and opens a PR
with the reconciliation report.

```
Convert the SAS program Programs/Banking/monthly_regulatory_reporting.sas in
the ts-sas-legacy-analytics estate into dbt models on Databricks, following
docs/CONVERSION_PLAYBOOK.md in uc-data-migration-sas-to-databricks.

- Treat the SAS source as the source of truth: reproduce its logic exactly,
  including any quirks, and flag (do not silently fix) anything that looks wrong.
- Add reconciliation controls: completeness, a control total, and a parity check
  for every CASE/mapping in the program.
- Build into the dev namespace and run dbt build + the reconciliation report
  until everything is green. Include the reconciliation report in the PR.
```

**The verification beat (use the real bug).** The SAS CASE maps `LOC` (line of
credit) → risk weight **1.00**. A plausible-looking conversion maps `LOC` → 0.75
to match the other revolving-credit products — which **diverges from the source**
and silently overstates capital relief. The parity control catches it:

```bash
# Simulate the wrong conversion, then watch the control fail:
make reconcile NS=dev
#   rwa_risk_weight_parity | FAIL | LOC=0.75 (source expects 1.00)
```

Devin restores `LOC` → 1.00 (source-faithful), re-runs, and the report goes green:

```bash
make reconcile NS=dev
#   rwa_risk_weight_parity | PASS | all account_types match the SAS risk-weight mapping
```

The takeaway to say out loud: *"looks reasonable" review would have shipped the
0.75 value; the parity check against the source did not.* See the full write-up
in `docs/CONVERSION_PLAYBOOK.md` → *Worked example: the LOC risk-weight divergence*.

<a id="act-3"></a>
## Act 3 — Fan out in parallel

Conversions are independent, so launch a Devin session per program. Each follows
the same playbook and opens its own verified PR — the same review bar applied N
times in parallel instead of once in series.

These are the programs **not yet on `main`** — the regulatory + insurance wave —
so each session genuinely produces a new conversion (Act 2's regulatory program
is the worked example; the rest fan out alongside it):

| Session | SAS program | Target |
|---|---|---|
| 1 | `Programs/Banking/monthly_regulatory_reporting.sas` | `mart_regulatory_rwa` + `mart_delinquency_aging` (the Act 2 worked example) |
| 2 | `Programs/Insurance/claims_processing.sas` | `stg_claims` + `int_claims_adjudication` (dbt) **and** the PySpark `claims_processing` job (procedural, multi-output) |
| 3 | `Programs/Insurance/policy_valuation.sas` | `int_policy_valuation` + `mart_loss_ratios` |
| 4 | `Programs/Reports/customer_profitability.sas` | `mart_customer_pnl` |

Each session uses its own namespace (`NS=session1`, …) so the live builds never
collide.

### Parallelize from a single session (parent → child)

Instead of launching each session by hand, run one **orchestrator** session that
spawns a child Devin session per program and monitors them — one agent fanning
itself out across the wave. Paste:

```
Act as the orchestrator for a SAS->Databricks migration across multiple
programs, using child Devin sessions to parallelize the work.

Repos: read Cognition-Partner-Workshops/ts-sas-legacy-analytics (the SAS
source), write Cognition-Partner-Workshops/uc-data-migration-sas-to-databricks
(follow docs/CONVERSION_PLAYBOOK.md).

Spawn one child Devin session per program below. Give each child both repos, its
own namespace (NS=child1, child2, ...), and this conversion contract: treat the
SAS source as the source of truth and reproduce its logic exactly; flag (do not
silently fix) anything that looks wrong; add reconciliation controls
(completeness, a control total, and a parity check for every CASE/mapping); and
build with `make demo-up NS=...` and `make reconcile NS=...` until everything is
green, with the reconciliation report included.

Programs:
1. Programs/Banking/monthly_regulatory_reporting.sas
   -> mart_regulatory_rwa + mart_delinquency_aging
2. Programs/Insurance/claims_processing.sas
   -> stg_claims + int_claims_adjudication (dbt) AND a PySpark job
      src/pyspark/claims_processing.py
3. Programs/Insurance/policy_valuation.sas
   -> int_policy_valuation + mart_loss_ratios
4. Programs/Reports/customer_profitability.sas -> mart_customer_pnl

After launching, monitor the child sessions until each program is converted with
a green reconciliation report. Then summarize the results and call out any
source-parity divergences the children caught (e.g. a risk-weight mapping that
did not match the SAS source).
```

The children inherit the organization's Databricks secrets, and each writes to
its own namespace (`child1`, `child2`, …) so the parallel builds never collide.
This is the same verified conversion loop as a single session — run many times at
once, from one parent.

<a id="act-4"></a>
## Act 4 — Confidence = programmatic verification

Show the gates that make every PR trustworthy:

- **CI** (`.github/workflows/dbt_ci.yml`): sqlfluff lint → `dbt parse` →
  `dbt test` (schema **and** reconciliation tests against the live workspace) →
  **reconciliation report** job that publishes the report as a build artifact.
- **Reconciliation controls** (`dbt_project/tests/reconcile_*.sql` +
  `verify/reconcile.py`): completeness, control totals, source-parity mappings,
  cross-engine referential integrity. See the contract in
  `docs/CONVERSION_PLAYBOOK.md`.
- **Devin Review**: an automated reviewer on every PR.

The story: a conversion is only "done" when the source-parity controls are green,
in CI, on the PR — not when the code merely runs.

---

<a id="part-2"></a>
# Part 2 — Run the produced artifact live (before/after)

The second half shows the converted estate actually running end to end, and the
repeatable before/after you can reset between runs.

<a id="build"></a>
## Build the "after" + reconcile

```bash
make demo-up   NS=dev    # seed (idempotent) + dbt build into dev_* schemas
make reconcile NS=dev    # source -> target reconciliation report
```

Expected: `dbt build` ends `PASS=104 ERROR=0` (13 models + schema tests + the
reconciliation tests), and the reconciliation report shows all controls **PASS**.

<a id="query"></a>
## Query before vs after

Both states are live in the warehouse at the same time:

```sql
SELECT count(*) FROM banking_analytics.raw.cust_accounts;            -- before
SELECT count(*) FROM banking_analytics.dev_marts.mart_risk_scores;  -- after

SELECT * FROM banking_analytics.dev_marts.mart_customer_pnl
ORDER BY net_profit DESC LIMIT 20;

SELECT account_type, risk_weight, n_accounts, total_exposure, rwa
FROM banking_analytics.dev_marts.mart_regulatory_rwa
ORDER BY account_type;     -- note LOC at risk_weight = 1.00, matching the SAS source
```

<a id="pyspark"></a>
## The PySpark alternative

dbt handles the set-based transforms; the procedural, multi-output
`claims_processing.sas` is shown as a **custom PySpark job** — the alternative
migration path (`src/pyspark/claims_processing.py`: SAS hash lookup → broadcast
join; IF/THEN output routing → `when().otherwise()`; PROC APPEND → `saveAsTable`).

```sql
SELECT * FROM banking_analytics.dev_curated.claims_register     LIMIT 20;
SELECT * FROM banking_analytics.dev_curated.fraud_alerts;        -- HIGH-risk only
SELECT * FROM banking_analytics.dev_curated.claims_review_queue; -- PEND adjudication
```

<a id="iac-cd"></a>
## IaC + CD: deploy, run, revert

The pipeline is a **Databricks Asset Bundle** (`databricks.yml` +
`resources/daily_banking_pipeline.job.yml`) — the IaC the legacy estate never had.

```bash
make deploy              # databricks bundle deploy -t dev (schedule PAUSED, per-user)
make run-job TARGET=dev  # run the deployed Workflow: dbt_staging -> intermediate -> marts -> test, + pyspark in parallel
```

To showcase **CD without merging to main**, trigger the GitHub Actions *deploy
bundle* workflow manually (Actions → Run workflow → pick target + namespace). On a
real merge to `main`, the same workflow deploys automatically.

Revert everything to the **before** state for the next run:

```bash
make destroy   TARGET=dev   # remove the deployed job (revert CD)
make demo-down NS=dev       # drop dev_* output schemas (raw data untouched)
```

`main` is never modified (you stayed on the demo branch and never merged) and
`banking_analytics.raw.*` is left intact.

---

<a id="concurrent"></a>
## Concurrent runs (isolated DB spaces)

Every output schema is prefixed by a namespace (`NS` / `DBT_SCHEMA`), so multiple
runs — and the parallel fan-out in Act 3 — coexist with zero collisions:

```bash
make demo-up   NS=alice     # builds alice_staging / _intermediate / _marts ...
make demo-up   NS=run2      # independent space, runs in parallel
make reconcile NS=alice     # reconcile a specific namespace
make demo-down NS=alice     # tears down only alice_*
```

For the deployed job, pass the namespace through the bundle variable:

```bash
databricks bundle deploy -t dev --var="dbt_schema=alice"
databricks bundle run daily_banking_pipeline -t dev
```

---

<a id="warehouse"></a>
## Warehouse sizing note

The demo data is tiny (hundreds–thousands of rows). Measured query durations on a
**2X-Small serverless** warehouse are ~0.5–3.3s each (median ~1.9s), dominated by
fixed planning/queueing rather than compute. **Increasing the warehouse size does
not speed this up** and only adds cost — leave it at 2X-Small. Wall-clock for a
full job run (~3–4 min) is mostly serverless cold-start, not warehouse size.

---

<a id="cheat-sheet"></a>
## Cheat sheet

```bash
make seed                 # seed before-state raw data (idempotent)
make demo-up   NS=dev     # build after-state into a namespace
make reconcile NS=dev     # source -> target reconciliation report
make demo-down NS=dev     # drop a namespace's schemas (raw untouched)
make deploy               # bundle deploy -t dev (IaC)
make run-job   TARGET=dev # run the deployed Workflow
make destroy   TARGET=dev # remove the deployed job (revert CD)
```

Key references: `docs/CONVERSION_PLAYBOOK.md` (the reusable procedure + the
reconciliation contract), `dbt_project/tests/reconcile_*.sql` and
`verify/reconcile.py` (the controls).
