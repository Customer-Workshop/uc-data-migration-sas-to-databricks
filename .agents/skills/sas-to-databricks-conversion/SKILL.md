---
name: sas-to-databricks-conversion
description: Repo mechanics for converting a SAS program to a verified Databricks model in this repo — build/reconcile commands, namespaces, where reconciliation controls live. Supplements the general !convert-sas-to-databricks playbook.
---

## When to use this

Use this skill whenever you are converting a SAS program into a dbt model or
PySpark job **in this repository**. It is the repo-specific companion to the
general procedure in the `!convert-sas-to-databricks` playbook
(`.workshop/playbooks/sas-to-databricks-conversion.devin.md`): the playbook says
*what* to do and *why* (source-parity principle, procedure, forbidden actions);
this skill says *how* to do it here (exact commands, paths, namespaces).

## Layout

- SAS source estate (read-only): the `ts-sas-legacy-analytics` repo.
- Target dbt project: `dbt_project/` — `models/staging`, `models/intermediate`,
  `models/marts`; macros in `dbt_project/macros/`.
- PySpark jobs: `src/pyspark/`.
- Reconciliation controls:
  - dbt singular tests (fail the build if they return rows):
    `dbt_project/tests/reconcile_*.sql`.
  - Cross-engine / report checks: `verify/reconcile.py`.
- Connection is env-var based (`dbt_project/profiles.yml`): `DATABRICKS_HOST`,
  `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`. Catalog is `DATABRICKS_CATALOG`
  (default `banking_analytics`). If `DATABRICKS_CLIENT_ID`/`_SECRET` are also
  exported, `unset` them — dbt-databricks refuses two auth methods.
- SAS golden baseline: the Dockerised estate (`ts-sas-legacy-analytics/docker`)
  exports every SAS output table plus `row_counts.csv`/`controls.csv`; the
  metadata copy lives in `verify/fixtures/sas_golden/` for fixture-first work.

## Namespaces (isolated, concurrent-safe)

Every run is namespaced by `NS`. Outputs land in
`banking_analytics.<NS>_staging / _intermediate / _marts / _curated`, so multiple
runs (`NS=dev`, `NS=child1`, …) never collide and the durable "before" raw data
in `banking_analytics.raw` is never touched. `RAW_SCHEMA` selects which durable
schema the models read (`raw` = synthetic, `raw_sas` = the SAS estate's own
extracts, loaded by `make seed SEED=sas`). Always build into the namespace you
were given; never write into another run's namespace or into any `raw*` schema.

## Build and verify

```bash
make demo-up NS=<ns> RAW_SCHEMA=raw_sas                  # seed (idempotent) + dbt build (models + tests + reconcile_*.sql)
make reconcile NS=<ns> RAW_SCHEMA=raw_sas SAS_GOLDEN=<dir>  # source->target + SAS-parity report (verify/reconcile.py)
make reconcile-test                                      # harness unit tests against the fixture, no Databricks
```

- `make demo-up` runs `dbt build`, which executes the `reconcile_*.sql` singular
  tests; any returning rows fail the build.
- `make reconcile` runs `verify/reconcile.py`, which exits non-zero on any failed
  control and prints an attachable report. With `SAS_GOLDEN` it also compares
  each SAS output table / control with the model that replaces it: PASS, FAIL,
  or SKIP ("not yet converted"). Converting a program = making its SKIPs PASS;
  the mapping is `GOLDEN_TABLES` / `GOLDEN_CONTROLS` in `verify/reconcile.py`.
- Tear down a namespace with `make demo-down NS=<ns>` (drops only that
  namespace's schemas; raw data untouched).

## Adding reconciliation controls for a new program

For each program you convert, add controls under `dbt_project/tests/` named
`reconcile_<thing>.sql`, covering at minimum:

- **completeness** — model row count equals the documented in-scope source
  population (no silent row loss, no fan-out);
- **control total** — a SUM (e.g. total exposure / total premium) that ties out
  to source balances;
- **parity** — every account-type / mapping / CASE branch matches the SAS source
  value-for-value.

For cross-engine (PySpark) outputs, add bounded/referential checks to
`verify/reconcile.py` so the curated tables are bounded by and reference the
source.

## Close the loop

If a control fails, investigate against the SAS source — **do not** relax,
delete, or hard-code the control to make it pass. Fix the model and re-run
`make demo-up NS=<ns>` and `make reconcile NS=<ns>` until both are green, then
open a PR that includes the reconciliation report output.

## Deploy / revert (optional, CD demo)

```bash
make deploy           # databricks bundle deploy -t dev (namespaced per user, schedule paused)
make run-job          # trigger the deployed pipeline job
make destroy          # databricks bundle destroy -t dev (revert)
```
