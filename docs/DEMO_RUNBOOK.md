# SAS → Databricks demo runbook

End-to-end: run the legacy estate in a container, load the same source extracts into
Databricks, build the converted models in an isolated namespace, and let the
reconciliation harness say what matches SAS and what does not. Then fan the remaining
gaps out to parallel sessions, one namespace each.

```
ts-sas-legacy-analytics (SAS estate, read-only)
   └─ docker/  OpenSAS container ──► /data/sas/golden/*.csv, row_counts.csv, controls.csv, manifest.json
                                            │
                Data/csv/* (the estate's source extracts) ──► banking_analytics.<RAW_SCHEMA>.*   (seed/load_sas_estate.py)
                                            │                          │
                                            │                dbt build ──► banking_analytics.<NS>_staging/_intermediate/_marts
                                            │                          │
                                            └────── verify/reconcile.py --sas-golden ◄──┘   PASS / FAIL / SKIP per SAS output
```

## 0. Prerequisites

| | |
|---|---|
| Docker | `docker compose` v2 |
| Python | 3.10+, `pip install -r requirements.txt -r verify/requirements.txt` (dbt-core, dbt-databricks, databricks-sql-connector) |
| Databricks | a SQL warehouse you can use, and a catalog where you have `CREATE SCHEMA` (default `banking_analytics`) |
| Env vars | `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`; optional `DATABRICKS_CATALOG` |

dbt-databricks refuses to start if a PAT *and* an OAuth client are both in the environment
(`more than one authorization method configured: oauth and pat`). If your shell exports
`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` and you are using a token, `unset` them.

Reference secrets by name only; never paste values into logs, PR bodies or reports.

## 1. Legacy side — run the SAS estate in Docker

```bash
cd ../ts-sas-legacy-analytics
ESTATE_COMMIT=$(git rev-parse --short HEAD) docker compose -f docker/compose.yml build
docker compose -f docker/compose.yml run --rm sas            # ≈ 1 min
```

Talk track: it is the estate's own programs, formats and seed data, unmodified, on the
production layout (`/opt/sas/custom`, `/data/sas`, `sas -autoexec … -sysin …`). The run
fails unless the log is clean and the row counts equal the checked-in baseline.

Expected tail:

```
run-banking: pipeline rc=0 log=/data/sas/logs/run_local_banking.log
TABLE_NAME,N_ROWS
STG_BANK.CUST_ACCOUNTS_DAILY,466
STG_BANK.ACCT_EXCEPTIONS,32
CURATED.DAILY_TRANSACTIONS,18903
CURATED.TXN_ANOMALIES,46
CURATED.RISK_SCORES,236
REPORTS.MONTHLY_RWA,59
REPORTS.DELINQUENCY_AGING,70
REPORTS.LLP_COVERAGE,6
run-banking: row counts match /opt/sas/custom/docker/expected/row_counts.csv
```

Pull the golden outputs onto the host:

```bash
cid=$(docker create -v ts-sas-legacy_sasdata:/data/sas alpine true)
docker cp "$cid":/data/sas/golden ./sas_golden && docker rm "$cid"
cat sas_golden/controls.csv        # anomaly split, txn total, RWA total …
```

Details, shims and known estate defects: `ts-sas-legacy-analytics/docker/README.md`.

## 2. Target side — isolated build in Databricks

Pick a namespace nobody else is using (`NS`). Everything the run writes lands in
`<catalog>.<NS>_staging / _intermediate / _marts / _curated`; nothing else is touched.

```bash
cd uc-data-migration-sas-to-databricks
make demo-up NS=sasdemo1 RAW_SCHEMA=raw_sas
```

`demo-up` = seed + `dbt build`. With the default `SEED=sas` the seed step loads the estate's
`Data/csv/*` extracts — the exact rows the container just ran on — as Delta tables into
`<catalog>.<RAW_SCHEMA>` (typed like `Data/load_seed_data.sas`, `date9.` parsed). Point
`RAW_SCHEMA` at a fresh schema so the durable synthetic `raw` other namespaces read stays
untouched. `SEED=synthetic` keeps the old Faker loader.

`dbt build` then builds the models and runs 44 tests (schema tests plus the singular
`reconcile_*.sql` controls); any failing test fails the build. This is where the SAS data
pays off: the synthetic `raw` hid a fan-out in `mart_risk_scores` (one row per bureau pull
instead of latest pull ≤ score date, as `credit_risk_scoring.sas` does); the estate's
`BUREAU_SCORES` extract has two pulls per customer and `unique_mart_risk_scores_account_id`
failed 236 times until the model was fixed.

## 3. Reconcile against the SAS baseline

```bash
make reconcile NS=sasdemo1 RAW_SCHEMA=raw_sas SAS_GOLDEN=./sas_golden
# → reconciliation_sasdemo1.md, exit 1 if anything FAILs
```

For each of the eight SAS output tables the harness compares row counts with the model that
replaces it, and for `controls.csv` it re-computes each control on the target:

| Result | Meaning |
|---|---|
| PASS | converted model reproduces SAS |
| FAIL | converted model exists and disagrees with SAS — blocks merge |
| SKIP | SAS output has no converted model in this namespace yet ("not yet converted") |

Current state of the estate against `main` (business date 31JAN2024):

| SAS output | Program | Verdict |
|---|---|---|
| `STG_BANK.CUST_ACCOUNTS_DAILY` 466 | load_customer_accounts | PASS |
| `STG_BANK.ACCT_EXCEPTIONS` 32 | load_customer_accounts | SKIP — no `stg_acct_exceptions` |
| `CURATED.RISK_SCORES` 236 + `N_ACCOUNTS` | credit_risk_scoring | PASS |
| `CURATED.DAILY_TRANSACTIONS` 18903, `SUM_AMOUNT` 8,231,436.81 | daily_transaction_processing | FAIL — model has 612 rows / 2,179,743.61: day's feed only, no 90-day history append, run date used where SAS uses the business date |
| `CURATED.TXN_ANOMALIES` 46 (16 HIGH_AMOUNT / 30 OVERDRAFT) | daily_transaction_processing | FAIL — 37 rows, 0 / 37 |
| `REPORTS.MONTHLY_RWA` 59 + `SUM_RWA` 14,156,900.58, `DELINQUENCY_AGING` 70, `LLP_COVERAGE` 6 | monthly_regulatory_reporting | SKIP — not converted |

Talk track: the harness is the merge authority. Nothing else — not a conversion tool, not
a passing `dbt build` — certifies a unit. FAIL and SKIP are the work list for step 5.

## 4. Clean up

```bash
make demo-down NS=sasdemo1          # drops only banking_analytics.sasdemo1_* ; raw / raw_sas untouched
```

Verified: after teardown `show schemas` lists `raw`, `raw_sas`, `dev_*` and no `sasdemo1_*`;
`raw_sas.curated_daily_transactions_history` still has 18293 rows.

## 5. Parallel sessions — one program per child, one namespace each

The remaining gaps split cleanly by SAS program, so they are independent migration units:

| Unit | Namespace | Brief | Gate (must PASS in `make reconcile NS=<ns> SAS_GOLDEN=…`) |
|---|---|---|---|
| U1 `daily_transaction_processing.sas` | `w1u1` | make `mart_daily_transactions` carry the 90-day history (`raw_sas.curated_daily_transactions_history`) + the business-date feed; anomaly rules value-for-value with the SAS DATA step | `CURATED.DAILY_TRANSACTIONS`, `CURATED.TXN_ANOMALIES`, `TXN_ANOMALIES.*`, `DAILY_TRANSACTIONS.SUM_AMOUNT` |
| U2 `load_customer_accounts.sas` (exceptions) | `w1u2` | add `stg_acct_exceptions` with the 32-row DQ exception logic (note the estate defect: no `EXCEPTION_CODE` column reaches the table) | `STG_BANK.ACCT_EXCEPTIONS` |
| U3 `monthly_regulatory_reporting.sas` | `w1u3` | `mart_regulatory_rwa`, `mart_delinquency_aging`, `mart_llp_coverage`; RWA = Σ EAD × RW with the `calculated` alias semantics | `REPORTS.*`, `MONTHLY_RWA.SUM_RWA` |

Rules every child follows (`.agents/skills/sas-to-databricks-conversion`, org AGENTS rules):

- Write only to its namespace (`w1uN_*`). Never `raw`, never `raw_sas`, never another unit's namespace. Targets are declared above before launch; a collision is a halt.
- Fixture first, one live run: develop against `verify/fixtures/sas_golden` + the estate CSVs, then one `make demo-up` / `make reconcile` in its namespace. Cap: 3 recon re-runs.
- Do not relax, delete or hard-code a control. If SAS looks wrong, say so in the PR and leave the control red.
- Each child opens its own PR containing its `reconciliation_<ns>.md`. A human merges, and only on a PASS verdict.
- Circuit breaker: three same-class failures across units (e.g. three children blocked on the same missing source column) halts the wave for a human decision instead of retrying.
- `make demo-down NS=<ns>` when the PR is merged or abandoned.

A minimal launch is three Devin sessions with the row of the table above as the brief plus
the two repo paths; a `migration-fanout`-style workflow adds the collision check and the
breaker automatically.

## Where things are

| | |
|---|---|
| Container, wrapper, shims, golden export | `ts-sas-legacy-analytics/docker/` |
| Expected SAS row counts | `ts-sas-legacy-analytics/docker/expected/row_counts.csv` |
| SAS extract → Delta loader | `seed/load_sas_estate.py` |
| Harness + SAS parity checks | `verify/reconcile.py` (`--sas-golden`, `--raw-schema`) |
| Harness unit tests (no Databricks needed) | `make reconcile-test` |
| Checked-in golden metadata for fixture work | `verify/fixtures/sas_golden/` |
| Program → model map | `docs/SAS_TO_DBT_MIGRATION_MAP.md` |
