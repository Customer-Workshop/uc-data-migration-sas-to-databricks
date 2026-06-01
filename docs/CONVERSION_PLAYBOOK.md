# SAS → Databricks Conversion & Verification Playbook

A reusable, paste-ready procedure for converting one SAS program into a runnable,
**verified** dbt model (or PySpark job) on Databricks. The point of the playbook
is consistency: every program is converted the same way, every conversion is
proven against the source with the same reconciliation controls, and every run
ends in a PR carrying a reconciliation report.

This is the procedure Devin follows when you paste the prompt in
[Using the playbook](#using-the-playbook). It is written so a person can follow
it by hand too.

## The one principle: the SAS source is the source of truth

A migration's job is to **reproduce the SAS numbers faithfully**, not to improve
them. If the legacy code has a quirk (an account type that falls through a CASE,
a hard-coded threshold, a rounding rule), reproduce it and **flag it** — never
silently "correct" it. Remediating a legacy behaviour is a separate, deliberate
decision made with the business, not a side effect of conversion.

This is why "looks reasonable" review is not enough, and why every conversion is
gated by a **parity check against the source**.

## Inputs

| Input | Example |
|---|---|
| SAS program | `Programs/Banking/monthly_regulatory_reporting.sas` |
| Target model(s) | `dbt_project/models/marts/mart_regulatory_rwa.sql` |
| Namespace | `dev` (outputs land in `banking_analytics.dev_*`) |

## Procedure

1. **Read the source.** Read the SAS program end to end, plus any macros
   (`Macro/`), formats (`Formats/`), and batch wrappers (`BatchJobs/`) it
   references. Identify:
   - **Inputs** — `LIBNAME`/dataset reads → dbt `source()` tables.
   - **Transforms** — DATA-step logic, PROC SQL, PROC FORMAT, hash objects,
     `MERGE BY`, `RETAIN`, `BY`-group processing.
   - **Outputs** — `PROC APPEND`/`CREATE TABLE`/`DATA` targets → models/tables.
   - **Filters & business rules** — every `WHERE`, `IF`, and CASE branch. These
     define the *scope contract* you will reconcile against.

2. **Map SAS constructs to dbt / Spark SQL.**
   | SAS | Databricks |
   |---|---|
   | `PROC SQL ... GROUP BY` | dbt model with `group by` |
   | DATA-step `IF/THEN/ELSE` | `case when ... end` |
   | `PROC FORMAT` / `format=` | `case` mapping or a Jinja macro |
   | hash object lookup | broadcast `join` |
   | `MERGE BY` | `join` on the by-keys |
   | `RETAIN` + `BY` (running totals) | window function (`sum() over (...)`) |
   | `PROC APPEND` | `saveAsTable` (PySpark) or incremental model |
   Use a PySpark job instead of dbt when the program is procedural and
   multi-output (e.g. row-by-row routing to several tables) rather than
   set-based.

3. **Write the model(s) preserving SAS logic exactly.** Mirror CASE branches
   account-value for account-value, including catch-all `else` behaviour. Where
   the source has a quirk, reproduce it and leave a comment noting it is
   source-faithful (not an endorsement).

4. **Add or extend reconciliation controls** (see [the contract](#reconciliation-contract)).
   At minimum every conversion gets: a **completeness** check (no silent row
   loss vs the documented scope), a **control total** (a SUM that must tie out),
   and — for any mapping/CASE — a **parity** check against the source mapping.
   dbt controls go in `dbt_project/tests/reconcile_*.sql`; cross-engine checks go
   in `verify/reconcile.py`.

5. **Build and verify into a namespace.**
   ```bash
   make build NS=dev          # dbt build (models + schema tests + reconciliation tests)
   make reconcile NS=dev      # human-facing reconciliation report
   ```

6. **Close the loop.** If any control fails, investigate **against the SAS
   source** — do not relax the check to make it pass. Correct the model and
   re-run until `dbt build` and the reconciliation report are green.

7. **Open a PR with the reconciliation report.** The PR includes the new/changed
   model, the reconciliation tests, and the report output so a reviewer sees the
   parity evidence, not just the code. CI re-runs every control on the PR.

<a id="reconciliation-contract"></a>
## The reconciliation contract

Confidence comes from controls that fail loudly when a conversion drifts from the
source. The standing controls in this repo:

| Control | What it proves | Lives in |
|---|---|---|
| `reconcile_account_completeness` | Model row count equals the documented in-scope source population (no silent row loss, no fan-out) | `dbt_project/tests/` |
| `reconcile_rwa_risk_weight_coverage` | Every account type's Basel risk weight matches the SAS CASE, value for value (parity) | `dbt_project/tests/` |
| `reconcile_rwa_exposure_control_total` | Total exposure ties out to source balances (control total) | `dbt_project/tests/` |
| `claims_register_within_source` + referential checks | PySpark curated outputs are bounded by and reference the source (cross-engine) | `verify/reconcile.py` |

dbt singular tests **fail the build** if they return any rows. `verify/reconcile.py`
exits non-zero on any failure, so it gates CI and produces an attachable report.

## Worked example: the LOC risk-weight divergence

This is a real defect the loop caught, and the canonical illustration of the
"source is truth" principle.

- `monthly_regulatory_reporting.sas` maps `LOC` (line of credit) → risk weight
  **1.00** explicitly, and leaves `IRA` on the `else` → 1.00 branch.
- An early dbt conversion **omitted `LOC`** from the CASE. It fell to the
  catch-all and — by luck — still produced 1.00, matching the source but leaving
  the code fragile.
- A plausible-looking "fix" then mapped `LOC` → **0.75** to match the other
  revolving-credit products (CC/PERS). That **diverges from the source** and
  silently overstates capital relief on every line of credit.
- The **parity control** (`reconcile_rwa_risk_weight_coverage`) compares the
  mart's per-type weights to the SAS mapping and fails: `LOC actual 0.75,
  expected 1.00`. The fix is to restore `LOC` → 1.00. "Looks reasonable" review
  never catches this; the source-parity check always does.

<a id="using-the-playbook"></a>
## Using the playbook

Point Devin at one SAS program. Paste a prompt like this (fill in the three
inputs):

```
Convert the SAS program Programs/Banking/monthly_regulatory_reporting.sas in
the ts-sas-legacy-analytics estate into dbt models on Databricks, following
docs/CONVERSION_PLAYBOOK.md in uc-data-migration-sas-to-databricks.

- Treat the SAS source as the source of truth: reproduce its logic exactly,
  including any quirks, and flag (do not silently fix) anything that looks wrong.
- Add reconciliation controls: completeness, a control total, and a parity check
  for every CASE/mapping in the program.
- Build into the dev namespace and run dbt build + the reconciliation report
  until everything is green.
- Include the reconciliation report in the PR.
```

Devin reads the program, writes the model(s) and reconciliation tests, builds and
verifies against the live workspace, fixes anything the controls catch, and opens
a PR with the report attached.

## Parallel fan-out

Each SAS program is independent, so conversions parallelise cleanly. Launch one
Devin session per program — each follows this same playbook and produces its own
verified PR:

| Session | SAS program | Target |
|---|---|---|
| 1 | `Programs/Banking/load_customer_accounts.sas` | `stg_cust_accounts` + `int_account_metrics` |
| 2 | `Programs/Banking/daily_transaction_processing.sas` | `stg_daily_transactions` + `mart_daily_transactions` |
| 3 | `Programs/Banking/monthly_regulatory_reporting.sas` | `mart_regulatory_rwa` + `mart_delinquency_aging` |
| 4 | `Programs/Insurance/claims_processing.sas` | PySpark `claims_processing` (procedural, multi-output) |

Because the playbook fixes the procedure and the reconciliation contract, every
session's output is consistent and independently verified — the same review bar
applied N times in parallel instead of once in series.
