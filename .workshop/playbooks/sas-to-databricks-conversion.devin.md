# Playbook: Convert one SAS program to a verified Databricks model

> **Facilitator / presenter:** this file is the source for a **Devin Playbook**.
> Copy its contents into your Devin organization (Settings → Playbooks → *Create
> a new Playbook*) so sessions can invoke it as `!convert-sas-to-databricks`.
> See [Creating Playbooks](https://docs.devin.ai/product-guides/creating-playbooks).
> The repo-specific commands (make targets, namespaces, harness paths) are kept
> in the companion Skill at `.agents/skills/sas-to-databricks-conversion/SKILL.md`,
> which Devin auto-loads when working in this repo.

## Overview

Convert **one** SAS program into a runnable, **verified** dbt model (or PySpark
job) on Databricks. The outcome is a PR containing the converted model, its
reconciliation controls, and a reconciliation report that proves the output ties
out to the SAS source. The value is consistency: every program is converted the
same way and every conversion is gated by a parity check against the source.

## The one principle: the SAS source is the source of truth

A migration reproduces the SAS numbers faithfully — it does not improve them. If
the legacy code has a quirk (an account type that falls through a CASE, a
hard-coded threshold, a rounding rule), reproduce it and **flag it** — never
silently "correct" it. Remediating a legacy behaviour is a separate, deliberate
decision made with the business, not a side effect of conversion. This is why
"looks reasonable" review is not enough and why every conversion is gated by a
parity check against the source.

## Required from user

- **SAS program** — path in the source estate, e.g.
  `Programs/Banking/monthly_regulatory_reporting.sas`.
- **Target model(s)** — the dbt model(s) and/or PySpark job to produce, e.g.
  `mart_regulatory_rwa` + `mart_delinquency_aging`.
- **Namespace** — an isolated build space so concurrent runs do not collide,
  e.g. `dev` (outputs land in `<catalog>.dev_*`).

## Procedure

1. Read the SAS program end to end, plus every macro, `PROC FORMAT`, and batch
   wrapper it references. Identify inputs (`LIBNAME`/dataset reads → dbt
   `source()`), transforms (DATA-step logic, PROC SQL, hash objects, `MERGE BY`,
   `RETAIN`/`BY`-group processing), outputs (`PROC APPEND`/`CREATE TABLE` → models),
   and every filter/business rule (each `WHERE`, `IF`, CASE branch — these define
   the scope contract you reconcile against).
2. Map each SAS construct to its Databricks equivalent: `PROC SQL ... GROUP BY` →
   dbt model with `group by`; DATA-step `IF/THEN/ELSE` → `case when ... end`;
   `PROC FORMAT`/`format=` → a CASE mapping or Jinja macro; hash lookup →
   broadcast `join`; `MERGE BY` → `join` on by-keys; `RETAIN`+`BY` running totals
   → window function. Choose a **PySpark job** instead of dbt when the program is
   procedural and multi-output (row-by-row routing to several tables) rather than
   set-based.
3. Write the model(s) preserving SAS logic **exactly** — mirror CASE branches
   value-for-value, including the catch-all `else`. Where the source has a quirk,
   reproduce it and add a short comment noting it is source-faithful (not an
   endorsement).
4. Add or extend reconciliation controls: at minimum a **completeness** check (no
   silent row loss vs the documented scope), a **control total** (a SUM that must
   tie out), and a **parity** check for every mapping/CASE against the source.
   (See the Skill for exactly where controls live in this repo.)
5. Build and verify into the requested namespace, then run the reconciliation
   report. (Commands are in the Skill.)
6. Close the loop: if any control fails, investigate **against the SAS source** —
   do not relax the check to make it pass. Correct the model and re-run until the
   build and the reconciliation report are green.
7. Deliver a PR that includes the new/changed model, the reconciliation tests,
   and the report output, so a reviewer sees the parity evidence, not just the
   code. CI re-runs every control on the PR.

## Specifications (postconditions)

- The converted model(s) build cleanly into the requested namespace.
- Every reconciliation control passes: completeness, control total(s), and a
  parity check for each mapping/CASE in the program.
- The PR contains the model, the controls, and the reconciliation report.
- Any source quirk reproduced is explicitly flagged in code and in the PR — never
  silently changed.

## Advice and pointers

- Parity is per-value, not aggregate: a mapping can produce a correct total while
  an individual branch is wrong. Compare each CASE branch to the source mapping.
- A control that is hard to make pass is usually telling you the conversion
  diverged — read the SAS source again before touching the control.
- Prefer dbt for set-based logic; reach for PySpark only when the program is
  genuinely procedural/multi-output.

### Worked example: the LOC risk-weight divergence

A real defect this loop caught, and the canonical illustration of "source is
truth":

- `monthly_regulatory_reporting.sas` maps `LOC` (line of credit) → risk weight
  **1.00** explicitly, and leaves `IRA` on the `else` → 1.00 branch.
- An early dbt conversion **omitted `LOC`** from the CASE. It fell to the
  catch-all and — by luck — still produced 1.00, matching the source but leaving
  the code fragile.
- A plausible-looking "fix" then mapped `LOC` → **0.75** to match the other
  revolving-credit products (CC/PERS). That **diverges from the source** and
  silently overstates capital relief on every line of credit.
- The parity control compares the mart's per-type weights to the SAS mapping and
  fails: `LOC actual 0.75, expected 1.00`. The fix is to restore `LOC` → 1.00.
  "Looks reasonable" review never catches this; the source-parity check always
  does.

## Forbidden actions

- Do **not** "improve", clean up, or modernise legacy logic during conversion —
  reproduce it faithfully and flag anomalies for a separate decision.
- Do **not** relax, delete, or hard-code a reconciliation control to make a build
  go green. Fix the model, not the check.
- Do **not** write into another run's namespace or the durable raw source tables.
- Do **not** convert more than the one program in scope for this session.

## Parallel fan-out

Each SAS program is independent, so conversions parallelise cleanly: run one
session per program (each with its own namespace), or one orchestrator session
that spawns a child per program. Because this playbook fixes the procedure and
the reconciliation contract, every session's output is consistent and
independently verified — the same review bar applied N times in parallel instead
of once in series.
