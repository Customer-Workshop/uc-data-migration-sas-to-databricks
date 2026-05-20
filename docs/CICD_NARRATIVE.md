# CI/CD Migration Narrative: SAS → dbt on Databricks

This document tells the operationalization story of migrating from a SAS analytics environment to dbt on Databricks. It covers version control, CI/CD, scheduling, testing, and deployment — the full DevOps lifecycle that did not exist in the SAS world.

## Executive Summary

The SAS environment operated without modern software engineering practices. Code lived on shared file systems, deployments were manual, and there was no automated testing or CI/CD pipeline. The migration to dbt on Databricks introduces Git-native version control, automated linting and testing on every pull request, declarative data quality checks, and infrastructure-as-code workflow orchestration — replacing manual processes with repeatable, auditable automation.

---

## Side-by-Side Comparison

| Capability | SAS (Before) | dbt/Databricks (After) |
|---|---|---|
| **Version Control** | None — SAS programs stored on shared file systems (`/data/sas/programs/`). No history, no branching, no audit trail. Multiple developers editing the same file risked overwrites. | Git (GitHub) — full history, branching, pull requests, blame, and diff. Every change is attributable and reversible. |
| **Code Review** | None — developers promoted code directly. At best, email-based review of `.sas` files. | Pull request workflow — every change requires review before merge. Inline comments, approval gates, and conversation threads. |
| **CI/CD Pipeline** | None — SAS had no equivalent. Code was deployed by copying files or importing `.spk` packages via SAS Management Console. | GitHub Actions — automated linting (sqlfluff), project validation (dbt parse), and schema testing (dbt test) on every PR. Merge is blocked until checks pass. |
| **Linting / Style** | None — SAS code style was informal and inconsistent across developers. No tooling existed to enforce standards. | sqlfluff with Databricks dialect — enforces consistent SQL style, keyword casing, aliasing rules. Runs in CI and as a pre-commit hook. |
| **Data Quality Testing** | Manual — ad hoc PROC SQL counts, PROC FREQ distributions, PROC PRINT spot checks. Results reviewed in SAS log files by analysts. | Declarative dbt tests — `not_null`, `unique`, `accepted_values`, `relationships` defined in YAML. Run automatically after every dbt invocation. |
| **Scheduling** | Control-M — external scheduler triggered `run_daily_banking.sas` master script via `%run_step` macro calls. Job definitions stored in Control-M GUI (not version-controlled). | Databricks Workflows — JSON definition version-controlled in Git. Cron scheduling with task-level dependency DAG, automatic retry, and "Repair Run" for partial re-execution. |
| **Deployment** | Manual — `.spk` packages created in SAS Enterprise Guide, imported via SAS Management Console. No rollback except restoring from backup. | Git merge to `main` triggers deployment. Databricks Repos auto-syncs. Rollback = `git revert`. Infrastructure defined as code (Workflow JSON, dbt project YAML). |
| **Environment Isolation** | Minimal — dev and prod often shared the same SAS libraries. Testing against production data was common. | Full isolation — CI uses dedicated schema (`ci_<run_id>`). Dev, staging, and prod environments are separate Databricks catalogs/schemas. |
| **Secret Management** | Embedded in code — Oracle passwords in `autoexec.sas`, hardcoded file paths. Secrets visible in SAS logs. | Environment variables + Databricks Secrets — credentials never appear in code or logs. `env_var()` in dbt profiles, `dbutils.secrets` in notebooks. |
| **Monitoring / Alerting** | SAS logs on file system — reviewed manually. `sendmail` macro for email alerts on failure (often unreliable). | Databricks workflow run history, Spark UI, email + PagerDuty webhooks on failure. Structured observability rather than log file scraping. |
| **Documentation** | Inline SAS comments (inconsistent). No living documentation system. | dbt docs — auto-generated from model YAML, column descriptions, and DAG visualization. Always in sync with the code. |
| **Dependency Management** | `%INCLUDE` chains and macro libraries — execution order maintained manually in master scripts. Circular dependencies possible and hard to detect. | dbt `ref()` DAG — dependencies are explicit, validated at compile time. Circular references are compile errors. DAG visualization shows the full data flow. |

---

## Detailed Migration Mapping

### 1. Version Control: Shared File System → Git

**Before (SAS):**
SAS programs were stored on network-mounted file systems (e.g., `/data/sas/programs/Banking/`). Multiple analysts could edit the same file simultaneously, with no merge capability — the last save wins. There was no history, no branching, and no way to determine who changed what or when. "Version control" meant naming files `credit_risk_scoring_v2_FINAL_brian.sas`.

**After (dbt/Databricks):**
All code lives in a Git repository. Developers work on feature branches, submit pull requests for review, and merge after approval. Every line has full `git blame` history. Branching enables parallel development without file conflicts.

### 2. CI Pipeline: Nothing → GitHub Actions

**Before (SAS):**
Code was deployed directly to production without automated checks. A developer would finish a program in SAS Enterprise Guide, save it to the shared drive, and it would be picked up by the next Control-M run. Syntax errors were discovered at runtime — during the production batch job.

**After (dbt/Databricks):**
The `.github/workflows/dbt_ci.yml` pipeline runs on every pull request:

1. **sqlfluff lint** — enforces SQL style consistency (SAS had no linting)
2. **dbt parse** — validates all Jinja templates, ref/source resolution, and YAML schemas without a live connection
3. **dbt test** — runs schema tests against a CI-specific schema (when credentials are available)

Merge is blocked until all checks pass. This catches errors hours or days before they would have been discovered in the SAS batch run.

### 3. Data Quality: Manual Spot Checks → Declarative Tests

**Before (SAS):**
```sas
/* Ad hoc QA — run manually after each batch */
proc sql;
  select count(*) as n_rows,
         count(distinct account_id) as n_accounts,
         sum(case when account_status = '' then 1 else 0 end) as n_missing_status
  from CURATED.DAILY_TRANSACTIONS;
quit;
/* Review output in SAS log... hope someone notices if counts look wrong */
```

**After (dbt):**
```yaml
# Declarative, version-controlled, automatically enforced
models:
  - name: stg_cust_accounts
    columns:
      - name: account_id
        data_tests:
          - unique
          - not_null
      - name: account_type
        data_tests:
          - accepted_values:
              values: [CHK, SAV, MMA, CD, IRA, LOC, MTG, AUTO, PERS, CC, HELC]
```

Tests run automatically after every `dbt test` invocation — in CI, in the Databricks Workflow, and during local development. Failures produce structured output, not buried log messages.

### 4. Scheduling: Control-M → Databricks Workflows

**Before (SAS + Control-M):**
The `run_daily_banking.sas` master script was triggered by Control-M at 06:00 daily, along with `claims_processing.sas`, `policy_valuation.sas`, `monthly_regulatory_reporting.sas`, and `customer_profitability.sas`. Each executed sequentially via `%run_step` macros. If Step 2 failed, an operator had to manually restart the entire job from the beginning — there was no partial re-run capability. The job definitions lived in the Control-M GUI and were not version-controlled.

**After (Databricks Workflows):**
The `workflows/daily_banking_pipeline.json` defines a four-task DAG:
```
dbt_staging → dbt_intermediate → dbt_marts → dbt_test
```

Key improvements:
- **Version-controlled**: the workflow definition is a JSON file in Git
- **Partial re-run**: Databricks "Repair Run" re-executes only the failed task and its downstream dependents
- **Automatic retry**: each task can be configured with `max_retries`
- **Structured alerting**: email + PagerDuty webhook (replacing the unreliable `sendmail` macro)

### 5. Secret Management: Hardcoded → Environment Variables

**Before (SAS):**
```sas
/* autoexec.sas — visible to anyone with file system access */
%let ora_uid = svc_sas_banking;
%let ora_pwd = Pr0d_P@ssw0rd!;  /* rotated annually... maybe */
libname ORA_DW oracle path="FINPROD" user=&ora_uid pw=&ora_pwd;
```

**After (dbt/Databricks):**
```yaml
# profiles.yml — no secrets in code
databricks:
  outputs:
    prod:
      host: "{{ env_var('DATABRICKS_HOST') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
```

Credentials are stored in GitHub Actions Secrets (for CI) and Databricks Secrets (for runtime). They never appear in code, logs, or version history.

### 6. Pre-commit Hooks: Nothing → Automated Guardrails

**Before (SAS):**
No pre-commit checks existed. A developer could commit (if version control was used at all) syntactically invalid SAS code, files with trailing whitespace, or malformed YAML.

**After (dbt/Databricks):**
`.pre-commit-config.yaml` defines hooks that run before every commit:
- **sqlfluff-lint** — catches SQL style violations before they reach CI
- **sqlfluff-fix** — auto-corrects fixable violations
- **yamllint** — validates YAML schema files
- **trailing-whitespace** / **end-of-file-fixer** — basic hygiene

---

## Governance Improvements Summary

| Risk Area | SAS Exposure | dbt/Databricks Mitigation |
|---|---|---|
| Unauthorized changes | Anyone with file system access could modify production code | Branch protection + PR approval required |
| Untested deployments | Code went to production without any automated testing | CI pipeline blocks merge until lint + compile + test pass |
| Credential exposure | Passwords in plaintext in autoexec.sas | Environment variables + Databricks Secrets |
| No audit trail | No record of who changed what or when | Full Git history + PR conversation threads |
| Inconsistent code style | Each developer had their own formatting | sqlfluff enforces organizational standards |
| Manual scheduling | Control-M job definitions not version-controlled | Databricks Workflow JSON in Git |
| No data contracts | Data quality assumptions implicit and undocumented | dbt tests make expectations explicit and enforced |
| Risky deployments | No rollback mechanism (restore from backup) | `git revert` provides instant, safe rollback |
