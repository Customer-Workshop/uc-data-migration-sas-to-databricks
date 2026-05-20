# Databricks Workflows — Replacing Control-M Batch Orchestration

## Overview

The `daily_banking_pipeline.json` in this directory is a Databricks Workflow definition that replaces the SAS/Control-M batch orchestration previously defined in `ts-sas-legacy-analytics/BatchJobs/run_daily_banking.sas`.

## Control-M → Databricks Workflow Mapping

| Control-M / SAS Construct | Databricks Workflow Equivalent |
|---|---|
| Control-M job definition (XML/GUI) | Workflow JSON definition (version-controlled) |
| `%run_step(1, ...)` sequential macro | `tasks[].depends_on` DAG dependencies |
| `run_daily_banking.sas` master script | `daily_banking_pipeline` workflow |
| `%check_step_status` / `%abort` macros | Task-level `max_retries` + failure notifications |
| `sendmail` on failure | `email_notifications.on_failure` + webhook to PagerDuty |
| Control-M calendar scheduling | `schedule.quartz_cron_expression` |
| SAS log files (`/logs/daily_banking_YYYYMMDD.log`) | Databricks workflow run history + Spark UI |
| Manual restart from failed step | Workflow "Repair Run" (re-run from failed task) |

## SAS Batch Flow vs Databricks Workflow

### Before (SAS + Control-M)

```
Control-M Server                SAS Compute
────────────────               ─────────────
06:00 trigger job  ──────────→  run_daily_banking.sas
                                  %run_step(1) → load_customer_accounts.sas
                                  %run_step(2) → daily_transaction_processing.sas
                                  %run_step(3) → credit_risk_scoring.sas
                                  %check_step_status → sendmail on error
```

- No version control for the job definition (stored in Control-M GUI)
- No automatic retry or partial re-run capability
- Manual restart required operator intervention
- Logs scattered across SAS server file system

### After (Databricks Workflows)

```
Databricks Workflow Engine
──────────────────────────
06:00 cron trigger
  ├── dbt_staging      (dbt run --select tag:staging)
  ├── dbt_intermediate (depends on staging)
  ├── dbt_marts        (depends on intermediate)
  └── dbt_test         (depends on marts)
  
  On failure → email + PagerDuty webhook
  Repair Run → re-execute from failed task only
```

- Workflow definition is version-controlled JSON
- Built-in retry with configurable `max_retries`
- "Repair Run" re-executes only the failed task and its dependents
- Full run history, logs, and Spark UI accessible from Databricks workspace

## Deployment

To deploy this workflow to a Databricks workspace using the CLI:

```bash
# Set up authentication
export DATABRICKS_HOST="https://adb-1234567890123456.7.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."

# Create the workflow
databricks jobs create --json @workflows/daily_banking_pipeline.json

# Or update an existing workflow
databricks jobs reset --job-id <JOB_ID> --json @workflows/daily_banking_pipeline.json
```

For production environments, deploy via Databricks Asset Bundles or Terraform rather than manual CLI commands.
