# Monitoring Comparison: SAS/Control-M → Databricks Workflows

This document maps legacy monitoring and operational capabilities from the SAS/Control-M environment to their Databricks Workflow equivalents, as implemented in `workflows/daily_banking_pipeline.json`.

---

## 1. Failure Notification

| Aspect | SAS / Control-M (Legacy) | Databricks Workflows (Current) |
|---|---|---|
| **Mechanism** | `%sendmail` SAS macro using SMTP (`sendmail.sas`). Requires SAS session and SMTP relay to be available. | Databricks-native `email_notifications` and `webhook_notifications` in the workflow JSON definition. |
| **Failure alerts** | On-call email (`EMAIL_ONCALL = oncall-data@corp.internal`) sent from within the `%run_step` macro when `ABORT_ON_ERR=Y` and a step returns `SYSCC > 4`. | Email to `data-engineering-alerts@example.com` + PagerDuty webhook + Slack webhook, all fired automatically by the Databricks control plane on task or job failure. |
| **Success/summary alerts** | Batch summary email to distribution list (`EMAIL_DL = sas-ops@corp.internal`) sent at end of batch with pass/fail counts and duration. | `on_success` email list (configurable). Run history and duration are available in the Databricks Jobs UI without requiring a summary email. |
| **Reliability** | Fragile — depends on SAS session being alive, SMTP relay reachable, and `%sendmail` macro executing without error. Failures in the notification itself are silent. | Managed by Databricks platform — notification delivery is decoupled from job execution. Webhook retries are handled by the platform. |
| **Configuration** | Hardcoded in `autoexec.sas` (`%let EMAIL_DL`, `%let EMAIL_ONCALL`). Changes require editing the SAS config file and restarting the batch environment. | Declared in version-controlled JSON. Changes go through PR review and are deployed via Git merge. |
| **Channels** | Email only. | Email, PagerDuty webhooks, Slack webhooks — all configurable per event type (`on_failure`, `on_success`, `on_duration_warning_threshold_exceeded`). |

### Legacy code reference (`BatchJobs/run_daily_banking.sas`)

```sas
/* On failure — notify on-call */
%sendmail(
  to=&EMAIL_ONCALL,
  subject=BATCH FAILURE: &batch_id at step &step_num,
  body=Step &step_num (&step_name) failed with SYSERR=&step_rc. Batch halted. Restart with restart_from=&step_num.
);

/* End of batch — summary to distribution list */
%sendmail(
  to=&EMAIL_DL,
  subject=Batch &batch_id: &job_pass pass / &job_fail fail of &job_count steps,
  body=See ARCHIVE.BATCH_HISTORY for details. Duration: ...
);
```

### Databricks equivalent (`workflows/daily_banking_pipeline.json`)

```json
"email_notifications": {
  "on_failure": ["data-engineering-alerts@example.com"],
  "on_duration_warning_threshold_exceeded": ["data-engineering-alerts@example.com"]
},
"webhook_notifications": {
  "on_failure": [
    { "id": "${var.pagerduty_webhook_id}" },
    { "id": "${var.slack_webhook_id}" }
  ],
  "on_duration_warning_threshold_exceeded": [
    { "id": "${var.slack_webhook_id}" }
  ]
}
```

---

## 2. SLA-Based Alerting

| Aspect | SAS / Control-M (Legacy) | Databricks Workflows (Current) |
|---|---|---|
| **Duration monitoring** | Duration logged to `BATCH_CONTROL` table and printed in the SAS log at batch end. No automated alerting based on duration. | `health.rules` with `RUN_DURATION_SECONDS` metric triggers a warning when the pipeline exceeds 10,800 seconds (3 hours). |
| **SLA enforcement** | None. Operators manually reviewed log files or the Control-M GUI to determine if a job was running long. Escalation was ad hoc (phone call, chat). | Automated: when the health rule fires, `on_duration_warning_threshold_exceeded` sends email and Slack notifications without human intervention. |
| **Timeout (hard kill)** | No per-step timeout in SAS. If a step hung (e.g., Oracle query waiting on a lock), the entire batch stalled until an operator killed the SAS session. | Per-task `timeout_seconds` (staging: 3600s, intermediate: 3600s, marts: 7200s, test: 1800s). Databricks terminates the task and can retry automatically via `retry_on_timeout: true`. |
| **Historical tracking** | `PROC APPEND` to `ARCHIVE.BATCH_HISTORY` — a SAS dataset on the shared file system. Querying trends required writing ad hoc SAS code. | Databricks run history retained with full metrics. Queryable via Jobs API. Integrates with monitoring dashboards (Datadog, Grafana, etc.) via API. |

### Legacy duration logging (`BatchJobs/run_daily_banking.sas`)

```sas
%put NOTE: Duration: %sysfunc(putn(%sysevalf(%sysfunc(datetime())-&start_tm), time8.));
```

### Databricks SLA rule (`workflows/daily_banking_pipeline.json`)

```json
"health": {
  "rules": [
    {
      "metric": "RUN_DURATION_SECONDS",
      "op": "GREATER_THAN",
      "value": 10800
    }
  ]
}
```

---

## 3. Retry and Restart Policies

| Aspect | SAS / Control-M (Legacy) | Databricks Workflows (Current) |
|---|---|---|
| **Automatic retry** | None. Failed steps were never automatically retried. | Per-task `max_retries` with `min_retry_interval_millis` backoff. Staging: 2 retries (60s backoff), intermediate/marts: 1 retry (30s backoff), test: 0 retries. |
| **Abort-on-error** | `ABORT_ON_ERR=Y` in `autoexec.sas`. On step failure, the `%run_step` macro sets `_batch_abort=1`, halting all subsequent steps. | DAG dependency chain achieves the same effect: if `dbt_staging` fails (after exhausting retries), `dbt_intermediate`, `dbt_marts`, and `dbt_test` are automatically skipped. |
| **Manual restart** | `restart_from=N` parameter on `%run_daily_banking`. Operator re-runs the entire SAS session with a step number to skip already-completed steps. Requires shell/Control-M access. | Databricks **Repair Run**: re-executes only the failed task and its downstream dependents from the Databricks Jobs UI or API. No re-processing of successful upstream tasks. |
| **Restart granularity** | Step-level (numbered 1–4 in banking, 1–2 in insurance). Restart skips steps sequentially — no dependency awareness. | Task-level with DAG awareness. Repair Run understands the dependency graph and only re-runs what is necessary. |
| **Retry on timeout** | Not applicable — no timeout mechanism existed. | `retry_on_timeout: true` on staging, intermediate, and marts tasks. A timed-out task is automatically retried without operator intervention. |
| **Notification on retry** | Not applicable. | `alert_on_last_attempt: true` — notifications fire only after the final retry fails, reducing noise from transient failures. |

### Legacy restart logic (`BatchJobs/run_daily_banking.sas`)

```sas
%macro run_daily_banking(run_date=&CURR_DT, restart_from=);
  ...
  %macro run_step(step_num, step_name, program);
    /* Skip if restarting past this step */
    %if %length(&restart_from) > 0 %then %do;
      %if &step_num < &restart_from %then %do;
        %put NOTE: Skipping step &step_num (&step_name) — restart from &restart_from;
        %return;
      %end;
    %end;
    ...
    /* Abort batch on failure if configured */
    %if &ABORT_ON_ERR = Y %then %do;
      %let _batch_abort = 1;
    %end;
  %mend run_step;
```

### Databricks retry configuration (`workflows/daily_banking_pipeline.json`)

```json
{
  "task_key": "dbt_staging",
  "max_retries": 2,
  "min_retry_interval_millis": 60000,
  "retry_on_timeout": true
}
```

---

## 4. Per-Task Retry Rationale

The retry counts are calibrated to reflect the nature of each pipeline stage and mirror the operational intent of the legacy `ABORT_ON_ERR` + manual restart pattern:

| Task | Retries | Rationale |
|---|---|---|
| `dbt_staging` | 2 | Source data ingestion is most susceptible to transient issues (network timeouts, source system unavailability). Mirrors legacy behavior where operators would manually retry the Load step first. |
| `dbt_intermediate` | 1 | Business logic transformations — less likely to have transient failures, but a single retry handles Spark executor preemption or warehouse scaling delays. |
| `dbt_marts` | 1 | Analytical output build — same rationale as intermediate. Timeout is longer (7200s) due to incremental merge and risk scoring complexity. |
| `dbt_test` | 0 | Test failures are deterministic: if data quality checks fail, retrying will produce the same result. Requires human investigation. Mirrors legacy behavior where QA failures were escalated, not retried. |

---

## 5. Summary: What Changed

| Legacy Limitation | Databricks Resolution |
|---|---|
| `%sendmail` unreliable — depends on SMTP relay and SAS session | Platform-managed email + webhooks (Slack, PagerDuty) |
| No SLA alerting — duration only logged, never acted on | `health.rules` with `RUN_DURATION_SECONDS` triggers automated alerts |
| No automatic retry — operator manually re-runs entire batch | Per-task `max_retries` with configurable backoff |
| Coarse restart — `restart_from=N` skips steps sequentially | Repair Run re-executes only failed tasks + downstream dependents |
| Email-only notification channel | Email + PagerDuty + Slack, per event type |
| No timeout — hung steps block the entire batch indefinitely | Per-task `timeout_seconds` with automatic retry on timeout |
| Manual log file review for monitoring | Databricks Jobs UI, run history API, webhook integrations |
| Config changes require editing `autoexec.sas` | Workflow definition is version-controlled JSON, deployed via Git |
