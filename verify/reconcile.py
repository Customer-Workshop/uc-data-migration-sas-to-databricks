#!/usr/bin/env python3
"""
reconcile.py — Source -> target reconciliation report for the SAS -> Databricks
migration (Banking nightly batch).

Why this exists
---------------
The point of the migration is not just to produce *some* output on Databricks —
it is to produce output we can *trust* matches what the legacy SAS estate
produced. Trust is established the way a SAS analyst established it by hand:
deterministic reconciliation controls between the raw source and the converted
marts, plus row-by-row parity against the estate's golden outputs.

dbt singular tests (dbt_project/tests/reconcile_*.sql) gate the same invariants
on every build. This script is the human-facing companion: it runs the control
families below and prints one report you can attach to a PR or publish as a job
artifact. It exits non-zero if any control fails, so it also works as a CI gate.

Control families
----------------
  completeness   raw population == model population for every program boundary
  control_total  money / count totals carried unchanged across the pipeline
  mapping        every CASE branch resolves to a legal SAS value
  golden         row-by-row parity with the SAS golden outputs (business date
                 31JAN2024) when the golden seeds are loaded in the namespace

Golden provenance
-----------------
The golden seeds (dbt_project/seeds/sas_golden/) were produced by running the
legacy Banking programs in the estate's own container runtime (OpenSAS) —
see verify/golden/manifest.json. They are what the SAS *code* produces on the
seeded estate; they are not an export from the bank's production SAS server.
The report says so on every run.

Usage
-----
    python verify/reconcile.py --namespace dev
    python verify/reconcile.py --namespace run1 --business-date 2024-01-31 --report-month 202401
    python verify/reconcile.py --namespace dev --report reconciliation_report.md --json report.json

Credentials are read from the environment (same vars dbt uses):
    DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
When none of them is set the script assumes it is running *inside* Databricks
(the `parity_report` task of resources/daily_banking_pipeline.job.yml) and
uses the job's Spark session instead; `--report-dir` then drops a timestamped
report into a Unity Catalog volume.
Raw source location follows the dbt sources: DBT_RAW_CATALOG / DBT_RAW_SCHEMA.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path


class SqlConnectorBackend:
    """Local / CI execution over a SQL warehouse (databricks-sql-connector)."""

    def __init__(self) -> None:
        from databricks import sql

        host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
        self.con = sql.connect(
            server_hostname=host,
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        )

    def rows(self, query: str) -> list[tuple]:
        cur = self.con.cursor()
        try:
            cur.execute(query)
            return cur.fetchall()
        finally:
            cur.close()

    def close(self) -> None:
        self.con.close()


class SparkBackend:
    """Execution inside a Databricks job task (serverless / cluster Spark session)."""

    def __init__(self) -> None:
        from pyspark.sql import SparkSession

        self.spark = SparkSession.builder.getOrCreate()

    def rows(self, query: str) -> list[tuple]:
        return [tuple(r) for r in self.spark.sql(query).collect()]

    def close(self) -> None:
        pass


CONNECTION_VARS = ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")


def make_backend():
    if not any(os.environ.get(v) for v in CONNECTION_VARS):
        return SparkBackend()
    missing = [v for v in CONNECTION_VARS if not os.environ.get(v)]
    if missing:
        print(f"ERROR: {', '.join(missing)} not set (all of {', '.join(CONNECTION_VARS)} are needed)",
              file=sys.stderr)
        raise SystemExit(2)
    return SqlConnectorBackend()


def as_bool(value: str | bool | None) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def default_golden_dir() -> Path:
    # A Databricks spark_python_task exec()s the script, so __file__ is not defined there;
    # fall back to argv[0], which the task runner sets to the uploaded file path.
    script = globals().get("__file__") or sys.argv[0]
    return Path(script).resolve().parent / "golden"


GOLDEN_DIR = default_golden_dir()
GOLDEN_MANIFEST = GOLDEN_DIR / "manifest.json"
KNOWN_DEVIATIONS = GOLDEN_DIR / "known_deviations.json"

LENDING = "('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')"

# Golden parity contract — keys, exact columns, tolerance columns per table.
# Mirrors dbt_project/tests/reconcile_golden_*.sql.
GOLDEN_TABLES: list[dict] = [
    dict(sas="STG_BANK.CUST_ACCOUNTS_DAILY", layer="intermediate", model="int_account_metrics",
         seed="sas_golden_cust_accounts_daily", keys=["account_id"],
         exact=["account_type", "account_status", "customer_id", "dormancy_flag", "high_balance_flag",
                "acct_age_months", "days_inactive", "risk_rating", "snapshot_date"],
         approx=["current_balance", "credit_limit", "utilization_pct"]),
    dict(sas="STG_BANK.ACCT_EXCEPTIONS", layer="intermediate", model="int_acct_exceptions",
         seed="sas_golden_acct_exceptions", keys=["account_id"], multiset=True,
         exact=[], approx=[]),
    dict(sas="CURATED.DAILY_TRANSACTIONS", layer="marts", model="mart_daily_transactions_curated",
         seed="sas_golden_daily_transactions", keys=["transaction_id"],
         exact=["account_id", "transaction_date", "transaction_type"], approx=["transaction_amount"]),
    dict(sas="CURATED.TXN_ANOMALIES", layer="marts", model="mart_transaction_anomalies",
         seed="sas_golden_txn_anomalies", keys=["transaction_id"],
         exact=["account_id", "anomaly_type"], approx=["running_balance", "pre_txn_balance", "z_score"]),
    dict(sas="CURATED.RUNNING_BALANCES", layer="marts", model="mart_running_balances",
         seed="sas_golden_running_balances", keys=["transaction_id"],
         exact=["account_id", "transaction_date"], approx=["running_balance"]),
    dict(sas="CURATED.RISK_SCORES", layer="marts", model="mart_risk_scores",
         seed="sas_golden_risk_scores", keys=["account_id"],
         exact=["account_type", "new_risk_rating", "fico_score", "pmt_late_90_12mo", "acct_age_months"],
         approx=["ltv", "pd", "lgd", "ead", "expected_loss"]),
    dict(sas="CURATED.RISK_MIGRATION", layer="marts", model="mart_risk_migration",
         seed="sas_golden_risk_migration", keys=["account_id"],
         exact=["prev_rating", "curr_rating", "migration_direction"], approx=["pd", "expected_loss"]),
    dict(sas="REPORTS.RISK_SUMMARY", layer="marts", model="mart_risk_summary",
         seed="sas_golden_risk_summary", keys=["account_type", "new_risk_rating"],
         exact=["n_accounts", "lgd", "ead", "expected_loss"], approx=["avg_pd", "avg_lgd", "total_ead", "total_el"]),
    dict(sas="REPORTS.MONTHLY_RWA", layer="marts", model="mart_regulatory_rwa",
         seed="sas_golden_monthly_rwa", keys=["report_month", "account_type", "customer_segment", "risk_weight"],
         exact=["n_accounts"], approx=["total_exposure", "rwa"]),
    dict(sas="REPORTS.DELINQUENCY_AGING", layer="marts", model="mart_delinquency_aging",
         seed="sas_golden_delinquency_aging", keys=["report_month", "account_type", "region_code", "delinq_bucket"],
         exact=["n_accounts"], approx=["total_balance", "total_past_due"]),
    dict(sas="REPORTS.LLP_COVERAGE", layer="marts", model="mart_llp_coverage",
         seed="sas_golden_llp_coverage", keys=["report_month", "account_type"],
         exact=["n_loans"], approx=["gross_loans", "total_allowance", "coverage_pct", "npl_balance", "npl_coverage_pct"],
         # KD-001 (verify/golden/known_deviations.json): golden NPL_COVERAGE_PCT is 0 where NPL_BALANCE > 0
         accept={"npl_coverage_pct": "(g.npl_coverage_pct = 0 and g.npl_balance > 0 and abs(cast(m.npl_coverage_pct as double)"
                                     " - cast(g.total_allowance as double) / cast(g.npl_balance as double) * 100)"
                                     " <= 1e-6 * greatest(abs(m.npl_coverage_pct), 1))"}),
    dict(sas="REPORTS.CAPITAL_ADEQUACY", layer="marts", model="mart_capital_adequacy",
         seed="sas_golden_capital_adequacy", keys=["report_month"],
         exact=["cet1_status", "tier1_status", "total_capital_status", "cet1_capital", "tier1_capital", "total_capital"],
         approx=["total_rwa", "cet1_ratio", "tier1_ratio", "total_capital_ratio"]),
]
RELATIVE_TOLERANCE_COLS = {"pd", "lgd", "expected_loss", "avg_pd", "avg_lgd", "total_el", "z_score", "ltv",
                           "coverage_pct", "npl_coverage_pct", "cet1_ratio", "tier1_ratio", "total_capital_ratio"}
ABS_TOL = 0.005
REL_TOL = 1e-6


@dataclass
class CheckResult:
    family: str
    name: str
    status: str  # PASS | FAIL | SKIP
    detail: str = ""
    metrics: dict = field(default_factory=dict)
    diffs: list = field(default_factory=list)


class Reconciler:
    def __init__(self, catalog: str, namespace: str, raw_catalog: str, raw_schema: str,
                 business_date: str, report_month: str):
        self.backend = make_backend()
        self.catalog = catalog
        self.ns = namespace
        self.raw = f"{raw_catalog}.{raw_schema}"
        self.staging = f"{catalog}.{namespace}_staging"
        self.intermediate = f"{catalog}.{namespace}_intermediate"
        self.marts = f"{catalog}.{namespace}_marts"
        self.seeds = f"{catalog}.{namespace}_seeds"
        self.business_date = business_date
        self.report_month = report_month
        self.month_end = f"last_day(to_date('{report_month}01', 'yyyyMMdd'))"
        self.results: list[CheckResult] = []

    # ---------------------------------------------------------------- helpers
    def _rows(self, query: str) -> list[tuple]:
        return self.backend.rows(query)

    def _scalar(self, query: str):
        rows = self._rows(query)
        return rows[0][0] if rows else None

    def _table_exists(self, fqn: str) -> bool:
        schema, table = fqn.rsplit(".", 1)
        catalog, schema_name = schema.split(".")
        n = self._scalar(
            f"select count(*) from {catalog}.information_schema.tables "
            f"where table_schema = '{schema_name}' and table_name = '{table}'"
        )
        return bool(n)

    def _add(self, family, name, ok, detail, metrics=None, diffs=None, skip=False):
        self.results.append(CheckResult(
            family, name, "SKIP" if skip else ("PASS" if ok else "FAIL"), detail, metrics or {}, diffs or []))

    @staticmethod
    def _close(a, b) -> bool:
        if a is None and b is None:
            return True
        if a is None or b is None:
            return False
        return abs(float(a) - float(b)) <= ABS_TOL

    # --------------------------------------------------- completeness controls
    def check_account_completeness(self):
        """load_customer_accounts.sas Step 1 scope: model accounts == in-scope raw accounts."""
        expected = self._scalar(f"""
            select count(*)
            from {self.raw}.cust_accounts a
            inner join {self.raw}.cust_demographics d on a.customer_id = d.customer_id
            where a.account_status not in ('W', 'C')
              and a.open_date <= to_date('{self.business_date}')
        """)
        actual = self._scalar(f"select count(*) from {self.intermediate}.int_account_metrics")
        self._add("completeness", "account_completeness", expected == actual,
                  f"in-scope raw accounts = {expected}, model accounts = {actual}",
                  {"expected": expected, "actual": actual})

    def check_txn_completeness(self):
        """daily_transaction_processing.sas Step 1: feed == validated + rejected; curated == history + validated."""
        feed_n, feed_amt = self._rows(
            f"select count(*), round(sum(transaction_amount), 2) from {self.raw}.daily_transactions")[0]
        val_n, val_amt = self._rows(
            f"select count(*), round(coalesce(sum(transaction_amount), 0), 2) from {self.staging}.stg_daily_transactions")[0]
        rej_n, rej_amt = self._rows(
            f"select count(*), round(coalesce(sum(transaction_amount), 0), 2) from {self.staging}.stg_txn_rejected")[0]
        hist_n = self._scalar(f"select count(*) from {self.raw}.curated_daily_transactions_history")
        cur_n = self._scalar(f"select count(*) from {self.marts}.mart_daily_transactions_curated")
        bal_n = self._scalar(f"select count(*) from {self.marts}.mart_running_balances")
        self._add("completeness", "txn_feed_split", feed_n == val_n + rej_n,
                  f"feed = {feed_n}, validated = {val_n}, rejected = {rej_n}",
                  {"feed": feed_n, "validated": val_n, "rejected": rej_n})
        self._add("control_total", "txn_feed_amount", self._close(feed_amt, (val_amt or 0) + (rej_amt or 0)),
                  f"feed sum = {feed_amt}, validated + rejected = {round((val_amt or 0) + (rej_amt or 0), 2)}",
                  {"feed": feed_amt, "validated": val_amt, "rejected": rej_amt})
        self._add("completeness", "curated_append", cur_n == hist_n + val_n,
                  f"history {hist_n} + validated {val_n} = {hist_n + val_n}, curated = {cur_n}",
                  {"history": hist_n, "validated": val_n, "curated": cur_n})
        self._add("completeness", "running_balances_rows", bal_n == val_n,
                  f"validated = {val_n}, running_balances = {bal_n}", {"validated": val_n, "running_balances": bal_n})

    def check_risk_completeness(self):
        """credit_risk_scoring.sas: every lending account in today's snapshot is scored once."""
        expected = self._scalar(f"""
            select count(*) from {self.intermediate}.int_account_metrics
            where account_type in {LENDING} and snapshot_date = to_date('{self.business_date}')
        """)
        actual, distinct = self._rows(f"""
            select count(*), count(distinct account_id) from {self.marts}.mart_risk_scores
            where score_date = to_date('{self.business_date}')
        """)[0]
        self._add("completeness", "risk_scores_completeness", expected == actual == distinct,
                  f"lending accounts = {expected}, scored rows = {actual}, distinct accounts = {distinct}",
                  {"expected": expected, "actual": actual, "distinct": distinct})

    # ------------------------------------------------- regulatory control totals
    def check_regulatory_totals(self):
        snap_n, snap_bal = self._rows(f"""
            select count(*), round(sum(current_balance), 2) from {self.intermediate}.int_account_metrics
            where snapshot_date = {self.month_end}
        """)[0]
        rwa_n, rwa_exp, rwa_rwa = self._rows(f"""
            select sum(n_accounts), round(sum(total_exposure), 2), round(sum(rwa), 2)
            from {self.marts}.mart_regulatory_rwa
        """)[0]
        cap_rwa = self._scalar(f"select round(total_rwa, 2) from {self.marts}.mart_capital_adequacy")
        self._add("completeness", "rwa_accounts", snap_n == rwa_n,
                  f"month-end snapshot = {snap_n}, sum(N_ACCOUNTS) = {rwa_n}", {"snapshot": snap_n, "rwa": rwa_n})
        self._add("control_total", "rwa_total_exposure", self._close(snap_bal, rwa_exp),
                  f"snapshot balance = {snap_bal}, sum(TOTAL_EXPOSURE) = {rwa_exp}",
                  {"snapshot": snap_bal, "rwa": rwa_exp})
        self._add("control_total", "capital_total_rwa", self._close(rwa_rwa, cap_rwa),
                  f"sum(RWA) = {rwa_rwa}, CAPITAL_ADEQUACY.TOTAL_RWA = {cap_rwa}", {"rwa": rwa_rwa, "capital": cap_rwa})

        lend_n, lend_bal = self._rows(f"""
            select count(*), round(sum(current_balance), 2) from {self.intermediate}.int_account_metrics
            where snapshot_date = {self.month_end} and account_type in {LENDING}
        """)[0]
        dq_n, dq_bal = self._rows(f"""
            select sum(n_accounts), round(sum(total_balance), 2) from {self.marts}.mart_delinquency_aging
        """)[0]
        self._add("completeness", "delinquency_accounts", lend_n == dq_n,
                  f"lending accounts = {lend_n}, sum(N_ACCOUNTS) = {dq_n}", {"lending": lend_n, "report": dq_n})
        self._add("control_total", "delinquency_total_balance", self._close(lend_bal, dq_bal),
                  f"lending balance = {lend_bal}, sum(TOTAL_BALANCE) = {dq_bal}", {"lending": lend_bal, "report": dq_bal})

        llp_exp_n, llp_exp_bal = self._rows(f"""
            select count(*), round(sum(a.current_balance), 2)
            from {self.intermediate}.int_account_metrics a
            inner join {self.staging}.stg_loan_details l on a.account_id = l.account_id
            where a.snapshot_date = {self.month_end} and a.account_type in {LENDING}
        """)[0]
        llp_n, llp_bal = self._rows(
            f"select sum(n_loans), round(sum(gross_loans), 2) from {self.marts}.mart_llp_coverage")[0]
        self._add("completeness", "llp_loans", llp_exp_n == llp_n,
                  f"lending accounts with loan detail = {llp_exp_n}, sum(N_LOANS) = {llp_n}",
                  {"expected": llp_exp_n, "report": llp_n})
        self._add("control_total", "llp_gross_loans", self._close(llp_exp_bal, llp_bal),
                  f"expected = {llp_exp_bal}, sum(GROSS_LOANS) = {llp_bal}", {"expected": llp_exp_bal, "report": llp_bal})

    # --------------------------------------------------------- mapping parity
    def check_mapping_parity(self):
        bad_rw = self._rows(f"""
            select account_type, risk_weight, n_accounts from {self.marts}.mart_regulatory_rwa
            where not (
                (account_type in ('CHK','SAV','MMA','CD') and risk_weight = 0.00)
                or (account_type = 'MTG' and risk_weight in (0.35, 0.50))
                or (account_type = 'HELC' and risk_weight = 0.50)
                or (account_type in ('AUTO','PERS','CC') and risk_weight = 0.75)
                or (account_type not in ('CHK','SAV','MMA','CD','MTG','HELC','AUTO','PERS','CC') and risk_weight = 1.00))
        """)
        branches = self._rows(f"select distinct account_type, risk_weight from {self.marts}.mart_regulatory_rwa order by 1, 2")
        self._add("mapping", "rwa_risk_weight_branches", not bad_rw,
                  f"{len(branches)} (ACCOUNT_TYPE, RISK_WEIGHT) branches observed, {len(bad_rw)} illegal",
                  {"branches": [list(b) for b in branches]}, [list(b) for b in bad_rw])

        bad_bkt = self._rows(f"""
            select delinq_bucket, bucket_sort_order from {self.marts}.mart_delinquency_aging
            where bucket_sort_order <> case delinq_bucket
                when 'Current' then 0 when '1-29' then 1 when '30-59' then 2 when '60-89' then 3
                when '90-119' then 4 when '120-179' then 5 when '180+' then 6 else 7 end
               or delinq_bucket not in ('Current','1-29','30-59','60-89','90-119','120-179','180+','Unknown')
        """)
        buckets = [r[0] for r in self._rows(
            f"select delinq_bucket from {self.marts}.mart_delinquency_aging "
            "group by delinq_bucket, bucket_sort_order order by bucket_sort_order")]
        self._add("mapping", "delinquency_buckets", not bad_bkt,
                  f"buckets present in severity order: {', '.join(buckets)}", {"buckets": buckets}, [list(b) for b in bad_bkt])

        bad_rating = self._scalar(f"""
            select count(*) from {self.marts}.mart_risk_scores
            where score_date = to_date('{self.business_date}')
              and new_risk_rating <> case when pd < 0.005 then 1 when pd < 0.01 then 2 when pd < 0.03 then 3
                  when pd < 0.07 then 4 when pd < 0.15 then 5 when pd < 0.30 then 6 else 7 end
        """)
        ratings = [r[0] for r in self._rows(
            f"select distinct new_risk_rating from {self.marts}.mart_risk_scores order by 1")]
        self._add("mapping", "risk_rating_bands", bad_rating == 0,
                  f"ratings observed: {ratings}; rows outside their PD band: {bad_rating}", {"ratings": ratings})

        bad_cap = self._rows(f"""
            select cet1_status, tier1_status, total_capital_status, cet1_ratio, tier1_ratio, total_capital_ratio
            from {self.marts}.mart_capital_adequacy
            where cet1_status <> case when total_rwa = 0 or cet1_ratio >= 4.5 then 'PASS' else 'FAIL' end
               or tier1_status <> case when total_rwa = 0 or tier1_ratio >= 6.0 then 'PASS' else 'FAIL' end
               or total_capital_status <> case when total_rwa = 0 or total_capital_ratio >= 8.0 then 'PASS' else 'FAIL' end
        """)
        self._add("mapping", "capital_status_thresholds", not bad_cap,
                  "PASS/FAIL flags agree with 4.5% / 6.0% / 8.0% minimums", {}, [list(b) for b in bad_cap])

        exc = self._rows(f"select exception_code, count(*) from {self.intermediate}.int_acct_exceptions group by 1 order by 1")
        self._add("mapping", "acct_exception_codes",
                  all(c in ("NEG_BAL", "HIGH_UTIL", "NO_RISK") for c, _ in exc),
                  "exception rows by code: " + ", ".join(f"{c}={n}" for c, n in exc), {"codes": dict(exc)})

        anomalies = self._rows(f"""
            select anomaly_type, count(*) from {self.marts}.mart_transaction_anomalies
            where transaction_date = to_date('{self.business_date}') group by 1 order by 1
        """)
        self._add("mapping", "anomaly_types",
                  all(t in ("HIGH_AMOUNT", "OVERDRAFT", "LARGE_WITHDRAWAL", "ORPHAN_ACCOUNT") for t, _ in anomalies),
                  "anomalies by type: " + ", ".join(f"{t}={n}" for t, n in anomalies), {"types": dict(anomalies)})

    # ---------------------------------------------------------- golden parity
    def check_golden(self):
        for spec in GOLDEN_TABLES:
            seed_fqn = f"{self.seeds}.{spec['seed']}"
            model_fqn = f"{self.catalog}.{self.ns}_{spec['layer']}.{spec['model']}"
            name = f"golden_{spec['seed'].replace('sas_golden_', '')}"
            if not self._table_exists(seed_fqn):
                self._add("golden", name, True, f"seed {seed_fqn} not loaded (run dbt seed --select tag:sas_golden)", skip=True)
                continue
            if spec.get("multiset"):
                self._check_golden_multiset(spec, name, model_fqn, seed_fqn)
                continue
            keys = spec["keys"]
            on = " and ".join(f"m.{k} <=> g.{k}" for k in keys)
            conds = [f"m.{keys[0]} is null", f"g.{keys[0]} is null"]
            labels = ["'missing_in_dbt'", "'missing_in_sas'"]
            for c in spec["exact"]:
                conds.append(f"not (m.{c} <=> g.{c})")
                labels.append(f"'{c}'")
            for c in spec["approx"]:
                a, b = f"cast(m.{c} as double)", f"cast(g.{c} as double)"
                if c in RELATIVE_TOLERANCE_COLS:
                    close = f"(({a} is null and {b} is null) or abs({a} - {b}) <= {REL_TOL} * greatest(abs({a}), abs({b}), 1))"
                else:
                    close = f"(({a} is null and {b} is null) or abs({a} - {b}) <= {ABS_TOL})"
                accept = spec.get("accept", {}).get(c)
                if accept:
                    close = f"({close} or {accept})"
                conds.append(f"not {close}")
                labels.append(f"'{c}'")
            mismatch = "case " + " ".join(f"when {c} then {l}" for c, l in zip(conds, labels)) + " else null end"
            key_sel = ", ".join(f"coalesce(cast(m.{k} as string), cast(g.{k} as string)) as {k}" for k in keys)
            compare_cols = spec["exact"] + spec["approx"]
            val_sel = ", ".join(f"m.{c} as dbt_{c}, g.{c} as sas_{c}" for c in compare_cols)
            q = f"""
                with compared as (
                    select {key_sel}, {mismatch} as mismatch{', ' + val_sel if val_sel else ''}
                    from {model_fqn} m
                    full outer join {seed_fqn} g on {on}
                )
                select * from compared where mismatch is not null
            """
            diffs = self._rows(q)
            n_model = self._scalar(f"select count(*) from {model_fqn}")
            n_seed = self._scalar(f"select count(*) from {seed_fqn}")
            by_kind: dict[str, int] = {}
            for d in diffs:
                by_kind[d[len(keys)]] = by_kind.get(d[len(keys)], 0) + 1
            detail = f"SAS rows = {n_seed}, dbt rows = {n_model}, mismatched rows = {len(diffs)}"
            if by_kind:
                detail += " (" + ", ".join(f"{k}: {v}" for k, v in sorted(by_kind.items())) + ")"
            self._add("golden", name, not diffs, detail,
                      {"sas": spec["sas"], "sas_rows": n_seed, "dbt_rows": n_model, "mismatch_by_kind": by_kind,
                       "columns": keys + ["mismatch"] + [x for c in compare_cols for x in (f"dbt_{c}", f"sas_{c}")]},
                      [[str(v) if v is not None else None for v in row] for row in diffs[:25]])

    def _check_golden_multiset(self, spec, name, model_fqn, seed_fqn):
        """ACCT_EXCEPTIONS has no persisted rule code: compare the multiset of account_ids."""
        diffs = self._rows(f"""
            with m as (select account_id, count(*) as n from {model_fqn} group by 1),
                 g as (select account_id, count(*) as n from {seed_fqn} group by 1)
            select coalesce(m.account_id, g.account_id) as account_id, m.n as dbt_rows, g.n as sas_rows
            from m full outer join g on m.account_id = g.account_id
            where not (m.n <=> g.n)
        """)
        n_model = self._scalar(f"select count(*) from {model_fqn}")
        n_seed = self._scalar(f"select count(*) from {seed_fqn}")
        self._add("golden", name, not diffs,
                  f"SAS rows = {n_seed}, dbt rows = {n_model}, accounts with different exception counts = {len(diffs)}",
                  {"sas": spec["sas"], "sas_rows": n_seed, "dbt_rows": n_model,
                   "columns": ["account_id", "dbt_rows", "sas_rows"]},
                  [[str(v) if v is not None else None for v in row] for row in diffs[:25]])

    # ------------------------------------------------------------------ driver
    def run(self, golden: bool) -> bool:
        self.check_account_completeness()
        self.check_txn_completeness()
        self.check_risk_completeness()
        self.check_regulatory_totals()
        self.check_mapping_parity()
        if golden:
            self.check_golden()
        self.backend.close()
        return all(r.status != "FAIL" for r in self.results)

    def render(self, golden: bool) -> str:
        manifest = json.loads(GOLDEN_MANIFEST.read_text()) if GOLDEN_MANIFEST.exists() else {}
        lines = [
            f"# Reconciliation Report — {self.catalog} / namespace `{self.ns}`",
            "",
            f"Business date `{self.business_date}`, report month `{self.report_month}`, "
            f"raw source `{self.raw}`, generated {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}.",
            "",
            "Source -> target controls proving the converted models match the legacy",
            "Banking nightly batch. FAIL blocks the migration; SKIP means a prerequisite",
            "(the golden seeds) is not loaded in this namespace.",
            "",
            "| Family | Control | Result | Detail |",
            "|---|---|---|---|",
        ]
        for r in self.results:
            lines.append(f"| {r.family} | `{r.name}` | {r.status} | {r.detail} |")
        passed = sum(r.status == "PASS" for r in self.results)
        failed = sum(r.status == "FAIL" for r in self.results)
        skipped = sum(r.status == "SKIP" for r in self.results)
        lines += ["", f"**{passed} passed, {failed} failed, {skipped} skipped**", ""]

        failing = [r for r in self.results if r.status == "FAIL" and r.diffs]
        if failing:
            lines += ["## Differences (first 25 rows per control)", ""]
            for r in failing:
                cols = r.metrics.get("columns") or [f"c{i}" for i in range(len(r.diffs[0]))]
                lines += [f"### `{r.name}`", "", "| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
                for d in r.diffs:
                    lines.append("| " + " | ".join("" if v is None else str(v) for v in d) + " |")
                lines.append("")

        lines += ["## Golden data provenance", ""]
        if golden and manifest:
            lines += [
                f"- Golden outputs come from running the legacy programs in the estate's container "
                f"runtime (`{manifest.get('runtime')}`), estate `{manifest.get('estate')}` @ "
                f"`{manifest.get('estate_commit')}`, business date `{manifest.get('business_date')}`.",
                "- They are what the SAS code produces on the seeded estate — not an export from the "
                "bank's production SAS server. Re-generate with `docker compose -f docker/compose.yml run --rm sas` "
                "in the estate repo and `python seed/import_sas_golden.py`.",
                "- SAS row counts: " + ", ".join(f"{k}={v}" for k, v in manifest.get("row_counts", {}).items()) + ".",
            ]
            if KNOWN_DEVIATIONS.exists():
                for kd in json.loads(KNOWN_DEVIATIONS.read_text()).get("deviations", []):
                    lines.append(f"- Known runtime deviation {kd['id']} ({kd['table']}.{kd['column']}): "
                                 f"{kd['migration_decision']}")
        else:
            lines.append("- Golden parity was not requested (`--golden`); rule-based controls only.")
        lines.append("")
        return "\n".join(lines)

    def to_json(self) -> dict:
        return {
            "catalog": self.catalog, "namespace": self.ns, "raw": self.raw,
            "business_date": self.business_date, "report_month": self.report_month,
            "passed": all(r.status != "FAIL" for r in self.results),
            "results": [asdict(r) for r in self.results],
        }


def default_report_month(business_date: str) -> str:
    d = dt.date.fromisoformat(business_date)
    return (d.replace(day=1) - dt.timedelta(days=1)).strftime("%Y%m")


def main() -> int:
    ap = argparse.ArgumentParser(description="SAS -> Databricks reconciliation report")
    ap.add_argument("--catalog", default=os.environ.get("DBT_CATALOG", "banking_analytics"))
    ap.add_argument("--namespace", default=os.environ.get("DBT_SCHEMA", "dev"),
                    help="Output namespace prefix (default: $DBT_SCHEMA or 'dev')")
    ap.add_argument("--raw-catalog", default=os.environ.get("DBT_RAW_CATALOG", "banking_analytics"))
    ap.add_argument("--raw-schema", default=os.environ.get("DBT_RAW_SCHEMA", "raw_sas"))
    ap.add_argument("--business-date", default=os.environ.get("SAS_CURR_DT", dt.date.today().isoformat()),
                    help="&CURR_DT the batch was run for (dbt var curr_dt), YYYY-MM-DD")
    ap.add_argument("--report-month", default=os.environ.get("SAS_REPORT_MONTH"),
                    help="report_month var, YYYYMM (default: month before business date, like &PREV_YM)")
    ap.add_argument("--golden", nargs="?", const="true", default="false", metavar="true|false",
                    help="Also run row-by-row parity against the SAS golden seeds (needs the golden "
                         "business date). Takes an optional value so a job parameter can drive it.")
    ap.add_argument("--report", help="Optional path to write the markdown report")
    ap.add_argument("--json", dest="json_path", help="Optional path to write machine-readable results")
    ap.add_argument("--report-dir",
                    help="Directory (e.g. a UC volume) receiving parity_report_<namespace>_<date>_<utc ts>.md/.json")
    ap.add_argument("--golden-dir",
                    help="Directory holding manifest.json / known_deviations.json (default: verify/golden next to this script)")
    args = ap.parse_args()

    if args.golden_dir:
        global GOLDEN_MANIFEST, KNOWN_DEVIATIONS
        GOLDEN_MANIFEST = Path(args.golden_dir) / "manifest.json"
        KNOWN_DEVIATIONS = Path(args.golden_dir) / "known_deviations.json"
    report_month = args.report_month or default_report_month(args.business_date)
    golden = as_bool(args.golden)
    rec = Reconciler(args.catalog, args.namespace, args.raw_catalog, args.raw_schema,
                     args.business_date, report_month)
    ok = rec.run(golden)
    report = rec.render(golden)
    print(report)
    report_path, json_path = args.report, args.json_path
    if args.report_dir:
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        stem = f"parity_report_{args.namespace}_{args.business_date}_{stamp}"
        Path(args.report_dir).mkdir(parents=True, exist_ok=True)
        report_path = report_path or str(Path(args.report_dir) / f"{stem}.md")
        json_path = json_path or str(Path(args.report_dir) / f"{stem}.json")
    if report_path:
        Path(report_path).write_text(report + "\n")
        print(f"(report written to {report_path})")
    if json_path:
        Path(json_path).write_text(json.dumps(rec.to_json(), indent=2, default=str) + "\n")
        print(f"(json written to {json_path})")
    return 0 if ok else 1


if __name__ == "__main__":
    rc = main()
    if rc:
        raise SystemExit(rc)
