#!/usr/bin/env python3
"""
reconcile.py — Source -> target reconciliation report for the SAS -> Databricks
migration.

Why this exists
---------------
The point of the migration is not just to produce *some* output on Databricks —
it is to produce output we can *trust* matches what the legacy SAS estate would
have produced. Because there is no live SAS runtime here, "trust" is established
the same way a SAS analyst established it by hand: deterministic reconciliation
controls (row counts, control totals, domain coverage, referential integrity)
between the raw source and the converted marts.

dbt schema/singular tests already gate these invariants on every build (see
dbt_project/tests/reconcile_*.sql). This script is the human-facing companion:
it runs the same family of controls and prints a single reconciliation report you
can show live and attach to a PR. It exits non-zero if any control fails, so it
also works as a CI / pre-merge gate.

This is the harness *framework* with the banking-domain control
(`account_completeness`). When a new program is converted, its conversion adds
the matching controls here (e.g. risk-weight parity, control totals, cross-engine
PySpark checks) — see .workshop/playbooks/sas-to-databricks-conversion.devin.md
and .agents/skills/sas-to-databricks-conversion/SKILL.md for the reconciliation contract.

Usage
-----
    python verify/reconcile.py --namespace dev
    python verify/reconcile.py --namespace run1 --catalog banking_analytics
    python verify/reconcile.py --namespace dev --report reconciliation_report.md

Credentials are read from the environment (same vars dbt uses):
    DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field

from databricks import sql


@dataclass
class CheckResult:
    name: str
    status: str  # PASS | FAIL | SKIP
    detail: str = ""
    metrics: dict = field(default_factory=dict)


class Reconciler:
    def __init__(self, catalog: str, namespace: str):
        host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
        self.con = sql.connect(
            server_hostname=host,
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        )
        self.catalog = catalog
        self.ns = namespace
        self.raw = f"{catalog}.raw"
        self.staging = f"{catalog}.{namespace}_staging"
        self.intermediate = f"{catalog}.{namespace}_intermediate"
        self.marts = f"{catalog}.{namespace}_marts"
        self.curated = f"{catalog}.{namespace}_curated"
        self.results: list[CheckResult] = []

    def _scalar(self, query: str):
        cur = self.con.cursor()
        try:
            cur.execute(query)
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            cur.close()

    def _row(self, query: str):
        cur = self.con.cursor()
        try:
            cur.execute(query)
            return cur.fetchone()
        finally:
            cur.close()

    # ------------------------------------------------------------------ checks
    def check_account_completeness(self):
        """Model accounts must equal the documented in-scope raw population."""
        expected = self._scalar(
            f"""
            select count(*)
            from {self.raw}.cust_accounts a
            inner join {self.raw}.cust_demographics d on a.customer_id = d.customer_id
            where a.account_status not in ('W', 'C')
              and a.open_date <= current_date()
            """
        )
        actual = self._scalar(f"select count(*) from {self.intermediate}.int_account_metrics")
        ok = expected == actual
        self.results.append(
            CheckResult(
                "account_completeness",
                "PASS" if ok else "FAIL",
                f"in-scope raw accounts = {expected}, model accounts = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_risk_score_completeness(self):
        expected = self._scalar(
            f"""
            select count(*) from {self.intermediate}.int_account_metrics
            where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
              and snapshot_date = current_date()
            """
        )
        actual = self._scalar(f"select count(*) from {self.marts}.mart_risk_scores")
        ok = expected == actual
        self.results.append(CheckResult(
            "risk_score_completeness", "PASS" if ok else "FAIL",
            f"expected scored accounts = {expected}, actual scored accounts = {actual}",
            {"expected": expected, "actual": actual},
        ))

    def check_risk_ead_control_total(self):
        expected = self._row(
            f"""
            select sum(current_balance), sum(
                case when account_type in ('CC', 'LOC', 'HELC')
                     then current_balance + 0.50 * (credit_limit - current_balance)
                     else current_balance end
            )
            from {self.intermediate}.int_account_metrics
            where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
              and snapshot_date = current_date()
            """
        )
        actual = self._row(
            f"select sum(current_balance), sum(ead) "
            f"from {self.marts}.mart_risk_scores"
        )
        expected_balance, expected_ead = expected or (None, None)
        actual_balance, actual_ead = actual or (None, None)
        ok = (
            expected_balance is not None
            and actual_balance is not None
            and expected_ead is not None
            and actual_ead is not None
            and abs(expected_balance - actual_balance) <= 0.01
            and abs(expected_ead - actual_ead) <= 0.01
        )
        self.results.append(CheckResult(
            "risk_ead_control_total", "PASS" if ok else "FAIL",
            f"expected balance = {expected_balance}, actual balance = {actual_balance}; "
            f"expected EAD = {expected_ead}, actual EAD = {actual_ead}; tolerance = 0.01",
            {"expected_balance": expected_balance, "actual_balance": actual_balance,
             "expected_ead": expected_ead, "actual_ead": actual_ead},
        ))

    def check_risk_pd_parity(self):
        max_diff = self._scalar(
            f"""
            with expected as (
                select
                    s.account_id,
                    s.pd,
                    1.0 / (1.0 + exp(-(
                        -3.2145
                        + 0.412 * case
                            when b.fico_score is null then 0.198
                            when b.fico_score >= 760 then -1.204
                            when b.fico_score >= 720 then -0.812
                            when b.fico_score >= 680 then -0.356
                            when b.fico_score >= 640 then 0.198
                            when b.fico_score >= 600 then 0.654
                            else 1.102 end
                        + 0.198 * case
                            when a.utilization_pct is null then 0
                            when a.utilization_pct <= 10 then -0.956
                            when a.utilization_pct <= 30 then -0.521
                            when a.utilization_pct <= 50 then -0.102
                            when a.utilization_pct <= 70 then 0.334
                            when a.utilization_pct <= 90 then 0.789
                            else 1.245 end
                        + 0.289 * case
                            when p.pmt_late_90_12mo = 0 then -0.678
                            when p.pmt_late_90_12mo = 1 then 0.445
                            when p.pmt_late_90_12mo is null then 0
                            else 1.567 end
                        + 0.067 * case
                            when a.acct_age_months is null then 0
                            when a.acct_age_months >= 120 then -0.534
                            when a.acct_age_months >= 60 then -0.289
                            when a.acct_age_months >= 24 then 0.045
                            else 0.456 end
                        + 0.134 * case
                            when a.account_type not in ('MTG', 'AUTO', 'HELC') then 0
                            when c.collateral_value is null or c.collateral_value <= 0 then 0
                            when a.current_balance / c.collateral_value <= 0.60 then -0.712
                            when a.current_balance / c.collateral_value <= 0.80 then -0.234
                            when a.current_balance / c.collateral_value <= 1.00 then 0.356
                            else 0.889 end
                    ))) as expected_pd
                from {self.marts}.mart_risk_scores s
                inner join {self.intermediate}.int_account_metrics a
                    on s.account_id = a.account_id
                   and a.snapshot_date = current_date()
                left join {self.raw}.bureau_scores b on a.customer_id = b.customer_id
                left join {self.raw}.payment_history p on a.account_id = p.account_id
                left join {self.raw}.collateral c on a.account_id = c.account_id
            )
            select max(abs(pd - expected_pd)) from expected
            """
        )
        max_diff = max_diff or 0.0
        ok = max_diff <= 1e-9
        self.results.append(CheckResult(
            "risk_pd_parity", "PASS" if ok else "FAIL",
            f"max absolute PD difference = {max_diff}; tolerance = 1e-9",
            {"max_abs_diff": max_diff},
        ))

    def check_risk_lgd_ead_parity(self):
        mismatches = self._scalar(
            f"""
            select count(*)
            from {self.marts}.mart_risk_scores s
            inner join {self.intermediate}.int_account_metrics a
                on s.account_id = a.account_id
               and a.snapshot_date = current_date()
            left join {self.raw}.collateral c on a.account_id = c.account_id
            where abs(s.lgd - case
                when a.account_type in ('MTG', 'AUTO', 'HELC') and c.collateral_value > 0
                    then greatest(0, least(1, (a.current_balance / c.collateral_value - 0.5) * 0.8))
                when a.account_type in ('MTG', 'AUTO', 'HELC') then 0.40
                when a.account_type = 'CC' then 0.75
                else 0.50 end) > 1e-9
               or abs(s.ead - case
                    when a.account_type in ('CC', 'LOC', 'HELC')
                        then a.current_balance + 0.50 * (a.credit_limit - a.current_balance)
                    else a.current_balance end) > 1e-9
            """
        )
        mismatches = mismatches or 0
        self.results.append(CheckResult(
            "risk_lgd_ead_parity", "PASS" if mismatches == 0 else "FAIL",
            f"mismatching LGD/EAD rows = {mismatches}",
            {"mismatches": mismatches},
        ))

    def check_risk_rating_parity(self):
        mismatches = self._scalar(
            f"""
            select count(*)
            from {self.marts}.mart_risk_scores
            where risk_rating <> case
                when pd is null or pd >= 0.30 then 7
                when pd < 0.005 then 1
                when pd < 0.01 then 2
                when pd < 0.03 then 3
                when pd < 0.07 then 4
                when pd < 0.15 then 5
                else 6 end
            """
        )
        mismatches = mismatches or 0
        self.results.append(CheckResult(
            "risk_rating_parity", "PASS" if mismatches == 0 else "FAIL",
            f"mismatching risk-rating rows = {mismatches}",
            {"mismatches": mismatches},
        ))

    def check_risk_migration_scope(self):
        expected = self._scalar(
            f"""
            with ranked as (
                select
                    s.account_id,
                    a.risk_rating,
                    case upper(a.risk_rating) when 'LOW' then 1 when 'MEDIUM' then 2
                        when 'HIGH' then 3 end as prev_rank,
                    case when s.risk_rating in (1, 2) then 1
                         when s.risk_rating in (3, 4, 5) then 2
                         when s.risk_rating in (6, 7) then 3 end as curr_rank
                from {self.marts}.mart_risk_scores s
                inner join {self.intermediate}.int_account_metrics a
                    on s.account_id = a.account_id
                   and a.snapshot_date = current_date()
            )
            select count(*) from ranked
            where risk_rating is null or prev_rank <> curr_rank
            """
        )
        actual = self._scalar(f"select count(*) from {self.marts}.mart_risk_migration")
        direction_mismatches = self._scalar(
            f"""
            with expected as (
                select
                    s.account_id,
                    case
                        when a.risk_rating is null then 'NEW'
                        when (case when s.risk_rating in (1, 2) then 1
                                   when s.risk_rating in (3, 4, 5) then 2
                                   else 3 end)
                           < (case upper(a.risk_rating) when 'LOW' then 1
                                   when 'MEDIUM' then 2 else 3 end) then 'UPGRADE'
                        else 'DOWNGRADE'
                    end as direction
                from {self.marts}.mart_risk_scores s
                inner join {self.intermediate}.int_account_metrics a
                    on s.account_id = a.account_id
                   and a.snapshot_date = current_date()
                where a.risk_rating is null
                   or upper(a.risk_rating) <> case
                        when s.risk_rating in (1, 2) then 'LOW'
                        when s.risk_rating in (3, 4, 5) then 'MEDIUM'
                        else 'HIGH' end
            )
            select count(*)
            from expected e
            inner join {self.marts}.mart_risk_migration m on e.account_id = m.account_id
            where e.direction <> m.migration_direction
            """
        )
        stable = self._scalar(
            f"select count(*) from {self.marts}.mart_risk_migration "
            "where migration_direction = 'STABLE'"
        )
        ok = expected == actual and direction_mismatches == 0 and stable == 0
        self.results.append(CheckResult(
            "risk_migration_scope", "PASS" if ok else "FAIL",
            f"expected migration rows = {expected}, actual = {actual}; "
            f"direction mismatches = {direction_mismatches}; STABLE rows = {stable}",
            {"expected": expected, "actual": actual,
             "direction_mismatches": direction_mismatches, "stable": stable},
        ))

    def check_risk_summary_totals(self):
        values = self._row(
            f"""
            select
                (select sum(n_accounts) from {self.marts}.mart_risk_summary),
                (select count(*) from {self.marts}.mart_risk_scores),
                (select sum(total_ead) from {self.marts}.mart_risk_summary),
                (select sum(ead) from {self.marts}.mart_risk_scores),
                (select sum(total_el) from {self.marts}.mart_risk_summary),
                (select sum(expected_loss) from {self.marts}.mart_risk_scores),
                (select count(*) from {self.marts}.mart_risk_summary),
                (select count(distinct concat(account_type, '|', cast(risk_rating as string)))
                 from {self.marts}.mart_risk_scores)
            """
        )
        if values is None:
            self.results.append(CheckResult(
                "risk_summary_totals",
                "FAIL",
                "summary totals query returned no row",
            ))
            return
        (summary_n, score_n, summary_ead, score_ead, summary_el, score_el,
         summary_groups, score_groups) = values
        ok = (
            summary_n is not None
            and score_n is not None
            and score_n is not None
            and summary_ead is not None
            and score_ead is not None
            and summary_el is not None
            and score_el is not None
            and summary_n == score_n
            and abs(summary_ead - score_ead) <= 0.01
            and abs(summary_el - score_el) <= 0.01
            and summary_groups == score_groups
        )
        self.results.append(CheckResult(
            "risk_summary_totals", "PASS" if ok else "FAIL",
            f"summary n = {summary_n}, scores n = {score_n}; "
            f"summary EAD = {summary_ead}, scores EAD = {score_ead}; "
            f"summary EL = {summary_el}, scores EL = {score_el}; "
            f"summary groups = {summary_groups}, score groups = {score_groups}",
            {"summary_n": summary_n, "score_n": score_n,
             "summary_ead": summary_ead, "score_ead": score_ead,
             "summary_el": summary_el, "score_el": score_el,
             "summary_groups": summary_groups, "score_groups": score_groups},
        ))

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_risk_score_completeness()
        self.check_risk_ead_control_total()
        self.check_risk_pd_parity()
        self.check_risk_lgd_ead_parity()
        self.check_risk_rating_parity()
        self.check_risk_migration_scope()
        self.check_risk_summary_totals()
        self.con.close()
        return all(r.status != "FAIL" for r in self.results)

    def render(self) -> str:
        width = max(len(r.name) for r in self.results) + 2
        icon = {"PASS": "PASS", "FAIL": "FAIL", "SKIP": "SKIP"}
        lines = [
            f"# Reconciliation Report — {self.catalog} / namespace `{self.ns}`",
            "",
            "Source -> target controls proving the converted marts match the legacy",
            "SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite",
            "(e.g. the PySpark curated outputs) has not been produced yet.",
            "",
            "| Control | Result | Detail |",
            "|---|---|---|",
        ]
        for r in self.results:
            lines.append(f"| `{r.name}` | {icon[r.status]} | {r.detail} |")
        passed = sum(r.status == "PASS" for r in self.results)
        failed = sum(r.status == "FAIL" for r in self.results)
        skipped = sum(r.status == "SKIP" for r in self.results)
        lines += ["", f"**{passed} passed, {failed} failed, {skipped} skipped**", ""]
        return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="SAS -> Databricks reconciliation report")
    ap.add_argument("--catalog", default="banking_analytics")
    ap.add_argument("--namespace", default=os.environ.get("DBT_SCHEMA", "dev"),
                    help="Output namespace prefix (default: $DBT_SCHEMA or 'dev')")
    ap.add_argument("--report", help="Optional path to write the markdown report")
    args = ap.parse_args()

    for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"):
        if not os.environ.get(var):
            print(f"ERROR: {var} is not set", file=sys.stderr)
            return 2

    rec = Reconciler(args.catalog, args.namespace)
    ok = rec.run()
    report = rec.render()
    print(report)
    if args.report:
        with open(args.report, "w") as fh:
            fh.write(report + "\n")
        print(f"(report written to {args.report})")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
