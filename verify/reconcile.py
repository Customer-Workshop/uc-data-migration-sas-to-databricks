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

    def _append_count_check(self, name: str, query: str, detail: str):
        mismatches = self._scalar(query)
        ok = mismatches == 0
        self.results.append(
            CheckResult(
                name,
                "PASS" if ok else "FAIL",
                detail.format(mismatches=mismatches),
                {"mismatches": mismatches},
            )
        )

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

    def check_rwa_completeness(self):
        self._append_count_check(
            "rwa_completeness",
            f"""
            with expected as (
                select count(*) as n
                from {self.intermediate}.int_account_metrics
            ),
            actual as (
                select sum(n_accounts) as n
                from {self.marts}.mart_regulatory_rwa
            )
            select case when e.n = a.n then 0 else 1 end
            from expected e cross join actual a
            """,
            "RWA population mismatches = {mismatches}",
        )

    def check_rwa_control_total(self):
        self._append_count_check(
            "rwa_control_total",
            f"""
            with account_ltv as (
                select
                    a.current_balance,
                    a.account_type,
                    case
                        when c.collateral_value > 0
                        then a.current_balance / c.collateral_value
                        else null
                    end as ltv
                from {self.intermediate}.int_account_metrics a
                left join {self.raw}.collateral c
                    on a.account_id = c.account_id
            ),
            expected as (
                select
                    sum(current_balance) as total_exposure,
                    sum(
                        current_balance
                        * case
                            when account_type in ('CHK', 'SAV', 'MMA') then 0.00
                            when account_type = 'CD' then 0.00
                            when account_type = 'MTG' and (ltv <= 0.80 or ltv is null) then 0.35
                            when account_type = 'MTG' and ltv > 0.80 then 0.50
                            when account_type = 'HELC' then 0.50
                            when account_type in ('AUTO', 'PERS') then 0.75
                            when account_type = 'CC' then 0.75
                            when account_type = 'LOC' then 1.00
                            else 1.00
                        end
                    ) as rwa
                from account_ltv
            ),
            actual as (
                select sum(total_exposure) as total_exposure, sum(rwa) as rwa
                from {self.marts}.mart_regulatory_rwa
            )
            select case
                when abs(coalesce(e.total_exposure, 0) - coalesce(a.total_exposure, 0)) <= 0.01
                 and abs(coalesce(e.rwa, 0) - coalesce(a.rwa, 0)) <= 0.01
                then 0 else 1 end
            from expected e cross join actual a
            """,
            "RWA control-total mismatches = {mismatches}",
        )

    def check_rwa_risk_weight_parity(self):
        self._append_count_check(
            "rwa_risk_weight_parity",
            f"""
            with account_ltv as (
                select
                    a.account_type,
                    a.customer_segment,
                    case
                        when c.collateral_value > 0
                        then a.current_balance / c.collateral_value
                        else null
                    end as ltv
                from {self.intermediate}.int_account_metrics a
                left join {self.raw}.collateral c
                    on a.account_id = c.account_id
            ),
            expected as (
                select distinct
                    account_type,
                    customer_segment,
                    case
                        when account_type in ('CHK', 'SAV', 'MMA') then 0.00
                        when account_type = 'CD' then 0.00
                        when account_type = 'MTG' and (ltv <= 0.80 or ltv is null) then 0.35
                        when account_type = 'MTG' and ltv > 0.80 then 0.50
                        when account_type = 'HELC' then 0.50
                        when account_type in ('AUTO', 'PERS') then 0.75
                        when account_type = 'CC' then 0.75
                        when account_type = 'LOC' then 1.00
                        else 1.00
                    end as risk_weight
                from account_ltv
            ),
            actual as (
                select distinct account_type, customer_segment, risk_weight
                from {self.marts}.mart_regulatory_rwa
            )
            select count(*)
            from expected e
            full outer join actual a
                on e.account_type = a.account_type
                and e.customer_segment = a.customer_segment
                and e.risk_weight = a.risk_weight
            where e.account_type is null or a.account_type is null
            """,
            "RWA risk-weight group mismatches = {mismatches}",
        )

    def check_delinquency_completeness(self):
        self._append_count_check(
            "delinquency_completeness",
            f"""
            with expected as (
                select count(*) as n
                from {self.intermediate}.int_account_metrics
                where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
            ),
            actual as (
                select sum(n_accounts) as n
                from {self.marts}.mart_delinquency_aging
            )
            select case when e.n = a.n then 0 else 1 end
            from expected e cross join actual a
            """,
            "Delinquency population mismatches = {mismatches}",
        )

    def check_delinquency_control_total(self):
        self._append_count_check(
            "delinquency_control_total",
            f"""
            with expected as (
                select sum(current_balance) as total_balance
                from {self.intermediate}.int_account_metrics
                where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
            ),
            actual as (
                select sum(total_balance) as total_balance
                from {self.marts}.mart_delinquency_aging
            )
            select case
                when abs(coalesce(e.total_balance, 0) - coalesce(a.total_balance, 0)) <= 0.01
                then 0 else 1 end
            from expected e cross join actual a
            """,
            "Delinquency balance mismatches = {mismatches}",
        )

    def check_delinquency_bucket_parity(self):
        self._append_count_check(
            "delinquency_bucket_parity",
            f"""
            with expected_accounts as (
                select
                    m.report_month,
                    a.account_type,
                    a.region_code,
                    case
                        when p.max_days_past_due_ever = 0 then 'Current'
                        when p.max_days_past_due_ever between 1 and 29 then '1-29'
                        when p.max_days_past_due_ever between 30 and 59 then '30-59'
                        when p.max_days_past_due_ever between 60 and 89 then '60-89'
                        when p.max_days_past_due_ever between 90 and 119 then '90-119'
                        when p.max_days_past_due_ever between 120 and 179 then '120-179'
                        when p.max_days_past_due_ever >= 180 then '180+'
                        else 'Unknown'
                    end as delinq_bucket
                from {self.intermediate}.int_account_metrics a
                cross join (
                    select distinct report_month
                    from {self.marts}.mart_delinquency_aging
                ) m
                left join {self.raw}.payment_history p
                    on a.account_id = p.account_id
                where a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
            ),
            expected as (
                select report_month, account_type, region_code, delinq_bucket, count(*) as n_accounts
                from expected_accounts
                group by report_month, account_type, region_code, delinq_bucket
            ),
            actual as (
                select report_month, account_type, region_code, delinq_bucket, n_accounts
                from {self.marts}.mart_delinquency_aging
            )
            select count(*)
            from expected e
            full outer join actual a
                on e.report_month = a.report_month
                and e.account_type = a.account_type
                and e.region_code = a.region_code
                and e.delinq_bucket = a.delinq_bucket
            where e.n_accounts is null or a.n_accounts is null or e.n_accounts <> a.n_accounts
            """,
            "Delinquency bucket mismatches = {mismatches}",
        )

    def check_capital_adequacy_tieout(self):
        self._append_count_check(
            "capital_adequacy_tieout",
            f"""
            with expected as (
                select report_month, sum(rwa) as total_rwa
                from {self.marts}.mart_regulatory_rwa
                group by report_month
            ),
            calculated as (
                select
                    report_month,
                    total_rwa,
                    case when total_rwa > 0 then 50000000 / total_rwa * 100 else null end as cet1_ratio,
                    case when total_rwa > 0 then 65000000 / total_rwa * 100 else null end as tier1_ratio,
                    case when total_rwa > 0 then 80000000 / total_rwa * 100 else null end as total_capital_ratio,
                    case
                        when total_rwa = 0 then 'PASS'
                        when 50000000 / total_rwa * 100 >= 4.5 then 'PASS'
                        else 'FAIL'
                    end as cet1_status,
                    case
                        when total_rwa = 0 then 'PASS'
                        when 65000000 / total_rwa * 100 >= 6.0 then 'PASS'
                        else 'FAIL'
                    end as tier1_status,
                    case
                        when total_rwa = 0 then 'PASS'
                        when 80000000 / total_rwa * 100 >= 8.0 then 'PASS'
                        else 'FAIL'
                    end as total_capital_status
                from expected
            )
            select count(*)
            from {self.marts}.mart_capital_adequacy a
            full outer join calculated e
                on a.report_month = e.report_month
            where a.report_month is null
               or e.report_month is null
               or abs(coalesce(a.total_rwa, 0) - coalesce(e.total_rwa, 0)) > 0.01
               or not (a.cet1_ratio <=> e.cet1_ratio)
               or not (a.tier1_ratio <=> e.tier1_ratio)
               or not (a.total_capital_ratio <=> e.total_capital_ratio)
               or a.cet1_status <> e.cet1_status
               or a.tier1_status <> e.tier1_status
               or a.total_capital_status <> e.total_capital_status
            """,
            "Capital adequacy mismatches = {mismatches}",
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_rwa_completeness()
        self.check_rwa_control_total()
        self.check_rwa_risk_weight_parity()
        self.check_delinquency_completeness()
        self.check_delinquency_control_total()
        self.check_delinquency_bucket_parity()
        self.check_capital_adequacy_tieout()
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
