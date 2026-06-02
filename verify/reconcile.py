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


# SAS source-of-truth Basel III risk-weight CASE from
# monthly_regulatory_reporting.sas (Step 1), reproduced value-for-value.
# LTV is derived from collateral (migrated raw schema has no LTV column).
# `a` is the account alias, `c` the collateral alias.
RISK_WEIGHT_CASE = """
        case
            when a.account_type in ('CHK','SAV','MMA') then 0.00
            when a.account_type = 'CD' then 0.00
            when a.account_type = 'MTG'
                and (case when c.collateral_value > 0 then a.current_balance / c.collateral_value end) <= 0.80
                then 0.35
            when a.account_type = 'MTG'
                and (case when c.collateral_value > 0 then a.current_balance / c.collateral_value end) > 0.80
                then 0.50
            when a.account_type = 'HELC' then 0.50
            when a.account_type in ('AUTO','PERS') then 0.75
            when a.account_type = 'CC' then 0.75
            when a.account_type = 'LOC' then 1.00
            else 1.00
        end
"""

# SAS delinquency aging-bucket CASE from Step 2 (days-past-due measure is
# payment_history.max_days_past_due_ever in the migrated raw schema).
DELINQ_BUCKET_CASE = """
        case
            when p.max_days_past_due_ever = 0 then 'Current'
            when p.max_days_past_due_ever between 1 and 29 then '1-29'
            when p.max_days_past_due_ever between 30 and 59 then '30-59'
            when p.max_days_past_due_ever between 60 and 89 then '60-89'
            when p.max_days_past_due_ever between 90 and 119 then '90-119'
            when p.max_days_past_due_ever between 120 and 179 then '120-179'
            when p.max_days_past_due_ever >= 180 then '180+'
            else 'Unknown'
        end
"""

LENDING_TYPES = "('MTG','AUTO','PERS','CC','LOC','HELC')"


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

    def _rows(self, query: str):
        cur = self.con.cursor()
        try:
            cur.execute(query)
            return cur.fetchall()
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

    def check_rwa_completeness(self):
        """Sum of RWA group N_ACCOUNTS must equal the in-scope account population."""
        expected = self._scalar(f"select count(*) from {self.intermediate}.int_account_metrics")
        actual = self._scalar(
            f"select coalesce(sum(n_accounts), 0) from {self.marts}.mart_regulatory_rwa"
        )
        ok = expected == actual
        self.results.append(
            CheckResult(
                "rwa_completeness",
                "PASS" if ok else "FAIL",
                f"in-scope accounts = {expected}, RWA model accounts = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_rwa_control_total(self):
        """Total exposure and total RWA must tie out to the re-derived source totals."""
        exp = self._rows(
            f"""
            select round(sum(a.current_balance), 2),
                   round(sum(a.current_balance * ({RISK_WEIGHT_CASE})), 2)
            from {self.intermediate}.int_account_metrics a
            left join {self.raw}.collateral c on a.account_id = c.account_id
            """
        )[0]
        act = self._rows(
            f"""
            select round(sum(total_exposure), 2), round(sum(rwa), 2)
            from {self.marts}.mart_regulatory_rwa
            """
        )[0]
        exp_exposure, exp_rwa = float(exp[0]), float(exp[1])
        act_exposure, act_rwa = float(act[0]), float(act[1])
        ok = exp_exposure == act_exposure and exp_rwa == act_rwa
        self.results.append(
            CheckResult(
                "rwa_control_total",
                "PASS" if ok else "FAIL",
                (
                    f"exposure src={exp_exposure:,.2f} mart={act_exposure:,.2f}; "
                    f"RWA src={exp_rwa:,.2f} mart={act_rwa:,.2f}"
                ),
                {"expected_rwa": exp_rwa, "actual_rwa": act_rwa},
            )
        )

    def check_rwa_parity(self):
        """Every account_type -> risk_weight must match the SAS mapping value-for-value."""
        mismatches = self._rows(
            f"""
            with expected as (
                select a.account_type,
                       cast(({RISK_WEIGHT_CASE}) as decimal(5,2)) as risk_weight,
                       count(*) as n
                from {self.intermediate}.int_account_metrics a
                left join {self.raw}.collateral c on a.account_id = c.account_id
                group by a.account_type, 2
            ),
            actual as (
                select account_type, cast(risk_weight as decimal(5,2)) as risk_weight,
                       sum(n_accounts) as n
                from {self.marts}.mart_regulatory_rwa
                group by account_type, cast(risk_weight as decimal(5,2))
            )
            select coalesce(e.account_type, a.account_type) as account_type,
                   e.risk_weight as expected_weight, a.risk_weight as actual_weight,
                   coalesce(e.n, 0) as expected_n, coalesce(a.n, 0) as actual_n
            from expected e
            full outer join actual a
                on e.account_type = a.account_type and e.risk_weight = a.risk_weight
            where coalesce(e.n, 0) <> coalesce(a.n, 0)
            order by account_type
            """
        )
        loc_weight = self._scalar(
            f"""
            select cast(max(risk_weight) as decimal(5,2))
            from {self.marts}.mart_regulatory_rwa where account_type = 'LOC'
            """
        )
        ok = len(mismatches) == 0
        if ok:
            detail = f"all account_type -> risk_weight match SAS source (LOC -> {loc_weight}, expected 1.00)"
        else:
            shown = "; ".join(
                f"{r[0]} expected {r[1]} else not {r[2]}" for r in mismatches[:6]
            )
            detail = f"{len(mismatches)} mapping mismatch(es): {shown}"
        self.results.append(
            CheckResult(
                "rwa_risk_weight_parity",
                "PASS" if ok else "FAIL",
                detail,
                {"mismatches": len(mismatches), "loc_weight": str(loc_weight)},
            )
        )

    def check_delinquency_completeness(self):
        """Sum of aging-bucket N_ACCOUNTS must equal the in-scope lending population."""
        expected = self._scalar(
            f"""
            select count(*) from {self.intermediate}.int_account_metrics
            where account_type in {LENDING_TYPES}
            """
        )
        actual = self._scalar(
            f"select coalesce(sum(n_accounts), 0) from {self.marts}.mart_delinquency_aging"
        )
        ok = expected == actual
        self.results.append(
            CheckResult(
                "delinquency_completeness",
                "PASS" if ok else "FAIL",
                f"in-scope lending accounts = {expected}, aging model accounts = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_delinquency_parity(self):
        """Every (account_type, region, bucket) count must match the SAS bucket logic."""
        mismatches = self._rows(
            f"""
            with expected as (
                select a.account_type, a.region_code,
                       ({DELINQ_BUCKET_CASE}) as delinq_bucket, count(*) as n
                from {self.intermediate}.int_account_metrics a
                left join {self.raw}.payment_history p on a.account_id = p.account_id
                where a.account_type in {LENDING_TYPES}
                group by a.account_type, a.region_code, 3
            ),
            actual as (
                select account_type, region_code, delinq_bucket, sum(n_accounts) as n
                from {self.marts}.mart_delinquency_aging
                group by account_type, region_code, delinq_bucket
            )
            select coalesce(e.account_type, a.account_type) as account_type,
                   coalesce(e.delinq_bucket, a.delinq_bucket) as delinq_bucket,
                   coalesce(e.n, 0) as expected_n, coalesce(a.n, 0) as actual_n
            from expected e
            full outer join actual a
                on e.account_type = a.account_type
                and e.region_code = a.region_code
                and e.delinq_bucket = a.delinq_bucket
            where coalesce(e.n, 0) <> coalesce(a.n, 0)
            """
        )
        ok = len(mismatches) == 0
        detail = (
            "all aging-bucket assignments match SAS source"
            if ok
            else f"{len(mismatches)} bucket mismatch(es) vs SAS source"
        )
        self.results.append(
            CheckResult(
                "delinquency_bucket_parity",
                "PASS" if ok else "FAIL",
                detail,
                {"mismatches": len(mismatches)},
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_rwa_completeness()
        self.check_rwa_control_total()
        self.check_rwa_parity()
        self.check_delinquency_completeness()
        self.check_delinquency_parity()
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
