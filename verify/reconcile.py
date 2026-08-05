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

    def _rows(self, query: str) -> list:
        cur = self.con.cursor()
        try:
            cur.execute(query)
            return cur.fetchall()
        finally:
            cur.close()

    # -- monthly_regulatory_reporting.sas controls ----------------------------
    IN_SCOPE_FILTER = """
        from {raw}.cust_accounts a
        inner join {raw}.cust_demographics d on a.customer_id = d.customer_id
        where a.account_status not in ('W', 'C')
          and a.open_date <= current_date()
    """

    def check_rwa_completeness(self):
        """Every in-scope account is represented once in the RWA mart."""
        expected = self._scalar(
            "select count(*)" + self.IN_SCOPE_FILTER.format(raw=self.raw)
        )
        actual = self._scalar(f"select sum(n_accounts) from {self.marts}.mart_regulatory_rwa")
        ok = expected == actual
        self.results.append(
            CheckResult(
                "rwa_completeness",
                "PASS" if ok else "FAIL",
                f"in-scope raw accounts = {expected}, accounts behind the RWA mart = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_rwa_control_total(self):
        """Total exposure ties to the source balances; RWA ties to exposure x weight."""
        expected = self._scalar(
            "select sum(a.current_balance)" + self.IN_SCOPE_FILTER.format(raw=self.raw)
        )
        actual, rwa, recomputed_rwa = self._rows(
            f"""
            select sum(total_exposure), sum(rwa), sum(total_exposure * risk_weight)
            from {self.marts}.mart_regulatory_rwa
            """
        )[0]
        ok = abs(float(actual) - float(expected)) <= 0.01 and \
            abs(float(rwa) - float(recomputed_rwa)) <= 0.01
        self.results.append(
            CheckResult(
                "rwa_control_total",
                "PASS" if ok else "FAIL",
                f"source exposure = {float(expected):,.2f}, mart exposure = {float(actual):,.2f}, "
                f"RWA = {float(rwa):,.2f}",
                {"expected_exposure": float(expected), "actual_exposure": float(actual),
                 "rwa": float(rwa)},
            )
        )

    # Risk weights transcribed from monthly_regulatory_reporting.sas Step 1.
    # LOC is an explicit 1.00 branch in the source, IRA reaches 1.00 via the
    # catch-all `else`; MTG is LTV-dependent and checked separately.
    SAS_RISK_WEIGHTS = {
        "CHK": 0.00, "SAV": 0.00, "MMA": 0.00, "CD": 0.00,
        "HELC": 0.50, "AUTO": 0.75, "PERS": 0.75, "CC": 0.75,
        "LOC": 1.00, "IRA": 1.00,
    }

    def check_rwa_risk_weight_parity(self):
        """Each fixed-weight CASE branch matches the SAS mapping value-for-value."""
        rows = self._rows(
            f"""
            select distinct account_type, risk_weight
            from {self.marts}.mart_regulatory_rwa
            where account_type <> 'MTG'
            order by account_type
            """
        )
        mismatches = [
            f"{t} actual {float(w):.2f}, expected {self.SAS_RISK_WEIGHTS[t]:.2f}"
            for t, w in rows
            if t in self.SAS_RISK_WEIGHTS
            and abs(float(w) - self.SAS_RISK_WEIGHTS[t]) > 1e-9
        ]
        unmapped = sorted({t for t, _ in rows if t not in self.SAS_RISK_WEIGHTS})
        detail = "; ".join(mismatches) if mismatches else (
            f"{len(rows)} account-type weights match the SAS mapping"
        )
        if unmapped:
            detail += f" (account types absent from the SAS mapping: {', '.join(unmapped)})"
        self.results.append(
            CheckResult(
                "rwa_risk_weight_parity",
                "FAIL" if mismatches else "PASS",
                detail,
                {"mismatches": mismatches},
            )
        )

    def check_rwa_mtg_ltv_parity(self):
        """Mortgage weights follow the source LTV bands (missing LTV -> 0.35, as in SAS)."""
        expected = dict(self._rows(
            f"""
            select
                case when l.ltv is null or l.ltv <= 0.80 then 0.35 else 0.50 end as w,
                count(*)
            from {self.raw}.cust_accounts a
            inner join {self.raw}.cust_demographics d on a.customer_id = d.customer_id
            left join {self.raw}.loan_details l on a.account_id = l.account_id
            where a.account_status not in ('W', 'C')
              and a.open_date <= current_date()
              and a.account_type = 'MTG'
            group by 1
            """
        ))
        actual = dict(self._rows(
            f"""
            select risk_weight, sum(n_accounts)
            from {self.marts}.mart_regulatory_rwa
            where account_type = 'MTG'
            group by 1
            """
        ))
        exp = {round(float(k), 2): int(v) for k, v in expected.items()}
        act = {round(float(k), 2): int(v) for k, v in actual.items()}
        ok = exp == act
        self.results.append(
            CheckResult(
                "rwa_mtg_ltv_parity",
                "PASS" if ok else "FAIL",
                f"source LTV bands = {exp}, mart = {act}",
                {"expected": exp, "actual": act},
            )
        )

    def check_delinquency_completeness(self):
        """Every in-scope lending account lands in exactly one aging bucket."""
        expected = self._scalar(
            "select count(*)" + self.IN_SCOPE_FILTER.format(raw=self.raw)
            + " and a.account_type in ('MTG','AUTO','PERS','CC','LOC','HELC')"
        )
        actual = self._scalar(f"select sum(n_accounts) from {self.marts}.mart_delinquency_aging")
        ok = expected == actual
        self.results.append(
            CheckResult(
                "delinquency_completeness",
                "PASS" if ok else "FAIL",
                f"in-scope lending accounts = {expected}, accounts in aging buckets = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_delinquency_bucket_parity(self):
        """Bucket populations match the SAS DAYS_PAST_DUE bands, bucket by bucket."""
        bucket_case = """
            case
                when l.days_past_due = 0 then 'Current'
                when l.days_past_due between 1 and 29 then '1-29'
                when l.days_past_due between 30 and 59 then '30-59'
                when l.days_past_due between 60 and 89 then '60-89'
                when l.days_past_due between 90 and 119 then '90-119'
                when l.days_past_due between 120 and 179 then '120-179'
                when l.days_past_due >= 180 then '180+'
                else 'Unknown'
            end
        """
        expected = dict(self._rows(
            f"""
            select {bucket_case} as bucket, count(*)
            from {self.raw}.cust_accounts a
            inner join {self.raw}.cust_demographics d on a.customer_id = d.customer_id
            left join {self.raw}.loan_details l on a.account_id = l.account_id
            where a.account_status not in ('W', 'C')
              and a.open_date <= current_date()
              and a.account_type in ('MTG','AUTO','PERS','CC','LOC','HELC')
            group by 1
            """
        ))
        actual = dict(self._rows(
            f"""
            select delinq_bucket, sum(n_accounts)
            from {self.marts}.mart_delinquency_aging
            group by 1
            """
        ))
        exp = {k: int(v) for k, v in expected.items()}
        act = {k: int(v) for k, v in actual.items()}
        diffs = [
            f"{b}: source {exp.get(b, 0)} vs mart {act.get(b, 0)}"
            for b in sorted(set(exp) | set(act))
            if exp.get(b, 0) != act.get(b, 0)
        ]
        self.results.append(
            CheckResult(
                "delinquency_bucket_parity",
                "FAIL" if diffs else "PASS",
                "; ".join(diffs) if diffs
                else f"{len(act)} buckets match the SAS DAYS_PAST_DUE bands",
                {"expected": exp, "actual": act},
            )
        )

    def check_delinquency_control_total(self):
        """Balance and past-due totals tie back to the source."""
        exp_balance, exp_past_due = self._rows(
            f"""
            select sum(a.current_balance), sum(l.past_due_amount)
            from {self.raw}.cust_accounts a
            inner join {self.raw}.cust_demographics d on a.customer_id = d.customer_id
            left join {self.raw}.loan_details l on a.account_id = l.account_id
            where a.account_status not in ('W', 'C')
              and a.open_date <= current_date()
              and a.account_type in ('MTG','AUTO','PERS','CC','LOC','HELC')
            """
        )[0]
        act_balance, act_past_due = self._rows(
            f"""
            select sum(total_balance), sum(total_past_due)
            from {self.marts}.mart_delinquency_aging
            """
        )[0]
        ok = abs(float(act_balance) - float(exp_balance)) <= 0.01 and \
            abs(float(act_past_due) - float(exp_past_due)) <= 0.01
        self.results.append(
            CheckResult(
                "delinquency_control_total",
                "PASS" if ok else "FAIL",
                f"balance source = {float(exp_balance):,.2f} / mart = {float(act_balance):,.2f}, "
                f"past due source = {float(exp_past_due):,.2f} / "
                f"mart = {float(act_past_due):,.2f}",
                {"expected_balance": float(exp_balance), "actual_balance": float(act_balance)},
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_rwa_completeness()
        self.check_rwa_control_total()
        self.check_rwa_risk_weight_parity()
        self.check_rwa_mtg_ltv_parity()
        self.check_delinquency_completeness()
        self.check_delinquency_control_total()
        self.check_delinquency_bucket_parity()
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
