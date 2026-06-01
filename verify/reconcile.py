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
PySpark checks) — see docs/CONVERSION_PLAYBOOK.md for the reconciliation contract.

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

    def _rows(self, query: str) -> list:
        cur = self.con.cursor()
        try:
            cur.execute(query)
            return cur.fetchall()
        finally:
            cur.close()

    def _table_exists(self, fq_table: str) -> bool:
        try:
            self._scalar(f"SELECT 1 FROM {fq_table} LIMIT 1")
            return True
        except Exception:
            return False

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

    # ---------------------------------------------- claims reconciliation checks
    def check_claims_completeness(self):
        """stg_claims row count matches the valid-claims population from raw."""
        expected = self._scalar(
            f"""
            select count(*)
            from {self.raw}.claims c
            inner join {self.raw}.policies p on c.policy_id = p.policy_id
            where p.policy_status = 'ACTIVE'
              and c.loss_date >= p.effective_date
              and c.loss_date <= p.expiry_date
              and c.claimed_amount <= p.sum_insured
            """
        )
        if not self._table_exists(f"{self.staging}.stg_claims"):
            self.results.append(
                CheckResult("claims_completeness", "SKIP",
                            "stg_claims table not found")
            )
            return
        actual = self._scalar(f"select count(*) from {self.staging}.stg_claims")
        ok = expected == actual
        self.results.append(
            CheckResult(
                "claims_completeness",
                "PASS" if ok else "FAIL",
                f"expected valid claims = {expected}, model claims = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_claims_control_total(self):
        """Total claimed_amount in adjudicated must tie to stg_claims."""
        if not self._table_exists(f"{self.staging}.stg_claims"):
            self.results.append(
                CheckResult("claims_control_total", "SKIP",
                            "stg_claims table not found")
            )
            return
        if not self._table_exists(f"{self.intermediate}.int_claims_adjudication"):
            self.results.append(
                CheckResult("claims_control_total", "SKIP",
                            "int_claims_adjudication table not found")
            )
            return
        stg_total = self._scalar(
            f"select round(sum(claimed_amount), 2) from {self.staging}.stg_claims"
        )
        adj_total = self._scalar(
            f"select round(sum(claimed_amount), 2) from {self.intermediate}.int_claims_adjudication"
        )
        ok = stg_total == adj_total
        self.results.append(
            CheckResult(
                "claims_control_total",
                "PASS" if ok else "FAIL",
                f"staging total = {stg_total}, adjudicated total = {adj_total}",
                {"staging": stg_total, "adjudicated": adj_total},
            )
        )

    def check_claims_adjudication_parity(self):
        """Adjudication result distribution matches SAS routing rules."""
        if not self._table_exists(f"{self.intermediate}.int_claims_adjudication"):
            self.results.append(
                CheckResult("claims_adjudication_parity", "SKIP",
                            "int_claims_adjudication table not found")
            )
            return
        rows = self._rows(
            f"""
            select adjudication_result, count(*) as n
            from {self.intermediate}.int_claims_adjudication
            group by adjudication_result
            order by adjudication_result
            """
        )
        dist = {r[0]: r[1] for r in rows}
        total = sum(dist.values())
        # Re-derive expected from raw using the same SAS rules
        expected_rows = self._rows(
            f"""
            with base as (
                select
                    c.claim_id,
                    c.claimed_amount,
                    p.policy_type,
                    p.sum_insured,
                    case
                        when coalesce(f.fraud_score, 0) >= 80 then 'HIGH'
                        when coalesce(f.fraud_score, 0) >= 50 then 'MEDIUM'
                        else 'LOW'
                    end as fraud_risk
                from {self.raw}.claims c
                inner join {self.raw}.policies p on c.policy_id = p.policy_id
                left join {self.raw}.fraud_indicators f on c.claim_id = f.claim_id
                where p.policy_status = 'ACTIVE'
                  and c.loss_date >= p.effective_date
                  and c.loss_date <= p.expiry_date
                  and c.claimed_amount <= p.sum_insured
            )
            select
                case
                    when fraud_risk = 'HIGH' then 'DENY'
                    when fraud_risk = 'LOW' and claimed_amount <= 5000
                         and policy_type in ('AUTO','HOME','RENT') then 'APPR'
                    when fraud_risk = 'LOW' and claimed_amount <= sum_insured * 0.25
                         and claimed_amount <= 50000 then 'APPR'
                    else 'PEND'
                end as expected_result,
                count(*) as n
            from base
            group by 1
            order by 1
            """
        )
        expected_dist = {r[0]: r[1] for r in expected_rows}
        ok = dist == expected_dist
        detail_parts = []
        for k in sorted(set(list(dist.keys()) + list(expected_dist.keys()))):
            detail_parts.append(f"{k}: actual={dist.get(k, 0)}, expected={expected_dist.get(k, 0)}")
        self.results.append(
            CheckResult(
                "claims_adjudication_parity",
                "PASS" if ok else "FAIL",
                f"total={total}; " + "; ".join(detail_parts),
                {"actual": dist, "expected": expected_dist},
            )
        )

    def check_claims_cross_engine(self):
        """PySpark curated outputs are referentially consistent with dbt models."""
        curated_table = f"{self.curated}.claims_register"
        if not self._table_exists(curated_table):
            self.results.append(
                CheckResult("claims_cross_engine", "SKIP",
                            "curated claims_register not found (run PySpark job first)")
            )
            return
        if not self._table_exists(f"{self.intermediate}.int_claims_adjudication"):
            self.results.append(
                CheckResult("claims_cross_engine", "SKIP",
                            "int_claims_adjudication not found")
            )
            return

        # Check 1: curated row count matches dbt
        dbt_count = self._scalar(
            f"select count(*) from {self.intermediate}.int_claims_adjudication"
        )
        curated_count = self._scalar(
            f"select count(*) from {curated_table}"
        )
        count_ok = dbt_count == curated_count

        # Check 2: every curated claim_id exists in dbt
        orphans = self._scalar(
            f"""
            select count(*)
            from {curated_table} cr
            left join {self.intermediate}.int_claims_adjudication adj
                on cr.claim_id = adj.claim_id
            where adj.claim_id is null
            """
        )
        ref_ok = orphans == 0

        # Check 3: adjudication results match between engines
        mismatches = self._scalar(
            f"""
            select count(*)
            from {curated_table} cr
            inner join {self.intermediate}.int_claims_adjudication adj
                on cr.claim_id = adj.claim_id
            where cr.adjudication_result <> adj.adjudication_result
            """
        )
        result_ok = mismatches == 0

        ok = count_ok and ref_ok and result_ok
        detail = (
            f"dbt rows={dbt_count}, curated rows={curated_count}, "
            f"orphans={orphans}, result mismatches={mismatches}"
        )
        self.results.append(
            CheckResult(
                "claims_cross_engine",
                "PASS" if ok else "FAIL",
                detail,
                {"dbt_count": dbt_count, "curated_count": curated_count,
                 "orphans": orphans, "mismatches": mismatches},
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_claims_completeness()
        self.check_claims_control_total()
        self.check_claims_adjudication_parity()
        self.check_claims_cross_engine()
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
