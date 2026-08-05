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

    def check_claims_completeness(self):
        """Claims register equals the independently derived valid population."""
        expected = self._scalar(
            f"""
            select count(*)
            from {self.raw}.claims c
            inner join {self.raw}.policies p
                on c.policy_id = p.policy_id
               and p.policy_status = 'ACTIVE'
            where c.loss_date is not null
              and c.loss_date >= p.effective_date
              and c.loss_date <= p.expiry_date
              and (c.claimed_amount is null or c.claimed_amount <= p.sum_insured)
            """
        )
        actual = self._scalar(f"select count(*) from {self.marts}.mart_claims_register")
        duplicate_claims = self._scalar(
            f"""
            select count(*)
            from (
                select claim_id
                from {self.marts}.mart_claims_register
                group by claim_id
                having count(*) > 1
            )
            """
        )
        ok = expected == actual and duplicate_claims == 0
        self.results.append(
            CheckResult(
                "claims_completeness",
                "PASS" if ok else "FAIL",
                f"in-scope raw claims = {expected}, register claims = {actual}, "
                f"duplicate claim ids = {duplicate_claims}",
                {
                    "expected": expected,
                    "actual": actual,
                    "duplicate_claims": duplicate_claims,
                },
            )
        )

    def check_claims_control_total(self):
        """Claimed and SAS-approved amount totals must tie to raw inputs."""
        expected_claimed = self._scalar(
            f"""
            select sum(c.claimed_amount)
            from {self.raw}.claims c
            inner join {self.raw}.policies p
                on c.policy_id = p.policy_id
               and p.policy_status = 'ACTIVE'
            where c.loss_date is not null
              and c.loss_date >= p.effective_date
              and c.loss_date <= p.expiry_date
              and (c.claimed_amount is null or c.claimed_amount <= p.sum_insured)
            """
        )
        expected_approved = self._scalar(
            f"""
            select sum(
                case
                    when f.fraud_score >= 80 then 0
                    when (f.fraud_score is null or f.fraud_score < 50)
                         and (c.claimed_amount is null or c.claimed_amount <= 5000)
                         and p.policy_type in ('AUTO', 'HOME', 'RENT')
                        then greatest(0, coalesce(c.claimed_amount - p.deductible, 0))
                    when (f.fraud_score is null or f.fraud_score < 50)
                         and (c.claimed_amount is null or c.claimed_amount <= p.sum_insured * 0.25)
                         and (c.claimed_amount is null or c.claimed_amount <= 50000)
                        then greatest(0, coalesce(c.claimed_amount - p.deductible, 0))
                    else null
                end
            )
            from {self.raw}.claims c
            inner join {self.raw}.policies p
                on c.policy_id = p.policy_id
               and p.policy_status = 'ACTIVE'
            left join {self.raw}.fraud_indicators f
                on c.claim_id = f.claim_id
            where c.loss_date is not null
              and c.loss_date >= p.effective_date
              and c.loss_date <= p.expiry_date
              and (c.claimed_amount is null or c.claimed_amount <= p.sum_insured)
            """
        )
        actual_claimed = self._scalar(
            f"select sum(claimed_amount) from {self.marts}.mart_claims_register"
        )
        actual_approved = self._scalar(
            f"select sum(approved_amount) from {self.marts}.mart_claims_register"
        )
        ok = expected_claimed == actual_claimed and expected_approved == actual_approved
        self.results.append(
            CheckResult(
                "claims_control_total",
                "PASS" if ok else "FAIL",
                f"claimed raw/model = {expected_claimed}/{actual_claimed}, "
                f"approved raw/model = {expected_approved}/{actual_approved}",
                {
                    "expected_claimed": expected_claimed,
                    "actual_claimed": actual_claimed,
                    "expected_approved": expected_approved,
                    "actual_approved": actual_approved,
                },
            )
        )

    def check_claims_adjudication_parity(self):
        """Every register row must match the independently derived SAS branch."""
        mismatches = self._scalar(
            f"""
            with expected as (
                select
                    c.claim_id,
                    case
                        when f.fraud_score >= 80 then 'DENY'
                        when (f.fraud_score is null or f.fraud_score < 50)
                             and (c.claimed_amount is null or c.claimed_amount <= 5000)
                             and p.policy_type in ('AUTO', 'HOME', 'RENT')
                            then 'APPR'
                        when (f.fraud_score is null or f.fraud_score < 50)
                             and (c.claimed_amount is null or c.claimed_amount <= p.sum_insured * 0.25)
                             and (c.claimed_amount is null or c.claimed_amount <= 50000)
                            then 'APPR'
                        else 'PEND'
                    end as adjudication_result,
                    case
                        when f.fraud_score >= 80 then 0
                        when (f.fraud_score is null or f.fraud_score < 50)
                             and (c.claimed_amount is null or c.claimed_amount <= 5000)
                             and p.policy_type in ('AUTO', 'HOME', 'RENT')
                            then greatest(0, coalesce(c.claimed_amount - p.deductible, 0))
                        when (f.fraud_score is null or f.fraud_score < 50)
                             and (c.claimed_amount is null or c.claimed_amount <= p.sum_insured * 0.25)
                             and (c.claimed_amount is null or c.claimed_amount <= 50000)
                            then greatest(0, coalesce(c.claimed_amount - p.deductible, 0))
                        else null
                    end as approved_amount,
                    case
                        when f.fraud_score >= 80 then 'MANUAL_REVIEW'
                        when (f.fraud_score is null or f.fraud_score < 50)
                             and (c.claimed_amount is null or c.claimed_amount <= 5000)
                             and p.policy_type in ('AUTO', 'HOME', 'RENT')
                            then 'AUTO_ADJUDICATED'
                        when (f.fraud_score is null or f.fraud_score < 50)
                             and (c.claimed_amount is null or c.claimed_amount <= p.sum_insured * 0.25)
                             and (c.claimed_amount is null or c.claimed_amount <= 50000)
                            then 'AUTO_ADJUDICATED'
                        else 'MANUAL_REVIEW'
                    end as routing_target
                from {self.raw}.claims c
                inner join {self.raw}.policies p
                    on c.policy_id = p.policy_id
                   and p.policy_status = 'ACTIVE'
                left join {self.raw}.fraud_indicators f
                    on c.claim_id = f.claim_id
                where c.loss_date is not null
                  and c.loss_date >= p.effective_date
                  and c.loss_date <= p.expiry_date
                  and (c.claimed_amount is null or c.claimed_amount <= p.sum_insured)
            )
            select count(*)
            from expected e
            left join {self.intermediate}.int_claims_adjudication a
                on e.claim_id = a.claim_id
            where a.claim_id is null
               or e.adjudication_result <> a.adjudication_result
               or not (e.approved_amount <=> a.approved_amount)
               or e.routing_target <> a.routing_target
            """
        )
        ok = mismatches == 0
        self.results.append(
            CheckResult(
                "claims_adjudication_parity",
                "PASS" if ok else "FAIL",
                f"adjudication parity mismatches = {mismatches}",
                {"mismatches": mismatches},
            )
        )

    def check_fraud_alert_boundedness(self):
        """Fraud alerts must reference valid claims and register rows."""
        out_of_scope = self._scalar(
            f"""
            select count(*)
            from {self.marts}.mart_fraud_alerts a
            left join (
                select distinct c.claim_id
                from {self.raw}.claims c
                inner join {self.raw}.policies p
                    on c.policy_id = p.policy_id
                   and p.policy_status = 'ACTIVE'
                where c.loss_date is not null
                  and c.loss_date >= p.effective_date
                  and c.loss_date <= p.expiry_date
                  and (c.claimed_amount is null or c.claimed_amount <= p.sum_insured)
            ) valid
                on a.claim_id = valid.claim_id
            where valid.claim_id is null
            """
        )
        not_in_register = self._scalar(
            f"""
            select count(*)
            from {self.marts}.mart_fraud_alerts a
            left join {self.marts}.mart_claims_register r
                on a.claim_id = r.claim_id
            where r.claim_id is null
            """
        )
        ok = out_of_scope == 0 and not_in_register == 0
        self.results.append(
            CheckResult(
                "fraud_alert_boundedness",
                "PASS" if ok else "FAIL",
                f"out-of-scope alerts = {out_of_scope}, alerts not in register = {not_in_register}",
                {
                    "out_of_scope": out_of_scope,
                    "not_in_register": not_in_register,
                },
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_claims_completeness()
        self.check_claims_control_total()
        self.check_claims_adjudication_parity()
        self.check_fraud_alert_boundedness()
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
