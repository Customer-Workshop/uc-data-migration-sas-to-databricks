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

    # ----------------------------------------------------- claims_processing.sas
    # The in-scope valid claims, scored for fraud risk, recomputed straight from
    # raw -- the independent "source" side for the claims controls. Mirrors
    # claims_processing.sas Step 1 (validate) + Step 2 (fraud bucket).
    @property
    def _scored_claims_sql(self) -> str:
        return f"""
            with active_policies as (
                select policy_id, policy_type, effective_date,
                       expiry_date as expiration_date, sum_insured, deductible
                from {self.raw}.policies
                where policy_status = 'ACTIVE'
            ),
            valid_claims as (
                select c.claim_id, c.policy_id, c.claimant_id, c.claimed_amount,
                       p.policy_type, p.sum_insured, p.deductible
                from {self.raw}.claims c
                inner join active_policies p on c.policy_id = p.policy_id
                where c.loss_date >= p.effective_date
                  and c.loss_date <= p.expiration_date
                  and c.claimed_amount <= p.sum_insured
            ),
            scored as (
                select v.*,
                    case
                        when f.fraud_score >= 80 then 'HIGH'
                        when f.fraud_score >= 50 then 'MEDIUM'
                        else 'LOW'
                    end as fraud_risk
                from valid_claims v
                left join {self.raw}.fraud_indicators f
                    on v.policy_id = f.policy_id and v.claimant_id = f.claimant_id
            )
        """

    def check_claims_completeness(self):
        """int_claims_adjudication must equal the in-scope valid-claim population."""
        expected = self._scalar(self._scored_claims_sql + "select count(*) from scored")
        actual = self._scalar(
            f"select count(*) from {self.intermediate}.int_claims_adjudication"
        )
        ok = expected == actual
        self.results.append(
            CheckResult(
                "claims_completeness",
                "PASS" if ok else "FAIL",
                f"in-scope valid claims = {expected}, model claims = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_claims_control_total(self):
        """Total claimed and total approved must tie out to a raw recompute."""
        row = None
        cur = self.con.cursor()
        try:
            cur.execute(
                self._scored_claims_sql
                + """
                , source_totals as (
                    select
                        round(sum(claimed_amount), 2) as total_claimed,
                        round(sum(case
                            when fraud_risk = 'HIGH' then 0
                            when fraud_risk = 'LOW' and claimed_amount <= 5000
                                 and policy_type in ('AUTO','HOME','RENT')
                                then greatest(0, claimed_amount - deductible)
                            when fraud_risk = 'LOW'
                                 and claimed_amount <= sum_insured * 0.25
                                 and claimed_amount <= 50000
                                then greatest(0, claimed_amount - deductible)
                            else 0 end), 2) as total_approved
                    from scored
                )
                select s.total_claimed, s.total_approved,
                       round((select sum(claimed_amount)
                              from {intermediate}.int_claims_adjudication), 2),
                       round((select sum(coalesce(approved_amount, 0))
                              from {intermediate}.int_claims_adjudication), 2)
                from source_totals s
                """.format(intermediate=self.intermediate)
            )
            row = cur.fetchone()
        finally:
            cur.close()
        s_claimed, s_appr, m_claimed, m_appr = row
        ok = abs(s_claimed - m_claimed) <= 0.01 and abs(s_appr - m_appr) <= 0.01
        self.results.append(
            CheckResult(
                "claims_control_total",
                "PASS" if ok else "FAIL",
                (
                    f"claimed src={s_claimed} model={m_claimed}; "
                    f"approved src={s_appr} model={m_appr}"
                ),
            )
        )

    def check_claims_fraud_parity(self):
        """Every claim's fraud_risk must match the raw-score recompute (per value)."""
        mismatches = self._scalar(
            f"""
            with model as (
                select claim_id, policy_id, claimant_id, fraud_risk
                from {self.intermediate}.int_claims_adjudication
            ),
            src_fraud as (
                select policy_id, claimant_id, fraud_score from {self.raw}.fraud_indicators
            )
            select count(*)
            from model m
            left join src_fraud f
                on m.policy_id = f.policy_id and m.claimant_id = f.claimant_id
            where m.fraud_risk <> case
                when f.fraud_score >= 80 then 'HIGH'
                when f.fraud_score >= 50 then 'MEDIUM'
                else 'LOW' end
            """
        )
        ok = mismatches == 0
        self.results.append(
            CheckResult(
                "claims_fraud_parity",
                "PASS" if ok else "FAIL",
                f"per-claim fraud_risk mismatches vs source = {mismatches}",
            )
        )

    def check_claims_adjudication_parity(self):
        """result / approved_amount / routing_target must match the SAS recompute."""
        mismatches = self._scalar(
            self._scored_claims_sql
            + """
            , expected as (
                select claim_id, claimed_amount, deductible,
                    case
                        when fraud_risk = 'HIGH' then 1
                        when fraud_risk = 'LOW' and claimed_amount <= 5000
                             and policy_type in ('AUTO','HOME','RENT') then 2
                        when fraud_risk = 'LOW' and claimed_amount <= sum_insured * 0.25
                             and claimed_amount <= 50000 then 3
                        else 4 end as branch
                from scored
            ),
            expected_final as (
                select claim_id,
                    case branch when 1 then 'DENY' when 2 then 'APPR'
                                when 3 then 'APPR' else 'PEND' end as result,
                    case branch when 1 then 0
                        when 2 then greatest(0, claimed_amount - deductible)
                        when 3 then greatest(0, claimed_amount - deductible)
                        else cast(null as double) end as approved_amount,
                    case branch when 2 then 'AUTO_ADJUDICATED'
                                when 3 then 'AUTO_ADJUDICATED'
                                else 'MANUAL_REVIEW' end as routing_target
                from expected
            )
            select count(*)
            from {intermediate}.int_claims_adjudication m
            inner join expected_final e on m.claim_id = e.claim_id
            where not (m.adjudication_result <=> e.result)
               or not (m.approved_amount <=> e.approved_amount)
               or not (m.routing_target <=> e.routing_target)
            """.format(intermediate=self.intermediate)
        )
        ok = mismatches == 0
        self.results.append(
            CheckResult(
                "claims_adjudication_parity",
                "PASS" if ok else "FAIL",
                f"per-claim adjudication mismatches vs source = {mismatches}",
            )
        )

    def check_claims_curated_routing(self):
        """Cross-engine: the PySpark curated outputs must be bounded by and
        reference int_claims_adjudication (no invented rows, exact routing)."""
        try:
            register_n = self._scalar(f"select count(*) from {self.curated}.claims_register")
            review_n = self._scalar(f"select count(*) from {self.curated}.claims_review_queue")
            fraud_n = self._scalar(f"select count(*) from {self.curated}.fraud_alerts")
        except Exception as exc:  # curated tables not produced yet
            self.results.append(
                CheckResult(
                    "claims_curated_routing",
                    "SKIP",
                    f"curated outputs not found in {self.curated} "
                    f"(run src/pyspark/claims_processing.py): {type(exc).__name__}",
                )
            )
            return

        model_n = self._scalar(f"select count(*) from {self.intermediate}.int_claims_adjudication")
        expected_review = self._scalar(
            f"select count(*) from {self.intermediate}.int_claims_adjudication "
            "where routing_target = 'MANUAL_REVIEW'"
        )
        expected_fraud = self._scalar(
            f"select count(*) from {self.intermediate}.int_claims_adjudication "
            "where fraud_alert_flag = true"
        )
        # Referential: every registered/alerted claim must exist in the model.
        orphans = self._scalar(
            f"""
            select
              (select count(*) from {self.curated}.claims_register r
               where not exists (select 1 from {self.intermediate}.int_claims_adjudication m
                                 where m.claim_id = r.claim_id))
            + (select count(*) from {self.curated}.fraud_alerts a
               where not exists (select 1 from {self.intermediate}.int_claims_adjudication m
                                 where m.claim_id = a.claim_id))
            """
        )
        ok = (
            register_n == model_n
            and review_n == expected_review
            and fraud_n == expected_fraud
            and orphans == 0
        )
        self.results.append(
            CheckResult(
                "claims_curated_routing",
                "PASS" if ok else "FAIL",
                (
                    f"register={register_n} (model {model_n}), "
                    f"review_queue={review_n} (expected {expected_review}), "
                    f"fraud_alerts={fraud_n} (expected {expected_fraud}), "
                    f"orphan rows={orphans}"
                ),
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_claims_completeness()
        self.check_claims_control_total()
        self.check_claims_fraud_parity()
        self.check_claims_adjudication_parity()
        self.check_claims_curated_routing()
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
