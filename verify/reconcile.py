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

    # --------------------------------------------------- transaction processing
    # Controls for Programs/Banking/daily_transaction_processing.sas.
    # In-scope population = the SAS Step 1 validation contract: a feed record is
    # rejected (WORK.TXN_REJECTED) when TRANSACTION_ID / ACCOUNT_ID /
    # TRANSACTION_AMOUNT is missing, abs(amount) > 10,000,000, the type is not one
    # of the ten valid codes, or the transaction is future dated.
    IN_SCOPE_TXN_PREDICATE = """
        transaction_id is not null
        and account_id is not null
        and transaction_amount is not null
        and abs(transaction_amount) <= 10000000
        and transaction_type in
            ('DEP','WDR','TRF','PMT','FEE','INT','ADJ','REV','CHG','REF')
        and transaction_date <= current_date()
    """

    # SAS Steps 2-3 balance movement mapping, transcribed from the source CASE /
    # IF-ELSE chain. TRF and ADJ add the *signed* amount (source quirk).
    SAS_MOVEMENT_CASE = """
        case
            when transaction_type in ('DEP','INT','REF','REV') then transaction_amount
            when transaction_type in ('WDR','PMT','FEE','CHG') then -abs(transaction_amount)
            when transaction_type in ('TRF','ADJ') then transaction_amount
            else 0
        end
    """

    def check_transaction_completeness(self):
        """Mart transactions must equal the SAS-validated in-scope feed."""
        expected = self._scalar(
            f"select count(*) from {self.raw}.daily_transactions"
            f" where {self.IN_SCOPE_TXN_PREDICATE}"
        )
        actual = self._scalar(f"select count(*) from {self.marts}.mart_daily_transactions")
        distinct = self._scalar(
            f"select count(distinct transaction_id) from {self.marts}.mart_daily_transactions"
        )
        ok = expected == actual == distinct
        self.results.append(
            CheckResult(
                "transaction_completeness",
                "PASS" if ok else "FAIL",
                f"in-scope feed rows = {expected}, mart rows = {actual}, "
                f"distinct transaction_id = {distinct}",
                {"expected": expected, "actual": actual, "distinct": distinct},
            )
        )

    def check_transaction_control_total(self):
        """Transaction value and net balance movement must tie out to the feed."""
        src_amount, src_move = self._row(
            f"""
            select round(sum(transaction_amount), 2),
                   round(sum({self.SAS_MOVEMENT_CASE}), 2)
            from {self.raw}.daily_transactions
            where {self.IN_SCOPE_TXN_PREDICATE}
            """
        )
        mart_amount, mart_move = self._row(
            f"""
            select round(sum(transaction_amount), 2),
                   round(sum(post_txn_balance - pre_txn_balance), 2)
            from {self.marts}.mart_daily_transactions
            """
        )
        ok = src_amount == mart_amount and src_move == mart_move
        self.results.append(
            CheckResult(
                "transaction_control_total",
                "PASS" if ok else "FAIL",
                f"sum(amount) source = {src_amount} / mart = {mart_amount}; "
                f"sum(balance movement) source = {src_move} / mart = {mart_move}",
                {
                    "source_amount": src_amount,
                    "mart_amount": mart_amount,
                    "source_movement": src_move,
                    "mart_movement": mart_move,
                },
            )
        )

    def check_txn_type_parity(self):
        """Every transaction type's balance direction must match the SAS mapping."""
        expected_sign = {
            "DEP": 1, "INT": 1, "REF": 1, "REV": 1,
            "WDR": -1, "PMT": -1, "FEE": -1, "CHG": -1,
            "TRF": 1, "ADJ": 1,
        }
        rows = self._rows(
            f"""
            select transaction_type,
                   min(signum(round(post_txn_balance - pre_txn_balance, 2))),
                   max(signum(round(post_txn_balance - pre_txn_balance, 2)))
            from {self.marts}.mart_daily_transactions
            where transaction_amount <> 0
            group by transaction_type
            order by transaction_type
            """
        )
        mismatches = [
            f"{t} actual {lo if lo == hi else f'{lo}..{hi}'}, "
            f"expected {expected_sign.get(t, 'n/a')}"
            for t, lo, hi in rows
            if not (lo == hi == expected_sign.get(t))
        ]
        self.results.append(
            CheckResult(
                "txn_type_direction_parity",
                "PASS" if not mismatches else "FAIL",
                f"{len(rows)} transaction types compared to the SAS mapping"
                + ("" if not mismatches else "; " + "; ".join(mismatches)),
                {"types_compared": len(rows), "mismatches": len(mismatches)},
            )
        )

    def check_running_balance_parity(self):
        """Per-row RETAIN emulation must match an independent recomputation."""
        mismatched = self._scalar(
            f"""
            with expected as (
                select t.transaction_id,
                       a.current_balance + sum({self.SAS_MOVEMENT_CASE}) over (
                           partition by t.account_id
                           order by t.transaction_date, t.transaction_id
                           rows between unbounded preceding and current row
                       ) as expected_running_balance
                from {self.raw}.daily_transactions t
                left join {self.intermediate}.int_account_metrics a
                    on t.account_id = a.account_id
                where {self.IN_SCOPE_TXN_PREDICATE}
            )
            select count(*)
            from {self.marts}.mart_daily_transactions m
            inner join expected e on m.transaction_id = e.transaction_id
            where round(m.running_balance, 2) <> round(e.expected_running_balance, 2)
            """
        )
        self.results.append(
            CheckResult(
                "running_balance_parity",
                "PASS" if mismatched == 0 else "FAIL",
                f"transactions whose running balance differs from the recomputed "
                f"SAS BY-group sequence = {mismatched}",
                {"mismatched_rows": mismatched},
            )
        )

    def check_anomaly_classification_parity(self):
        """Anomaly branches must match the SAS CASE, in the same precedence."""
        mismatched = self._scalar(
            f"""
            with stats as (
                select account_id,
                       avg(abs(transaction_amount)) as avg_txn_amt,
                       stddev(abs(transaction_amount)) as std_txn_amt
                from {self.marts}.mart_daily_transactions
                where transaction_date >= date_add(current_date(), -90)
                  and transaction_date < current_date()
                group by account_id
            ),
            expected as (
                select t.transaction_id,
                       case
                           when s.std_txn_amt > 0
                                and (abs(t.transaction_amount) - s.avg_txn_amt)
                                    / s.std_txn_amt > 3 then 'HIGH_AMOUNT'
                           when t.running_balance < 0 then 'OVERDRAFT'
                           when t.transaction_type = 'WDR'
                                and abs(t.transaction_amount) > t.pre_txn_balance * 0.9
                               then 'LARGE_WITHDRAWAL'
                           when t.customer_id is null then 'ORPHAN_ACCOUNT'
                       end as expected_anomaly_type
                from {self.marts}.mart_daily_transactions t
                left join stats s on t.account_id = s.account_id
            )
            select count(*)
            from expected e
            full outer join {self.marts}.mart_transaction_anomalies a
                on e.transaction_id = a.transaction_id
            where coalesce(e.expected_anomaly_type, '') <> coalesce(a.anomaly_type, '')
            """
        )
        flagged = self._scalar(
            f"select count(*) from {self.marts}.mart_transaction_anomalies"
        )
        self.results.append(
            CheckResult(
                "anomaly_classification_parity",
                "PASS" if mismatched == 0 else "FAIL",
                f"{flagged} flagged transactions; rows whose classification differs "
                f"from the SAS CASE = {mismatched}",
                {"flagged": flagged, "mismatched_rows": mismatched},
            )
        )

    def check_running_balances_persistence(self):
        """CURATED.RUNNING_BALANCES must mirror the transaction mart row for row."""
        mismatched = self._scalar(
            f"""
            select count(*)
            from {self.marts}.mart_daily_transactions t
            full outer join {self.marts}.mart_running_balances b
                on t.transaction_id = b.transaction_id
            where t.transaction_id is null
               or b.transaction_id is null
               or t.account_id <> b.account_id
               or t.transaction_date <> b.transaction_date
               or round(t.running_balance, 2) <> round(b.running_balance, 2)
            """
        )
        persisted = self._scalar(f"select count(*) from {self.marts}.mart_running_balances")
        self.results.append(
            CheckResult(
                "running_balances_persistence",
                "PASS" if mismatched == 0 else "FAIL",
                f"persisted running-balance rows = {persisted}, mismatched rows = {mismatched}",
                {"persisted": persisted, "mismatched_rows": mismatched},
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_transaction_completeness()
        self.check_transaction_control_total()
        self.check_txn_type_parity()
        self.check_running_balance_parity()
        self.check_anomaly_classification_parity()
        self.check_running_balances_persistence()
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
