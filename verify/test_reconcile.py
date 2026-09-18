#!/usr/bin/env python3
"""
Fixture tests for verify/reconcile.py — no Databricks connection.

The SAS golden fixture under verify/fixtures/sas_golden/ is a real export from
the Dockerised SAS runtime (ts-sas-legacy-analytics: `docker compose -f
docker/compose.yml run --rm sas`). A stub connection stands in for the target
so the harness's verdict logic (PASS / FAIL / SKIP, exit status) is pinned down
before anyone runs it live.

    python -m unittest verify/test_reconcile.py
"""
from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import reconcile  # noqa: E402

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "sas_golden"


class NotFound(Exception):
    def __str__(self) -> str:
        return "[TABLE_OR_VIEW_NOT_FOUND] The table or view cannot be found."


class StubCursor:
    def __init__(self, answers: dict[str, object]):
        self.answers = answers
        self.row = None

    def execute(self, query: str) -> None:
        q = " ".join(query.split())
        for pattern, value in self.answers.items():
            if re.search(pattern, q):
                if value is NotFound:
                    raise NotFound()
                self.row = (value,)
                return
        raise AssertionError(f"unexpected query: {q}")

    def fetchone(self):
        return self.row

    def close(self) -> None:
        pass


class StubConnection:
    def __init__(self, answers: dict[str, object]):
        self.answers = answers
        self.closed = False

    def cursor(self) -> StubCursor:
        return StubCursor(self.answers)

    def close(self) -> None:
        self.closed = True


def run(answers: dict[str, object], golden: Path | None = FIXTURE):
    rec = reconcile.Reconciler("cat", "t1", con=StubConnection(answers), sas_golden=golden)
    ok = rec.run()
    return ok, {r.name: r for r in rec.results}, rec


# A target where the four models that exist today reproduce SAS exactly and the
# regulatory-reporting models do not exist yet.
CONVERTED_SO_FAR = {
    r"from cat\.raw\.cust_accounts a": 466,
    r"count\(\*\) from cat\.t1_intermediate\.int_account_metrics": 466,
    r"count\(\*\) from cat\.t1_staging\.stg_cust_accounts": 466,
    r"cat\.t1_staging\.stg_acct_exceptions": NotFound,
    r"count\(\*\) from cat\.t1_marts\.mart_daily_transactions": 18903,
    r"count\(\*\) from cat\.t1_marts\.mart_transaction_anomalies where anomaly_type = 'HIGH_AMOUNT'": 16,
    r"count\(\*\) from cat\.t1_marts\.mart_transaction_anomalies where anomaly_type = 'OVERDRAFT'": 30,
    r"count\(\*\) from cat\.t1_marts\.mart_transaction_anomalies$": 46,
    r"count\(\*\) from cat\.t1_marts\.mart_risk_scores": 236,
    r"sum\(transaction_amount\).*mart_daily_transactions": 8231436.81,
    r"mart_regulatory_rwa": NotFound,
    r"mart_delinquency_aging": NotFound,
    r"mart_llp_coverage": NotFound,
}


class GoldenFixture(unittest.TestCase):
    def test_fixture_is_the_full_sas_export(self):
        counts, controls, manifest = reconcile.read_golden(FIXTURE)
        self.assertEqual({t.sas for t in reconcile.GOLDEN_TABLES}, set(counts))
        self.assertEqual(set(reconcile.GOLDEN_CONTROLS), set(controls))
        self.assertEqual(manifest["business_date"], "31JAN2024")
        self.assertEqual(counts["CURATED.TXN_ANOMALIES"], controls["TXN_ANOMALIES.HIGH_AMOUNT"] + controls["TXN_ANOMALIES.OVERDRAFT"])
        self.assertIsNotNone(controls["MONTHLY_RWA.SUM_RWA"])


class Verdicts(unittest.TestCase):
    def test_converted_models_pass_and_unconverted_skip(self):
        ok, res, rec = run(CONVERTED_SO_FAR)
        self.assertTrue(ok)
        self.assertTrue(rec.con.closed)
        self.assertEqual(res["account_completeness"].status, "PASS")
        self.assertEqual(res["sas_rowcount:CURATED.TXN_ANOMALIES"].status, "PASS")
        self.assertEqual(res["sas_control:DAILY_TRANSACTIONS.SUM_AMOUNT"].status, "PASS")
        self.assertEqual(res["sas_rowcount:REPORTS.MONTHLY_RWA"].status, "SKIP")
        self.assertIn("not yet converted", res["sas_rowcount:REPORTS.MONTHLY_RWA"].detail)
        self.assertEqual(res["sas_control:MONTHLY_RWA.SUM_RWA"].status, "SKIP")
        self.assertEqual(res["sas_rowcount:STG_BANK.ACCT_EXCEPTIONS"].status, "SKIP")
        statuses = {r.status for r in res.values()}
        self.assertEqual(statuses, {"PASS", "SKIP"})

    def test_row_count_divergence_fails(self):
        answers = dict(CONVERTED_SO_FAR)
        answers[r"count\(\*\) from cat\.t1_marts\.mart_transaction_anomalies$"] = 92  # double-appended history
        ok, res, _ = run(answers)
        self.assertFalse(ok)
        self.assertEqual(res["sas_rowcount:CURATED.TXN_ANOMALIES"].status, "FAIL")
        self.assertIn("SAS CURATED.TXN_ANOMALIES = 46 rows", res["sas_rowcount:CURATED.TXN_ANOMALIES"].detail)
        self.assertIn("= 92 rows", res["sas_rowcount:CURATED.TXN_ANOMALIES"].detail)

    def test_control_total_off_by_a_cent_fails_but_rounding_noise_passes(self):
        answers = dict(CONVERTED_SO_FAR)
        answers[r"sum\(transaction_amount\).*mart_daily_transactions"] = 8231436.82
        ok, res, _ = run(answers)
        self.assertFalse(ok)
        self.assertEqual(res["sas_control:DAILY_TRANSACTIONS.SUM_AMOUNT"].status, "FAIL")

        answers[r"sum\(transaction_amount\).*mart_daily_transactions"] = 8231436.8100001
        ok, res, _ = run(answers)
        self.assertTrue(ok)

    def test_null_control_on_target_fails(self):
        answers = dict(CONVERTED_SO_FAR)
        answers[r"count\(\*\) from cat\.t1_marts\.mart_transaction_anomalies where anomaly_type = 'OVERDRAFT'"] = None
        ok, res, _ = run(answers)
        self.assertFalse(ok)
        self.assertEqual(res["sas_control:TXN_ANOMALIES.OVERDRAFT"].status, "FAIL")

    def test_blank_sas_control_is_a_fail_not_a_skip(self):
        import shutil
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            g = Path(tmp)
            for f in ("row_counts.csv", "manifest.json"):
                shutil.copy(FIXTURE / f, g / f)
            (g / "controls.csv").write_text("CONTROL,VALUE\nMONTHLY_RWA.SUM_RWA,\n")
            ok, res, _ = run(CONVERTED_SO_FAR, golden=g)
            self.assertFalse(ok)
            self.assertEqual(res["sas_control:MONTHLY_RWA.SUM_RWA"].status, "FAIL")
            self.assertIn("blank", res["sas_control:MONTHLY_RWA.SUM_RWA"].detail)
            self.assertEqual(res["sas_control:RISK_SCORES.N_ACCOUNTS"].status, "FAIL")
            self.assertIn("missing from controls.csv", res["sas_control:RISK_SCORES.N_ACCOUNTS"].detail)

    def test_without_golden_dir_only_source_controls_run(self):
        ok, res, _ = run({r"from cat\.raw\.cust_accounts a": 10,
                          r"int_account_metrics": 10}, golden=None)
        self.assertTrue(ok)
        self.assertEqual(list(res), ["account_completeness"])

    def test_report_names_the_sas_baseline(self):
        _, _, rec = run(CONVERTED_SO_FAR)
        report = rec.render()
        self.assertIn("business date `31JAN2024`", report)
        self.assertIn("| `sas_rowcount:REPORTS.MONTHLY_RWA` | SKIP |", report)
        self.assertRegex(report, r"\*\*\d+ passed, 0 failed, \d+ skipped\*\*")


if __name__ == "__main__":
    unittest.main()
