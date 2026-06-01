#!/usr/bin/env python3
"""
Synthetic data generator + loader for the SAS -> Databricks migration demo.

In the legacy SAS estate these tables were read via LIBNAME connections to
Oracle / Teradata / flat files (see ts-sas-legacy-analytics/Config/autoexec.sas).
Here we materialize equivalent "raw" tables as Delta tables in Unity Catalog
(catalog `banking_analytics`, schema `raw`) so the dbt project has real data to
run against end to end.

Deterministic: a fixed RNG seed produces the same data every run, so dbt tests
are stable.

Usage:
    python seed/generate_and_load.py            # default volumes
    python seed/generate_and_load.py --customers 200 --days 30

Auth (env vars, same ones dbt uses):
    DATABRICKS_HOST        e.g. https://dbc-xxxx.cloud.databricks.com
    DATABRICKS_HTTP_PATH   e.g. /sql/1.0/warehouses/xxxxxxxx
    DATABRICKS_TOKEN       dapi...
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import os
import random
import sys

from databricks import sql as dbsql
from faker import Faker

# ---------------------------------------------------------------------------
# Reference domains (aligned with dbt accepted_values tests + format macros)
# ---------------------------------------------------------------------------
ACCOUNT_TYPES = ["CHK", "SAV", "MMA", "CD", "IRA", "LOC", "MTG", "AUTO", "PERS", "CC", "HELC"]
REVOLVING = {"CC", "LOC", "HELC"}
LENDING = {"MTG", "AUTO", "PERS", "CC", "LOC", "HELC"}
# Secured products carry a stored LTV in LOAN_DETAILS (SAS: ORA_DW.LOAN_DETAILS).
SECURED = {"MTG", "AUTO", "HELC"}
# staging keeps only these; 'W'/'C' are filtered out by stg_cust_accounts
ACTIVE_STATUSES = ["A", "A", "A", "A", "D", "F", "R", "S", "P"]
FILTERED_STATUSES = ["W", "C"]
TXN_TYPES = ["DEP", "WDR", "TRF", "PMT", "FEE", "INT", "ADJ", "REV", "CHG", "REF"]
SEGMENTS = ["RETAIL", "PREMIER", "PRIVATE", "BUSINESS", "STUDENT", "SENIOR"]
RISK_RATINGS = ["LOW", "MEDIUM", "HIGH"]
REGIONS = ["NE", "SE", "MW", "SW", "W"]

CLAIM_STATUSES = ["OPEN", "CLOSED", "PENDING", "DENIED", "SETTLED", "REOPENED"]
CLAIM_TYPES = ["AUTO", "PROPERTY", "LIABILITY", "HEALTH", "LIFE"]
POLICY_TYPES = ["AUTO", "HOME", "LIFE", "HEALTH", "COMMERCIAL"]

TODAY = dt.date.today()


def _d(days_ago: int) -> dt.date:
    return TODAY - dt.timedelta(days=days_ago)


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------
def generate(customers: int, accounts: int, days: int, seed: int):
    rnd = random.Random(seed)
    fake = Faker()
    Faker.seed(seed)

    demographics, bureau = [], []
    for i in range(1, customers + 1):
        cid = f"CUST{i:06d}"
        first, last = fake.first_name(), fake.last_name()
        demographics.append({
            "customer_id": cid,
            "first_name": first,
            "last_name": last,
            "ssn_hash": hashlib.sha256(f"{cid}{seed}".encode()).hexdigest()[:32],
            "date_of_birth": _d(rnd.randint(20 * 365, 80 * 365)),
            "customer_segment": rnd.choice(SEGMENTS),
            "risk_rating": rnd.choices(RISK_RATINGS, weights=[6, 3, 1])[0],
            "region_code": rnd.choice(REGIONS),
            "primary_email": f"{first}.{last}.{i}@example.com".lower(),
            "phone_number": fake.numerify("###-###-####"),
        })
        bureau.append({
            "customer_id": cid,
            "fico_score": int(rnd.triangular(520, 840, 710)),
            "bureau_inqs_6mo": rnd.choices([0, 1, 2, 3, 5], weights=[5, 4, 2, 1, 1])[0],
            "bureau_derogs": rnd.choices([0, 1, 2, 4], weights=[7, 2, 1, 1])[0],
        })

    accts, pay_hist, collateral = [], [], []
    for j in range(1, accounts + 1):
        aid = f"ACCT{j:07d}"
        cid = f"CUST{rnd.randint(1, customers):06d}"
        atype = rnd.choice(ACCOUNT_TYPES)
        # most accounts active; a few carry filtered statuses to prove the filter works
        status = rnd.choice(FILTERED_STATUSES) if rnd.random() < 0.05 else rnd.choice(ACTIVE_STATUSES)
        open_d = _d(rnd.randint(30, 12 * 365))
        last_act = _d(rnd.randint(0, 800))
        bal = round(rnd.uniform(-5_000, 400_000), 2)
        credit_limit = round(rnd.uniform(5_000, 100_000), 2) if atype in REVOLVING else 0.0
        # days_past_due: 0 for most accounts; a fraction of credit products are
        # delinquent. Read by monthly_regulatory_reporting.sas Step 2 from the
        # daily account snapshot (STG_BANK.CUST_ACCOUNTS_DAILY).
        dpd = 0
        if atype in LENDING and rnd.random() < 0.25:
            dpd = rnd.choices([0, 15, 45, 75, 100, 150, 200], weights=[3, 2, 2, 1, 1, 1, 1])[0]
        past_due_amt = round(abs(bal) * rnd.uniform(0.01, 0.15), 2) if dpd > 0 else 0.0
        accts.append({
            "account_id": aid,
            "customer_id": cid,
            "account_type": atype,
            "account_status": status,
            "open_date": open_d,
            "close_date": (last_act if status in ("W", "C") else None),
            "current_balance": bal,
            "available_balance": round(bal * rnd.uniform(0.7, 1.0), 2),
            "credit_limit": credit_limit,
            "interest_rate": round(rnd.uniform(0.5, 24.99), 2),
            "branch_id": f"BR{rnd.randint(1, 40):03d}",
            "officer_id": f"OFF{rnd.randint(1, 120):04d}",
            "last_activity_date": last_act,
            "days_past_due": dpd,
            "past_due_amount": past_due_amt,
        })
        pay_hist.append({
            "account_id": aid,
            "pmt_late_90_12mo": rnd.choices([0, 1, 2, 3], weights=[7, 2, 1, 1])[0],
            "max_days_past_due_ever": rnd.choices([0, 30, 60, 90, 120], weights=[5, 3, 2, 1, 1])[0],
        })
        if atype in ("MTG", "AUTO", "HELC"):
            collateral.append({
                "account_id": aid,
                "collateral_value": round(abs(bal) * rnd.uniform(1.0, 1.8) + 10_000, 2),
            })

    txns = []
    t = 0
    for acct in accts:
        if acct["account_status"] in ("W", "C"):
            continue
        for d in range(days):
            if rnd.random() < 0.55:  # not every account transacts every day
                continue
            t += 1
            ttype = rnd.choice(TXN_TYPES)
            txns.append({
                "transaction_id": f"TXN{t:09d}",
                "account_id": acct["account_id"],
                "transaction_amount": round(rnd.uniform(5, 9_500), 2),
                "transaction_type": ttype,
                "transaction_date": _d(d),
                "description": f"{ttype} {fake.bs()[:40]}",
            })

    loans = []
    for acct in accts:
        if acct["account_type"] in LENDING:
            principal = round(abs(acct["current_balance"]) + rnd.uniform(1_000, 50_000), 2)
            # LTV (loan-to-value) is stored only for secured products; the SAS
            # RWA query bands MTG by it. allowance_amt feeds the (out-of-scope)
            # LLP step but is part of the LOAN_DETAILS record.
            ltv = None
            if acct["account_type"] in SECURED:
                coll_val = round(principal * rnd.uniform(1.0, 2.0) + 10_000, 2)
                ltv = round(abs(acct["current_balance"]) / coll_val, 4) if coll_val > 0 else None
            allowance = round(abs(acct["current_balance"]) * rnd.uniform(0.005, 0.05), 2)
            loans.append({
                "loan_id": f"LN{acct['account_id'][4:]}",
                "account_id": acct["account_id"],
                "principal": principal,
                "rate": acct["interest_rate"],
                "term_months": rnd.choice([36, 48, 60, 120, 180, 360]),
                "origination_date": acct["open_date"],
                "ltv": ltv,
                "allowance_amt": allowance,
            })

    return {
        "cust_demographics": demographics,
        "cust_accounts": accts,
        "daily_transactions": txns,
        "bureau_scores": bureau,
        "payment_history": pay_hist,
        "collateral": collateral,
        "loan_details": loans,
    }


def generate_insurance(policies_n: int, seed: int, customers_hint: int = 200):
    rnd = random.Random(seed + 1)
    fake = Faker()
    Faker.seed(seed + 1)

    policies, premiums, claims, fraud = [], [], [], []
    for p in range(1, policies_n + 1):
        pid = f"POL{p:06d}"
        sum_insured = round(rnd.uniform(25_000, 1_000_000), 2)
        annual_premium = round(sum_insured * rnd.uniform(0.005, 0.04), 2)
        eff = _d(rnd.randint(30, 3 * 365))
        policies.append({
            "policy_id": pid,
            "policy_type": rnd.choice(POLICY_TYPES),
            "policyholder_id": f"CUST{rnd.randint(1, customers_hint):06d}",
            "sum_insured": sum_insured,
            "deductible": round(rnd.choice([250, 500, 1_000, 2_500, 5_000]) * 1.0, 2),
            "annual_premium": annual_premium,
            "effective_date": eff,
            "expiry_date": eff + dt.timedelta(days=365),
            "policy_status": rnd.choices(["ACTIVE", "LAPSED", "CANCELLED"], weights=[7, 2, 1])[0],
        })
        premiums.append({
            "policy_id": pid,
            "premium_due": annual_premium,
            "premium_paid": round(annual_premium * rnd.uniform(0.5, 1.0), 2),
            "due_date": eff + dt.timedelta(days=rnd.randint(0, 365)),
        })
        if rnd.random() < 0.4:
            cid = f"CLM{p:06d}"
            loss = eff + dt.timedelta(days=rnd.randint(1, 360))
            claims.append({
                "claim_id": cid,
                "policy_id": pid,
                "claimant_id": f"CUST{rnd.randint(1, customers_hint):06d}",
                "claim_type": rnd.choice(CLAIM_TYPES),
                "claim_status": rnd.choice(CLAIM_STATUSES),
                "claimed_amount": round(min(sum_insured, rnd.uniform(500, sum_insured)), 2),
                "loss_date": loss,
                "reported_date": loss + dt.timedelta(days=rnd.randint(0, 30)),
            })
            fraud.append({
                "claim_id": cid,
                "fraud_score": round(rnd.uniform(0, 1), 4),
                "model_version": "v2.3",
            })

    return {
        "policies": policies,
        "premiums": premiums,
        "claims": claims,
        "fraud_indicators": fraud,
    }


# ---------------------------------------------------------------------------
# Schema (column -> Spark SQL type) for CREATE TABLE
# ---------------------------------------------------------------------------
SCHEMAS = {
    "cust_demographics": {
        "customer_id": "STRING", "first_name": "STRING", "last_name": "STRING",
        "ssn_hash": "STRING", "date_of_birth": "DATE", "customer_segment": "STRING",
        "risk_rating": "STRING", "region_code": "STRING", "primary_email": "STRING",
        "phone_number": "STRING",
    },
    "cust_accounts": {
        "account_id": "STRING", "customer_id": "STRING", "account_type": "STRING",
        "account_status": "STRING", "open_date": "DATE", "close_date": "DATE",
        "current_balance": "DOUBLE", "available_balance": "DOUBLE", "credit_limit": "DOUBLE",
        "interest_rate": "DOUBLE", "branch_id": "STRING", "officer_id": "STRING",
        "last_activity_date": "DATE",
        "days_past_due": "INT", "past_due_amount": "DOUBLE",
    },
    "daily_transactions": {
        "transaction_id": "STRING", "account_id": "STRING", "transaction_amount": "DOUBLE",
        "transaction_type": "STRING", "transaction_date": "DATE", "description": "STRING",
    },
    "bureau_scores": {
        "customer_id": "STRING", "fico_score": "INT", "bureau_inqs_6mo": "INT",
        "bureau_derogs": "INT",
    },
    "payment_history": {
        "account_id": "STRING", "pmt_late_90_12mo": "INT", "max_days_past_due_ever": "INT",
    },
    "collateral": {"account_id": "STRING", "collateral_value": "DOUBLE"},
    "loan_details": {
        "loan_id": "STRING", "account_id": "STRING", "principal": "DOUBLE",
        "rate": "DOUBLE", "term_months": "INT", "origination_date": "DATE",
        "ltv": "DOUBLE", "allowance_amt": "DOUBLE",
    },
    "policies": {
        "policy_id": "STRING", "policy_type": "STRING", "policyholder_id": "STRING",
        "sum_insured": "DOUBLE", "deductible": "DOUBLE", "annual_premium": "DOUBLE",
        "effective_date": "DATE", "expiry_date": "DATE", "policy_status": "STRING",
    },
    "premiums": {
        "policy_id": "STRING", "premium_due": "DOUBLE", "premium_paid": "DOUBLE",
        "due_date": "DATE",
    },
    "claims": {
        "claim_id": "STRING", "policy_id": "STRING", "claimant_id": "STRING",
        "claim_type": "STRING", "claim_status": "STRING", "claimed_amount": "DOUBLE",
        "loss_date": "DATE", "reported_date": "DATE",
    },
    "fraud_indicators": {
        "claim_id": "STRING", "fraud_score": "DOUBLE", "model_version": "STRING",
    },
}


def _sql_literal(val) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, dt.date):
        return f"DATE'{val.isoformat()}'"
    if isinstance(val, (int, float)):
        return repr(val)
    return "'" + str(val).replace("'", "''") + "'"


def load(cursor, full_schema: str, table: str, rows: list, batch: int = 1000):
    cols = list(SCHEMAS[table].keys())
    ddl_cols = ", ".join(f"{c} {SCHEMAS[table][c]}" for c in cols)
    fq = f"{full_schema}.{table}"
    cursor.execute(f"CREATE OR REPLACE TABLE {fq} ({ddl_cols}) USING DELTA")
    if not rows:
        print(f"  {table}: 0 rows (table created)")
        return
    inserted = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        values = ", ".join(
            "(" + ", ".join(_sql_literal(r.get(c)) for c in cols) + ")" for r in chunk
        )
        cursor.execute(f"INSERT INTO {fq} ({', '.join(cols)}) VALUES {values}")
        inserted += len(chunk)
    print(f"  {table}: {inserted} rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="banking_analytics")
    ap.add_argument("--schema", default="raw")
    ap.add_argument("--customers", type=int, default=200)
    ap.add_argument("--accounts", type=int, default=500)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--policies", type=int, default=300)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--no-unity-catalog", action="store_true",
                    help="Use hive_metastore.<schema> instead of a UC catalog")
    args = ap.parse_args()

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]

    print("Generating synthetic data...")
    data = generate(args.customers, args.accounts, args.days, args.seed)
    data.update(generate_insurance(args.policies, args.seed, customers_hint=args.customers))
    for k, v in data.items():
        print(f"  generated {k}: {len(v)}")

    conn = dbsql.connect(server_hostname=host, http_path=http_path, access_token=token)
    cur = conn.cursor()

    if args.no_unity_catalog:
        full_schema = f"hive_metastore.{args.schema}"
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
    else:
        cur.execute(f"CREATE CATALOG IF NOT EXISTS {args.catalog}")
        cur.execute(f"USE CATALOG {args.catalog}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
        full_schema = f"{args.catalog}.{args.schema}"

    print(f"Loading into {full_schema} ...")
    for table in SCHEMAS:
        load(cur, full_schema, table, data.get(table, []))

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    sys.exit(main())
