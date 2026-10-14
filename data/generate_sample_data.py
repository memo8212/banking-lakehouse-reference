"""Generate synthetic source feeds for the lakehouse MVP (customer + cards).

  data/customer.csv    (CRM extract)
  data/cards_txn.csv   (card transaction feed)
  data/fx_rates.csv    (reference)

Run:  python data/generate_sample_data.py
"""
from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

DATA = Path(__file__).resolve().parent
SEGMENTS = ["RETAIL", "MASS", "AFFLUENT", "SME", "CORP"]
KYC = ["VERIFIED", "VERIFIED", "VERIFIED", "PENDING", "REJECTED"]
COUNTRIES = ["CA", "US", "GB", "DE"]
STATUS = ["POSTED", "POSTED", "POSTED", "REVERSAL", "PENDING", "DECLINED"]
CCYS = ["CAD", "USD", "EUR", "GBP"]
MERCHANTS = ["STORE 4412", "AMAZON", "TIM HORTONS", "UBER", "HYDRO", "AIRLINE"]
NAMES = ["JOHN SMITH", "MARIA GARCIA", "LI WEI", "AISHA KHAN", "DAVID LEE",
         "SOFIA ROSSI", "OMAR HADDAD", "EMMA JOHNSON", "YUKI TANAKA", "NOAH MARTIN"]


def gen_customers(path: Path, n: int = 2000, seed: int = 11) -> None:
    rng = random.Random(seed)
    start = date(2015, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "party_id", "full_name", "dob", "email",
                    "phone", "segment", "country", "kyc_status", "onboard_date"])
        for i in range(1, n + 1):
            name = rng.choice(NAMES)
            cid = f"C{i:07d}"
            w.writerow([
                cid, f"P{i:07d}", name,
                (date(1960, 1, 1) + timedelta(days=rng.randrange(14000))).isoformat(),
                f"user{i}@example.com", f"+1-555-{rng.randint(1000, 9999)}",
                rng.choice(SEGMENTS), rng.choice(COUNTRIES), rng.choice(KYC),
                (start + timedelta(days=rng.randrange(3650))).isoformat(),
            ])
        # a few deliberately bad rows (should be quarantined by the contract)
        for j in range(20):
            w.writerow([f"BAD{j:05d}", f"P9{j:06d}", "BAD ROW",
                        "1980-01-01", "bad@example.com", "x", "UNKNOWN",
                        "CA", "VERIFIED", "2020-01-01"])
        for j in range(10):
            w.writerow([f"NULL{j:04d}", f"P8{j:06d}", "NO COUNTRY",
                        "1980-01-01", "nc@example.com", "x", "RETAIL",
                        "", "VERIFIED", "2020-01-01"])


def gen_cards(path: Path, n_customers: int = 2000, n: int = 20000, seed: int = 12) -> None:
    rng = random.Random(seed)
    start = datetime(2026, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["txn_id", "card_id", "customer_id", "account_no", "amount",
                    "ccy", "merchant", "mcc", "txn_ts", "status"])
        for i in range(1, n + 1):
            cid = rng.randint(1, n_customers)
            ts = start + timedelta(seconds=rng.randrange(31 * 86400))
            w.writerow([
                f"T{i:09d}", f"CARD{rng.randint(1, 3000):06d}", f"C{cid:07d}",
                f"ACC{rng.randint(1, 5000):08d}", round(rng.uniform(3, 4000), 2),
                rng.choice(CCYS), rng.choice(MERCHANTS),
                f"{rng.randint(1000, 9999)}", ts.strftime("%Y-%m-%d %H:%M:%S"),
                "BOGUS" if i % 1000 == 0 else rng.choice(STATUS),
            ])


def gen_fx(path: Path) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ccy", "fx_rate"])
        for c, r in {"CAD": 1.0, "USD": 1.36, "EUR": 1.47, "GBP": 1.72}.items():
            w.writerow([c, r])


LOAN_PRODUCTS = ["MORTGAGE", "PERSONAL", "AUTO", "LINE_OF_CREDIT", "CREDIT_CARD"]
LOAN_STATUS = ["ACTIVE", "ACTIVE", "ACTIVE", "DELINQUENT", "CLOSED", "WRITTEN_OFF"]
COLLATERAL = ["PROPERTY", "VEHICLE", "NONE", "SECURITIES"]


def gen_loans(path: Path, n_customers: int = 2000, n: int = 8000, seed: int = 14) -> None:
    rng = random.Random(seed)
    start = date(2016, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["loan_id", "customer_id", "account_no", "product_type", "principal",
                    "ccy", "interest_rate", "term_months", "origination_date",
                    "maturity_date", "status", "outstanding_balance", "days_past_due",
                    "ltv", "collateral_type"])
        for i in range(1, n + 1):
            cid = rng.randint(1, n_customers)
            orig = start + timedelta(days=rng.randrange(3000))
            term = rng.choice([12, 24, 36, 60, 120, 240, 360])
            principal = round(rng.uniform(2000, 900000), 2)
            w.writerow([
                f"LN{i:08d}", f"C{cid:07d}", f"ACC{rng.randint(1, 5000):08d}",
                rng.choice(LOAN_PRODUCTS), principal, rng.choice(CCYS),
                round(rng.uniform(0.02, 0.19), 4), term, orig.isoformat(),
                (orig + timedelta(days=term * 30)).isoformat(),
                "BOGUS" if i % 800 == 0 else rng.choice(LOAN_STATUS),
                round(principal * rng.uniform(0.1, 1.0), 2),
                rng.choice([0, 0, 0, 15, 45, 95]), round(rng.uniform(0.1, 1.2), 2),
                rng.choice(COLLATERAL),
            ])
        for j in range(10):
            w.writerow([f"LNBAD{j:05d}", "", f"ACC9{j:07d}", "PERSONAL", 1000.0, "CAD",
                        0.05, 12, "2020-01-01", "2021-01-01", "ACTIVE", 500.0, 0, 0.5, "NONE"])


PAY_TYPES = ["SWIFT", "SEPA", "EFT", "WIRE", "INTERNAL"]
CHANNELS = ["MOBILE", "ONLINE", "BRANCH", "ATM", "API"]
PAY_STATUS = ["SETTLED", "SETTLED", "SETTLED", "PENDING", "REJECTED", "RETURNED"]
CP = ["ACME LTD", "GLOBEX", "INITECH", "UMBRELLA CORP", "WAYNE ENT"]


def gen_payments(path: Path, n_customers: int = 2000, n: int = 15000, seed: int = 13) -> None:
    rng = random.Random(seed)
    start = datetime(2026, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["payment_id", "customer_id", "account_no", "direction", "amount",
                    "ccy", "payment_type", "channel", "counterparty",
                    "beneficiary_country", "status", "value_date", "created_ts"])
        for i in range(1, n + 1):
            cid = rng.randint(1, n_customers)
            ts = start + timedelta(seconds=rng.randrange(31 * 86400))
            w.writerow([
                f"PAY{i:09d}", f"C{cid:07d}", f"ACC{rng.randint(1, 5000):08d}",
                rng.choice(["DEBIT", "CREDIT"]), round(rng.uniform(10, 50000), 2),
                rng.choice(CCYS), rng.choice(PAY_TYPES), rng.choice(CHANNELS),
                rng.choice(CP), rng.choice(COUNTRIES),
                "BOGUS" if i % 1000 == 0 else rng.choice(PAY_STATUS),
                ts.date().isoformat(), ts.strftime("%Y-%m-%d %H:%M:%S"),
            ])
        # a few null-customer rows (quarantined)
        for j in range(10):
            w.writerow([f"PAYBAD{j:05d}", "", f"ACC9{j:07d}", "DEBIT", 100.0, "CAD",
                        "EFT", "ONLINE", "ACME LTD", "CA", "SETTLED",
                        "2026-01-15", "2026-01-15 10:00:00"])


if __name__ == "__main__":
    gen_customers(DATA / "customer.csv")
    gen_cards(DATA / "cards_txn.csv")
    gen_payments(DATA / "payments.csv")
    gen_loans(DATA / "loans.csv")
    gen_fx(DATA / "fx_rates.csv")
    print("wrote", DATA / "customer.csv")
    print("wrote", DATA / "cards_txn.csv")
    print("wrote", DATA / "payments.csv")
    print("wrote", DATA / "loans.csv")
    print("wrote", DATA / "fx_rates.csv")
