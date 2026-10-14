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

ACCT_TYPES = ["CURRENT", "SAVINGS", "TERM", "LOAN"]
ACCT_STATUS = ["ACTIVE", "ACTIVE", "ACTIVE", "DORMANT", "CLOSED", "FROZEN"]
BRANCHES = ["BR-001", "BR-002", "BR-003", "BR-004", "BR-005"]

EXPOSURE_TYPES = ["LOAN", "DEPOSIT", "CARD", "DERIVATIVE", "COMMITMENT"]
RATINGS = ["AAA", "AA", "A", "A", "BBB", "BB", "B", "CCC", "D"]
RISK_STATUS = ["ACTIVE", "ACTIVE", "ACTIVE", "MATURED", "DEFAULT"]


def gen_risk(path: Path, n_customers: int = 2000, n: int = 10000, seed: int = 16) -> None:
    rng = random.Random(seed)
    start = date(2026, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["exposure_id", "customer_id", "facility_id", "exposure_type", "ccy",
                    "ead", "pd", "lgd", "risk_weight", "rating", "limit_amount",
                    "as_of_date", "status"])
        for i in range(1, n + 1):
            cid = rng.randint(1, n_customers)
            ead = round(rng.uniform(1000, 2_000_000), 2)
            w.writerow([
                f"EX{i:08d}", f"C{cid:07d}", f"FAC{rng.randint(1, 3000):06d}",
                rng.choice(EXPOSURE_TYPES), rng.choice(CCYS), ead,
                round(rng.uniform(0.001, 0.25), 4), round(rng.uniform(0.3, 0.6), 2),
                round(rng.uniform(0.2, 1.5), 2), rng.choice(RATINGS),
                round(ead * rng.uniform(1.0, 2.5), 2),
                (start + timedelta(days=rng.randrange(31))).isoformat(),
                "BOGUS" if i % 900 == 0 else rng.choice(RISK_STATUS),
            ])
        for j in range(10):
            w.writerow([f"EXBAD{j:05d}", "", f"FAC9{j:06d}", "LOAN", "CAD", 1000.0,
                        0.02, 0.4, 1.0, "A", 2000.0, "2026-01-01", "ACTIVE"])


def gen_accounts(path: Path, n_customers: int = 2000, n: int = 12000, seed: int = 15) -> None:
    rng = random.Random(seed)
    start = date(2015, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["account_id", "customer_id", "account_no", "account_type",
                    "product_code", "ccy", "balance", "available_balance",
                    "opened_date", "status", "branch", "interest_rate"])
        for i in range(1, n + 1):
            cid = rng.randint(1, n_customers)
            atype = rng.choice(ACCT_TYPES)
            balance = round(rng.uniform(0, 250000), 2)
            w.writerow([
                f"AC{i:08d}", f"C{cid:07d}", f"ACC{rng.randint(1, 5000):08d}",
                atype, f"{atype[:3]}01", rng.choice(CCYS), balance,
                round(balance * rng.uniform(0.8, 1.0), 2),
                (start + timedelta(days=rng.randrange(3650))).isoformat(),
                "BOGUS" if i % 900 == 0 else rng.choice(ACCT_STATUS),
                rng.choice(BRANCHES), round(rng.uniform(0.0, 0.05), 4),
            ])
        for j in range(10):
            w.writerow([f"ACBAD{j:05d}", "", f"ACC9{j:07d}", "CURRENT", "CUR01",
                        "CAD", 0.0, 0.0, "2020-01-01", "ACTIVE", "BR-001", 0.0])


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


AML_RULE_TYPES = ["THRESHOLD", "VELOCITY", "STRUCTURING", "SANCTIONS"]
AML_SEVERITY = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
AML_SOURCES = ["CARDS", "PAYMENTS", "LOANS", "ACCOUNTS"]
AML_STATUS = ["OPEN", "IN_REVIEW", "ESCALATED", "CLOSED", "DISMISSED"]
ANALYSTS = ["analyst01", "analyst02", "analyst03", "analyst04"]


def gen_aml_rules(path: Path, n: int = 10, seed: int = 17) -> None:
    rng = random.Random(seed)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["rule_id", "rule_name", "rule_type", "severity", "threshold", "enabled"])
        for i in range(1, n + 1):
            rtype = rng.choice(AML_RULE_TYPES)
            w.writerow([f"R{i:03d}", f"{rtype.title()} rule {i}", rtype,
                        rng.choice(AML_SEVERITY), round(rng.uniform(1000, 100000), 2), "Y"])


def gen_aml_alerts(path: Path, n_customers: int = 2000, n_rules: int = 10,
                   n: int = 6000, seed: int = 18) -> None:
    rng = random.Random(seed)
    start = date(2026, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["alert_id", "customer_id", "rule_id", "source_system", "source_ref",
                    "ccy", "amount", "score", "alert_date", "status", "assigned_to"])
        for i in range(1, n + 1):
            w.writerow([
                f"AL{i:08d}", f"C{rng.randint(1, n_customers):07d}",
                f"R{rng.randint(1, n_rules):03d}", rng.choice(AML_SOURCES),
                f"REF{rng.randint(1, 999999):06d}", rng.choice(CCYS),
                round(rng.uniform(100, 500000), 2), round(rng.uniform(0.1, 1.0), 3),
                (start + timedelta(days=rng.randrange(31))).isoformat(),
                "BOGUS" if i % 700 == 0 else rng.choice(AML_STATUS),
                rng.choice(ANALYSTS),
            ])
        for j in range(10):
            w.writerow([f"ALBAD{j:05d}", "", "R001", "CARDS", "REF0", "CAD", 100.0,
                        0.5, "2026-01-01", "OPEN", "analyst01"])


REPORT_CODES = ["COREP", "FINREP", "BASEL3", "CCAR", "FRTB", "LCR"]
REG_STATUS = ["DRAFT", "SUBMITTED", "SUBMITTED", "ACCEPTED", "REJECTED"]
ENTITIES = ["BANK_CA", "BANK_US", "BANK_UK", "BANK_EU"]
REG_METRICS = ["CAPITAL_RATIO", "RWA", "LIQUIDITY_COVERAGE", "LEVERAGE",
               "NET_STABLE_FUNDING", "LCR_RATIO"]


def gen_regulatory(path: Path, n: int = 5000, seed: int = 19) -> None:
    rng = random.Random(seed)
    start = date(2025, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["return_id", "report_code", "reporting_date", "entity",
                    "metric_name", "value", "ccy", "version", "submitted_by",
                    "status", "submitted_date"])
        for i in range(1, n + 1):
            rdate = start + timedelta(days=rng.randrange(365))
            w.writerow([
                f"RET{i:08d}", rng.choice(REPORT_CODES), rdate.isoformat(),
                rng.choice(ENTITIES), rng.choice(REG_METRICS),
                round(rng.uniform(0, 50_000_000_000), 2), rng.choice(CCYS),
                rng.randint(1, 3), rng.choice(ANALYSTS),
                "BOGUS" if i % 800 == 0 else rng.choice(REG_STATUS),
                (rdate + timedelta(days=rng.randrange(1, 30))).isoformat(),
            ])
        for j in range(10):
            w.writerow([f"RETBAD{j:05d}", "COREP", "2025-01-01", "BANK_CA", "RWA",
                        0.0, "CAD", 1, "analyst01", "DRAFT", "2025-01-02"])


DEPOSIT_TYPES = ["TERM", "SAVINGS", "NOTICE", "CALL"]
DEPOSIT_STATUS = ["ACTIVE", "ACTIVE", "ACTIVE", "MATURED", "WITHDRAWN", "ROLLED"]


def gen_deposits(path: Path, n_customers: int = 2000, n: int = 9000, seed: int = 20) -> None:
    rng = random.Random(seed)
    start = date(2019, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["deposit_id", "account_id", "customer_id", "deposit_type",
                    "principal", "ccy", "interest_rate", "term_months", "opened_date",
                    "maturity_date", "accrued_interest", "status", "branch"])
        for i in range(1, n + 1):
            cid = rng.randint(1, n_customers)
            opened = start + timedelta(days=rng.randrange(2000))
            term = rng.choice([6, 12, 24, 36, 60])
            principal = round(rng.uniform(1000, 500000), 2)
            w.writerow([
                f"DEP{i:08d}", f"AC{rng.randint(1, 12000):08d}", f"C{cid:07d}",
                rng.choice(DEPOSIT_TYPES), principal, rng.choice(CCYS),
                round(rng.uniform(0.005, 0.055), 4), term, opened.isoformat(),
                (opened + timedelta(days=term * 30)).isoformat(),
                round(principal * rng.uniform(0.0, 0.05), 2),
                "BOGUS" if i % 900 == 0 else rng.choice(DEPOSIT_STATUS),
                rng.choice(BRANCHES),
            ])
        for j in range(10):
            w.writerow([f"DEPBAD{j:05d}", f"AC9{j:07d}", "", "TERM", 1000.0, "CAD",
                        0.02, 12, "2020-01-01", "2021-01-01", 0.0, "ACTIVE", "BR-001"])


if __name__ == "__main__":
    gen_customers(DATA / "customer.csv")
    gen_cards(DATA / "cards_txn.csv")
    gen_payments(DATA / "payments.csv")
    gen_loans(DATA / "loans.csv")
    gen_accounts(DATA / "accounts.csv")
    gen_risk(DATA / "risk_exposure.csv")
    gen_aml_rules(DATA / "aml_rules.csv")
    gen_aml_alerts(DATA / "aml_alerts.csv")
    gen_regulatory(DATA / "regulatory_return.csv")
    gen_deposits(DATA / "deposits.csv")
    gen_fx(DATA / "fx_rates.csv")
    print("wrote", DATA / "customer.csv")
    print("wrote", DATA / "cards_txn.csv")
    print("wrote", DATA / "payments.csv")
    print("wrote", DATA / "loans.csv")
    print("wrote", DATA / "accounts.csv")
    print("wrote", DATA / "risk_exposure.csv")
    print("wrote", DATA / "aml_rules.csv")
    print("wrote", DATA / "aml_alerts.csv")
    print("wrote", DATA / "regulatory_return.csv")
    print("wrote", DATA / "deposits.csv")
    print("wrote", DATA / "fx_rates.csv")
