# Data Model (starter)

The MVP ships two domains end-to-end (**customer**, **cards**); the model below
extends to the full bank.

## Domains
| Domain | Core entities |
|---|---|
| Party / Customer 360 | party, customer, contact, KYC, relationship, segment |
| Accounts | account, account_balance, account_holder, product |
| Payments & Transfers | payment, transfer, wire (SWIFT/ISO20022), direct debit |
| Cards | card, card_txn, authorization, chargeback |
| Loans & Credit | loan, disbursement, repayment, collateral, delinquency |
| Deposits | deposit, term_deposit, interest_accrual |
| Wealth / Investments | portfolio, position, holding, trade |
| Treasury & Markets | trade, position, fx_rate, curve, limit |
| Finance / GL | gl_account, journal_entry, cost_center, p&l |
| Risk | exposure, pd_lgd_ead, limit_utilization, stress_scenario |
| AML / Fraud | alert, case, transaction_monitoring, watchlist |
| Regulatory | returns (BCBS239/FRTB/CCAR), reporting_lineage |
| Reference | currency, fx_rate, calendar, branch, gl_mapping, product_code |

## MVP entities

### `silver.customer` (contract: `customer.contract.yaml`)
`customer_id (PK)`, `party_id`, `full_name (masked)`, `dob`, `email (token)`,
`phone (masked)`, `segment`, `country`, `kyc_status`, `onboard_date`.

### `silver.cards_txn` (contract: `cards_txn.contract.yaml`)
`txn_id (PK)`, `card_id`, `customer_id`, `account_no (token)`, `amount`, `ccy`,
`merchant`, `mcc`, `txn_ts`, `status`, `fx_rate`, `amount_base`.

### `gold.customer_360`
`customer_id`, `segment`, `country`, `kyc_status`, `total_spend`, `txn_count`,
`last_txn_ts`.

### `gold.card_spend_by_segment`
`segment`, `month`, `spend`, `txns`.

## Relationships
```
party 1─* customer 1─* account 1─* card 1─* card_txn
                          │                     │
                          └──── SRF / settlement ┘ (join key: account / txn)
```

## Extending
Add a domain by: writing `contracts/<feed>.contract.yaml`, adding a Bronze
ingest (Auto Loader), a Silver conform step (types + masks + dedupe), and one or
more Gold marts. Everything else (registry, reconciliation, monitoring) reuses
the shared utilities.
