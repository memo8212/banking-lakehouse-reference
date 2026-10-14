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

### `silver.payments` (contract: `payments.contract.yaml`)
`payment_id (PK)`, `customer_id`, `account_no (token)`, `direction`, `amount`,
`ccy`, `payment_type`, `channel`, `counterparty (masked)`, `beneficiary_country`,
`status`, `value_date`, `created_ts`, `fx_rate`, `amount_base`.

### `silver.loans` (contract: `loans.contract.yaml`)
`loan_id (PK)`, `customer_id`, `account_no (token)`, `product_type`, `principal`,
`ccy`, `interest_rate`, `term_months`, `origination_date`, `maturity_date`,
`status`, `outstanding_balance`, `days_past_due`, `ltv`, `collateral_type`,
`fx_rate`, `principal_base`, `outstanding_base`.

### `silver.accounts` (contract: `accounts.contract.yaml`)
`account_id (PK)`, `customer_id`, `account_no (token)`, `account_type`,
`product_code`, `ccy`, `balance`, `available_balance`, `opened_date`, `status`,
`branch`, `interest_rate`, `fx_rate`, `balance_base`, `available_base`.

### `silver.risk_exposure` (contract: `risk_exposure.contract.yaml`)
`exposure_id (PK)`, `customer_id`, `facility_id`, `exposure_type`, `ccy`, `ead`,
`pd`, `lgd`, `risk_weight`, `rating`, `limit_amount`, `as_of_date`, `status`,
`fx_rate`, `ead_base`, `limit_base`.

### `silver.aml_rules` (contract: `aml_rules.contract.yaml`)
`rule_id (PK)`, `rule_name`, `rule_type`, `severity`, `threshold`, `enabled`.

### `silver.aml_alerts` (contract: `aml_alerts.contract.yaml`)
`alert_id (PK)`, `customer_id`, `rule_id`, `rule_name`, `severity`, `source_system`,
`source_ref`, `ccy`, `amount`, `amount_base`, `score`, `alert_date`, `status`,
`assigned_to`.

### `silver.regulatory_return` (contract: `regulatory_return.contract.yaml`)
`return_id (PK)`, `report_code`, `reporting_date`, `entity`, `metric_name`, `value`,
`ccy`, `version`, `submitted_by`, `status`, `submitted_date`, `fx_rate`,
`value_base`.

### `silver.deposits` (contract: `deposits.contract.yaml`)
`deposit_id (PK)`, `account_id`, `customer_id`, `deposit_type`, `principal`, `ccy`,
`interest_rate`, `term_months`, `opened_date`, `maturity_date`, `accrued_interest`,
`status`, `branch`, `fx_rate`, `principal_base`, `accrued_interest_base`.

### `gold.customer_360`
`customer_id`, `segment`, `country`, `kyc_status`, `total_spend`, `txn_count`,
`last_txn_ts`, `total_balance`.

### `gold.card_spend_by_segment`
`segment`, `month`, `spend`, `txns`.

### `gold.payments_flow`
`channel`, `direction`, `month`, `amount_base`, `payments`.

### `gold.payments_by_segment`
`segment`, `payment_type`, `amount_base`, `payments`.

### `gold.loan_portfolio`
`product_type`, `status`, `loans`, `principal_base`, `outstanding_base`,
`avg_rate`, `avg_dpd`.

### `gold.loan_risk_by_segment`
`segment`, `loans`, `outstanding_base`, `delinquent`, `delinquency_rate`.

### `gold.account_balances`
`account_type`, `status`, `accounts`, `balance_base`, `available_base`, `avg_balance`.

### `gold.deposits_by_segment`
`segment`, `account_type`, `accounts`, `balance_base`.

### `gold.risk_exposure_by_rating`
`rating`, `exposures`, `ead_base`, `expected_loss`, `avg_pd`.

### `gold.risk_limit_utilisation`
`exposure_type`, `limit_base`, `ead_base`, `utilisation`.

### `gold.risk_by_segment`
`segment`, `ead_base`, `expected_loss`, `defaults`.

### `gold.aml_alerts_by_rule`
`rule_id`, `rule_name`, `severity`, `alerts`, `amount_base`, `avg_score`.

### `gold.aml_alert_summary`
`status`, `alerts`, `amount_base`, `avg_score`.

### `gold.aml_high_risk_customers`
`customer_id`, `alerts`, `rules`, `amount_base`, `max_score`.

### `gold.regulatory_by_report`
`report_code`, `entity`, `returns`, `value_base`, `accepted`, `acceptance_rate`.

### `gold.regulatory_submission_status`
`status`, `submissions`, `value_base`.

### `gold.regulatory_by_metric`
`report_code`, `metric_name`, `submissions`, `value_base`, `avg_value`.

### `gold.deposit_portfolio`
`deposit_type`, `status`, `deposits`, `principal_base`, `accrued_interest_base`, `avg_rate`.

### `gold.deposit_maturity_ladder`
`maturity_month`, `deposits`, `principal_base`. (Active term deposits bucketed by maturity month.)

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
