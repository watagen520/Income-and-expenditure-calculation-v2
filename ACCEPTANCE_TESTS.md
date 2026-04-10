# ACCEPTANCE_TESTS.md

## Purpose
These are the minimum validation conditions for considering the repair successful.

## A. Scope validation
### Test A1: Gold analysis scope starts at 2025-09
Expected:
- no Gold user-facing monthly rows before 2025-09

## B. Duplicate validation
### Test B1: canonical duplicate count
Expected:
- duplicate rows reduced to zero, or to a clearly documented and justified exception set

Suggested check:
- duplicate count by canonical business key
- before/after comparison

## C. transaction_kind validation
### Test C1: no inflow-as-expense anomalies
Expected:
- zero rows where:
  - transaction_kind='expense'
  - direction='inflow'
  - and business semantics clearly imply income/internal_transfer/investment

### Test C2: excluded types are not spend
Expected:
- `internal_transfer`, `card_payment`, `investment`, `emoney_transfer`, `refund`, `fee` never contribute to real spend

## D. KPI contract validation
### Test D1: monthly cashflow has stable month key
Expected:
- Gold monthly cashflow output exposes a stable month key (`year_month` preferred)

### Test D2: net cashflow formula
Expected:
- `net_cashflow = income_amount - real_spend_amount`

### Test D3: spend positivity in presentation outputs
Expected:
- user-facing spend metrics are positive display numbers, not negative signed amounts

## E. Category quality validation
### Test E1: category coverage improvement
Expected:
- obvious expense records no longer collapse almost entirely into `その他`

Practical v1 threshold:
- `その他` still allowed
- but should materially improve from the current state
- or, if not yet improved, unresolved categories must be explicitly documented as a remaining limitation

## F. Review queue validation
### Test F1: review queue volume becomes more targeted
Expected:
- `rule_not_confident` volume is materially reduced
- ambiguous person-to-person and e-money items remain visible

## G. Gold contract validation
### Test G1: Gold outputs are dashboard-safe
Expected:
- Gold outputs used for dashboarding expose stable fields and documented semantics
- no dependency on raw CSV access or ephemeral temp views

## H. Manual validation SQL requirement
Codex must provide Databricks SQL snippets for validating:
1. scope
2. duplicates
3. transaction_kind distribution
4. spend exclusion logic
5. monthly cashflow output
6. category coverage
7. review queue counts