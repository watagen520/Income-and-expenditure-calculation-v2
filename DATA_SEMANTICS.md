# DATA_SEMANTICS.md

## Month key
Canonical month key must be derived from `txn_date`, not from ad hoc dashboard logic.
Preferred canonical field:
- `year_month` in `yyyy-MM` format

`statement_month` may still exist for card/billing lineage, but dashboard monthly reporting should rely on canonical transaction month unless there is a specific business reason not to.

## Signed amount semantics
Canonical transaction handling should distinguish:
- raw observed amount
- absolute amount for display
- signed amount for directional reasoning

Recommended canonical fields:
- `amount_raw`
- `amount_abs`
- `amount_signed`

Interpretation:
- inflow => positive signed amount
- outflow => negative signed amount

## transaction_kind definitions
Use exactly one of:
- income
- expense
- internal_transfer
- card_payment
- investment
- emoney_transfer
- refund
- fee
- unknown

Definitions:
- income: real income to the household / reporting scope
- expense: true spend that should count toward normal spend metrics
- internal_transfer: movement between known household-controlled accounts or equivalent rails
- card_payment: bank-side settlement of card bill; do not count as spend because card usage already represents the spend
- investment: movement to or from investment funding context; exclude from normal spend
- emoney_transfer: top-up or movement into an e-money rail such as PayPay when downstream use is not visible
- refund: reversal or repayment that should not be treated as normal spend
- fee: explicit financial fee
- unknown: unresolved classification

## is_spend_target
`is_spend_target = true` only when the transaction is a true normal spend transaction.
The following must default to `false`:
- income
- internal_transfer
- card_payment
- investment
- emoney_transfer
- refund
- fee

Only `expense` can generally be `true`.

## High-confidence reclassification rules
### Bank inflow -> income
If:
- `source_type='bank'`
- `direction='inflow'`
- and `description_raw` matches any of:
  - `給料振込`
  - `賞与振込`
  - `普通預金利息`
  - `受取利子`
  - `利子`
  - `振込　ｶ)ﾏｸﾆｶ`
Then reclassify to:
- `transaction_kind='income'`
- `is_spend_target=false`

### Bank inflow -> investment
If:
- `source_type='bank'`
- `direction='inflow'`
- and `description_raw` matches:
  - `楽天証券`
  - `ﾗｸﾃﾝｼﾖｳｹﾝ`
Then reclassify to:
- `transaction_kind='investment'`
- `is_spend_target=false`

### Bank inflow -> internal_transfer
If:
- `source_type='bank'`
- `direction='inflow'`
- and `description_raw` matches:
  - `^CT `
  - `ことら`
Then reclassify to:
- `transaction_kind='internal_transfer'`
- `is_spend_target=false`
- `needs_review_flag=true`
- `review_reason='person_to_person_transfer'`

Rationale:
These are not safe to count as spend.
They are also not always safe to silently classify as fully resolved income.
Keeping them reviewable is preferred.

## Category semantics
V1 category semantics:
- category_lv1 is required for most expense rows
- category_lv2 is desirable but not required for all rows
- `その他` is allowed, but should not dominate once obvious rules are applied

Suggested broad categories:
- 生活
- 食費
- 交通
- 車
- 家族
- 交際
- 娯楽
- 健康
- 固定費
- デジタル
- 投資
- その他

## Savings semantics
Recommended v1 definition:
- `estimated_savings = income - real_spend`

Do not include:
- internal_transfer
- card_payment
- investment funding
- emoney top-up
in normal spend.
Do not silently treat investment movement as savings unless that is explicitly modeled later.
This keeps v1 conservative and explainable.

## Duplicate semantics
Preferred duplicate handling rule:
1. use `txn_id` if it is stable and present
2. otherwise fallback to a composite business key:
   - source_name
   - txn_date
   - amount
   - merchant_raw
   - description_raw
   - transaction_kind
3. keep the latest `run_id` when duplicates conflict

## Canonical implementation recommendation
Preferred canonical stages:
- typed
- deduped
- reclassified
- qa

Preferred implementation form:
- start with views, not materialized tables
Reason:
- minimal structural disruption
- easy comparison with existing Gold
- lower operational burden in Databricks Repos workflow