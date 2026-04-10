# TARGET_STATE.md

## Core target
The system should produce trustworthy monthly household finance outputs from 2025-09 onward.

## Canonical repair layer
Introduce a canonical stabilization layer between current Silver and Gold.
This layer may be implemented as views or internal transformation steps, but it must provide:
- typed dates and stable month key
- analysis window filtering from 2025-09-01 onward
- duplicate removal
- deterministic reclassification of obvious inflow mislabels
- stable spend inclusion semantics
- QA visibility

## Stable Gold outputs
Gold outputs must be rebuilt from the canonical layer.
At minimum, corrected Gold outputs must provide:
- monthly_cashflow
- monthly_spend_by_category
- monthly_spend_by_owner
- monthly_savings_estimate
- data_completeness
- review_queue_count

If possible, add:
- review_queue_detail

## KPI semantics
- income = true income only
- real spend = transaction_kind='expense' and is_spend_target=true
- net cashflow = income - real spend
- estimated savings = explicitly documented derivation
- excluded from spend:
  - internal_transfer
  - card_payment
  - investment
  - emoney_transfer
  - refund
  - fee

## Review strategy
The review queue should remain visible, but become manageable.
The queue should primarily contain:
- person-to-person ambiguous transfers
- ambiguous e-money movements
- unclear merchants that deterministic rules cannot safely resolve

## Category strategy
V1 does not require perfect category coverage.
V1 should:
- classify obvious categories deterministically
- materially reduce `その他`
- leave ambiguous items visible for later refinement

## Change philosophy
This is a repair task, not a redesign task.
Prefer the smallest high-confidence set of changes that produces a stable semantic foundation.