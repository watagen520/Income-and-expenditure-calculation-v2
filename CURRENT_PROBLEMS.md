# CURRENT_PROBLEMS.md

## Analysis scope leakage
The intended business analysis window starts from 2025-09-01.
However, the current Silver table includes rows from 2025-03 onward.
This means downstream Gold outputs may include out-of-scope months.

## Duplicate transaction rows
The current Silver table contains repeated business events.
Many diagnostics show 2x duplication; some rows appear 4x.
This strongly suggests missing idempotency, reprocessing duplication, or row multiplication during transformation.

## Wrong transaction_kind on bank inflows
A large number of bank inflow records are currently labeled as `expense`.
Examples include:
- salary or bonus-like inflows
- interest income
- person-to-person incoming transfers
- securities-related incoming transfers

This is semantically invalid and directly corrupts KPI logic.

## Broken spend semantics
Current spend metrics are contaminated because:
- some inflows are treated as expense
- duplicates amplify spend
- out-of-scope periods are present
- excluded flow types are not sufficiently separated before Gold

## Category collapse
Most expense rows fall into `category_lv1='その他'`.
This makes category dashboards nearly useless and hides whether rules are working.

## Review queue overload
The review queue is dominated by `rule_not_confident`.
The queue size is too large for practical manual operation.
This indicates missing deterministic rule coverage, not merely cautious design.

## Gold contract mismatch
The dashboard expects a stable month key and dashboard-safe aggregates.
Current Gold outputs are not stable enough for that use.
At least one Gold object did not support the expected `year_month` contract.

## Missing QA layer
The current medallion flow appears to go from Silver directly to Gold without a robust canonical/QA stabilization layer.
As a result, defects propagate downstream instead of being intercepted.

## Priority order of repair
Fix in this order:
1. period scope
2. duplication
3. transaction_kind and sign semantics
4. spend exclusion semantics
5. Gold contract stability
6. category quality
7. review queue usability