# SYSTEM_FACTS.md

## Repository and runtime facts
The repository already exists and is actively used.
The current execution flow must remain compatible with:
VSCode -> GitHub push -> Databricks Repos pull -> run `databricks/lakeflow/manual_batch_medallion.py`.

## Current repository structure
Known relevant paths:
- `README.md`
- `README_IMPLEMENTATION.md`
- `AGENTS.md`
- `src/household_finance/contracts.py`
- `src/household_finance/source_specs.py`
- `src/household_finance/processing.py`
- `databricks/lakeflow/manual_batch_medallion.py`
- `databricks/genie/GENIE_SETUP.md`

## Current tables actually observed
Silver:
- `watanabe.finance_silver.transactions`

Gold:
- `watanabe.finance_gold.data_completeness`
- `watanabe.finance_gold.monthly_cashflow`
- `watanabe.finance_gold.monthly_savings_estimate`
- `watanabe.finance_gold.monthly_spend_by_category`
- `watanabe.finance_gold.monthly_spend_by_owner`
- `watanabe.finance_gold.review_queue_count`

Bronze existence is known but is not the focus of this repair phase.

## Current Silver column contract actually observed
Observed columns in `watanabe.finance_silver.transactions`:
- amount (double)
- category_lv1 (string)
- category_lv2 (string)
- data_completeness_status (string)
- description_raw (string)
- direction (string)
- funding_account (string)
- is_spend_target (boolean)
- matched_group_id (string)
- merchant_raw (string)
- needs_review_flag (boolean)
- owner_scope (string)
- payment_due_date (string)
- review_reason (string)
- source_name (string)
- source_type (string)
- statement_month (string)
- transaction_kind (string)
- txn_date (string)
- txn_id (string)
- run_id (string)

## Observed source behavior from diagnostics
- Silver currently includes records from 2025-03 through 2026-04
- many rows are duplicated, usually 2x, sometimes 4x
- `expense` includes inflow bank records with positive values
- category coverage is effectively collapsed to `その他`
- Gold does not expose a stable `year_month`-style dashboard contract

## Current implementation assumption
There is already an end-to-end medallion pipeline in place.
This is a repair task, not a greenfield implementation.

## Non-negotiable system constraints
- Preserve the current repo-based workflow
- Keep Databricks manual execution via existing script path
- Do not require a brand-new orchestration layer
- Do not depend on chat-only context; repository files must be the durable instructions