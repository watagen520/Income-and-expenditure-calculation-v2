# REPAIR_SCOPE.md

## Repair objective
Stabilize the current pipeline enough that the Gold outputs become trustworthy for monthly reporting.

## In scope
The following are in scope:
- inspect and correct transformation logic in the current repo
- add a canonical stabilization layer between existing Silver and Gold
- repair period filtering
- repair duplicate handling
- repair obvious `transaction_kind` mislabels
- repair Gold month-key stability
- rebuild Gold aggregates from canonical semantics
- add validation SQL or validation guidance

## Out of scope
The following are out of scope for this repair:
- full architecture rewrite
- brand-new orchestration framework
- LLM-based classification
- dashboard redesign
- production-grade monitoring framework
- exhaustive category enrichment
- manual review UI

## Files most likely allowed to change
Likely change candidates:
- `src/household_finance/processing.py`
- `src/household_finance/contracts.py`
- `src/household_finance/source_specs.py`
- `databricks/lakeflow/manual_batch_medallion.py`
- README-level repair documentation if needed

## Files / areas to avoid changing without strong reason
Avoid broad repo changes such as:
- moving major directories
- replacing the entrypoint script
- redesigning the whole source ingestion model
- changing GitHub -> Databricks Repos workflow

## Gold migration strategy
Preferred approach:
- create corrected Gold outputs with `_v2` suffix first if needed
- verify numerics
- then replace legacy outputs only after validation

Rationale:
This enables comparison and rollback.

## Dashboard scope
Do not fix the dashboard in this repair task.
Repair semantics first.
Dashboard updates should happen only after corrected Gold outputs are available.

## Change budget philosophy
Choose the smallest coherent set of changes that fixes the data semantics.
Avoid elegance-driven broad refactors.