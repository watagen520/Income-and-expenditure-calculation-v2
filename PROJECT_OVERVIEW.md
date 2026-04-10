# PROJECT_OVERVIEW.md

## Project summary
This repository implements a Databricks-based household finance pipeline.
The goal is to ingest multiple CSV sources, normalize them into a unified transaction model,
avoid double counting, and produce trustworthy monthly reporting.

This is not a generic BI project.
It is a household-finance semantics project with strong business rules:
- internal transfers must not count as spend
- card settlement debits must not count as spend
- investment funding must not count as normal spend
- ambiguous transactions must remain visible in a review queue
- dashboards must show “real spend”, not raw cash movement

## Why this project exists
The user wants to use Databricks Free Edition as a personal but production-like analytics environment.
The purpose is not just to make a one-off household spreadsheet.
The purpose is to create a repeatable flow:
CSV upload -> Databricks ingestion -> normalized transaction model -> monthly Gold outputs -> dashboard / Genie exploration.

## User intent
The user wants to answer questions like:
- 今月いくら使ったか
- 何に一番使ったか
- 家族でいくら使ったか / 個人でいくら使ったか
- どれくらい貯蓄できたか
- どの月はデータが欠けているか
- どの取引は人がレビューすべきか

The user values:
- correct semantics over pretty dashboards
- minimal operational burden
- deterministic logic before AI/LLM logic
- explainable outputs
- compatibility with GitHub + Databricks Repos workflow

## Data sources
Five source types exist:
1. SMBC personal bank account
2. Japan Post Bank family account
3. Rakuten family card
4. View personal card
5. JAL DC personal card

These sources interact.
The cards settle to different bank accounts, so the pipeline must distinguish:
- card usage transactions
- card settlement debits
- internal household transfers
- investment moves
- true expenses
- true income

## Target analysis window
Business analysis scope starts from 2025-09-01.
Historical data before that may exist in Silver for lineage/debugging,
but Gold outputs for user-facing monthly reporting must be constrained to 2025-09 onward.

## Operating model
The practical workflow is:
- code is edited in VSCode
- pushed to GitHub
- Databricks Repos pulls latest code
- `databricks/lakeflow/manual_batch_medallion.py` is run manually
- Bronze / Silver / Gold outputs are refreshed

The repository is the durable source of truth.
Databricks workspace editing should not become the primary source of truth.

## Current business pain
The current dashboard looks superficially correct, but the underlying numbers are not trustworthy.
The user is primarily blocked by data quality and semantics, not by visualization.

The major pain points are:
- duplicated transactions
- wrong transaction kind on inflow bank records
- poor category assignment
- excessive review queue
- unstable Gold contract for dashboarding

## What “good enough v1” means
V1 is successful if:
- monthly cashflow is numerically trustworthy
- real spend excludes known non-spend flows
- review queue becomes manageable
- obvious categories are assigned
- Gold outputs have a stable, dashboard-safe schema
- Databricks dashboard can be rebuilt on top of corrected Gold outputs

## What is explicitly not required in this repair phase
- perfect classification of all merchants
- full automation / jobs / alerting
- LLM-based classification
- dashboard beautification
- large repo redesign
- migration to a new architecture

## Preferred implementation posture
Repair the current implementation with the smallest safe set of changes.
Do not re-platform the project.
Do not introduce speculative abstractions unless they directly reduce current defects.