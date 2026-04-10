# TASK.md

## Objective
Repair the existing Databricks household-finance pipeline with minimal architectural disruption.
The goal is to stabilize semantics before any dashboard work.

## Read first
Before doing any work, read:
- README.md
- README_IMPLEMENTATION.md
- AGENTS.md
- PROJECT_OVERVIEW.md
- SYSTEM_FACTS.md
- CURRENT_PROBLEMS.md
- TARGET_STATE.md
- DATA_SEMANTICS.md
- REPAIR_SCOPE.md
- ACCEPTANCE_TESTS.md

## Working mode
This is a difficult bug-repair task.
Plan first before coding.
Do not jump directly into broad edits.

## Required sequence
### Step 1: Diagnosis
- summarize current implementation path
- identify the specific root causes of:
  - period leakage
  - duplication
  - inflow misclassification
  - Gold contract mismatch
  - category collapse
- list the files you plan to inspect/change
- list any assumptions

### Step 2: Minimal repair plan
Propose a small, concrete repair plan that:
- preserves the current repo structure
- preserves the VSCode -> GitHub -> Databricks Repos workflow
- keeps `databricks/lakeflow/manual_batch_medallion.py` as the practical execution path
- introduces a canonical stabilization layer before Gold

### Step 3: Implementation
Implement only after the diagnosis and plan are stated.

Expected implementation shape:
- typed normalization
- deduplication
- deterministic reclassification of obvious inflow mislabels
- corrected Gold outputs from canonical semantics
- validation guidance

### Step 4: Validation
Provide validation SQL or validation instructions for Databricks.
Validation must cover:
- 2025-09 scope
- duplicate reduction
- no obvious inflow-as-expense anomalies
- correct spend exclusion
- stable monthly Gold contract
- improved review targeting

## Hard constraints
- do not redesign the project from scratch
- do not introduce LLM classification
- do not prioritize dashboard work yet
- do not silently hide unresolved records
- do not widen repo changes beyond what is necessary

## Output requirements
Before coding, output:
1. understanding summary
2. root causes
3. proposed file changes
4. acceptance criteria mapping

After coding, output:
1. changed files
2. what was fixed
3. what remains unresolved
4. exact validation steps to run in Databricks