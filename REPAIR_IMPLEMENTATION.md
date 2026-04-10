# Repair implementation (minimal, repo-safe)

## 1) Understanding summary
This repository is currently documentation-only in this checkout, but the intended runtime is a Databricks medallion pipeline where Silver already exists (`watanabe.finance_silver.transactions`) and Gold tables are built for dashboards.

Given the constraints, the smallest safe repair is to provide a **drop-in SQL repair script** that introduces a canonical stabilization layer (as views) and then rebuilds Gold outputs from that canonical layer, while preserving:

- VSCode -> GitHub -> Databricks Repos workflow
- manual Databricks execution model
- existing semantics and table names (with `_v2` outputs first)

## 2) Root causes (mapped from observed symptoms)
1. **Period leakage**: Gold aggregates are not constrained to business scope (`2025-09-01` onward).
2. **Duplicate amplification**: no canonical dedup stage before Gold aggregation.
3. **Inflow mislabeling**: deterministic bank-inflow rules are missing/insufficient, leaving obvious inflows as `expense`.
4. **Spend contamination**: spend KPI logic is applied too early/loosely, allowing excluded kinds into spend.
5. **Gold contract instability**: monthly outputs do not consistently expose stable `year_month` and explicit KPI derivations.
6. **Review queue overload**: too many generic `rule_not_confident` without targeted routing for ambiguous transfer patterns.

## 3) Defects -> likely code/data locations
In the current checkout, executable pipeline files are not present. Therefore the repair is targeted to Databricks SQL objects over the existing observed Silver table:

- Source: `watanabe.finance_silver.transactions`
- New canonical views: `watanabe.finance_gold.canonical_*`
- Repaired Gold outputs: `watanabe.finance_gold.*_v2`

## 4) Smallest safe repair plan
1. Add typed canonical view with normalized dates, month key, and signed/absolute amounts.
2. Add dedup canonical view using `txn_id` primary preference and documented business-key fallback; keep latest `run_id`.
3. Add deterministic reclassification view for high-confidence inflow fixes.
4. Add QA view for anomaly visibility.
5. Rebuild Gold `_v2` aggregates from canonical semantics only.
6. Provide validation SQL snippets for all acceptance checks.

## 5) Acceptance-tests mapping (how this plan satisfies tests)
- **A1 Scope**: canonical layer filters to `txn_date >= '2025-09-01'`.
- **B1 Duplicates**: dedup CTE retains one row per canonical key with latest `run_id`.
- **C1/C2 Transaction semantics**: inflow reclassification and explicit spend filter (`transaction_kind='expense' AND is_spend_target=true`).
- **D1/D2/D3 KPI contract**: stable `year_month`; explicit net formula; spend shown as positive display metric.
- **E1 Category quality**: category outputs from repaired spend semantics; includes visibility queries for `その他` share.
- **F1 Review targeting**: ambiguous transfer patterns get targeted review reason.
- **G1 Dashboard-safe Gold**: stable `_v2` outputs with documented fields.
- **H Manual validation SQL**: included in dedicated section of script.

## Notes
- This is intentionally SQL-first to minimize structural disruption.
- Once validated, `_v2` objects can replace legacy Gold outputs in a controlled cutover.
