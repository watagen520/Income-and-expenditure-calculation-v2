-- Canonical + Gold v2 repair script
-- Run in Databricks SQL / notebook against Unity Catalog.

-- =====================================================================
-- 0) Base assumptions
-- =====================================================================
-- Source table:
--   watanabe.finance_silver.transactions
-- Analysis window:
--   2025-09-01 onward

-- =====================================================================
-- 1) Canonical typed layer
-- =====================================================================
CREATE OR REPLACE VIEW watanabe.finance_gold.canonical_typed_v1 AS
WITH base AS (
  SELECT
    txn_id,
    source_name,
    source_type,
    merchant_raw,
    description_raw,
    transaction_kind,
    direction,
    owner_scope,
    category_lv1,
    category_lv2,
    data_completeness_status,
    statement_month,
    funding_account,
    payment_due_date,
    needs_review_flag,
    review_reason,
    is_spend_target,
    matched_group_id,
    run_id,
    CAST(txn_date AS DATE) AS txn_date,
    CAST(amount AS DOUBLE) AS amount_raw
  FROM watanabe.finance_silver.transactions
), typed AS (
  SELECT
    *,
    DATE_FORMAT(txn_date, 'yyyy-MM') AS year_month,
    ABS(amount_raw) AS amount_abs,
    CASE
      WHEN lower(direction) = 'inflow' THEN ABS(amount_raw)
      WHEN lower(direction) = 'outflow' THEN -ABS(amount_raw)
      ELSE CASE WHEN amount_raw >= 0 THEN amount_raw ELSE amount_raw END
    END AS amount_signed
  FROM base
)
SELECT *
FROM typed
WHERE txn_date >= DATE '2025-09-01';

-- =====================================================================
-- 2) Canonical dedup layer
--    Rule: prefer txn_id; fallback to business key; keep latest run_id
-- =====================================================================
CREATE OR REPLACE VIEW watanabe.finance_gold.canonical_deduped_v1 AS
WITH keyed AS (
  SELECT
    *,
    COALESCE(
      NULLIF(TRIM(txn_id), ''),
      SHA2(CONCAT_WS('||',
        COALESCE(source_name, ''),
        COALESCE(CAST(txn_date AS STRING), ''),
        COALESCE(CAST(amount_raw AS STRING), ''),
        COALESCE(merchant_raw, ''),
        COALESCE(description_raw, ''),
        COALESCE(transaction_kind, '')
      ), 256)
    ) AS canonical_dedup_key
  FROM watanabe.finance_gold.canonical_typed_v1
), ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY canonical_dedup_key
      ORDER BY run_id DESC, txn_date DESC
    ) AS rn
  FROM keyed
)
SELECT *
FROM ranked
WHERE rn = 1;

-- =====================================================================
-- 3) Deterministic reclassification layer
-- =====================================================================
CREATE OR REPLACE VIEW watanabe.finance_gold.canonical_reclassified_v1 AS
SELECT
  txn_id,
  source_name,
  source_type,
  merchant_raw,
  description_raw,
  direction,
  owner_scope,
  category_lv1,
  category_lv2,
  data_completeness_status,
  statement_month,
  funding_account,
  payment_due_date,
  matched_group_id,
  run_id,
  txn_date,
  year_month,
  amount_raw,
  amount_abs,
  amount_signed,
  canonical_dedup_key,
  CASE
    WHEN lower(source_type) = 'bank'
      AND lower(direction) = 'inflow'
      AND (
        description_raw RLIKE '給料振込|賞与振込|普通預金利息|受取利子|利子|振込　ｶ\)ﾏｸﾆｶ'
      ) THEN 'income'
    WHEN lower(source_type) = 'bank'
      AND lower(direction) = 'inflow'
      AND description_raw RLIKE '楽天証券|ﾗｸﾃﾝｼﾖｳｹﾝ'
      THEN 'investment'
    WHEN lower(source_type) = 'bank'
      AND lower(direction) = 'inflow'
      AND (description_raw RLIKE '^CT ' OR description_raw RLIKE 'ことら')
      THEN 'internal_transfer'
    ELSE transaction_kind
  END AS transaction_kind,
  CASE
    WHEN lower(source_type) = 'bank'
      AND lower(direction) = 'inflow'
      AND (
        description_raw RLIKE '給料振込|賞与振込|普通預金利息|受取利子|利子|振込　ｶ\)ﾏｸﾆｶ|楽天証券|ﾗｸﾃﾝｼﾖｳｹﾝ|^CT |ことら'
      ) THEN false
    WHEN transaction_kind = 'expense' AND COALESCE(is_spend_target, true) = true THEN true
    ELSE false
  END AS is_spend_target,
  CASE
    WHEN lower(source_type) = 'bank'
      AND lower(direction) = 'inflow'
      AND (description_raw RLIKE '^CT ' OR description_raw RLIKE 'ことら')
      THEN true
    ELSE COALESCE(needs_review_flag, false)
  END AS needs_review_flag,
  CASE
    WHEN lower(source_type) = 'bank'
      AND lower(direction) = 'inflow'
      AND (description_raw RLIKE '^CT ' OR description_raw RLIKE 'ことら')
      THEN 'person_to_person_transfer'
    ELSE review_reason
  END AS review_reason
FROM watanabe.finance_gold.canonical_deduped_v1;

-- =====================================================================
-- 4) QA visibility layer
-- =====================================================================
CREATE OR REPLACE VIEW watanabe.finance_gold.canonical_qa_v1 AS
SELECT
  *,
  CASE
    WHEN transaction_kind = 'expense' AND lower(direction) = 'inflow' THEN 'expense_inflow_anomaly'
    WHEN transaction_kind IN ('internal_transfer', 'card_payment', 'investment', 'emoney_transfer', 'refund', 'fee')
      AND is_spend_target = true THEN 'excluded_kind_marked_spend'
    ELSE NULL
  END AS qa_issue
FROM watanabe.finance_gold.canonical_reclassified_v1;

-- =====================================================================
-- 5) Repaired Gold outputs (_v2)
-- =====================================================================

CREATE OR REPLACE TABLE watanabe.finance_gold.monthly_cashflow_v2 AS
SELECT
  year_month,
  SUM(CASE WHEN transaction_kind = 'income' THEN amount_abs ELSE 0 END) AS income_amount,
  SUM(CASE WHEN transaction_kind = 'expense' AND is_spend_target = true THEN amount_abs ELSE 0 END) AS real_spend_amount,
  SUM(CASE WHEN transaction_kind = 'income' THEN amount_abs ELSE 0 END)
    - SUM(CASE WHEN transaction_kind = 'expense' AND is_spend_target = true THEN amount_abs ELSE 0 END) AS net_cashflow
FROM watanabe.finance_gold.canonical_reclassified_v1
GROUP BY year_month;

CREATE OR REPLACE TABLE watanabe.finance_gold.monthly_spend_by_category_v2 AS
SELECT
  year_month,
  COALESCE(category_lv1, 'その他') AS category_lv1,
  COALESCE(category_lv2, '未分類') AS category_lv2,
  SUM(amount_abs) AS spend_amount
FROM watanabe.finance_gold.canonical_reclassified_v1
WHERE transaction_kind = 'expense'
  AND is_spend_target = true
GROUP BY year_month, COALESCE(category_lv1, 'その他'), COALESCE(category_lv2, '未分類');

CREATE OR REPLACE TABLE watanabe.finance_gold.monthly_spend_by_owner_v2 AS
SELECT
  year_month,
  COALESCE(owner_scope, 'unknown') AS owner_scope,
  SUM(amount_abs) AS spend_amount
FROM watanabe.finance_gold.canonical_reclassified_v1
WHERE transaction_kind = 'expense'
  AND is_spend_target = true
GROUP BY year_month, COALESCE(owner_scope, 'unknown');

CREATE OR REPLACE TABLE watanabe.finance_gold.monthly_savings_estimate_v2 AS
SELECT
  year_month,
  SUM(CASE WHEN transaction_kind = 'income' THEN amount_abs ELSE 0 END) AS income_amount,
  SUM(CASE WHEN transaction_kind = 'expense' AND is_spend_target = true THEN amount_abs ELSE 0 END) AS real_spend_amount,
  SUM(CASE WHEN transaction_kind = 'income' THEN amount_abs ELSE 0 END)
    - SUM(CASE WHEN transaction_kind = 'expense' AND is_spend_target = true THEN amount_abs ELSE 0 END) AS estimated_savings
FROM watanabe.finance_gold.canonical_reclassified_v1
GROUP BY year_month;

CREATE OR REPLACE TABLE watanabe.finance_gold.data_completeness_v2 AS
SELECT
  year_month,
  data_completeness_status,
  COUNT(*) AS row_count
FROM watanabe.finance_gold.canonical_reclassified_v1
GROUP BY year_month, data_completeness_status;

CREATE OR REPLACE TABLE watanabe.finance_gold.review_queue_count_v2 AS
SELECT
  year_month,
  COALESCE(review_reason, 'unspecified') AS review_reason,
  COUNT(*) AS review_count
FROM watanabe.finance_gold.canonical_reclassified_v1
WHERE COALESCE(needs_review_flag, false) = true
GROUP BY year_month, COALESCE(review_reason, 'unspecified');

CREATE OR REPLACE TABLE watanabe.finance_gold.review_queue_detail_v2 AS
SELECT
  year_month,
  txn_date,
  source_name,
  source_type,
  owner_scope,
  direction,
  transaction_kind,
  amount_abs,
  merchant_raw,
  description_raw,
  review_reason,
  run_id
FROM watanabe.finance_gold.canonical_reclassified_v1
WHERE COALESCE(needs_review_flag, false) = true;

-- =====================================================================
-- 6) Manual validation SQL (Acceptance Tests A-H)
-- =====================================================================

-- A1: scope starts at 2025-09
SELECT MIN(year_month) AS min_month, MAX(year_month) AS max_month
FROM watanabe.finance_gold.monthly_cashflow_v2;

-- B1: duplicate count by canonical key after dedup
SELECT COUNT(*) AS duplicate_key_count
FROM (
  SELECT canonical_dedup_key, COUNT(*) AS c
  FROM watanabe.finance_gold.canonical_deduped_v1
  GROUP BY canonical_dedup_key
  HAVING COUNT(*) > 1
) t;

-- C1: inflow-as-expense anomalies
SELECT COUNT(*) AS inflow_expense_anomaly_count
FROM watanabe.finance_gold.canonical_reclassified_v1
WHERE transaction_kind = 'expense'
  AND lower(direction) = 'inflow';

-- C2: excluded kinds accidentally included in spend
SELECT transaction_kind, COUNT(*) AS bad_rows
FROM watanabe.finance_gold.canonical_reclassified_v1
WHERE transaction_kind IN ('internal_transfer', 'card_payment', 'investment', 'emoney_transfer', 'refund', 'fee')
  AND is_spend_target = true
GROUP BY transaction_kind;

-- D1 / D2 / D3: cashflow contract and formula consistency
SELECT
  year_month,
  income_amount,
  real_spend_amount,
  net_cashflow,
  (income_amount - real_spend_amount) AS recomputed_net
FROM watanabe.finance_gold.monthly_cashflow_v2
ORDER BY year_month;

-- E1: category coverage and 'その他' share
SELECT
  year_month,
  SUM(CASE WHEN category_lv1 = 'その他' THEN spend_amount ELSE 0 END) AS other_spend,
  SUM(spend_amount) AS total_spend,
  CASE WHEN SUM(spend_amount) = 0 THEN 0
       ELSE SUM(CASE WHEN category_lv1 = 'その他' THEN spend_amount ELSE 0 END) / SUM(spend_amount)
  END AS other_share
FROM watanabe.finance_gold.monthly_spend_by_category_v2
GROUP BY year_month
ORDER BY year_month;

-- F1: targeted review distribution
SELECT year_month, review_reason, review_count
FROM watanabe.finance_gold.review_queue_count_v2
ORDER BY year_month, review_count DESC;

-- G1: dashboard-safe schema smoke check
DESCRIBE TABLE watanabe.finance_gold.monthly_cashflow_v2;
DESCRIBE TABLE watanabe.finance_gold.monthly_spend_by_category_v2;
DESCRIBE TABLE watanabe.finance_gold.monthly_spend_by_owner_v2;
DESCRIBE TABLE watanabe.finance_gold.monthly_savings_estimate_v2;
DESCRIBE TABLE watanabe.finance_gold.data_completeness_v2;
DESCRIBE TABLE watanabe.finance_gold.review_queue_count_v2;

