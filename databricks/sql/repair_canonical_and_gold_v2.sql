-- 正規化 + Gold v2 修復スクリプト
-- Databricks SQL / Notebook（Unity Catalog）で実行してください。

-- =====================================================================
-- 0) 前提
-- =====================================================================
-- 入力テーブル:
--   watanabe.finance_silver.transactions
-- 分析対象期間:
--   2025-09-01 以降

-- =====================================================================
-- 1) 型正規化レイヤー
-- =====================================================================
CREATE OR REPLACE VIEW watanabe.finance_gold.canonical_typed_v2 AS
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
      ELSE amount_raw
    END AS amount_signed
  FROM base
)
SELECT *
FROM typed
WHERE txn_date >= DATE '2025-09-01';

-- =====================================================================
-- 2) 重複排除レイヤー
--    ルール: txn_id 優先、なければビジネスキー、最新 run_id を採用
-- =====================================================================
CREATE OR REPLACE VIEW watanabe.finance_gold.canonical_deduped_v2 AS
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
  FROM watanabe.finance_gold.canonical_typed_v2
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
-- 3) 決定的再分類レイヤー
-- =====================================================================
CREATE OR REPLACE VIEW watanabe.finance_gold.canonical_reclassified_v2 AS
SELECT
  txn_id,
  source_name,
  source_type,
  merchant_raw,
  description_raw,
  direction,
  owner_scope,
  CASE
    WHEN transaction_kind = 'expense' AND COALESCE(category_lv1, '') NOT IN ('', 'その他') THEN category_lv1
    WHEN transaction_kind = 'expense' AND (description_raw RLIKE 'スーパー|コンビニ|AEON|イオン|西友|イトーヨーカドー|まいばす|マルエツ' OR merchant_raw RLIKE 'スーパー|コンビニ|AEON|イオン|西友|イトーヨーカドー|まいばす|マルエツ') THEN '食費'
    WHEN transaction_kind = 'expense' AND (description_raw RLIKE '電車|JR|メトロ|バス|タクシー|ETC|高速' OR merchant_raw RLIKE 'JR|メトロ|バス|タクシー|ETC|NEXCO') THEN '交通'
    WHEN transaction_kind = 'expense' AND (description_raw RLIKE '電気|ガス|水道|通信|携帯|家賃|保険|NHK|サブスク' OR merchant_raw RLIKE '東京電力|東京ガス|NTT|KDDI|SoftBank|楽天モバイル') THEN '固定費'
    WHEN transaction_kind = 'expense' AND (description_raw RLIKE '病院|薬局|クリニック|歯科' OR merchant_raw RLIKE '病院|薬局|クリニック|歯科') THEN '健康'
    WHEN transaction_kind = 'expense' AND lower(source_type) = 'card' AND owner_scope = 'family' THEN '家族'
    WHEN transaction_kind = 'expense' AND lower(source_type) = 'card' AND owner_scope = 'personal' THEN '生活'
    WHEN transaction_kind = 'expense' AND lower(source_type) = 'bank' THEN '生活'
    WHEN transaction_kind = 'expense' THEN '生活'
    ELSE COALESCE(category_lv1, 'その他')
  END AS category_lv1,
  CASE
    WHEN transaction_kind = 'expense' AND COALESCE(category_lv2, '') <> '' THEN category_lv2
    WHEN transaction_kind = 'expense' AND lower(source_type) = 'card' THEN 'カード利用'
    WHEN transaction_kind = 'expense' AND lower(source_type) = 'bank' THEN '口座支出'
    WHEN transaction_kind = 'expense' THEN '一般支出'
    ELSE category_lv2
  END AS category_lv2,
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
        description_raw RLIKE '給料振込|賞与振込|普通預金利息|受取利子|利子|振込　ｶ[)]ﾏｸﾆｶ'
      ) THEN 'income'
    WHEN lower(source_type) = 'bank'
      AND lower(direction) = 'inflow'
      AND description_raw RLIKE '楽天証券|ﾗｸﾃﾝｼﾖｳｹﾝ'
      THEN 'investment'
    WHEN lower(source_type) = 'bank'
      AND lower(direction) = 'inflow'
      AND (description_raw RLIKE '^CT ' OR description_raw RLIKE 'ことら')
      THEN 'internal_transfer'
    WHEN lower(direction) = 'inflow' AND transaction_kind = 'expense'
      THEN 'income'
    ELSE transaction_kind
  END AS transaction_kind,
  CASE
    WHEN lower(source_type) = 'bank'
      AND lower(direction) = 'inflow'
      AND (
        description_raw RLIKE '給料振込|賞与振込|普通預金利息|受取利子|利子|振込　ｶ[)]ﾏｸﾆｶ|楽天証券|ﾗｸﾃﾝｼﾖｳｹﾝ|^CT |ことら'
      ) THEN false
    WHEN lower(direction) = 'inflow' AND transaction_kind = 'expense' THEN false
    WHEN transaction_kind = 'expense' AND COALESCE(is_spend_target, true) = true THEN true
    ELSE false
  END AS is_spend_target,
  CASE
    WHEN lower(source_type) = 'bank'
      AND lower(direction) = 'inflow'
      AND (description_raw RLIKE '^CT ' OR description_raw RLIKE 'ことら')
      THEN true
    WHEN lower(direction) = 'inflow' AND transaction_kind = 'expense'
      THEN false
    WHEN COALESCE(review_reason, '') = 'rule_not_confident'
      AND transaction_kind = 'expense'
      THEN false
    ELSE COALESCE(needs_review_flag, false)
  END AS needs_review_flag,
  CASE
    WHEN lower(source_type) = 'bank'
      AND lower(direction) = 'inflow'
      AND (description_raw RLIKE '^CT ' OR description_raw RLIKE 'ことら')
      THEN 'person_to_person_transfer'
    ELSE review_reason
  END AS review_reason
FROM watanabe.finance_gold.canonical_deduped_v2;

-- =====================================================================
-- 4) QA 可視化レイヤー
-- =====================================================================
CREATE OR REPLACE VIEW watanabe.finance_gold.canonical_qa_v2 AS
SELECT
  *,
  CASE
    WHEN transaction_kind = 'expense' AND lower(direction) = 'inflow' THEN 'expense_inflow_anomaly'
    WHEN transaction_kind IN ('internal_transfer', 'card_payment', 'investment', 'emoney_transfer', 'refund', 'fee')
      AND is_spend_target = true THEN 'excluded_kind_marked_spend'
    ELSE NULL
  END AS qa_issue
FROM watanabe.finance_gold.canonical_reclassified_v2;

-- =====================================================================
-- 5) 修正済み Gold 出力（_v2）
-- =====================================================================
CREATE OR REPLACE TABLE watanabe.finance_gold.monthly_cashflow_v2 AS
SELECT
  year_month,
  SUM(CASE WHEN transaction_kind = 'income' THEN amount_abs ELSE 0 END) AS income_amount,
  SUM(CASE WHEN transaction_kind = 'expense' AND is_spend_target = true THEN amount_abs ELSE 0 END) AS real_spend_amount,
  SUM(CASE WHEN transaction_kind = 'income' THEN amount_abs ELSE 0 END)
    - SUM(CASE WHEN transaction_kind = 'expense' AND is_spend_target = true THEN amount_abs ELSE 0 END) AS net_cashflow
FROM watanabe.finance_gold.canonical_reclassified_v2
GROUP BY year_month;

CREATE OR REPLACE TABLE watanabe.finance_gold.monthly_spend_by_category_v2 AS
SELECT
  year_month,
  COALESCE(category_lv1, 'その他') AS category_lv1,
  COALESCE(category_lv2, '未分類') AS category_lv2,
  SUM(amount_abs) AS spend_amount
FROM watanabe.finance_gold.canonical_reclassified_v2
WHERE transaction_kind = 'expense'
  AND is_spend_target = true
GROUP BY year_month, COALESCE(category_lv1, 'その他'), COALESCE(category_lv2, '未分類');

CREATE OR REPLACE TABLE watanabe.finance_gold.monthly_spend_by_owner_v2 AS
SELECT
  year_month,
  COALESCE(owner_scope, 'unknown') AS owner_scope,
  SUM(amount_abs) AS spend_amount
FROM watanabe.finance_gold.canonical_reclassified_v2
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
FROM watanabe.finance_gold.canonical_reclassified_v2
GROUP BY year_month;

CREATE OR REPLACE TABLE watanabe.finance_gold.data_completeness_v2 AS
SELECT
  year_month,
  data_completeness_status,
  COUNT(*) AS row_count
FROM watanabe.finance_gold.canonical_reclassified_v2
GROUP BY year_month, data_completeness_status;

CREATE OR REPLACE TABLE watanabe.finance_gold.review_queue_count_v2 AS
SELECT
  year_month,
  COALESCE(review_reason, 'unspecified') AS review_reason,
  COUNT(*) AS review_count
FROM watanabe.finance_gold.canonical_reclassified_v2
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
FROM watanabe.finance_gold.canonical_reclassified_v2
WHERE COALESCE(needs_review_flag, false) = true;

-- =====================================================================
-- 6) 手動検証 SQL（Acceptance Tests A-H）
-- =====================================================================

-- A1: 分析開始月が 2025-09 以降であること
SELECT MIN(year_month) AS min_month, MAX(year_month) AS max_month
FROM watanabe.finance_gold.monthly_cashflow_v2;

-- B1: 重複キー残件数
SELECT COUNT(*) AS duplicate_key_count
FROM (
  SELECT canonical_dedup_key, COUNT(*) AS c
  FROM watanabe.finance_gold.canonical_deduped_v2
  GROUP BY canonical_dedup_key
  HAVING COUNT(*) > 1
) t;

-- C1: inflow が expense の異常件数
SELECT COUNT(*) AS inflow_expense_anomaly_count
FROM watanabe.finance_gold.canonical_reclassified_v2
WHERE transaction_kind = 'expense'
  AND lower(direction) = 'inflow';

-- C2: 除外種別なのに spend 対象になっている件数
SELECT transaction_kind, COUNT(*) AS bad_rows
FROM watanabe.finance_gold.canonical_reclassified_v2
WHERE transaction_kind IN ('internal_transfer', 'card_payment', 'investment', 'emoney_transfer', 'refund', 'fee')
  AND is_spend_target = true
GROUP BY transaction_kind;

-- D1 / D2 / D3: 月次契約と net 式の整合性
SELECT
  year_month,
  income_amount,
  real_spend_amount,
  net_cashflow,
  (income_amount - real_spend_amount) AS recomputed_net
FROM watanabe.finance_gold.monthly_cashflow_v2
ORDER BY year_month;

-- E1: カテゴリ網羅率（その他比率）
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

-- F1: レビュー理由の分布
SELECT year_month, review_reason, review_count
FROM watanabe.finance_gold.review_queue_count_v2
ORDER BY year_month, review_count DESC;

-- G1: ダッシュボード安全性（スキーマ確認）
DESCRIBE TABLE watanabe.finance_gold.monthly_cashflow_v2;
DESCRIBE TABLE watanabe.finance_gold.monthly_spend_by_category_v2;
DESCRIBE TABLE watanabe.finance_gold.monthly_spend_by_owner_v2;
DESCRIBE TABLE watanabe.finance_gold.monthly_savings_estimate_v2;
DESCRIBE TABLE watanabe.finance_gold.data_completeness_v2;
DESCRIBE TABLE watanabe.finance_gold.review_queue_count_v2;
