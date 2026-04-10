-- v2受け入れ判定（複数観点）
-- 目的: 単一観点での誤判定を防ぐため、複数フレームで品質を確認する。

CREATE OR REPLACE TABLE watanabe.finance_gold.acceptance_gate_v2 AS
WITH base AS (
  SELECT *
  FROM watanabe.finance_gold.canonical_reclassified_v1
),
checks AS (
  SELECT 'A1_scope_min_month' AS check_id,
         CASE WHEN MIN(year_month) >= '2025-09' THEN 'PASS' ELSE 'FAIL' END AS status,
         CONCAT('min_year_month=', MIN(year_month)) AS detail
  FROM base

  UNION ALL

  SELECT 'B1_duplicate_keys',
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
         CONCAT('duplicate_keys=', COUNT(*))
  FROM (
    SELECT canonical_dedup_key
    FROM watanabe.finance_gold.canonical_deduped_v1
    GROUP BY canonical_dedup_key
    HAVING COUNT(*) > 1
  ) d

  UNION ALL

  SELECT 'C1_inflow_expense_anomaly',
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
         CONCAT('rows=', COUNT(*))
  FROM base
  WHERE transaction_kind = 'expense' AND lower(direction) = 'inflow'

  UNION ALL

  SELECT 'C2_excluded_kind_spend',
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
         CONCAT('rows=', COUNT(*))
  FROM base
  WHERE transaction_kind IN ('internal_transfer','card_payment','investment','emoney_transfer','refund','fee')
    AND is_spend_target = true

  UNION ALL

  SELECT 'D2_cashflow_formula',
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
         CONCAT('mismatch_months=', COUNT(*))
  FROM (
    SELECT year_month
    FROM watanabe.finance_gold.monthly_cashflow_v2
    WHERE net_cashflow <> (income_amount - real_spend_amount)
  ) f

  UNION ALL

  SELECT 'E1_other_ratio_le_20pct',
         CASE WHEN MAX(other_share) <= 0.20 THEN 'PASS' ELSE 'FAIL' END,
         CONCAT('max_other_share=', CAST(MAX(other_share) AS STRING))
  FROM (
    SELECT
      year_month,
      CASE WHEN SUM(spend_amount)=0 THEN 0.0
           ELSE SUM(CASE WHEN category_lv1='その他' THEN spend_amount ELSE 0 END)/SUM(spend_amount)
      END AS other_share
    FROM watanabe.finance_gold.monthly_spend_by_category_v2
    GROUP BY year_month
  ) c

  UNION ALL

  SELECT 'F1_review_queue_le_20',
         CASE WHEN MAX(monthly_review_count) <= 20 THEN 'PASS' ELSE 'FAIL' END,
         CONCAT('max_monthly_review_count=', CAST(MAX(monthly_review_count) AS STRING))
  FROM (
    SELECT year_month, SUM(review_count) AS monthly_review_count
    FROM watanabe.finance_gold.review_queue_count_v2
    GROUP BY year_month
  ) r

  UNION ALL

  -- 追加観点1: データ完全性ステータスの偏り監視
  SELECT 'X1_data_completeness_presence',
         CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
         CONCAT('distinct_status=', COUNT(*))
  FROM (
    SELECT DISTINCT data_completeness_status
    FROM watanabe.finance_gold.data_completeness_v2
  ) s

  UNION ALL

  -- 追加観点2: 収支スケール異常（異常に大きい月）
  SELECT 'X2_spend_scale_sanity',
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END,
         CONCAT('months_over_5m=', COUNT(*))
  FROM watanabe.finance_gold.monthly_cashflow_v2
  WHERE real_spend_amount > 5000000
)
SELECT current_timestamp() AS checked_at, *
FROM checks;

CREATE OR REPLACE VIEW watanabe.finance_gold.acceptance_gate_v2_summary AS
SELECT
  checked_at,
  SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) AS fail_count,
  SUM(CASE WHEN status='WARN' THEN 1 ELSE 0 END) AS warn_count,
  SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) AS pass_count
FROM watanabe.finance_gold.acceptance_gate_v2
GROUP BY checked_at;
