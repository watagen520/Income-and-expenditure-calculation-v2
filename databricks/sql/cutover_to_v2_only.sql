-- v2専用運用への切替
-- 方針: ダッシュボード互換のため legacy 名称を VIEW として v2 へエイリアス

CREATE OR REPLACE VIEW watanabe.finance_gold.monthly_cashflow AS
SELECT * FROM watanabe.finance_gold.monthly_cashflow_v2;

CREATE OR REPLACE VIEW watanabe.finance_gold.monthly_spend_by_category AS
SELECT * FROM watanabe.finance_gold.monthly_spend_by_category_v2;

CREATE OR REPLACE VIEW watanabe.finance_gold.monthly_spend_by_owner AS
SELECT * FROM watanabe.finance_gold.monthly_spend_by_owner_v2;

CREATE OR REPLACE VIEW watanabe.finance_gold.monthly_savings_estimate AS
SELECT * FROM watanabe.finance_gold.monthly_savings_estimate_v2;

CREATE OR REPLACE VIEW watanabe.finance_gold.data_completeness AS
SELECT * FROM watanabe.finance_gold.data_completeness_v2;

CREATE OR REPLACE VIEW watanabe.finance_gold.review_queue_count AS
SELECT * FROM watanabe.finance_gold.review_queue_count_v2;

CREATE OR REPLACE VIEW watanabe.finance_gold.review_queue_detail AS
SELECT * FROM watanabe.finance_gold.review_queue_detail_v2;
