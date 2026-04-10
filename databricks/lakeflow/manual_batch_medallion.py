"""Lakeflow手動実行エントリポイント（v2専用運用）。

実行順序:
1. repair_canonical_and_gold_v2.sql を実行して canonical / Gold _v2 を構築
2. acceptance_gate_v2.sql で複数観点の検証結果を生成
3. cutover_to_v2_only.sql で legacy 名称を v2 へ切替（VIEWエイリアス）

使い方（Databricksノートブック or Pythonタスク）:
  %run /Repos/<user>/<repo>/databricks/lakeflow/manual_batch_medallion.py

前提:
- SparkSession が有効であること
- Unity Catalog に書き込み権限があること
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from pyspark.sql import SparkSession


def _resolve_repo_root() -> Path:
    """Repo root を解決する。

    優先順:
    1) 通常Python実行（__file__あり）
    2) Databricks %run 実行（__file__なし）: notebookPath から推定
    """
    if "__file__" in globals():
        return Path(__file__).resolve().parents[2]

    # Databricks notebook %run では __file__ が未定義なため、context から補完
    dbutils_obj = globals().get("dbutils")
    if dbutils_obj is not None:
        notebook_path = (
            dbutils_obj.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        # 例: /Repos/<user>/<repo>/databricks/lakeflow/manual_batch_medallion
        marker = "/databricks/lakeflow/"
        if marker in notebook_path:
            repo_ws_path = notebook_path.split(marker)[0]
            # Workspace Files は /Workspace 配下でローカルパスとして参照可能
            return Path("/Workspace") / repo_ws_path.lstrip("/")

    raise RuntimeError(
        "Repo root を解決できませんでした。"
        "Databricks Repos配下から %run 実行するか、Pythonタスクで file:/Workspace/... を指定してください。"
    )


def _split_sql_statements(sql_text: str) -> Iterable[str]:
    """非常に単純なSQL分割。コメントと空行を除外して `;` 単位で実行する。"""
    chunks = sql_text.split(";")
    for chunk in chunks:
        stmt = chunk.strip()
        if not stmt:
            continue
        if stmt.startswith("--") and "\n" not in stmt:
            continue
        yield stmt


def _run_sql_file(spark: SparkSession, path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    for stmt in _split_sql_statements(text):
        spark.sql(stmt)




def _drop_legacy_object_if_exists(spark: SparkSession, full_name: str) -> None:
    """table/view どちらでも安全に置換できるよう事前削除する。"""
    try:
        spark.sql(f"DROP VIEW IF EXISTS {full_name}")
    except Exception:
        pass
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    except Exception:
        pass


def _prepare_cutover_targets(spark: SparkSession) -> None:
    targets = [
        "watanabe.finance_gold.monthly_cashflow",
        "watanabe.finance_gold.monthly_spend_by_category",
        "watanabe.finance_gold.monthly_spend_by_owner",
        "watanabe.finance_gold.monthly_savings_estimate",
        "watanabe.finance_gold.data_completeness",
        "watanabe.finance_gold.review_queue_count",
        "watanabe.finance_gold.review_queue_detail",
    ]
    for name in targets:
        _drop_legacy_object_if_exists(spark, name)
def main() -> None:
    repo_root = _resolve_repo_root()
    sql_dir = repo_root / "databricks" / "sql"

    build_sql = sql_dir / "repair_canonical_and_gold_v2.sql"
    gate_sql = sql_dir / "acceptance_gate_v2.sql"
    cutover_sql = sql_dir / "cutover_to_v2_only.sql"

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    print(f"Repo root: {repo_root}")
    print("[1/3] Build: canonical + Gold _v2 を構築します")
    _run_sql_file(spark, build_sql)

    print("[2/3] Gate: 受け入れ判定テーブルを更新します")
    _run_sql_file(spark, gate_sql)

    print("[3/3] Cutover: v2専用運用へ切替えます")
    _prepare_cutover_targets(spark)
    _run_sql_file(spark, cutover_sql)

    print("完了: v2専用運用の手動バッチが終了しました。")


if __name__ == "__main__":
    main()
