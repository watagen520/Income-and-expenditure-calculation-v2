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


REPO_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = REPO_ROOT / "databricks" / "sql"

BUILD_SQL = SQL_DIR / "repair_canonical_and_gold_v2.sql"
GATE_SQL = SQL_DIR / "acceptance_gate_v2.sql"
CUTOVER_SQL = SQL_DIR / "cutover_to_v2_only.sql"


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


def main() -> None:
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    print("[1/3] Build: canonical + Gold _v2 を構築します")
    _run_sql_file(spark, BUILD_SQL)

    print("[2/3] Gate: 受け入れ判定テーブルを更新します")
    _run_sql_file(spark, GATE_SQL)

    print("[3/3] Cutover: v2専用運用へ切替えます")
    _run_sql_file(spark, CUTOVER_SQL)

    print("完了: v2専用運用の手動バッチが終了しました。")


if __name__ == "__main__":
    main()
