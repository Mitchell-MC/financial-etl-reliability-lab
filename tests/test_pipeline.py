from pathlib import Path

from src.config import ARTIFACT_DIR, DB_PATH
from src.db import get_connection
from src.run_pipeline import main as run_main


def _run_pipeline_twice(monkeypatch):
    monkeypatch.setattr("sys.argv", ["run_pipeline.py"])
    run_main()
    monkeypatch.setattr("sys.argv", ["run_pipeline.py"])
    run_main()


def test_pipeline_idempotent(monkeypatch):
    if DB_PATH.exists():
        DB_PATH.unlink()

    metrics_file = ARTIFACT_DIR / "pipeline_metrics.jsonl"
    if metrics_file.exists():
        metrics_file.unlink()

    _run_pipeline_twice(monkeypatch)

    conn = get_connection(DB_PATH)
    silver_count = conn.execute("SELECT count(*) FROM silver_transactions").fetchone()[0]
    gold_count = conn.execute("SELECT count(*) FROM gold_daily_account_summary").fetchone()[0]
    run_count = conn.execute("SELECT count(*) FROM pipeline_runs").fetchone()[0]
    conn.close()

    assert silver_count > 0
    assert gold_count > 0
    assert run_count == 2
