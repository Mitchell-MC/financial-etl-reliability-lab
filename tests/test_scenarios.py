import sys

from src.config import ARTIFACT_DIR, DB_PATH
from src.db import get_connection


def _reset_warehouse():
    if DB_PATH.exists():
        DB_PATH.unlink()
    metrics = ARTIFACT_DIR / "pipeline_metrics.jsonl"
    if metrics.exists():
        metrics.unlink()


def _seed_base_pipeline(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run_pipeline.py"])
    from src.run_pipeline import main as run_main
    run_main()


def test_timeout_scenario_no_duplicates(monkeypatch):
    _reset_warehouse()
    _seed_base_pipeline(monkeypatch)

    from src.simulate_failures import scenario_timeout
    scenario_timeout()

    conn = get_connection(DB_PATH)
    dupes = conn.execute(
        """
        SELECT count(*) FROM (
            SELECT natural_key, count(*) AS c
            FROM silver_transactions
            GROUP BY natural_key
            HAVING c > 1
        )
        """
    ).fetchone()[0]
    conn.close()

    assert dupes == 0, "Duplicate natural keys found after timeout/backfill scenario"


def test_schema_drift_quarantines_bad_rows(monkeypatch):
    _reset_warehouse()
    _seed_base_pipeline(monkeypatch)

    from src.simulate_failures import scenario_schema_drift
    scenario_schema_drift()

    conn = get_connection(DB_PATH)
    quarantine_count = conn.execute(
        "SELECT count(*) FROM quarantine_transactions WHERE source_name = 'scenario_source'"
    ).fetchone()[0]
    conn.close()

    assert quarantine_count > 0, "Expected drifted rows in quarantine but found none"


def test_late_arrival_backfills_silver(monkeypatch):
    _reset_warehouse()
    _seed_base_pipeline(monkeypatch)

    conn = get_connection(DB_PATH)
    silver_before = conn.execute("SELECT count(*) FROM silver_transactions").fetchone()[0]
    conn.close()

    from src.simulate_failures import scenario_late_arrival
    scenario_late_arrival()

    conn = get_connection(DB_PATH)
    silver_after = conn.execute("SELECT count(*) FROM silver_transactions").fetchone()[0]
    conn.close()

    assert silver_after > silver_before, "Late-arriving rows should have increased silver count"
