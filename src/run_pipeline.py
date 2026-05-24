import argparse
import uuid
from datetime import UTC, datetime

from src.config import ARTIFACT_DIR, DB_PATH, METRICS_PATH, SOURCE_FILES
from src.db import get_connection, initialize_schema
from src.ingestion import ingest_source_csv
from src.transform import build_silver_table, publish_gold_tables
from src.quality import run_quality_checks
from src.ai_ready import publish_ai_ready_dataset
from src.metrics import append_metrics


def main():
    parser = argparse.ArgumentParser(description="Run financial ETL reliability pipeline")
    parser.add_argument("--run-date", required=False, help="Logical run date in YYYY-MM-DD")
    args = parser.parse_args()

    run_id = f"run-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    start_ts = datetime.now(UTC)

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_connection(DB_PATH)
    initialize_schema(conn)

    total_ingested = 0
    total_quarantined = 0
    error_message = None
    run_status = "success"

    try:
        for source_name, source_path in SOURCE_FILES.items():
            result = ingest_source_csv(conn, run_id, source_name, source_path)
            total_ingested += result["ingested"]
            total_quarantined += result["quarantined"]

        build_silver_table(conn, run_id)
        quality_result = run_quality_checks(conn)

        if not quality_result["passed"]:
            run_status = "failed_quality"
            error_message = "Quality checks failed: " + ", ".join(quality_result["failures"])
            raise RuntimeError(error_message)

        publish_gold_tables(conn)
        ai_ready_output = ARTIFACT_DIR / "ai_ready_financial_notes.csv"
        publish_ai_ready_dataset(conn, ai_ready_output)

        silver_count = conn.execute("SELECT count(*) FROM silver_transactions").fetchone()[0]
        freshness_lag = quality_result["freshness_lag_minutes"]

    except Exception as exc:
        run_status = "failed"
        error_message = str(exc)
        silver_count = conn.execute("SELECT count(*) FROM silver_transactions").fetchone()[0]
        freshness_lag = None

    end_ts = datetime.now(UTC)

    conn.execute(
        """
        INSERT INTO pipeline_runs (
            run_id, start_ts, end_ts, status, records_ingested,
            records_silver, records_quarantine, freshness_lag_minutes, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            start_ts,
            end_ts,
            run_status,
            total_ingested,
            silver_count,
            total_quarantined,
            freshness_lag,
            error_message,
        ],
    )

    append_metrics(
        METRICS_PATH,
        {
            "run_id": run_id,
            "status": run_status,
            "records_ingested": total_ingested,
            "records_silver": silver_count,
            "records_quarantine": total_quarantined,
            "freshness_lag_minutes": freshness_lag,
        },
    )

    conn.close()

    if run_status != "success":
        raise SystemExit(f"Pipeline finished with status={run_status}: {error_message}")

    print(
        f"Pipeline success | run_id={run_id} ingested={total_ingested} "
        f"silver={silver_count} quarantined={total_quarantined}"
    )


if __name__ == "__main__":
    main()
