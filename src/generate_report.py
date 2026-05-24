import csv
from datetime import UTC, datetime
from pathlib import Path

from src.config import ARTIFACT_DIR, DB_PATH, ROOT_DIR
from src.db import get_connection

REPORT_SQL_PATH = ROOT_DIR / "reports" / "pipeline_health_report.sql"
REPORT_MD_PATH = ARTIFACT_DIR / "reports" / "pipeline_health_report.md"
REPORT_CSV_PATH = ARTIFACT_DIR / "reports" / "pipeline_health_report.csv"


def _format_markdown(row):
    generated_at = datetime.now(UTC).isoformat()
    return "\n".join(
        [
            "# Pipeline Health Report",
            "",
            f"Generated at: {generated_at}",
            "",
            "## Latest Run",
            f"- Run ID: {row['run_id']}",
            f"- Status: {row['latest_status']}",
            f"- Duration (s): {row['duration_seconds']}",
            f"- Ingested: {row['records_ingested']}",
            f"- Silver Records: {row['records_silver']}",
            f"- Quarantined: {row['records_quarantine']}",
            f"- Freshness Lag (min): {row['freshness_lag_minutes']}",
            "",
            "## 7-Day Reliability",
            f"- Runs: {row['runs_last_7d']}",
            f"- Successful Runs: {row['successful_runs_last_7d']}",
            f"- Success Rate (%): {row['success_rate_last_7d']}",
            f"- Avg Quarantined: {row['avg_quarantine_last_7d']}",
            f"- Avg Freshness Lag (min): {row['avg_freshness_lag_last_7d']}",
            f"- Avg Duration (s): {row['avg_duration_seconds_last_7d']}",
            "",
            "## Data Quality Hotspots",
            f"- Top quarantine reasons: {row['top_quarantine_reasons']}",
            f"- Latest error: {row['error_message']}",
            "",
        ]
    )


def _write_csv(row):
    REPORT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_CSV_PATH, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)


def main():
    if not DB_PATH.exists():
        raise SystemExit("Warehouse DB not found. Run pipeline first: python -m src.run_pipeline")

    sql = REPORT_SQL_PATH.read_text(encoding="utf-8")

    conn = get_connection(DB_PATH)
    result = conn.execute(sql).fetchdf()
    conn.close()

    if result.empty:
        raise SystemExit("No pipeline run data found. Execute the pipeline at least once.")

    row = result.iloc[0].to_dict()

    report_markdown = _format_markdown(row)
    REPORT_MD_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_MD_PATH.write_text(report_markdown, encoding="utf-8")

    _write_csv(row)

    print(f"Report generated: {REPORT_MD_PATH}")
    print(f"Report generated: {REPORT_CSV_PATH}")


if __name__ == "__main__":
    main()
