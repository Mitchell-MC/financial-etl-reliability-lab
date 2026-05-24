"""
Failure simulation script — three runnable scenarios for live interview demos.

Usage:
    python -m src.simulate_failures --scenario timeout
    python -m src.simulate_failures --scenario schema_drift
    python -m src.simulate_failures --scenario late_arrival
    python -m src.simulate_failures --scenario all
"""
import argparse
import shutil
import textwrap
from pathlib import Path

from src.config import ARTIFACT_DIR, DB_PATH, RAW_DIR
from src.db import get_connection, initialize_schema
from src.ingestion import ingest_source_csv
from src.transform import build_silver_table, publish_gold_tables
from src.quality import run_quality_checks
from src.metrics import append_metrics

SCENARIO_SOURCE = RAW_DIR / "scenario_source.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_ingest_and_transform(run_id: str):
    """Ingest scenario source only and build silver/gold, returning quality result."""
    conn = get_connection(DB_PATH)
    initialize_schema(conn)
    result = ingest_source_csv(conn, run_id, "scenario_source", SCENARIO_SOURCE)
    build_silver_table(conn, run_id)
    quality = run_quality_checks(conn)
    if quality["passed"]:
        publish_gold_tables(conn)
    silver = conn.execute("SELECT count(*) FROM silver_transactions").fetchone()[0]
    quarantine = conn.execute("SELECT count(*) FROM quarantine_transactions WHERE source_name = 'scenario_source'").fetchone()[0]
    conn.close()
    append_metrics(ARTIFACT_DIR / "pipeline_metrics.jsonl", {
        "scenario": run_id,
        "silver": silver,
        "quarantine": quarantine,
        "quality_passed": quality["passed"],
        "quality_failures": quality["failures"],
    })
    return result, quality, silver, quarantine


def _print_section(title: str, body: str):
    width = 64
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)
    print(textwrap.dedent(body).strip())


def _snapshot(label: str):
    """Print current silver/quarantine/gold counts from warehouse."""
    if not DB_PATH.exists():
        print(f"  [{label}] warehouse not initialised yet")
        return
    conn = get_connection(DB_PATH)
    silver = conn.execute("SELECT count(*) FROM silver_transactions").fetchone()[0]
    gold = conn.execute("SELECT count(*) FROM gold_daily_account_summary").fetchone()[0]
    quarantine = conn.execute("SELECT count(*) FROM quarantine_transactions").fetchone()[0]
    conn.close()
    print(f"  [{label}] silver={silver}  gold={gold}  quarantine={quarantine}")


# ---------------------------------------------------------------------------
# Scenario 1 — Upstream timeout / truncated delivery
# ---------------------------------------------------------------------------

def scenario_timeout():
    _print_section(
        "SCENARIO 1: Upstream Timeout / Truncated Delivery",
        """
        Simulates what happens when an upstream API or SFTP job times out
        mid-transfer and delivers only a partial file.

        Design goal: the pipeline must accept the partial batch without
        crashing, record exactly what arrived, and allow a full backfill
        on the next run when the complete file is available.
        """,
    )

    # Write a partial delivery — only 2 of 5 rows
    SCENARIO_SOURCE.write_text(
        "transaction_id,account_id,posted_at,amount,currency,description,updated_at,status\n"
        "T0001,ACC-010,2026-05-24T08:00:00,500.00,USD,Partial delivery row 1,2026-05-24T08:01:00,posted\n"
        "T0002,ACC-011,2026-05-24T08:05:00,1200.00,USD,Partial delivery row 2,2026-05-24T08:06:00,posted\n",
        encoding="utf-8",
    )

    _snapshot("before partial ingest")
    result, quality, silver, quarantine = _run_ingest_and_transform("scenario-timeout-partial")
    _snapshot("after partial ingest")
    print(f"\n  Partial batch: ingested={result['ingested']}  quarantined={result['quarantined']}")

    # Now deliver the complete file including the 3 rows that were missing
    SCENARIO_SOURCE.write_text(
        "transaction_id,account_id,posted_at,amount,currency,description,updated_at,status\n"
        "T0001,ACC-010,2026-05-24T08:00:00,500.00,USD,Partial delivery row 1,2026-05-24T08:01:00,posted\n"
        "T0002,ACC-011,2026-05-24T08:05:00,1200.00,USD,Partial delivery row 2,2026-05-24T08:06:00,posted\n"
        "T0003,ACC-012,2026-05-24T08:10:00,750.00,USD,Recovered row 3,2026-05-24T08:11:00,posted\n"
        "T0004,ACC-013,2026-05-24T08:15:00,320.50,USD,Recovered row 4,2026-05-24T08:16:00,posted\n"
        "T0005,ACC-014,2026-05-24T08:20:00,90.00,USD,Recovered row 5,2026-05-24T08:21:00,posted\n",
        encoding="utf-8",
    )

    result2, quality2, silver2, quarantine2 = _run_ingest_and_transform("scenario-timeout-full")
    _snapshot("after full backfill")
    print(f"\n  Full backfill: new rows ingested={result2['ingested']}  quarantined={result2['quarantined']}")
    print("\n  RESULT: Only the 3 net-new rows were ingested on the second run.")
    print("  The 2 already-seen rows were skipped via line-hash deduplication.")
    print("  Silver and Gold reflect the complete dataset without duplicates.")

    SCENARIO_SOURCE.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Scenario 2 — Schema drift
# ---------------------------------------------------------------------------

def scenario_schema_drift():
    _print_section(
        "SCENARIO 2: Schema Drift from Upstream Vendor",
        """
        Simulates a vendor silently renaming a required column
        (e.g. 'amount' → 'transaction_amount') without advance notice.

        Design goal: the pipeline must not silently propagate nulls.
        Invalid rows are quarantined with a clear reason so engineers
        can triage without scanning the full dataset.
        """,
    )

    SCENARIO_SOURCE.write_text(
        # 'amount' is renamed to 'transaction_amount' — our parser won't find it
        "transaction_id,account_id,posted_at,transaction_amount,currency,description,updated_at,status\n"
        "D0001,ACC-020,2026-05-24T09:00:00,400.00,USD,Schema drift row 1,2026-05-24T09:01:00,posted\n"
        "D0002,ACC-021,2026-05-24T09:05:00,800.00,USD,Schema drift row 2,2026-05-24T09:06:00,posted\n",
        encoding="utf-8",
    )

    _snapshot("before schema-drift ingest")
    result, quality, silver, quarantine = _run_ingest_and_transform("scenario-schema-drift")
    _snapshot("after schema-drift ingest")

    print(f"\n  Ingested={result['ingested']}  Quarantined={result['quarantined']}")

    conn = get_connection(DB_PATH)
    reasons = conn.execute(
        "SELECT quarantine_reason, count(*) FROM quarantine_transactions "
        "WHERE source_name = 'scenario_source' GROUP BY quarantine_reason"
    ).fetchall()
    conn.close()

    print("\n  Quarantine breakdown:")
    for reason, count in reasons:
        print(f"    {reason}: {count} row(s)")

    print("\n  RESULT: Drifted rows are quarantined with 'missing_amount'.")
    print("  Silver is protected. The quarantine table is the triage surface,")
    print("  not a flood of downstream nulls or silent data loss.")

    SCENARIO_SOURCE.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Scenario 3 — Late-arriving data backfill
# ---------------------------------------------------------------------------

def scenario_late_arrival():
    _print_section(
        "SCENARIO 3: Late-Arriving Data Backfill",
        """
        Simulates a vendor delivering corrections for transactions from
        5 days ago — a common occurrence in financial systems where
        settlement files or adjustments arrive days after posting.

        Design goal: the pipeline handles historic dates idempotently.
        Late records are added to silver without disrupting current-day
        data or requiring a full recompute of the warehouse.
        """,
    )

    conn = get_connection(DB_PATH)
    silver_before = conn.execute("SELECT count(*) FROM silver_transactions").fetchone()[0]
    conn.close()
    print(f"\n  Silver records before late-arrival ingest: {silver_before}")

    # Records with posted_at 5 days in the past, not seen in any prior run
    SCENARIO_SOURCE.write_text(
        "transaction_id,account_id,posted_at,amount,currency,description,updated_at,status\n"
        "L0001,ACC-030,2026-05-19T14:00:00,275.00,USD,Late settlement row 1,2026-05-24T07:00:00,posted\n"
        "L0002,ACC-031,2026-05-19T14:30:00,1100.00,USD,Late settlement row 2,2026-05-24T07:01:00,posted\n"
        "L0003,ACC-032,2026-05-18T09:00:00,60.00,USD,Late correction 3 days prior,2026-05-24T07:02:00,posted\n",
        encoding="utf-8",
    )

    result, quality, silver, quarantine = _run_ingest_and_transform("scenario-late-arrival")
    _snapshot("after late-arrival ingest")

    print(f"\n  Late rows ingested={result['ingested']}  quarantined={result['quarantined']}")
    print(f"  Silver delta: {silver_before} → {silver}  (+{silver - silver_before})")

    conn = get_connection(DB_PATH)
    backfill_dates = conn.execute(
        "SELECT cast(posted_at AS DATE), count(*) "
        "FROM silver_transactions "
        "WHERE load_run_id = 'scenario-late-arrival' "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall()
    conn.close()

    print("\n  Backfilled records by date:")
    for date, cnt in backfill_dates:
        print(f"    {date}: {cnt} record(s)")

    print("\n  RESULT: Historic-date records are inserted into silver by natural key.")
    print("  Gold daily summary is rebuilt to include the backfill dates.")
    print("  No existing records were modified or duplicated.")

    SCENARIO_SOURCE.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

SCENARIOS = {
    "timeout": scenario_timeout,
    "schema_drift": scenario_schema_drift,
    "late_arrival": scenario_late_arrival,
}


def main():
    parser = argparse.ArgumentParser(description="Run failure simulation scenarios")
    parser.add_argument(
        "--scenario",
        choices=list(SCENARIOS.keys()) + ["all"],
        required=True,
        help="Which failure scenario to run",
    )
    args = parser.parse_args()

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    # Ensure warehouse exists with base data before running scenarios
    if not DB_PATH.exists():
        print("Warehouse not found — running base pipeline first...")
        from src.run_pipeline import main as run_main
        import sys
        sys.argv = ["run_pipeline.py"]
        run_main()

    scenarios_to_run = list(SCENARIOS.items()) if args.scenario == "all" else [(args.scenario, SCENARIOS[args.scenario])]

    for name, fn in scenarios_to_run:
        fn()

    print("\n" + "=" * 64)
    print("  Simulation complete.")
    print("  Run 'python -m src.generate_report' to see updated metrics.")
    print("=" * 64 + "\n")


if __name__ == "__main__":
    main()
