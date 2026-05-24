from datetime import datetime, timezone


def run_quality_checks(conn):
    checks = []

    null_key_count = conn.execute(
        "SELECT count(*) FROM silver_transactions WHERE account_id IS NULL OR currency IS NULL"
    ).fetchone()[0]
    checks.append(("null_key_check", null_key_count == 0, null_key_count))

    future_date_count = conn.execute(
        "SELECT count(*) FROM silver_transactions WHERE posted_at > current_timestamp + interval '1 day'"
    ).fetchone()[0]
    checks.append(("future_timestamp_check", future_date_count == 0, future_date_count))

    duplicate_natural_keys = conn.execute(
        """
        SELECT count(*)
        FROM (
            SELECT natural_key, count(*) AS c
            FROM silver_transactions
            GROUP BY natural_key
            HAVING c > 1
        ) t
        """
    ).fetchone()[0]
    checks.append(("duplicate_natural_key_check", duplicate_natural_keys == 0, duplicate_natural_keys))

    freshest = conn.execute("SELECT max(posted_at) FROM silver_transactions").fetchone()[0]
    freshness_lag_minutes = None
    if freshest is not None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        delta = now - freshest
        freshness_lag_minutes = int(delta.total_seconds() // 60)

    failures = [name for name, passed, _ in checks if not passed]

    return {
        "checks": checks,
        "passed": len(failures) == 0,
        "failures": failures,
        "freshness_lag_minutes": freshness_lag_minutes,
    }
