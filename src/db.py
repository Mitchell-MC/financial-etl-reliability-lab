import duckdb


def get_connection(db_path):
    return duckdb.connect(str(db_path))


def initialize_schema(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze_transactions (
            load_run_id VARCHAR,
            source_name VARCHAR,
            raw_transaction_id VARCHAR,
            account_id VARCHAR,
            posted_at TIMESTAMP,
            amount DOUBLE,
            currency VARCHAR,
            description VARCHAR,
            updated_at TIMESTAMP,
            status VARCHAR,
            raw_line_hash VARCHAR,
            ingested_at TIMESTAMP DEFAULT current_timestamp
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quarantine_transactions (
            load_run_id VARCHAR,
            source_name VARCHAR,
            raw_transaction_id VARCHAR,
            account_id VARCHAR,
            posted_at_text VARCHAR,
            amount_text VARCHAR,
            currency VARCHAR,
            description VARCHAR,
            updated_at_text VARCHAR,
            status VARCHAR,
            quarantine_reason VARCHAR,
            quarantined_at TIMESTAMP DEFAULT current_timestamp
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS silver_transactions (
            natural_key VARCHAR,
            source_name VARCHAR,
            raw_transaction_id VARCHAR,
            account_id VARCHAR,
            posted_at TIMESTAMP,
            amount DOUBLE,
            currency VARCHAR,
            description VARCHAR,
            updated_at TIMESTAMP,
            status VARCHAR,
            load_run_id VARCHAR,
            refreshed_at TIMESTAMP DEFAULT current_timestamp
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gold_daily_account_summary (
            run_date DATE,
            account_id VARCHAR,
            currency VARCHAR,
            total_amount DOUBLE,
            transaction_count BIGINT,
            last_refresh_ts TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR,
            start_ts TIMESTAMP,
            end_ts TIMESTAMP,
            status VARCHAR,
            records_ingested BIGINT,
            records_silver BIGINT,
            records_quarantine BIGINT,
            freshness_lag_minutes BIGINT,
            error_message VARCHAR
        );
        """
    )
