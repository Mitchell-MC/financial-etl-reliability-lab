"""
snowflake_loader.py — Load the Gold layer from DuckDB into Snowflake.

This module represents the production promotion path: after the pipeline
validates and aggregates data locally (or in a staging DuckDB warehouse),
the Gold tables are written to Snowflake for downstream BI and ML consumption.

Usage (standalone):
    python -m src.snowflake_loader

Required environment variables:
    SNOWFLAKE_ACCOUNT    e.g. xy12345.us-east-1
    SNOWFLAKE_USER       your Snowflake username
    SNOWFLAKE_PASSWORD   your Snowflake password  (or use key-pair auth below)
    SNOWFLAKE_WAREHOUSE  compute warehouse, e.g. TRANSFORMING_WH
    SNOWFLAKE_DATABASE   target database, e.g. FINANCE_PROD
    SNOWFLAKE_SCHEMA     target schema, e.g. GOLD

Optional key-pair auth (preferred for service accounts):
    SNOWFLAKE_PRIVATE_KEY_PATH   path to PEM private key file
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE  passphrase if key is encrypted

Design notes:
  - Uses write_pandas() for efficient bulk load via Snowflake PUT/COPY internals.
  - Gold tables are written with if_exists='replace' so each pipeline run is
    a full refresh of the daily summary — safe because DuckDB Gold is the SOR.
  - Extend to 'append' mode when you need incremental Snowflake loads.
  - Snowflake table names are uppercased (Snowflake convention).
  - Credentials are NEVER hardcoded; always sourced from environment.
"""

import os
from pathlib import Path

from src.config import DB_PATH

# Gold tables to promote: DuckDB table name → Snowflake table name
GOLD_TABLE_MAP: dict[str, str] = {
    "gold_daily_account_summary": "GOLD_DAILY_ACCOUNT_SUMMARY",
    "silver_transactions": "SILVER_TRANSACTIONS",
}


def _get_snowflake_connection():
    """Build a Snowflake connection from environment variables."""
    try:
        import snowflake.connector
    except ImportError as exc:
        raise ImportError(
            "snowflake-connector-python is required. "
            "Install with: pip install -r requirements-snowflake.txt"
        ) from exc

    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_WAREHOUSE",
                "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(
            f"Missing required Snowflake environment variables: {missing}\n"
            "Set them before running: SNOWFLAKE_ACCOUNT=xy12345.us-east-1 ..."
        )

    connect_kwargs = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )

    # Key-pair auth (preferred for service accounts / CI)
    private_key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if private_key_path:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.serialization import (
            Encoding, NoEncryption, PrivateFormat,
            load_pem_private_key,
        )
        passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").encode() or None
        with open(private_key_path, "rb") as key_file:
            p_key = load_pem_private_key(key_file.read(), password=passphrase, backend=default_backend())
        connect_kwargs["private_key"] = p_key.private_bytes(
            Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
        )
    else:
        password = os.environ.get("SNOWFLAKE_PASSWORD")
        if not password:
            raise EnvironmentError(
                "Set SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH for authentication."
            )
        connect_kwargs["password"] = password

    return snowflake.connector.connect(**connect_kwargs)


def load_gold_to_snowflake(tables: list[str] | None = None) -> dict[str, int]:
    """
    Fetch Gold tables from DuckDB and write them to Snowflake.

    Args:
        tables: list of DuckDB table names to load; defaults to all in GOLD_TABLE_MAP.

    Returns:
        dict mapping snowflake_table_name → row count written.
    """
    import duckdb
    from snowflake.connector.pandas_tools import write_pandas

    tables = tables or list(GOLD_TABLE_MAP.keys())
    results: dict[str, int] = {}

    duck_conn = duckdb.connect(str(DB_PATH))
    sf_conn = _get_snowflake_connection()

    try:
        for duck_table in tables:
            sf_table = GOLD_TABLE_MAP[duck_table]
            print(f"  Loading {duck_table}  →  Snowflake:{sf_table} ...", end=" ")

            df = duck_conn.execute(f"SELECT * FROM {duck_table}").fetchdf()

            success, num_chunks, num_rows, output = write_pandas(
                sf_conn,
                df,
                sf_table,
                auto_create_table=True,
                overwrite=True,
            )

            if not success:
                raise RuntimeError(f"write_pandas failed for {sf_table}: {output}")

            results[sf_table] = num_rows
            print(f"{num_rows} rows ✓")
    finally:
        duck_conn.close()
        sf_conn.close()

    return results


def main():
    print("Snowflake Gold Loader")
    print(f"  Source DB : {DB_PATH}")
    print(f"  Target    : {os.environ.get('SNOWFLAKE_DATABASE')}.{os.environ.get('SNOWFLAKE_SCHEMA')}\n")

    results = load_gold_to_snowflake()
    total = sum(results.values())
    print(f"\nDone. {len(results)} table(s) loaded, {total} total rows written to Snowflake.")


if __name__ == "__main__":
    main()
