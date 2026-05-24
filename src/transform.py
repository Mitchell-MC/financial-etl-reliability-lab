def build_silver_table(conn, load_run_id):
    conn.execute(
        """
        CREATE OR REPLACE TABLE silver_transactions AS
        WITH standardized AS (
            SELECT
                source_name,
                raw_transaction_id,
                account_id,
                posted_at,
                amount,
                upper(currency) AS currency,
                trim(description) AS description,
                coalesce(updated_at, posted_at) AS updated_at,
                lower(coalesce(status, 'posted')) AS status,
                load_run_id,
                concat(source_name, '|', raw_transaction_id, '|', cast(posted_at AS VARCHAR)) AS natural_key
            FROM bronze_transactions
            WHERE posted_at IS NOT NULL
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY natural_key
                    ORDER BY updated_at DESC, load_run_id DESC
                ) AS row_num
            FROM standardized
        )
        SELECT
            natural_key,
            source_name,
            raw_transaction_id,
            account_id,
            posted_at,
            amount,
            currency,
            description,
            updated_at,
            status,
            load_run_id,
            current_timestamp AS refreshed_at
        FROM ranked
        WHERE row_num = 1
          AND account_id IS NOT NULL
          AND amount IS NOT NULL
                    AND posted_at <= current_timestamp + interval '1 day'
          AND status != 'deleted';
        """
    )


def publish_gold_tables(conn):
    conn.execute(
        """
        CREATE OR REPLACE TABLE gold_daily_account_summary AS
        SELECT
            cast(posted_at AS DATE) AS run_date,
            account_id,
            currency,
            round(sum(amount), 2) AS total_amount,
            count(*) AS transaction_count,
            current_timestamp AS last_refresh_ts
        FROM silver_transactions
        GROUP BY 1, 2, 3
        ORDER BY 1, 2;
        """
    )
