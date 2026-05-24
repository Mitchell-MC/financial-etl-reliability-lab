def publish_ai_ready_dataset(conn, output_path):
    conn.execute(
        """
        CREATE OR REPLACE TABLE ai_ready_financial_notes AS
        SELECT
            natural_key,
            account_id,
            cast(posted_at AS DATE) AS transaction_date,
            amount,
            currency,
            concat(
                'Account ', account_id,
                ' posted ', cast(amount AS VARCHAR), ' ', currency,
                ' on ', cast(cast(posted_at AS DATE) AS VARCHAR),
                '. Description: ', coalesce(description, 'n/a')
            ) AS semantic_text
        FROM silver_transactions
        WHERE description IS NOT NULL;
        """
    )

    escaped_path = str(output_path).replace("'", "''")
    conn.execute(
        f"""
        COPY (
            SELECT *
            FROM ai_ready_financial_notes
            ORDER BY transaction_date, account_id
        ) TO '{escaped_path}' (HEADER, DELIMITER ',');
        """
    )
