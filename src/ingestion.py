import csv
import hashlib
from datetime import datetime, timedelta


def _safe_parse_timestamp(value):
    if value is None or value == "":
        return None
    return datetime.fromisoformat(value)


def _safe_parse_amount(value):
    if value is None or value == "":
        return None
    return float(value)


def _line_hash(row, source_name):
    raw = "|".join(
        [
            source_name,
            row.get("transaction_id", ""),
            row.get("account_id", ""),
            row.get("posted_at", ""),
            row.get("amount", ""),
            row.get("currency", ""),
            row.get("description", ""),
            row.get("updated_at", ""),
            row.get("status", ""),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def ingest_source_csv(conn, load_run_id, source_name, source_path):
    existing_hashes = {
        row[0]
        for row in conn.execute(
            "SELECT raw_line_hash FROM bronze_transactions WHERE source_name = ?",
            [source_name],
        ).fetchall()
    }

    ingested_count = 0
    quarantined_count = 0

    with open(source_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw_hash = _line_hash(row, source_name)
            if raw_hash in existing_hashes:
                continue

            posted_at_text = row.get("posted_at")
            updated_at_text = row.get("updated_at")
            amount_text = row.get("amount")

            quarantine_reason = None
            try:
                posted_at = _safe_parse_timestamp(posted_at_text)
                updated_at = _safe_parse_timestamp(updated_at_text)
                amount = _safe_parse_amount(amount_text)

                if row.get("account_id") in (None, ""):
                    quarantine_reason = "missing_account_id"
                elif amount is None:
                    quarantine_reason = "missing_amount"
                elif posted_at is None:
                    quarantine_reason = "missing_posted_at"
                elif posted_at > datetime.now() + timedelta(days=1):
                    quarantine_reason = "future_posted_at"
            except ValueError:
                quarantine_reason = "parsing_error"
                posted_at = None
                updated_at = None
                amount = None

            if quarantine_reason:
                conn.execute(
                    """
                    INSERT INTO quarantine_transactions (
                        load_run_id, source_name, raw_transaction_id, account_id,
                        posted_at_text, amount_text, currency, description,
                        updated_at_text, status, quarantine_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        load_run_id,
                        source_name,
                        row.get("transaction_id"),
                        row.get("account_id"),
                        posted_at_text,
                        amount_text,
                        row.get("currency"),
                        row.get("description"),
                        updated_at_text,
                        row.get("status"),
                        quarantine_reason,
                    ],
                )
                quarantined_count += 1
                existing_hashes.add(raw_hash)
                continue

            conn.execute(
                """
                INSERT INTO bronze_transactions (
                    load_run_id, source_name, raw_transaction_id, account_id,
                    posted_at, amount, currency, description, updated_at,
                    status, raw_line_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    load_run_id,
                    source_name,
                    row.get("transaction_id"),
                    row.get("account_id"),
                    posted_at,
                    amount,
                    row.get("currency"),
                    row.get("description"),
                    updated_at,
                    row.get("status"),
                    raw_hash,
                ],
            )
            ingested_count += 1
            existing_hashes.add(raw_hash)

    return {"ingested": ingested_count, "quarantined": quarantined_count}
