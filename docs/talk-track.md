# 7-Minute Interview Talk Track

## 1) Problem
I built this project to mirror a real financial data engineering challenge: multiple messy feeds with duplicates, malformed values, and late-arriving records. The goal is to create a reliable foundation for analytics and AI workloads.

## 2) Design choices
- Bronze/Silver/Gold layering for traceability and isolation.
- Replay-safe ingestion keyed by source-aware line hash.
- Deterministic silver refresh with latest-record selection.
- Quality checks before gold publication.
- Quarantine table for invalid payloads.

## 3) Reliability posture
- Pipeline run audit table stores status, counts, and lag.
- Metrics are emitted per run for SLO dashboards.
- Designed around failure visibility, not happy path only.

## 4) AI-readiness
- Produced an AI-ready semantic text table and CSV from conformed silver data.
- This keeps LLM feature generation isolated from raw financial noise.

## 5) Tradeoffs
- Chose full silver rebuild for deterministic correctness in MVP.
- In production, would switch to incremental merge and partition pruning for scale.

## 6) Production stack wiring (already built)

**AWS S3:**
- `src/s3_source.py` downloads source CSVs from S3 before ingestion using boto3.
- Credentials resolve through the standard IAM chain — no secrets in code.
- Run with `python -m src.run_pipeline --source s3 --s3-bucket <bucket>`.
- Supports `--dry-run` for safe demo without live credentials.

**Snowflake:**
- `src/snowflake_loader.py` promotes Gold tables from DuckDB to Snowflake using `write_pandas()`.
- Supports password auth and key-pair auth (service account pattern).
- `dbt/profiles.yml` has a `prod` target pointing at Snowflake via environment variables.
- Switch dbt targets with `dbt run --profiles-dir . --target prod`.

**Apache Airflow:**
- DAG at `airflow/dags/financial_etl_reliability_dag.py` schedules the pipeline at 06:00 UTC daily.
- Extend to wire S3 ingest and Snowflake promotion as downstream tasks.

## 7) What I would do next
- Add Airflow alert routing to Slack/PagerDuty on quality gate failure.
- Add cost telemetry per DAG run and Snowflake query profile tracking.
- Add incremental silver merge and partition pruning for production scale.
