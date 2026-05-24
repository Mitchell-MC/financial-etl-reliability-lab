# Financial ETL Reliability Lab

A portfolio-grade project designed for Senior Data Engineer interviews.

## What this project demonstrates

- Ingestion of messy multi-source financial data.
- Replay-safe, idempotent ETL behavior.
- Bronze/Silver/Gold data modeling.
- Data quality gates and quarantine workflow.
- Operational metrics for reliability and freshness.
- AI-ready serving output for downstream embedding workflows.

## Architecture

1. Ingestion:
- Source A and Source B CSV feeds under `data/raw`.
- Duplicate protection at the raw-line hash level.
- Parsing failures and missing critical fields routed to quarantine.

2. Normalization:
- Silver layer applies standardization and deterministic dedupe.
- Uses latest `updated_at` record per natural key.

3. Quality checks:
- Null key check.
- Duplicate natural key check.
- Future timestamp check.

4. Publishing:
- Gold daily account summary table.
- AI-ready semantic text dataset for downstream LLM pipelines.

5. Observability:
- Pipeline run metadata captured in `pipeline_runs`.
- JSONL operational metrics emitted per run.

## Quick start

1. Create virtual environment and install dependencies.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Run pipeline.

```powershell
python -m src.run_pipeline
```

3. Validate tables in DuckDB file.

```powershell
python -c "import duckdb; c=duckdb.connect('data/artifacts/warehouse.duckdb'); print(c.execute('select * from gold_daily_account_summary').fetchall())"
```

4. Run tests.

```powershell
python -m pytest -q
```

5. Generate reliability report artifacts.

```powershell
python -m src.generate_report
```

Artifacts are written to `data/artifacts/reports/` as both Markdown and CSV.

## Airflow orchestration

A starter DAG is provided at `airflow/dags/financial_etl_reliability_dag.py`.

## dbt models and tests

The project includes dbt staging and mart models under `dbt/models`.

1. Install dbt adapter dependencies:

```powershell
pip install -r requirements-dbt.txt
```

2. Run dbt models and tests from the dbt folder:

```powershell
Push-Location dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
Pop-Location
```

The dbt project reads from `data/artifacts/warehouse.duckdb`.

## Demo script for interviews

1. Explain problem framing:
- Financial source systems are messy, late, and occasionally malformed.

2. Show reliability design:
- Idempotent ingest and deterministic silver rebuild.
- Quarantine path instead of silent failures.

3. Run a scenario live:
- Execute pipeline twice to prove no duplicate gold data.
- Point to `pipeline_runs` and metrics JSONL.
- Generate `pipeline_health_report.md` to show reliability and quality trends.

4. Discuss production scaling:
- S3 ingest path already wired — run `python -m src.run_pipeline --source s3 --s3-bucket <bucket>`.
- Snowflake Gold loader ships as `src/snowflake_loader.py` — runs after pipeline to promote Gold tables.
- dbt Snowflake profile in `dbt/profiles.yml` — switch targets with `--target prod`.
- Add alerting integrations and SLA dashboards.

## Important files

- `src/run_pipeline.py` entry point.
- `src/ingestion.py` parsing and quarantine logic.
- `src/transform.py` silver and gold transformations.
- `src/quality.py` quality gate checks.
- `src/ai_ready.py` AI-oriented output generation.
- `src/s3_source.py` AWS S3 source download layer.
- `src/snowflake_loader.py` Snowflake Gold promotion layer.
- `tests/test_pipeline.py` idempotency-focused test.

## AWS S3 integration

Download source files from S3 before ingestion:

```powershell
pip install -r requirements-aws.txt

# Dry-run (no credentials needed — lists what would be fetched)
python -m src.s3_source --bucket my-finance-bucket --prefix feeds/daily/ --dry-run

# Live run
python -m src.run_pipeline --source s3 --s3-bucket my-finance-bucket --s3-prefix feeds/daily/
```

Credentials are never hardcoded. Boto3 resolves them via the standard chain:
environment variables → `~/.aws/credentials` → EC2/ECS instance metadata (IAM role).

## Snowflake integration

Load Gold tables from DuckDB to Snowflake after each pipeline run:

```powershell
pip install -r requirements-snowflake.txt

# Set credentials
$env:SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
$env:SNOWFLAKE_USER="etl_service"
$env:SNOWFLAKE_PASSWORD="..."
$env:SNOWFLAKE_WAREHOUSE="TRANSFORMING_WH"
$env:SNOWFLAKE_DATABASE="FINANCE_PROD"
$env:SNOWFLAKE_SCHEMA="GOLD"

# Promote Gold tables to Snowflake
python -m src.snowflake_loader
```

To run dbt models against Snowflake instead of DuckDB:

```powershell
Push-Location dbt
dbt run --profiles-dir . --target prod
dbt test --profiles-dir . --target prod
Pop-Location
```
