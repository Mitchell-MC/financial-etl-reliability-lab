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
- Replace local files with S3/API ingestion.
- Move warehouse target to Snowflake and keep same layering patterns.
- Add alerting integrations and SLA dashboards.

## Important files

- `src/run_pipeline.py` entry point.
- `src/ingestion.py` parsing and quarantine logic.
- `src/transform.py` silver and gold transformations.
- `src/quality.py` quality gate checks.
- `src/ai_ready.py` AI-oriented output generation.
- `tests/test_pipeline.py` idempotency-focused test.
