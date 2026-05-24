from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = Path(__file__).resolve().parents[2]

with DAG(
    dag_id="financial_etl_reliability_lab",
    start_date=datetime(2026, 5, 24),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["financial", "etl", "reliability"],
) as dag:
    run_financial_pipeline = BashOperator(
        task_id="run_financial_pipeline",
        bash_command="python -m src.run_pipeline",
        cwd=str(PROJECT_DIR),
    )

    run_financial_pipeline
