from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
ARTIFACT_DIR = DATA_DIR / "artifacts"
DB_PATH = ARTIFACT_DIR / "warehouse.duckdb"
METRICS_PATH = ARTIFACT_DIR / "pipeline_metrics.jsonl"

SOURCE_FILES = {
    "source_a": RAW_DIR / "source_a_transactions.csv",
    "source_b": RAW_DIR / "source_b_transactions.csv",
}
