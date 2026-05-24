from pathlib import Path

from src.config import DB_PATH
from src.generate_report import main as report_main
from src.run_pipeline import main as run_main


def test_report_generation(monkeypatch):
    if DB_PATH.exists():
        DB_PATH.unlink()

    monkeypatch.setattr("sys.argv", ["run_pipeline.py"])
    run_main()

    report_main()

    md = Path("data/artifacts/reports/pipeline_health_report.md")
    csv = Path("data/artifacts/reports/pipeline_health_report.csv")

    assert md.exists()
    assert csv.exists()
    assert "Pipeline Health Report" in md.read_text(encoding="utf-8")
