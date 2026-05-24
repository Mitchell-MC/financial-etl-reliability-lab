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

## 6) What I would do next
- Add dbt tests and model contracts.
- Add Airflow alert routing to Slack/PagerDuty.
- Add cost telemetry per DAG run and warehouse query profile tracking.
