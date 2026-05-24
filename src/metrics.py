import json
from datetime import UTC, datetime


def append_metrics(metrics_path, payload):
    payload = dict(payload)
    payload["event_ts"] = datetime.now(UTC).isoformat()

    with open(metrics_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")
