"""Prometheus metrics for the Spark streaming queries.

A StreamingQueryListener receives every micro-batch progress report from the
Spark driver and exposes it on http://localhost:8000/metrics for Prometheus.
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, start_http_server
from pyspark.sql.streaming import StreamingQueryListener

from pulse.progress import progress_values

LABELS = ["query"]

INPUT_ROWS = Counter("pulse_input_rows_total", "Rows read from the source", LABELS)
INPUT_RATE = Gauge("pulse_input_rows_per_second", "Input rate of the last batch", LABELS)
PROCESSED_RATE = Gauge("pulse_processed_rows_per_second", "Processing rate of the last batch", LABELS)
BATCH_DURATION = Gauge("pulse_batch_duration_seconds", "Wall time of the last micro-batch", LABELS)
LATE_DROPPED = Counter(
    "pulse_rows_dropped_by_watermark_total", "Rows dropped because they arrived after the watermark", LABELS
)
OFFSETS_BEHIND = Gauge(
    "pulse_kafka_offsets_behind_latest",
    "Max offsets the query is behind the latest Kafka offset (consumer lag; Spark does not commit offsets "
    "to a Kafka consumer group, so lag comes from the query progress)",
    LABELS,
)
STATE_ROWS = Gauge("pulse_state_rows", "Rows held in streaming state (dedup + windows)", LABELS)
LAST_PROGRESS = Gauge(
    "pulse_last_progress_timestamp_seconds", "Unix time of the last progress report", LABELS
)
QUERY_FAILED = Counter(
    "pulse_query_failures_total", "Streaming queries that terminated with an error", LABELS
)


class PrometheusListener(StreamingQueryListener):
    def onQueryStarted(self, event) -> None:  # noqa: N802 (Spark API name)
        pass

    def onQueryProgress(self, event) -> None:  # noqa: N802
        import json
        import time

        p = json.loads(event.progress.json)
        name = p.get("name") or "unnamed"
        v = progress_values(p)
        INPUT_ROWS.labels(name).inc(v["input_rows"])
        INPUT_RATE.labels(name).set(v["input_rate"])
        PROCESSED_RATE.labels(name).set(v["processed_rate"])
        BATCH_DURATION.labels(name).set(v["batch_seconds"])
        LATE_DROPPED.labels(name).inc(v["late_dropped"])
        STATE_ROWS.labels(name).set(v["state_rows"])
        OFFSETS_BEHIND.labels(name).set(v["offsets_behind"])
        LAST_PROGRESS.labels(name).set(time.time())

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        if event.exception:
            QUERY_FAILED.labels(str(event.id)).inc()


def start(port: int = 8000) -> None:
    start_http_server(port)
