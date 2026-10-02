"""Pure parsing of Spark StreamingQueryProgress JSON (no Spark import, unit-testable)."""
from __future__ import annotations


def progress_values(progress: dict) -> dict:
    """Extract the numbers we export from a StreamingQueryProgress JSON (pure, testable)."""
    ops = progress.get("stateOperators") or []
    behind = [
        float(src.get("metrics", {}).get("maxOffsetsBehindLatest") or 0)
        for src in progress.get("sources") or []
    ]
    return {
        "offsets_behind": max(behind, default=0.0),
        "input_rows": int(progress.get("numInputRows") or 0),
        "input_rate": float(progress.get("inputRowsPerSecond") or 0.0),
        "processed_rate": float(progress.get("processedRowsPerSecond") or 0.0),
        "batch_seconds": float((progress.get("durationMs") or {}).get("triggerExecution", 0)) / 1000,
        "late_dropped": sum(int(o.get("numRowsDroppedByWatermark") or 0) for o in ops),
        "state_rows": sum(int(o.get("numRowsTotal") or 0) for o in ops),
    }
