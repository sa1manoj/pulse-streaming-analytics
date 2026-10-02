from pulse.progress import progress_values

PROGRESS = {
    "name": "events",
    "numInputRows": 1200,
    "inputRowsPerSecond": 80.0,
    "processedRowsPerSecond": 400.0,
    "durationMs": {"triggerExecution": 3000},
    "stateOperators": [
        {"numRowsTotal": 5000, "numRowsDroppedByWatermark": 7},
        {"numRowsTotal": 120, "numRowsDroppedByWatermark": 3},
    ],
    "sources": [{"metrics": {"maxOffsetsBehindLatest": "42", "avgOffsetsBehindLatest": "10.0"}}],
}


def test_extracts_exported_values():
    v = progress_values(PROGRESS)
    assert v["input_rows"] == 1200
    assert v["batch_seconds"] == 3.0
    assert v["late_dropped"] == 10
    assert v["state_rows"] == 5120
    assert v["offsets_behind"] == 42.0


def test_tolerates_missing_fields():
    v = progress_values({"name": "x"})
    assert v == {
        "offsets_behind": 0.0,
        "input_rows": 0,
        "input_rate": 0.0,
        "processed_rate": 0.0,
        "batch_seconds": 0.0,
        "late_dropped": 0,
        "state_rows": 0,
    }
