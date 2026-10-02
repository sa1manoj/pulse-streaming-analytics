"""Spark tests for parsing, watermark dedup and windowed metrics. Run in CI (needs pyspark)."""
from __future__ import annotations

import json

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from pulse.transforms import dedupe, minute_metrics, parse  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("pulse-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


def ev(event_id, ts, event_type="page_view", price=None, variant="control"):
    return json.dumps(
        {
            "event_id": event_id,
            "user_id": "u1",
            "session_id": "s1",
            "event_type": event_type,
            "variant": variant,
            "product_id": "P001",
            "price": price,
            "event_ts": f"2025-01-01T{ts}:00Z",
        }
    )


def test_parse_splits_valid_and_quarantined(spark):
    rows = [
        (ev("a", "10:00"),),
        (ev("b", "10:01", "purchase", price=9.5),),
        ("{not json",),
        (ev("c", "10:02", "purchase", price=None),),
        (ev("d", "10:03", "hover"),),
    ]
    raw = spark.createDataFrame(rows, "value string")
    valid, quarantine = parse(raw)
    assert sorted(r.event_id for r in valid.collect()) == ["a", "b"]
    reasons = sorted(r.reject_reason for r in quarantine.collect())
    assert reasons == ["purchase_without_price", "unknown_event_type", "unparseable_json"]


def _dropped(progress) -> int:
    total = 0
    for p in progress:
        ops = p["stateOperators"] if isinstance(p, dict) else p.stateOperators
        for op in ops:
            total += op["numRowsDroppedByWatermark"] if isinstance(op, dict) else op.numRowsDroppedByWatermark
    return total


def _run_batches(spark, tmp_path, build, name):
    src = tmp_path / f"in_{name}"
    src.mkdir()
    staging = tmp_path / f"staging_{name}"
    staging.mkdir()

    def put(fname, lines):  # write then rename, so the file source never sees a partial file
        f = staging / fname
        f.write_text("\n".join(lines))
        f.rename(src / fname)

    valid, _ = parse(spark.readStream.text(str(src)))
    q = (
        build(valid)
        .writeStream.format("memory")
        .queryName(name)
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / f"chk_{name}"))
        .start()
    )
    # batch 1 sets the watermark to max(event_ts) - 10 min = 10:10
    put("a.json", [ev("e1", "10:00"), ev("e2", "10:05"), ev("e3", "10:20")])
    q.processAllAvailable()
    # batch 2: a retry of e3 (duplicate), a late event at 10:02 (behind watermark), a valid one at 10:15
    put("b.json", [ev("e3", "10:20"), ev("late", "10:02"), ev("e5", "10:15")])
    q.processAllAvailable()
    # batch 3 moves event time forward (watermark -> 10:30 after this batch) ...
    put("c.json", [ev("e6", "10:40")])
    q.processAllAvailable()
    # ... and batch 4 runs with that watermark, so windows ending by 10:30 are emitted
    put("d.json", [ev("e7", "10:41")])
    q.processAllAvailable()
    progress = q.recentProgress
    q.stop()
    return spark.table(name), progress


def test_dedupe_drops_retries_and_events_behind_watermark(spark, tmp_path):
    out, progress = _run_batches(spark, tmp_path, dedupe, "deduped")
    ids = sorted(r.event_id for r in out.collect())
    assert ids == ["e1", "e2", "e3", "e5", "e6", "e7"]
    assert _dropped(progress) >= 1


def test_minute_windows_are_emitted_after_watermark(spark, tmp_path):
    out, _ = _run_batches(spark, tmp_path, lambda df: minute_metrics(dedupe(df)), "minutes")
    rows = {r.window_start.strftime("%H:%M"): r.page_views for r in out.collect()}
    # watermark 10:30 in batch 4 -> windows 10:00, 10:05, 10:15, 10:20 closed; 10:40/10:41 still open
    assert rows == {"10:00": 1, "10:05": 1, "10:15": 1, "10:20": 1}
