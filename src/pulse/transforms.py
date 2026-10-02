"""Spark transformations for the clickstream job (kept separate so tests can drive them).

Correctness rules
-----------------
* Malformed JSON or events failing validation go to a quarantine output with a
  reason, instead of silently disappearing.
* **Late events**: a 10-minute event-time watermark. Events that arrive more
  than 10 minutes behind the newest event time seen are dropped (from both the
  deduplicated events and the aggregates). Spark reports how many in
  ``numRowsDroppedByWatermark``, which is exported to Prometheus and alerted on.
  Events up to 10 minutes late are still counted in the right minute.
* **Duplicates**: ``dropDuplicatesWithinWatermark(["event_id"])`` keeps the
  first copy of each event_id while it is within the watermark, so dedup state
  stays bounded instead of growing forever.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pulse.events import EVENT_TYPES, FIELDS, VARIANTS

WATERMARK = "10 minutes"
WINDOW = "1 minute"

_TYPES = {"string": T.StringType(), "double": T.DoubleType(), "timestamp": T.StringType()}
CORRUPT = "_corrupt_record"
EVENT_SCHEMA = T.StructType(
    [T.StructField(n, _TYPES[t]) for n, t in FIELDS] + [T.StructField(CORRUPT, T.StringType())]
)


def parse(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Kafka rows (value: string/binary) -> (valid events, quarantined rows with a reason)."""
    value = F.col("value").cast("string")
    # PERMISSIVE mode yields a struct of nulls for malformed JSON (not a null struct);
    # the corrupt-record column is how we tell "malformed" apart from "missing fields".
    opts = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT}
    parsed = raw.select(value.alias("raw"), F.from_json(value, EVENT_SCHEMA, opts).alias("e"))
    e = F.col("e")
    ts = F.to_timestamp(e.getField("event_ts"))

    reason = (
        F.when(e.isNull() | e.getField(CORRUPT).isNotNull(), F.lit("unparseable_json"))
        .when(e.getField("event_id").isNull(), F.lit("missing_event_id"))
        .when(ts.isNull(), F.lit("bad_event_ts"))
        .when(~e.getField("event_type").isin(*EVENT_TYPES), F.lit("unknown_event_type"))
        .when(~e.getField("variant").isin(*VARIANTS), F.lit("unknown_variant"))
        .when(
            (e.getField("event_type") == "purchase")
            & (e.getField("price").isNull() | (e.getField("price") <= 0)),
            F.lit("purchase_without_price"),
        )
    )
    tagged = parsed.withColumn("reject_reason", reason)

    valid = tagged.where(F.col("reject_reason").isNull()).select(
        *[e.getField(n).alias(n) for n, t in FIELDS if t != "timestamp"],
        ts.alias("event_ts"),
    )
    quarantine = tagged.where(F.col("reject_reason").isNotNull()).select(
        "raw", "reject_reason", F.current_timestamp().alias("quarantined_at")
    )
    return valid, quarantine


def dedupe(events: DataFrame) -> DataFrame:
    """Drop retried sends (same event_id) and bound state with the watermark."""
    return events.withWatermark("event_ts", WATERMARK).dropDuplicatesWithinWatermark(["event_id"])


def minute_metrics(deduped: DataFrame) -> DataFrame:
    """Per-minute funnel counts and revenue, emitted once the watermark passes the window.

    Expects the output of ``dedupe`` (which already carries the event-time watermark).
    """
    return (
        deduped.groupBy(F.window("event_ts", WINDOW).alias("w"), "variant")
        .agg(
            F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_views"),
            F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_carts"),
            F.count(F.when(F.col("event_type") == "checkout", 1)).alias("checkouts"),
            F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases"),
            F.coalesce(F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))), F.lit(0.0)).alias(
                "revenue"
            ),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "variant",
            "page_views",
            "add_to_carts",
            "checkouts",
            "purchases",
            "revenue",
        )
    )
