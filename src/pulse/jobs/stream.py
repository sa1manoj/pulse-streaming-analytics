"""Streaming job: Kafka clickstream -> validated, deduplicated events + per-minute metrics.

Three streaming queries share one parsed source:

* ``events``      deduplicated events (append)                 -> sink table ``events``
* ``minute``      per-minute funnel metrics by variant (append) -> sink table ``minute_metrics``
* ``quarantine``  rejected messages with a reason              -> sink table ``quarantine``

Checkpoints live under ``--checkpoint-root``, so a restarted job resumes from
the last committed Kafka offsets.

Usage:
    python -m pulse.jobs.stream --sink local --out ./data
    python -m pulse.jobs.stream --sink bigquery --project my-proj --dataset pulse_raw
"""
from __future__ import annotations

import argparse

import pyspark
from pyspark.sql import DataFrame, SparkSession

from pulse import metrics
from pulse.sinks import BigQuerySink, LocalParquetSink
from pulse.transforms import dedupe, minute_metrics, parse


def build_spark() -> SparkSession:
    pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
    return (
        SparkSession.builder.appName("pulse-stream")
        .master("local[*]")
        .config("spark.jars.packages", pkg)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .getOrCreate()
    )


def start_queries(raw: DataFrame, sink, checkpoint_root: str, trigger: dict):
    valid, quarantine = parse(raw)
    deduped = dedupe(valid)

    def writer(df: DataFrame, table: str):
        return (
            df.writeStream.queryName(table)
            .foreachBatch(lambda batch, batch_id: sink.write(batch, table, batch_id))
            .option("checkpointLocation", f"{checkpoint_root}/{table}")
            .trigger(**trigger)
        )

    return [
        writer(deduped, "events").outputMode("append").start(),
        writer(minute_metrics(deduped), "minute_metrics").outputMode("append").start(),
        writer(quarantine, "quarantine").outputMode("append").start(),
    ]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="clickstream")
    parser.add_argument("--sink", choices=["local", "bigquery"], default="local")
    parser.add_argument("--out", default="./data", help="local sink root")
    parser.add_argument("--project", help="GCP project (bigquery sink)")
    parser.add_argument("--dataset", default="pulse_raw", help="BigQuery dataset (bigquery sink)")
    parser.add_argument("--checkpoint-root", default="./checkpoints")
    parser.add_argument("--metrics-port", type=int, default=8000)
    parser.add_argument("--once", action="store_true", help="process what is available, then stop")
    args = parser.parse_args(argv)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    metrics.start(args.metrics_port)
    spark.streams.addListener(metrics.PrometheusListener())

    sink = (
        LocalParquetSink(args.out)
        if args.sink == "local"
        else BigQuerySink(project=args.project, dataset=args.dataset)
    )
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 200_000)
        .load()
    )
    trigger = {"availableNow": True} if args.once else {"processingTime": "15 seconds"}
    start_queries(raw, sink, args.checkpoint_root, trigger)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
