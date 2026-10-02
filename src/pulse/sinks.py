"""Micro-batch sinks used from ``foreachBatch``.

``foreachBatch`` is at-least-once: after a failure Spark can replay a batch
with the same ``batch_id``. Both sinks make replays harmless:

* LocalParquetSink overwrites the folder for that batch_id.
* BigQuerySink tags rows with ``_batch_id``; dbt staging keeps one row per
  ``event_id`` (and per window/variant for metrics), so a replayed load does
  not double count.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class LocalParquetSink:
    root: str

    def write(self, df: DataFrame, table: str, batch_id: int) -> None:
        (
            df.withColumn("_batch_id", F.lit(batch_id))
            .write.mode("overwrite")
            .parquet(f"{self.root}/{table}/batch={batch_id:010d}")
        )


@dataclass
class BigQuerySink:
    project: str
    dataset: str

    def write(self, df: DataFrame, table: str, batch_id: int) -> None:
        from google.cloud import bigquery

        pdf = df.withColumn("_batch_id", F.lit(batch_id)).toPandas()
        if pdf.empty:
            return
        client = bigquery.Client(project=self.project)
        job = client.load_table_from_dataframe(
            pdf,
            f"{self.project}.{self.dataset}.{table}",
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        )
        job.result()
