"""Small query helpers for the Airflow tasks, for the local (DuckDB) or BigQuery target."""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd


def _target() -> str:
    return os.getenv("PULSE_DBT_TARGET", "local")


def _query(sql_local: str, sql_bq: str) -> pd.DataFrame:
    if _target() == "bigquery":
        from google.cloud import bigquery

        return bigquery.Client(project=os.environ["GCP_PROJECT"]).query(sql_bq).to_dataframe()
    import duckdb

    con = duckdb.connect(os.getenv("DUCKDB_PATH", "pulse.duckdb"))
    try:
        return con.execute(sql_local).fetchdf()
    finally:
        con.close()


def latest_event_ts() -> datetime | None:
    data = os.getenv("PULSE_DATA", "./data")
    raw_ds = os.getenv("PULSE_RAW_DATASET", "pulse_raw")
    df = _query(
        f"select max(event_ts) as ts from read_parquet('{data}/events/*/*.parquet', union_by_name = true)",
        f"select max(event_ts) as ts from `{os.getenv('GCP_PROJECT')}.{raw_ds}.events`",
    )
    ts = df["ts"].iloc[0]
    if ts is None or pd.isna(ts):
        return None
    ts = pd.Timestamp(ts)
    return (ts.tz_convert("UTC") if ts.tzinfo else ts.tz_localize("UTC")).to_pydatetime()


def variant_summary() -> pd.DataFrame:
    table = f"`{os.getenv('GCP_PROJECT')}.pulse_analytics.ab_variant_summary`"
    return _query(
        "select variant, sessions, conversions from ab_variant_summary",
        f"select variant, sessions, conversions from {table}",
    )


def hours_since(ts: datetime | None, now: datetime | None = None) -> float:
    if ts is None:
        return float("inf")
    now = now or datetime.now(timezone.utc)
    return (now - ts).total_seconds() / 3600
