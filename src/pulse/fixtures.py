"""Write a synthetic raw layer (parquet) so dbt can be built and tested without Kafka or Spark.

Mimics what the stream job lands: deduplicated events and per-minute metrics,
one folder per micro-batch. One batch is deliberately written twice under a
new batch id, the way a replayed ``foreachBatch`` load would look in
BigQuery, so the dbt staging dedup is exercised.

Usage:
    python -m pulse.fixtures --out ./data_fixture --sessions 4000
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from pulse.generator import GeneratorConfig, generate


def events_frame(sessions: int, seed: int) -> pd.DataFrame:
    rows = [e.__dict__ for e in generate(sessions, GeneratorConfig(seed=seed))]
    df = pd.DataFrame(rows).drop_duplicates(subset=["event_id"], keep="first")
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)
    return df.reset_index(drop=True)


def minute_metrics_frame(events: pd.DataFrame) -> pd.DataFrame:
    e = events.assign(window_start=events["event_ts"].dt.floor("min"))
    out = (
        e.assign(
            page_views=e["event_type"].eq("page_view"),
            add_to_carts=e["event_type"].eq("add_to_cart"),
            checkouts=e["event_type"].eq("checkout"),
            purchases=e["event_type"].eq("purchase"),
            revenue=e["price"].where(e["event_type"].eq("purchase"), 0.0).fillna(0.0),
        )
        .groupby(["window_start", "variant"], as_index=False)[
            ["page_views", "add_to_carts", "checkouts", "purchases", "revenue"]
        ]
        .sum()
    )
    out["window_end"] = out["window_start"] + pd.Timedelta(minutes=1)
    for c in ("page_views", "add_to_carts", "checkouts", "purchases"):
        out[c] = out[c].astype("int64")
    return out[["window_start", "window_end", "variant", "page_views", "add_to_carts", "checkouts",
                "purchases", "revenue"]]


def write(out: str, sessions: int, seed: int, batches: int = 5) -> dict[str, int]:
    root = Path(out)
    events = events_frame(sessions, seed)
    chunks = [events.iloc[i::batches] for i in range(batches)]
    for i, chunk in enumerate(chunks):
        _write(root / "events", i, chunk)
    _write(root / "events", batches, chunks[0])  # replayed batch -> must be deduped by dbt
    _write(root / "minute_metrics", 0, minute_metrics_frame(events))
    return {"events": len(events), "replayed_rows": len(chunks[0])}


def _write(table_root: Path, batch_id: int, df: pd.DataFrame) -> None:
    d = table_root / f"batch={batch_id:010d}"
    d.mkdir(parents=True, exist_ok=True)
    df.assign(_batch_id=batch_id).to_parquet(d / "part-0.parquet", index=False)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="./data_fixture")
    parser.add_argument("--sessions", type=int, default=4000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)
    print(f"wrote fixture to {args.out}: {write(args.out, args.sessions, args.seed)}")


if __name__ == "__main__":
    main()
