"""Daily Pulse batch: freshness gate -> dbt build (models + tests) -> A/B significance report.

The streaming job runs continuously and lands raw events. This DAG builds the
analytics layer once a day and refuses to do so on stale input, so dashboards
never silently show yesterday's numbers as today's.

Environment (set on the Airflow workers):
    PULSE_HOME         repo checkout (default /opt/pulse)
    PULSE_DBT_TARGET   local | bigquery
    PULSE_MAX_LAG_H    max hours since the newest raw event (default 2)
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PULSE_HOME = os.getenv("PULSE_HOME", "/opt/pulse")
DBT_TARGET = os.getenv("PULSE_DBT_TARGET", "local")


def check_freshness(**_) -> str:
    from pulse.warehouse import hours_since, latest_event_ts

    max_lag = float(os.getenv("PULSE_MAX_LAG_H", "2"))
    ts = latest_event_ts()
    lag = hours_since(ts)
    if lag > max_lag:
        raise AirflowException(f"raw events are stale: newest event {ts}, {lag:.1f}h old (limit {max_lag}h)")
    return f"newest event {ts} ({lag:.2f}h old)"


def ab_report(**_) -> str:
    from pulse.abtest import two_proportion_test
    from pulse.warehouse import variant_summary

    s = variant_summary().set_index("variant")
    r = two_proportion_test(
        int(s.loc["control", "conversions"]),
        int(s.loc["control", "sessions"]),
        int(s.loc["treatment", "conversions"]),
        int(s.loc["treatment", "sessions"]),
    )
    summary = r.summary()
    print(summary)
    if r.srm_detected:
        raise AirflowException(f"sample ratio mismatch, experiment split is broken: {summary}")
    return summary


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=45),
}

with DAG(
    dag_id="pulse_daily",
    description="Freshness gate, dbt build + tests, A/B report",
    start_date=datetime(2025, 1, 1),
    schedule="15 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["pulse"],
) as dag:
    freshness = PythonOperator(task_id="check_freshness", python_callable=check_freshness)

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PULSE_HOME}/dbt && dbt build --profiles-dir . --target {DBT_TARGET}",
    )

    report = PythonOperator(task_id="ab_report", python_callable=ab_report)

    freshness >> dbt_build >> report
