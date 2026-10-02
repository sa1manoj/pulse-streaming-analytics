"""DAG integrity: the DAG file imports cleanly and has the expected shape. Needs apache-airflow."""
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")  # not just the repo's airflow/ folder

from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_pulse_daily_shape(dagbag):
    dag = dagbag.get_dag("pulse_daily")
    assert dag is not None
    assert [t.task_id for t in dag.topological_sort()] == ["check_freshness", "dbt_build", "ab_report"]
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_every_task_retries(dagbag):
    for task in dagbag.get_dag("pulse_daily").tasks:
        assert task.retries >= 1, task.task_id
