"""Approve or reject Quarantine corrections, then rebuild the affected Gold datasets."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from spark.common.orchestration import airflow_default_args, required_string_list, submit_spark_job


def _config(context: dict[str, Any]) -> dict[str, Any]:
    return dict((context["dag_run"].conf or {}))


def _resolve_corrections(**context: Any) -> None:
    config = _config(context)
    quarantine_ids = required_string_list(config, "quarantine_ids")
    action = config.get("action")
    if action not in {"approve", "reject", "under_review"}:
        raise ValueError("DAG run config requires action: approve, reject, or under_review.")
    note = config.get("resolution_note", "")
    if not isinstance(note, str):
        raise ValueError("resolution_note must be a string when provided.")
    arguments = ["--pipeline-run-id", context["dag_run"].run_id, "--action", action, "--resolution-note", note]
    for quarantine_id in quarantine_ids:
        arguments.extend(["--quarantine-id", quarantine_id])
    submit_spark_job("spark/jobs/resolve_corrections.py", arguments)


def _silver_to_gold(**context: Any) -> None:
    config = _config(context)
    if config.get("action") != "approve":
        return
    match_ids = required_string_list(config, "match_ids")
    arguments = ["--pipeline-run-id", context["dag_run"].run_id]
    for match_id in match_ids:
        arguments.extend(["--match-id", match_id])
    submit_spark_job("spark/jobs/silver_to_gold.py", arguments)


def _publish_serving(**context: Any) -> None:
    if _config(context).get("action") == "approve":
        submit_spark_job(
            "serving/publish_postgres/publish.py",
            ["--pipeline-run-id", context["dag_run"].run_id],
        )


with DAG(
    dag_id="pipeline_resolve_correction",
    description="Apply an explicit Quarantine correction decision with an auditable run ID.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=airflow_default_args(),
    render_template_as_native_obj=False,
    tags=["pitchflow", "correction", "v2-reliability"],
) as dag:
    resolve_corrections = PythonOperator(task_id="resolve_corrections", python_callable=_resolve_corrections)
    silver_to_gold = PythonOperator(task_id="silver_to_gold", python_callable=_silver_to_gold)
    publish_serving = PythonOperator(task_id="publish_serving", python_callable=_publish_serving)

    resolve_corrections >> silver_to_gold >> publish_serving
