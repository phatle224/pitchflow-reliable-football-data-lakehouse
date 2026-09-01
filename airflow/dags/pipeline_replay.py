"""Replay selected Bronze records through Silver, Gold, and serving without shell interpolation."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from spark.common.orchestration import airflow_default_args, required_string_list, submit_spark_job, validate_replay_reason


def _config(context: dict[str, Any]) -> dict[str, Any]:
    return dict((context["dag_run"].conf or {}))


def _bronze_to_silver(**context: Any) -> None:
    config = _config(context)
    validate_replay_reason(config)
    bronze_record_ids = required_string_list(config, "bronze_record_ids")
    arguments = ["--pipeline-run-id", context["dag_run"].run_id]
    for record_id in bronze_record_ids:
        arguments.extend(["--bronze-record-id", record_id])
    submit_spark_job("spark/jobs/bronze_to_silver.py", arguments)


def _silver_to_gold(**context: Any) -> None:
    config = _config(context)
    match_ids = required_string_list(config, "match_ids")
    arguments = ["--pipeline-run-id", context["dag_run"].run_id]
    for match_id in match_ids:
        arguments.extend(["--match-id", match_id])
    submit_spark_job("spark/jobs/silver_to_gold.py", arguments)


def _publish_serving(**context: Any) -> None:
    submit_spark_job(
        "serving/publish_postgres/publish.py",
        ["--pipeline-run-id", context["dag_run"].run_id],
    )


def _update_quarantine(**context: Any) -> None:
    """Mark replayed quarantine records as REPROCESSED after a successful replay."""
    config = _config(context)
    bronze_record_ids = required_string_list(config, "bronze_record_ids")
    replay_reason = validate_replay_reason(config)
    arguments = [
        "--pipeline-run-id", context["dag_run"].run_id,
        "--replay-reason", replay_reason,
    ]
    for record_id in bronze_record_ids:
        arguments.extend(["--bronze-record-id", record_id])
    submit_spark_job("spark/jobs/update_quarantine_replay.py", arguments)


with DAG(
    dag_id="pipeline_replay",
    description="Replay selected Bronze records after a DQ, mapping, or reference-data fix.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=airflow_default_args(),
    render_template_as_native_obj=False,
    tags=["pitchflow", "replay", "v2-reliability"],
) as dag:
    bronze_to_silver = PythonOperator(task_id="bronze_to_silver", python_callable=_bronze_to_silver)
    silver_to_gold = PythonOperator(task_id="silver_to_gold", python_callable=_silver_to_gold)
    publish_serving = PythonOperator(task_id="publish_serving", python_callable=_publish_serving)
    update_quarantine = PythonOperator(task_id="update_quarantine", python_callable=_update_quarantine)

    bronze_to_silver >> silver_to_gold >> publish_serving >> update_quarantine

