"""Safe Airflow-facing helpers for retry policy, alerts and replay submission."""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta
from typing import Any, Mapping

from spark.common.alerts import send_webhook_alert


SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"


def airflow_default_args() -> dict[str, Any]:
    """Return retry settings controlled by environment variables for all V2 DAGs."""

    retries = int(os.getenv("PITCHFLOW_AIRFLOW_RETRIES", "2"))
    retry_minutes = int(os.getenv("PITCHFLOW_AIRFLOW_RETRY_DELAY_MINUTES", "5"))
    max_retry_minutes = int(os.getenv("PITCHFLOW_AIRFLOW_MAX_RETRY_DELAY_MINUTES", "30"))
    if retries < 0 or retry_minutes <= 0 or max_retry_minutes < retry_minutes:
        raise ValueError("Invalid PitchFlow Airflow retry configuration.")
    return {
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_minutes),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=max_retry_minutes),
        "on_failure_callback": airflow_failure_alert,
        "on_retry_callback": airflow_retry_alert,
    }


def airflow_failure_alert(context: Mapping[str, Any]) -> None:
    """Alert after retries are exhausted without raising a secondary exception."""

    dag_id = context.get("dag", getattr(context.get("task_instance"), "dag", None))
    dag_name = getattr(dag_id, "dag_id", "unknown_dag")
    task_instance = context.get("task_instance")
    task_id = getattr(task_instance, "task_id", "unknown_task")
    run_id = getattr(task_instance, "run_id", context.get("run_id", "unknown_run"))
    error = str(context.get("exception", "Task failed after retries."))
    send_webhook_alert(
        severity="FAILED",
        title=f"{dag_name}.{task_id}",
        message=f"run_id={run_id}; reason={error}",
    )


def airflow_retry_alert(context: Mapping[str, Any]) -> None:
    """Alert when a task is about to be retried so operators see early signals."""

    dag_id = context.get("dag", getattr(context.get("task_instance"), "dag", None))
    dag_name = getattr(dag_id, "dag_id", "unknown_dag")
    task_instance = context.get("task_instance")
    task_id = getattr(task_instance, "task_id", "unknown_task")
    run_id = getattr(task_instance, "run_id", context.get("run_id", "unknown_run"))
    try_number = getattr(task_instance, "try_number", "?")
    error = str(context.get("exception", "Task retrying."))
    send_webhook_alert(
        severity="WARNING",
        title=f"{dag_name}.{task_id} retry",
        message=f"run_id={run_id}; attempt={try_number}; reason={error}",
    )


def required_string_list(config: Mapping[str, Any] | None, key: str) -> list[str]:
    """Validate replay selections before passing values as subprocess arguments."""

    values = (config or {}).get(key)
    if not isinstance(values, list) or not values or not all(isinstance(value, str) and value.strip() for value in values):
        raise ValueError(f"DAG run config must include a non-empty list of strings named '{key}'.")
    return [value.strip() for value in values]


def validate_replay_reason(config: Mapping[str, Any]) -> str:
    """Require a non-empty replay_reason for audit traceability."""
    reason = config.get("replay_reason", "").strip()
    if not reason:
        raise ValueError("DAG run config must include a non-empty 'replay_reason' string.")
    return reason


def submit_spark_job(script: str, arguments: list[str]) -> None:
    """Run a project Spark job without rendering operator-provided values into a shell."""

    command = [
        "spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--packages",
        SPARK_PACKAGES,
        "--conf",
        "spark.executorEnv.PYTHONPATH=/opt/pitchflow",
        f"/opt/pitchflow/{script}",
        *arguments,
    ]
    subprocess.run(command, cwd="/opt/pitchflow", check=True)
