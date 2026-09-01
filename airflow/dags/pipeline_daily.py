"""Daily orchestration for the PitchFlow V1 ELT lakehouse."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from spark.common.orchestration import airflow_default_args

SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"
SPARK_SUBMIT = (
    "cd /opt/pitchflow && spark-submit --master spark://spark-master:7077 --deploy-mode client "
    "--packages " + SPARK_PACKAGES + " "
    "--conf spark.executorEnv.PYTHONPATH=/opt/pitchflow "
)
RUN_ID = "{{ run_id }}"


with DAG(
    dag_id="pipeline_daily",
    description="Ingest the pinned StatsBomb Premier League snapshot and publish Gold football analytics.",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=airflow_default_args(),
    render_template_as_native_obj=False,
    tags=["pitchflow", "delta", "football", "v2-reliability"],
) as dag:
    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command=(
            SPARK_SUBMIT
            + "/opt/pitchflow/spark/jobs/ingest_raw.py --pipeline-run-id '"
            + RUN_ID
            + "' {% if dag_run.conf and dag_run.conf.get('match_limit') %} --match-limit {{ dag_run.conf.get('match_limit') }} {% endif %}"
            + " {% if dag_run.conf and dag_run.conf.get('inject_chaos', false) %} --inject-chaos {% endif %}"
        ),
    )
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=SPARK_SUBMIT + "/opt/pitchflow/spark/jobs/bronze_to_silver.py --pipeline-run-id '" + RUN_ID + "'",
    )
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=SPARK_SUBMIT + "/opt/pitchflow/spark/jobs/silver_to_gold.py --pipeline-run-id '" + RUN_ID + "'",
    )
    publish_serving = BashOperator(
        task_id="publish_serving",
        bash_command=SPARK_SUBMIT + "/opt/pitchflow/serving/publish_postgres/publish.py --pipeline-run-id '" + RUN_ID + "'",
    )

    ingest_raw >> bronze_to_silver >> silver_to_gold >> publish_serving
