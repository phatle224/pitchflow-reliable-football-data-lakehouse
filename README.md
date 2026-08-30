# PitchFlow — Reliable Football Data Lakehouse

PitchFlow is a local, Docker Compose ELT lakehouse for football analytics and data reliability. It stores Delta Lake Bronze, Silver, Gold, Quarantine, and quality-metric tables in MinIO; Airflow orchestrates Spark jobs; PostgreSQL serves selected Gold tables to Metabase.

V1 uses a pinned StatsBomb Open Data FIFA World Cup 2022 snapshot and optional controlled bad-data variants. See [architecture](docs/architecture.md), [data sources](docs/data-sources.md), and [DQ rules](docs/dq_rules.md).

## Start locally

```powershell
Copy-Item .env.example .env
docker compose up --build -d
```

Open Airflow at `http://localhost:8088`, MinIO at `http://localhost:9001`, and Metabase at `http://localhost:3000`. Trigger `pipeline_daily` manually. To exercise Quarantine/DQ behavior, pass DAG run config:

```json
{
  "inject_chaos": true
}
```

The DAG starts paused, so first unpause it in the Airflow UI or run:

```powershell
docker compose exec -T airflow-scheduler airflow dags unpause pipeline_daily
```

The first full run downloads the pinned source snapshot. For a smaller smoke run, use:

```json
{
  "match_limit": 2,
  "inject_chaos": true
}
```

## Replay a Bronze selection

V1 supports a focused manual replay without a separate replay DAG. Run `bronze_to_silver.py` with one or more `--bronze-record-id` values, then rebuild the affected match in Gold and republish serving data. Use a new replay run ID for auditability.

```powershell
docker compose exec -T airflow-scheduler spark-submit --master spark://spark-master:7077 --deploy-mode client --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.executorEnv.PYTHONPATH=/opt/pitchflow /opt/pitchflow/spark/jobs/bronze_to_silver.py --pipeline-run-id replay-001 --bronze-record-id <bronze-record-id>
docker compose exec -T airflow-scheduler spark-submit --master spark://spark-master:7077 --deploy-mode client --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.executorEnv.PYTHONPATH=/opt/pitchflow /opt/pitchflow/spark/jobs/silver_to_gold.py --pipeline-run-id replay-001 --match-id <match-id>
docker compose exec -T airflow-scheduler spark-submit --master spark://spark-master:7077 --deploy-mode client --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.executorEnv.PYTHONPATH=/opt/pitchflow /opt/pitchflow/serving/publish_postgres/publish.py --pipeline-run-id replay-001
```

## Validation

```powershell
python -m unittest discover -s tests -v
python scripts/validate_project.py
docker compose config --quiet
```

## Data attribution

StatsBomb Open Data is the V1 external source. Publish the required StatsBomb attribution with any shared analysis or insight derived from its data.
