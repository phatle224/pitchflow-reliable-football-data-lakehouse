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

The first full run downloads the pinned source snapshot. For a smaller smoke run, use:

```json
{
  "match_limit": 2,
  "inject_chaos": true
}
```

## Validation

```powershell
python -m unittest discover -s tests -v
python scripts/validate_project.py
docker compose config --quiet
```

## Data attribution

StatsBomb Open Data is the V1 external source. Publish the required StatsBomb attribution with any shared analysis or insight derived from its data.
