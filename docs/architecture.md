# Architecture

## Current status

V1 is implemented as a local Docker Compose stack. V2 adds configurable quality gates, persisted processing watermarks, retry/alert policy, dedicated replay/correction DAGs, and correction-resolution audit fields. The service topology, data contracts, and workflow below describe the running implementation.

## Component boundaries

```text
StatsBomb Open Data Premier League 2015/16 JSON snapshot
        + controlled synthetic/chaos event variants
        -> Python ingestion
        -> Bronze Delta tables on MinIO
        -> Spark transformation and data-quality checks
        -> Silver Delta tables + Quarantine Delta tables on MinIO
        -> Spark aggregations
        -> Gold Delta tables on MinIO
        -> PostgreSQL serving tables
        -> Metabase dashboards

Airflow schedules and coordinates each stage.
```

- **Ingestion** performs accessibility, parsing, and envelope checks only. It attaches lineage metadata and writes Bronze.
- **External source** is the pinned Premier League 2015/16 snapshot (competition 2, season 27) from StatsBomb Open Data. Competition, match, lineup, and event JSON files produce the reference entities and match events; the source snapshot is pinned to a Git commit SHA.
- **Bronze** is the append-only raw source of truth. It contains raw payloads and ingestion metadata.
- **Spark transformation and quality** parses, standardizes, validates, deduplicates, and routes invalid records to Quarantine.
- **Silver** contains typed, conformed, deduplicated entities. **Gold** contains analytics-ready aggregates.
- **MinIO** is the mandatory V1 S3-compatible object store for Delta tables. PostgreSQL is not a replacement lakehouse store.
- **PostgreSQL and Metabase** expose only selected Gold datasets for dashboard queries.

## Dependency direction

```text
Airflow -> ingestion / Spark jobs / serving publish
ingestion -> Bronze storage contract
Spark jobs -> Bronze, Silver, Gold, Quarantine storage contracts
serving publish -> Gold contract + PostgreSQL
Metabase -> PostgreSQL only
```

Lower data layers must not depend on dashboards or PostgreSQL. Airflow must not contain the core transformation logic that belongs in Spark jobs.

## V1 Delta layout

All locations use the MinIO bucket `pitchflow`:

```text
s3a://pitchflow/bronze/<entity>
s3a://pitchflow/silver/<entity>
s3a://pitchflow/gold/<dataset>
s3a://pitchflow/quarantine/<entity>
```

Bronze and Silver entities are matches, match events, teams, players, and stadiums. Gold datasets are match summary, team performance, and event distribution. See `IMPLEMENTATION_PLAN.md` for exact V1 behavior.

## Data reliability policies

- Bronze records have deterministic record IDs so a retry of the same source record is safe.
- Same event ID with an identical payload is measured and deduplicated in Silver.
- Same event ID with a changed payload is quarantined for review in V1; it is not silently overwritten.
- Valid late events are accepted, marked, and cause the affected match aggregates to be rebuilt.
- Quarantine references the original Bronze record; it never becomes a second raw-data source of truth.
- DQ metrics have a configurable healthy/warning/failed gate. A failed gate preserves its metrics and Quarantine evidence before the Spark task fails.
- `ops/processing_watermarks` stores a monotonic `match_events` high watermark independently of Silver table layout.
- Event corrections require an explicit V2 review/approve/reject action. Approval updates the existing Silver event only after revalidation, then rebuilds Gold.
- Controlled synthetic records start from valid StatsBomb events and inject known failures; they are labeled with a distinct source and never replace the original event payload.
