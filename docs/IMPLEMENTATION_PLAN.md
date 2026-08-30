# PitchFlow V1 — Delta Lake + MinIO implementation plan

## Summary

Update [PitchFlow_PRD.md](D:\project\pitchflow-reliable-football-data-lakehouse\PitchFlow_PRD.md) in English so V1 is an implementable Docker Compose lakehouse: Delta tables stored in MinIO; Airflow orchestrates Spark; only Gold datasets publish to PostgreSQL/Metabase.

V1 uses a version-pinned StatsBomb Open Data FIFA World Cup 2022 JSON snapshot (`competition_id=43`, `season_id=106`) plus controlled synthetic/chaos event variants. Live API ingestion, a dedicated replay DAG, and broad observability remain later phases.

## PRD and architecture changes

- Make **MinIO the required V1 object store**, replacing “Delta + local disk” as the default. Document MinIO as the local S3 equivalent and AWS S3 as the production mapping.
- Define one MinIO bucket, `pitchflow`, with Delta locations:
  - `s3a://pitchflow/bronze/<entity>`
  - `s3a://pitchflow/silver/<entity>`
  - `s3a://pitchflow/gold/<dataset>`
  - `s3a://pitchflow/quarantine/<entity>`
- Define V1 source objects: `competitions.json`, `matches/43/106.json`, `lineups/<match_id>.json`, and `events/<match_id>.json` from a StatsBomb snapshot pinned to a commit SHA. Derive teams, players, stadiums, matches, lineups, and match events from those raw objects.
- Generate controlled duplicate, malformed, correction, and late-event variants from valid StatsBomb events. Mark them as synthetic source records and retain the original raw event unchanged.
- Define Delta tables:
  - Bronze/Silver: matches, match events, teams, players, stadiums.
  - Gold/PostgreSQL: `gold_match_summary`, `gold_team_performance`, `gold_event_distribution`.
- Add a Docker Compose architecture: Airflow scheduler/webserver, Spark execution environment, MinIO plus bucket initialization, PostgreSQL, and Metabase. Pin compatible Spark, Delta Lake, Hadoop S3A, and MinIO client dependencies in the implementation.
- State clear ownership: Delta/MinIO stores raw through analytics data; PostgreSQL holds only dashboard-serving Gold datasets; Metabase queries PostgreSQL only.

## Data contracts and pipeline behavior

- Bronze remains append-only and stores raw payload plus `bronze_record_id`, source/entity metadata, source timestamp, ingestion timestamp, `pipeline_run_id`, source locator, schema version, payload hash, and partition date.
- Make `bronze_record_id` deterministic from source, source object, stable source-record locator, and payload hash. This makes an Airflow retry safe without dropping genuinely repeated upstream records.
- Enforce required Silver schema and types. Missing/renamed required fields, invalid values, and referential failures go to Quarantine; unexpected extra fields remain preserved in Bronze and create a warning metric.
- Define event handling:
  - Same `event_id` and same payload hash: deduplicate in Silver and emit a duplicate metric.
  - Same `event_id` with changed payload: quarantine as a correction for review; never silently overwrite history in V1.
  - Valid late event: accept it, mark/measure it as late, upsert Silver, rebuild Gold datasets for its affected `match_id`, then UPSERT PostgreSQL.
- Use run-based incremental processing in V1: Bronze-to-Silver reads the current `pipeline_run_id`; Silver-to-Gold rebuilds only affected matches/teams. Add watermark-based processing as V2 work.
- Define Quarantine as metadata referencing `bronze_record_id`, with failed rule, error message/column, status, retry count, timestamps, and rule version. Raw data remains authoritative only in Bronze.

## Interfaces and orchestration

- Define `pipeline_daily` with scheduled daily execution and manual triggering. Its four tasks are:
  1. `ingest_raw`
  2. `bronze_to_silver`
  3. `silver_to_gold`
  4. `publish_serving`
- Standardize DAG/job parameters: `pipeline_run_id`, `run_date`, optional `source`, and optional `match_id` for scoped downstream rebuilds.
- Define a documented manual replay command/interface taking `bronze_record_id`, `pipeline_run_id`, or `match_id`. A separate Airflow replay DAG is explicitly deferred to V2.
- Publish Gold data to PostgreSQL via idempotent UPSERT keyed by each dataset’s business grain; validate row counts before marking the pipeline run complete.
- Require configuration through environment variables for MinIO endpoint/credentials, bucket, PostgreSQL connection, schedule, DQ threshold, and late-event threshold. Keep secrets out of the PRD and repository.

## Test and acceptance plan

- Validate that a full Docker Compose stack can ingest the pinned StatsBomb snapshot and generated event variants through Bronze, Silver, Gold, PostgreSQL, and Metabase.
- Re-run the same logical batch and verify no duplicate Silver, Gold, or PostgreSQL business rows.
- Verify exact duplicate events are measured but not duplicated in Silver.
- Verify malformed/missing-required-field events enter Quarantine with a valid Bronze reference while healthy records continue.
- Verify an event-ID correction is quarantined rather than overwriting Silver.
- Verify a valid late event updates the affected match’s Gold results and PostgreSQL rows only.
- Verify MinIO contains Delta Parquet files and `_delta_log` for each table.
- Verify Bronze lineage contains the StatsBomb URI and resolved source commit SHA.
- Update V1 acceptance criteria and README/documentation requirements to demonstrate all behaviors above.

## Assumptions locked

- PRD remains English; discussion and implementation guidance can be Vietnamese.
- Docker Compose is the only supported local runtime for V1.
- MinIO is mandatory in V1; local-disk Delta is not a supported primary profile.
- V1 uses a pinned StatsBomb Open Data JSON snapshot plus controlled synthetic variants, not a live football API or independently maintained CSV master data.
- V1 has documented manual replay, not a dedicated replay DAG.
- V2 adds controlled correction processing, watermark refinement, configurable replay DAG, and broader reliability automation.
