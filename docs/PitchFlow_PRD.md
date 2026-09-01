# PRD — PitchFlow: Football Match Intelligence & Data Reliability Platform

## 1. Product Overview

**Project name:** PitchFlow  
**Project type:** Data Engineering / ELT Lakehouse / Data Reliability  
**Primary domain:** Football analytics  
**Target users:** Data Engineers, Data Analysts, football analysts, portfolio reviewers/recruiters  
**Primary objective:** Build a reliable, reproducible ELT data platform that ingests football data from multiple sources, preserves raw data, validates and transforms it through Bronze–Silver–Gold layers, isolates invalid records, supports replay, and publishes analytics-ready datasets to a serving layer.

PitchFlow is not intended to be only a football dashboard. The project is designed primarily as a **Data Engineering portfolio project** that demonstrates production-oriented concepts such as:

- ELT architecture
- Lakehouse / Medallion architecture
- Multi-source ingestion
- Incremental processing
- Data quality
- Quarantine
- Replay / reprocessing
- Idempotency
- Schema drift handling
- Late-arriving data
- Data observability
- Orchestration
- Serving-layer design

The football domain is used because it naturally provides entities, relationships, time-series events, business rules, and failure scenarios that are well suited to demonstrating these concepts.

---

# 2. Problem Statement

Football data may arrive from multiple heterogeneous sources such as APIs, CSV files, and generated match events.

These sources may contain:

- Duplicate events
- Missing identifiers
- Invalid timestamps
- Invalid match minutes
- Incorrect player/team relationships
- Late-arriving events
- Schema changes
- Malformed payloads
- Unexpected values
- Partial data

A simple ETL demo may process only valid static data and generate a dashboard. PitchFlow instead aims to answer:

> Can a football data platform continue to operate reliably when upstream data is incomplete, late, duplicated, malformed, or structurally changed?

The system must preserve raw source data, detect quality issues, isolate invalid records without unnecessarily blocking healthy data, allow historical reprocessing, and expose both football analytics and pipeline health metrics.

---

# 3. Goals

## 3.1 Primary Goals

1. Build an end-to-end **ELT pipeline**.
2. Use a **Bronze–Silver–Gold Medallion Architecture**.
3. Preserve Bronze data as an append-only source of truth.
4. Support multiple source styles across the roadmap:
   - version-pinned open-data snapshots
   - controlled generated variants
   - later API and CSV sources
5. Perform transformations using PySpark.
6. Implement configurable data quality rules.
7. Route invalid records into a Quarantine layer.
8. Support replay after transformation or rule fixes.
9. Make pipeline tasks idempotent where applicable.
10. Support incremental processing.
11. Publish Gold datasets into PostgreSQL.
12. Build football and data-reliability dashboards in Metabase.
13. Orchestrate the workflow using Apache Airflow.
14. Track pipeline runs using `pipeline_run_id`.
15. Add monitoring and alerting progressively.

## 3.2 Portfolio Goals

The project should demonstrate that the developer understands **why each component exists**, not only how to configure the tool.

A reviewer should be able to identify evidence of:

- SQL
- Python
- PySpark
- Data modeling
- ETL/ELT concepts
- Data Warehouse concepts
- Airflow
- Delta Lake
- PostgreSQL
- Docker
- Data Quality
- Production failure handling

---

# 4. Non-Goals

The first version will NOT prioritize:

- Machine Learning prediction
- Real-time Kafka streaming
- Kubernetes
- Terraform
- Multi-cloud deployment
- Large-scale distributed infrastructure
- Complex authentication
- Enterprise governance
- High-availability clusters

These may be added later only if they solve a clear project requirement.

---

# 5. Architecture

```text
                 ┌────────────────────────────┐
                 │          AIRFLOW           │
                 │                            │
                 │  ingest_raw                │
                 │      ↓                     │
                 │  bronze_to_silver          │
                 │      ↓                     │
                 │  silver_to_gold            │
                 │      ↓                     │
                 │  publish_serving           │
                 └────────────┬───────────────┘
                              │
                              ▼

                     SOURCE SYSTEMS
               ┌──────────┬───────────┐
               │          │           │
              API       Faker       Files
               │          │           │
               └──────────┼───────────┘
                          │
                          ▼
                  PYTHON INGESTION
                  format/envelope check
                          │
                          │ write raw
                          ▼

                    ┌───────────┐
                    │  BRONZE   │
                    │           │
                    │ raw       │
                    │ append-only
                    │ Delta Lake│
                    └─────┬─────┘
                          │
                          ▼

                 DATABRICKS / PySpark
                          │
                          ▼
              ┌──────────────────────┐
              │ TRANSFORM + DQ       │
              │                      │
              │ schema validation    │
              │ type validation      │
              │ deduplication        │
              │ business rules       │
              │ null/range checks    │
              └──────────┬───────────┘
                         │
                ┌────────┴────────┐
                │                 │
              PASS              FAIL
                │                 │
                ▼                 ▼

          ┌───────────┐     ┌──────────────┐
          │  SILVER   │     │  QUARANTINE │
          │           │     │              │
          │ clean     │     │ error_type   │
          │ conformed │     │ error_msg    │
          │ deduped   │     │ run_id       │
          └─────┬─────┘     │ detected_at  │
                │           │ bronze_ref   │
                │           └──────┬───────┘
                │                  │
                │             fix logic/rule
                │                  │
                │                  ▼
                │              REPROCESS
                │                  │
                │                  ▼
                │           Bronze → Silver
                │                  │
                └──────────┬───────┘
                           ▼

                     ┌───────────┐
                     │   GOLD    │
                     │           │
                     │ business  │
                     │ aggregates│
                     └─────┬─────┘
                           │
                           ▼

                    ┌────────────┐
                    │ PostgreSQL │
                    │  Serving   │
                    └─────┬──────┘
                          │
                          ▼
                      METABASE
```

---

# 6. ELT Design

PitchFlow follows an **ELT** model:

```text
Extract
   ↓
Load raw data
   ↓
Bronze
   ↓
Transform
   ↓
Silver
   ↓
Transform
   ↓
Gold
```

## 6.1 Before Bronze

Only lightweight technical checks are allowed.

Examples:

- HTTP response is readable
- JSON can be parsed
- file is accessible
- event envelope exists
- source metadata can be attached
- `pipeline_run_id` can be attached

Business-level validation should NOT happen here.

## 6.2 After Bronze

Core transformations are performed after the raw source data has been persisted.

Examples:

- schema validation
- type casting
- null validation
- deduplication
- referential integrity
- business rules
- normalization
- joins
- derived columns
- aggregation

This preserves Bronze as the source of truth and allows historical replay.

---

# 7. Data Sources

V1 uses one real, version-pinned external dataset and controlled synthetic variants. This gives the pipeline internally consistent entity and event relationships while still exercising unreliable upstream-data scenarios.

## 7.1 V1 Primary Source — StatsBomb Open Data

The active dataset is the Premier League 2015/16 snapshot from the [StatsBomb Open Data repository](https://github.com/hudl/open-data):

```text
competition_id = 2
season_id = 27
```

The snapshot contains the following raw JSON source objects:

```text
competitions.json
matches/2/27.json
lineups/<match_id>.json
events/<match_id>.json
```

The ingestion process must resolve and store the source repository commit SHA, source URI, retrieval timestamp, and source-object path in Bronze metadata. Raw payloads are retained exactly as received. Teams, players, stadiums, matches, lineups, and events are derived after Bronze from the related JSON objects.

When analysis or insights based on this data are published or shared, PitchFlow must credit StatsBomb and follow the source terms for attribution.

## 7.2 Controlled Synthetic Event Variants

The generator starts from valid StatsBomb event records and injects controlled failures. It is a separate source label, not a replacement for the original event.

Generated variants may include:

```text
event_id
match_id
team_id
player_id
minute
second
event_type
event_timestamp
```

Possible event types:

- GOAL
- SHOT
- PASS
- FOUL
- YELLOW_CARD
- RED_CARD
- CORNER
- OFFSIDE
- SUBSTITUTION

V1 supports a small deterministic set of variants for testing. The generator will later evolve into a configurable **Chaos Generator**.

## 7.3 Optional Future Sources

Live API and CSV ingestion are deferred until after V1. Candidate future sources include football-data.org for API ingestion and football-data.co.uk for historical CSV match results. They must be added only with documented licensing, source-specific contracts, and clear entity-key reconciliation with StatsBomb.

---

# 8. Core Data Model

## 8.1 Main Entities

```text
Competition
    │
Season
    │
Match
 ├──────── home_team_id ──────→ Team
 ├──────── away_team_id ──────→ Team
 ├──────── stadium_id ─────────→ Stadium
 │
 └──────── match_id
              │
              ├────────→ Match Event
              │             │
              │             ├── player_id → Player
              │             └── team_id   → Team
              │
              └────────→ Lineup
                            │
                            └── player_id → Player
```

## 8.2 Business Keys

Recommended keys:

| Entity | Business Key |
|---|---|
| Match | `match_id` |
| Match Event | `event_id` |
| Team | `team_id` |
| Player | `player_id` |
| Stadium | `stadium_id` |
| Competition | `competition_id` |
| Season | `season_id` |

These keys are used for:

- deduplication
- MERGE/upsert
- incremental loading
- idempotency
- lineage
- replay

---

# 9. Bronze Layer

## 9.1 Purpose

Bronze stores the raw representation of upstream data.

Principles:

- append-only
- source-of-truth
- minimal transformation
- ingestion metadata attached
- historical data preserved

## 9.2 Required Metadata

Recommended fields:

```text
bronze_record_id
source
source_object
source_timestamp
ingestion_timestamp
pipeline_run_id
raw_payload
source_uri
source_commit_sha
```

Optional:

```text
source_file
api_endpoint
partition_date
schema_version
```

## 9.3 Storage

```text
Delta Lake
└── MinIO
    └── S3-compatible object storage
```

MinIO is required for V1. All Delta tables are stored in the `pitchflow` bucket using `s3a://pitchflow/<layer>/<entity>` paths.

---

# 10. Silver Layer

## 10.1 Purpose

Silver contains clean, typed, conformed, deduplicated data.

Example tables:

```text
silver_matches
silver_match_events
silver_players
silver_teams
silver_stadiums
```

## 10.2 Transformation Responsibilities

Bronze → Silver performs:

- parsing
- data-type conversion
- schema enforcement
- standardization
- deduplication
- null validation
- range validation
- referential checks
- business-rule checks
- late-data handling

Example normalization:

```text
goal
Goal
GOAL

→ GOAL
```

---

# 11. Data Quality

Data quality is a core feature of PitchFlow.

## 11.1 Rule Categories

### Schema

Examples:

```text
required columns exist
expected types are compatible
unknown schema version detected
```

### Completeness

Examples:

```text
match_id IS NOT NULL
event_id IS NOT NULL
team_id IS NOT NULL
```

### Validity

Examples:

```text
minute >= 0
minute <= configured upper bound
score >= 0
stadium_capacity > 0
```

### Uniqueness

Examples:

```text
event_id unique
match_id unique in match table
```

### Referential Integrity

Examples:

```text
event.match_id exists in matches
event.player_id exists in players
event.team_id belongs to the match
```

### Business Rules

Examples:

- home team cannot equal away team
- player must belong to a participating team where applicable
- red-card event requires a player
- match event cannot occur before kickoff
- completed match must have valid final score

---

# 12. Data Quality Policy

Each rule must map to an action.

Example:

| Rule | Severity | Action |
|---|---|---|
| Missing `event_id` | Critical | Quarantine |
| Duplicate `event_id` | Warning | Deduplicate + metric |
| Invalid match minute | Critical | Quarantine |
| Unknown optional player | Warning | Continue / flag |
| Missing `match_id` | Critical | Quarantine |
| Invalid team relationship | Critical | Quarantine |
| Schema drift detected | Warning/Critical | Alert or fail |
| DQ failure rate > threshold | Critical | Fail task |

Recommended initial thresholds:

```text
DQ pass rate >= 95%      → HEALTHY
80% <= DQ pass rate <95% → WARNING
DQ pass rate < 80%       → CRITICAL / FAIL
```

Thresholds should be configurable.

---

# 13. Quarantine Layer

## 13.1 Purpose

Invalid records must not simply be dropped.

Quarantine stores information needed to:

- investigate failures
- trace the pipeline run
- identify the failed rule
- find the raw Bronze record
- replay after fixes

## 13.2 Suggested Schema

```text
quarantine_id
bronze_record_id
source
record_key
pipeline_run_id
error_type
error_message
failed_rule
detected_at
retry_count
status
reprocessed_at
```

Possible status values:

```text
NEW
UNDER_REVIEW
FIXED
REPROCESSED
FAILED
```

## 13.3 Source of Truth

Bronze remains the source of truth.

Quarantine should preferably reference:

```text
bronze_record_id
```

rather than becoming a second authoritative copy of raw data.

---

# 14. Replay / Reprocessing

A failed record may be retried after:

- transformation code changes
- schema handling changes
- data-quality rule fixes
- reference/master data corrections

Replay flow:

```text
Quarantine
     │
     │ bronze_record_id
     ▼
Bronze
     │
     ▼
updated bronze_to_silver logic
     │
     ▼
Silver
     │
     ▼
affected Gold datasets rebuilt
     │
     ▼
Serving layer updated
```

A dedicated Airflow DAG may later be created:

```text
pipeline_reprocess
```

Possible parameters:

```text
date
pipeline_run_id
record_id
match_id
source
```

---

# 15. Incremental Processing

The pipeline should avoid rebuilding all historical data for every daily run.

Possible incremental fields:

```text
event_timestamp
source_updated_at
ingestion_timestamp
pipeline_run_id
```

Example:

```text
Day 1 → 100,000 records
Day 2 → 10,000 new records

Day 2 pipeline should primarily process the 10,000 new/changed records.
```

The specific strategy may use:

- timestamp watermark
- partition-based filtering
- run-based filtering
- Delta MERGE

---

# 16. Idempotency

Retrying a task must not corrupt downstream data.

Example failure:

```text
Airflow run
    ↓
Silver write
    ↓
task crashes
    ↓
Airflow retry
```

The second attempt must not create duplicate entities/events.

Recommended techniques:

- business-key deduplication
- deterministic partitions
- Delta MERGE
- PostgreSQL UPSERT
- `pipeline_run_id`
- transactional writes where available

Acceptance rule:

> Re-running the same logical batch must produce the same business result.

---

# 17. Gold Layer

Gold contains analytics-ready datasets.

Recommended datasets:

## 17.1 `gold_match_summary`

Possible columns:

```text
match_id
competition
season
home_team
away_team
home_score
away_score
winner
goal_count
yellow_card_count
red_card_count
shot_count
```

## 17.2 `gold_team_performance`

```text
team_id
matches_played
wins
draws
losses
goals_for
goals_against
goal_difference
points
```

## 17.3 `gold_player_performance`

```text
player_id
matches
goals
assists
shots
yellow_cards
red_cards
minutes_played
```

## 17.4 `gold_league_table`

```text
rank
team_id
played
wins
draws
losses
goals_for
goals_against
goal_difference
points
```

## 17.5 `gold_event_distribution`

```text
event_type
minute_bucket
event_count
```

---

# 18. Data Warehouse Modeling

A star-schema-inspired model may be introduced for analytics.

Example:

```text
                 dim_team
                    │
                    │
dim_player ─── fact_match_event ─── dim_match
                    │
                    │
                 dim_date
                    │
                    │
              dim_event_type
```

A second fact table may model matches:

```text
            dim_home_team
                  │
                  │
dim_away_team ─ fact_match ─ dim_date
                  │
                  │
              dim_stadium
```

The exact warehouse model should be finalized after source schemas are selected.

---

# 19. PostgreSQL Serving Layer

PostgreSQL is used as the serving layer.

It should NOT duplicate every Delta table.

Only business-ready datasets required by dashboards should be published.

Example:

```text
Gold Delta
   │
   ├── gold_team_performance
   ├── gold_player_performance
   └── gold_league_table
          │
          ▼
      PostgreSQL
```

Publishing should use idempotent strategies such as:

```text
UPSERT
ON CONFLICT DO UPDATE
```

---

# 20. Airflow Orchestration

## 20.1 Primary DAG

```text
pipeline_daily

ingest_raw
    ↓
bronze_to_silver
    ↓
silver_to_gold
    ↓
publish_serving
```

## 20.2 Responsibilities

### `ingest_raw`

- call football API
- read source files
- run synthetic generator
- attach ingestion metadata
- persist raw data into Bronze

### `bronze_to_silver`

- trigger PySpark transformation
- apply schema/data-quality rules
- deduplicate data
- write valid records to Silver
- write invalid records to Quarantine
- emit DQ metrics

### `silver_to_gold`

- join Silver datasets
- calculate business metrics
- build analytics tables
- update affected Gold datasets

### `publish_serving`

- publish selected Gold datasets
- UPSERT into PostgreSQL
- validate serving-table row counts
- mark run completion

## 20.3 Failure Behavior

Example:

```text
ingest_raw FAILED
→ downstream tasks do not run

bronze_to_silver FAILED
→ Gold and serving tasks do not run

silver_to_gold FAILED
→ serving layer does not update
```

Airflow is responsible for:

- scheduling
- dependency management
- retry
- logs
- task status
- run history
- notifications

Airflow is NOT the main data-processing engine.

---

# 21. Pipeline Run Tracking

Every DAG run should generate or propagate:

```text
pipeline_run_id
```

Example:

```text
scheduled__2026-08-30T01:00:00
```

This value should appear in:

- Bronze
- Quarantine
- Silver metadata where useful
- pipeline metrics
- logs

Benefits:

- traceability
- debugging
- replay
- lineage
- run-level quality analysis

---

# 22. Chaos Generator

The synthetic event generator should later evolve into a controlled chaos-testing component.

## 22.1 Normal Events

Example:

```json
{
  "event_id": "EV10023",
  "match_id": 1201,
  "minute": 67,
  "player_id": 502,
  "team_id": 12,
  "event_type": "GOAL"
}
```

## 22.2 Failure Injection

Supported scenarios:

- duplicate event
- missing event ID
- null player
- invalid match minute
- unknown match
- invalid player/team relationship
- malformed timestamp
- wrong data type
- late-arriving event
- extra field
- missing field
- schema rename
- simulated API failure

Example configuration:

```yaml
chaos:
  enabled: true

  duplicate_rate: 0.02
  null_rate: 0.01
  invalid_type_rate: 0.01
  late_event_rate: 0.03
  schema_drift_rate: 0.005
```

This feature is not required for the first MVP.

---

# 23. Observability

Observability should be introduced progressively.

## 23.1 Level 1 — Airflow

Use:

- DAG status
- task status
- retry history
- Airflow logs
- failure notifications

## 23.2 Level 2 — Data Metrics

Track custom metrics such as:

```text
input_rows
valid_rows
quarantine_rows
duplicate_rows
late_event_count
schema_violation_count
DQ_pass_rate
processing_duration
freshness
```

## 23.3 Level 3 — Monitoring Stack

Optional later architecture:

```text
Airflow / Pipeline Metrics
          ↓
    OpenTelemetry
          ↓
      Prometheus
          ↓
        Grafana
```

This stack is optional until the core ELT pipeline is stable.

---

# 24. Alerting

At minimum, alerts should exist for:

- DAG failure
- task retry exhaustion
- DQ pass rate below threshold
- no new data
- abnormal row-count drop
- freshness violation
- schema drift
- serving publish failure

Possible channels:

- Email
- Discord
- Slack

Example alert:

```text
Pipeline: pipeline_daily
Task: bronze_to_silver
Status: FAILED
Run: 2026-08-30
DQ pass rate: 62.8%
Reason: Schema mismatch
```

---

# 25. Dashboard Requirements

Metabase should be run locally using Docker.

Two dashboards are recommended.

## 25.1 Football Analytics Dashboard

Possible widgets:

- league standings
- recent match results
- top scorers
- team performance
- player performance
- goals by minute
- home vs away performance
- cards by team
- event distribution

## 25.2 Data Reliability Dashboard

Possible widgets:

- pipeline success rate
- last successful run
- DQ pass rate
- quarantined records
- duplicate event count
- late-event count
- schema violations
- records processed
- data freshness
- pipeline duration

---

# 26. Technology Stack

## Core

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow |
| Ingestion | Python |
| Raw football data | StatsBomb Open Data — Premier League 2015/16 snapshot (380 matches) |
| Synthetic data | Controlled fault generator based on valid StatsBomb events |
| Processing | PySpark |
| Table format | Delta Lake |
| Data organization | Bronze / Silver / Gold |
| Object storage | MinIO (S3-compatible) |
| Serving | PostgreSQL |
| Dashboard | Metabase |
| Containers | Docker / Docker Compose |
| Version control | Git / GitHub |

## Optional

| Capability | Technology |
|---|---|
| Metrics | OpenTelemetry |
| Metric store | Prometheus |
| Monitoring | Grafana |
| Notifications | Email / Discord / Slack |

---

# 27. Local vs Cloud Mapping

The initial project should remain low-cost and reproducible.

Example mapping:

| Local Project | Cloud Equivalent |
|---|---|
| Delta Lake + MinIO | Delta Lake + AWS S3 |
| Airflow Docker | AWS MWAA / managed Airflow |
| PySpark local | Databricks / EMR |
| PostgreSQL Docker | RDS PostgreSQL |
| Metabase Docker | Managed BI layer |

The project should NOT claim cloud deployment unless actually deployed.

---

# 28. Suggested Repository Structure

```text
pitchflow/
│
├── airflow/
│   ├── dags/
│   │   ├── pipeline_daily.py
│   │   └── pipeline_reprocess.py
│   └── plugins/
│
├── ingestion/
│   ├── statsbomb/
│   ├── generator/
│   └── common/
│
├── spark/
│   ├── bronze_to_silver/
│   ├── silver_to_gold/
│   └── common/
│
├── quality/
│   ├── rules/
│   ├── checks/
│   └── config/
│
├── serving/
│   └── publish_postgres/
│
├── data/
│   ├── raw/
│   └── reference/
│
├── sql/
│   ├── ddl/
│   ├── gold/
│   └── serving/
│
├── docker/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── data_quality/
│
├── docs/
│   ├── architecture.md
│   ├── data_model.md
│   └── dq_rules.md
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

# 29. Development Phases

## V1 — Functional ELT Pipeline

Goal:

> Make the complete pipeline run end-to-end.

Scope:

```text
StatsBomb Open Data snapshot / controlled synthetic variants
        ↓
Python ingestion
        ↓
Bronze Delta
        ↓
PySpark
        ↓
Silver
        ↓
Gold
        ↓
PostgreSQL
        ↓
Metabase
```

Required:

- Airflow DAG
- Bronze/Silver/Gold
- basic data modeling
- basic DQ
- PostgreSQL serving
- one football dashboard

---

## V2 — Reliable Pipeline

Add:

- Quarantine
- configurable DQ rules
- `pipeline_run_id`
- incremental processing
- idempotency
- late-arriving data handling
- retry strategy
- failure alerts
- replay DAG

Goal:

> Pipeline can recover from failures without corrupting data.

---

## V3 — Chaos & Observability

Add:

- Chaos Generator
- schema drift scenarios
- malformed records
- abnormal volume scenarios
- DQ thresholds
- reliability dashboard
- Prometheus/Grafana if desired
- pipeline health metrics

Goal:

> Demonstrate how the system behaves when upstream data becomes unreliable.

---

# 30. MVP Acceptance Criteria

V1 is complete when:

- [ ] Airflow can trigger the full daily DAG.
- [ ] Both the StatsBomb snapshot and controlled synthetic source variants are ingested with distinct source labels.
- [ ] The pinned StatsBomb Premier League 2015/16 snapshot is ingested with source URI and commit-SHA lineage.
- [ ] Raw records are stored in Bronze.
- [ ] Bronze is append-only.
- [ ] PySpark transforms Bronze into Silver.
- [ ] Silver data is typed and deduplicated.
- [ ] At least five DQ rules exist.
- [ ] Invalid records can be identified.
- [ ] Silver data is transformed into at least three Gold datasets.
- [ ] Gold datasets can be published to PostgreSQL.
- [ ] Metabase can query PostgreSQL.
- [ ] At least one football analytics dashboard exists.
- [ ] Re-running a completed batch does not duplicate serving-layer data.
- [ ] README explains architecture and design decisions.

---

# 31. V2 Acceptance Criteria

- [ ] Quarantine table stores rejected-record metadata.
- [ ] Every quarantined record references its Bronze source.
- [ ] A failed record can be replayed.
- [ ] Incremental loading is implemented.
- [ ] Pipeline runs have unique run IDs.
- [ ] DQ pass rate is calculated.
- [ ] DQ thresholds can fail or warn the pipeline.
- [ ] Late-arriving events are supported.
- [ ] Airflow sends failure notifications.
- [ ] Retry does not duplicate Silver/Gold/serving data.

---

# 32. V3 Acceptance Criteria

- [ ] Chaos Generator can intentionally inject bad data.
- [ ] Duplicate events are detected.
- [ ] Schema drift is detected.
- [ ] Invalid references are quarantined.
- [ ] Late events are measured.
- [ ] Data-quality metrics are persisted.
- [ ] Reliability dashboard exists.
- [ ] Pipeline behavior under failure is documented.
- [ ] Demo video shows failure → detection → quarantine → fix → replay.

---

# 33. Key Engineering Questions the Project Must Be Able to Answer

The implementation and README should clearly answer:

1. Why ELT instead of traditional ETL?
2. Why is Bronze append-only?
3. Why are business validations performed after Bronze?
4. How are duplicate football events handled?
5. How does the pipeline behave when Airflow retries a task?
6. How is idempotency guaranteed?
7. How are late-arriving events processed?
8. What happens when the upstream schema changes?
9. Why quarantine instead of simply dropping bad records?
10. How can a quarantined record be replayed?
11. What is the business key for each major entity?
12. How does incremental processing work?
13. What makes the pipeline fail versus continue with warning?
14. Why is PostgreSQL used as a serving layer?
15. Why is Airflow not used as the processing engine?
16. What would change if MinIO storage were migrated to AWS S3?
17. How can a pipeline technically succeed while the data is still unhealthy?
18. How is data freshness measured?

---

# 34. Risks

## Scope Creep

Risk:

Adding Kafka, Kubernetes, cloud services, ML, or additional tools before the core system works.

Mitigation:

Complete each version before adding another infrastructure component.

## API Availability

Risk:

Future live football APIs may have quotas or availability limitations.

Mitigation:

V1 uses a pinned StatsBomb snapshot with no live API dependency. Persist future API responses in Bronze and keep the snapshot as a deterministic fallback.

## Data Licensing

Risk:

Football datasets may have redistribution limitations.

Mitigation:

Use public/open datasets where possible and document source terms and attribution. V1 uses StatsBomb Open Data; published analysis must credit StatsBomb according to its terms. Keep a source manifest rather than committing unverified or restricted third-party raw datasets.

## Local Resource Constraints

Risk:

Airflow + Spark + PostgreSQL + Metabase + monitoring may consume significant RAM.

Mitigation:

Start with core services only and add optional observability services later.

---

# 35. Success Metrics

The project is successful when it demonstrates:

### Functional Reliability

```text
daily DAG success
safe retries
repeatable runs
replay support
```

### Data Reliability

```text
DQ pass rate
duplicate detection
quarantine rate
freshness
schema compliance
```

### Engineering Quality

```text
clear repository structure
config-driven rules
tests
documentation
reproducible setup
meaningful commit history
```

### Portfolio Quality

A reviewer should understand the core architecture and project differentiation within several minutes of reading the README.

---

# 36. Suggested CV Positioning

Possible project title:

**PitchFlow — Reliable Football Data Lakehouse**

Example summary:

> Built a fault-tolerant ELT lakehouse for multi-source football data using Airflow, PySpark and Delta Lake, implementing Bronze–Silver–Gold processing, configurable data-quality rules, quarantine/replay workflows, incremental loading and a PostgreSQL serving layer for analytics.

Possible advanced bullet:

> Designed failure-handling workflows for duplicate events, late-arriving data, schema drift and referential-integrity violations, with pipeline-level DQ metrics and safe historical reprocessing.

---

# 37. Final Product Vision

PitchFlow should evolve from:

```text
Football data pipeline
```

into:

```text
Reliable Football Data Platform
```

The defining question of the project is:

> **Can the platform continue producing trustworthy football analytics when upstream data is imperfect?**

The football dashboard is the visible product.

The real project is the **data engineering system behind it**.
