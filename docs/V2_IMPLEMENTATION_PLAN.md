# PitchFlow V2 — Reliable Pipeline implementation

## Goal

V2 makes the V1 lakehouse recoverable and operable: an unhealthy batch is measured and gated, retries do not duplicate business data, selected Bronze records can be replayed through an Airflow DAG, and an event correction requires an explicit audited decision before Silver is changed.

## Boundaries and invariants

- Bronze remains append-only and is the only raw-data source of truth.
- Spark owns transformation, DQ, watermarks and correction application.
- Airflow owns retries, DAG execution history and failure callback delivery.
- Delta tables on MinIO own operational state; PostgreSQL receives only Gold serving datasets.
- Every new or replayed operation receives the Airflow `run_id` as `pipeline_run_id`.

## V2 data contracts

### Quality gate

`ops/quality_metrics` is keyed by `pipeline_run_id` and now includes `dq_status` in addition to input, valid, quarantine, duplicate, late-event and pass-rate metrics.

| Status | Condition | Behaviour |
|---|---|---|
| `HEALTHY` | pass rate >= `PITCHFLOW_DQ_WARNING_PASS_RATE` | Finish successfully |
| `WARNING` | failure threshold <= pass rate < warning threshold | Persist output/metrics, finish successfully, send webhook alert when configured |
| `FAILED` | pass rate < `PITCHFLOW_DQ_FAILURE_PASS_RATE` | Persist output/metrics, fail the Spark task so Airflow retries and alerts |

Defaults are 95% warning and 80% failure. The failure threshold must not exceed the warning threshold. Writing evidence before raising is deliberate: operators can inspect Quarantine and metrics while the retry remains idempotent.

### Processing watermark

`ops/processing_watermarks` has business key `entity` and stores:

```text
entity = match_events
watermark_timestamp
pipeline_run_id
updated_at
```

The watermark is monotonic. New Bronze runs are still the primary incremental selection mechanism; the persistent event watermark detects and measures late events even after Silver table maintenance. Valid late events are accepted and trigger affected-match Gold rebuilds.

### Correction resolution audit

Only Quarantine records with `error_type=EVENT_CORRECTION_REQUIRES_REVIEW` can be resolved. V2 adds these fields to the existing Quarantine Delta table through Delta schema evolution:

```text
resolution_action
resolution_note
resolution_run_id
resolved_at
```

Supported statuses are `NEW`, `UNDER_REVIEW`, `REJECTED`, `REPROCESSED`, and `FAILED`. Approval revalidates the Bronze payload and updates its existing Silver `event_id`; it never edits Bronze. The affected Gold match must then be rebuilt and published.

## Orchestration

### Retry and alert policy

All V2 DAGs use the same environment-controlled policy:

```text
PITCHFLOW_AIRFLOW_RETRIES=2
PITCHFLOW_AIRFLOW_RETRY_DELAY_MINUTES=5
PITCHFLOW_AIRFLOW_MAX_RETRY_DELAY_MINUTES=30
PITCHFLOW_ALERT_WEBHOOK_URL=
```

Retries use exponential backoff. The webhook is optional so no credential is committed. When configured, the payload includes both `text` and `content`, which works with common Slack/Discord-style endpoints. A missing or failed alert endpoint never hides the original task failure.

### `pipeline_replay`

This manual DAG replaces the V1-only shell recipe for ordinary Bronze reprocessing. It requires the following Trigger DAG configuration:

```json
{
  "bronze_record_ids": ["<bronze-record-id>"],
  "match_ids": ["<match-id>"]
}
```

It runs `bronze_to_silver`, `silver_to_gold`, then `publish_serving`. IDs are validated and passed to `spark-submit` as subprocess arguments rather than rendered into a shell command.

### `pipeline_resolve_correction`

This DAG records an explicit correction decision. For review or rejection:

```json
{
  "quarantine_ids": ["<quarantine-id>"],
  "action": "under_review",
  "resolution_note": "Investigating source correction"
}
```

For approval, add the match IDs to rebuild:

```json
{
  "quarantine_ids": ["<quarantine-id>"],
  "action": "approve",
  "resolution_note": "Verified against official correction",
  "match_ids": ["<match-id>"]
}
```

An approval that cannot be revalidated or cannot find its original Silver event moves the Quarantine record to `FAILED`; it does not silently insert an orphan correction.

## Validation evidence required for V2 completion

1. Run a normal/chaos smoke batch above the default DQ thresholds.
2. Trigger a deliberately low threshold to prove `WARNING` and `FAILED` behavior.
3. Configure a disposable webhook and force a task failure to verify alert delivery.
4. Replay a selected valid-after-fix Bronze record through `pipeline_replay` and verify no duplicate Gold/PostgreSQL row.
5. Resolve a synthetic correction through review, approval, Gold rebuild and serving publish; verify Bronze is unchanged and Quarantine audit fields are populated.
6. Verify `ops/processing_watermarks` is monotonic and a new older valid event is measured as late.

## Relationship to V3

V3 remains optional after V2: broader schema-drift/volume/API failure generation, Prometheus/Grafana, a reliability dashboard, and a recorded demonstration. V2 is the planned completion point for the portfolio project.
