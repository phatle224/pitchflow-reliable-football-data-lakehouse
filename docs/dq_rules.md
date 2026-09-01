# V1 Data-Quality Rules

| Rule | Severity | Action |
|---|---|---|
| `event_id` is missing | Critical | Quarantine |
| Source match ID is missing | Critical | Quarantine |
| Team ID is missing | Critical | Quarantine |
| Minute is outside 0–130 | Critical | Quarantine |
| Event match is absent from Silver matches | Critical | Quarantine |
| Event team is not home or away team | Critical | Quarantine |
| Same event ID and same payload | Warning | Keep one Silver event; count duplicate |
| Same event ID and changed synthetic payload | Critical | Quarantine the correction; preserve the original event |
| Event precedes existing Silver watermark | Warning | Accept and mark `is_late=true` |
| DQ pass rate below warning threshold | Warning | Persist output/metrics and send optional webhook alert |
| DQ pass rate below failure threshold | Critical | Persist output/metrics, fail task and let Airflow retry/alert |
| Quarantine rate above warning threshold | Warning | Persist metrics and send optional webhook alert |
| Quarantine rate above failure threshold | Critical | Persist metrics, fail task and let Airflow retry/alert |
| Late event rate above warning threshold | Warning | Persist metrics and send optional webhook alert |
| Late event rate above failure threshold | Critical | Persist metrics, fail task and let Airflow retry/alert |

## Threshold configuration

V2 evaluates three independent dimensions. The overall DQ gate status is the worst of all three:

| Environment Variable | Default | Description |
|---|---|---|
| `PITCHFLOW_DQ_WARNING_PASS_RATE` | `95` | Pass rate below this triggers a WARNING |
| `PITCHFLOW_DQ_FAILURE_PASS_RATE` | `80` | Pass rate below this fails the task |
| `PITCHFLOW_DQ_QUARANTINE_RATE_WARNING` | `20` | Quarantine rate at or above this triggers WARNING |
| `PITCHFLOW_DQ_QUARANTINE_RATE_FAILURE` | `40` | Quarantine rate at or above this fails the task |
| `PITCHFLOW_DQ_LATE_EVENT_WARNING` | `10` | Late event rate at or above this triggers WARNING |
| `PITCHFLOW_DQ_LATE_EVENT_FAILURE` | `25` | Late event rate at or above this fails the task |

Thresholds must satisfy `0 <= failure <= warning <= 100` (pass rate) and `0 <= warning <= failure <= 100` (quarantine/late rates).

Quarantine records retain their `bronze_record_id`, rule/version, message, run ID, source, status, and timestamps. V2 correction decisions also record action, note, resolution run ID and resolution timestamp. DQ run metrics are stored at `s3a://pitchflow/ops/quality_metrics`; the monotonic event watermark is stored at `s3a://pitchflow/ops/processing_watermarks`.
