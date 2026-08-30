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

Quarantine records retain their `bronze_record_id`, rule/version, message, run ID, source, status, and timestamps. DQ run metrics are stored at `s3a://pitchflow/ops/quality_metrics`.
