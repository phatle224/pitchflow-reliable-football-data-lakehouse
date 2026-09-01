# Testing and Validation

## Validated commands

The repository contains harness, source-contract, and V2 reliability tests. They use Python's standard-library `unittest` and need no dependency installation.

Run them from the repository root:

```powershell
python -m unittest discover -s tests -v
python scripts/validate_project.py
docker compose config --quiet
```

`validate_project.py` checks the harness documentation contract, repository-local Skill metadata, and committed-secret hygiene. `docker compose config --quiet` checks Compose interpolation and topology.

## Test coverage

The test suite covers:

- **Source records** — deterministic Bronze identity, record-locator sensitivity, manifest validation, snapshot labeling.
- **DQ thresholds** — pass-rate boundaries (HEALTHY/WARNING/FAILED), invalid threshold order rejection, numeric validation.
- **Multi-dimensional DQ assessment (V2)** — quarantine-rate thresholds, late-event-rate thresholds, worst-status-wins logic, empty batch handling, environment variable loading for all six threshold env vars.
- **Orchestration** — replay config validation, retry policy with exponential backoff, `on_retry_callback` presence.
- **Replay validation (V2)** — replay_reason required and non-empty.
- **Correction workflow (V2)** — action validation (approve/reject/under_review), invalid action detection.
- **Alerting** — missing webhook is an intentional noop.

## Docker smoke test

Start the stack with `docker compose up --build -d`, then trigger `pipeline_daily` with this Airflow run configuration:

```json
{
  "match_limit": 1,
  "inject_chaos": true
}
```

The expected result is Delta tables in MinIO for Bronze, Silver, Quarantine, Gold, and `ops/quality_metrics`, plus rows in PostgreSQL's `gold_match_summary`, `gold_team_performance`, and `gold_event_distribution` tables. A validated smoke run processed 3,393 event rows, retained 3,389 valid events, deduplicated 1, and quarantined 3 deliberate invalid/correction variants.

## Docker integration test (V2)

An automated integration test script is available:

```bash
bash scripts/integration_test.sh
```

It triggers `pipeline_daily` with chaos variants, waits for completion, and validates that Delta tables and PostgreSQL serving rows exist.

## Reliability demo (V2)

A full reliability cycle demo is available:

```bash
bash scripts/demo_reliability_cycle.sh
```

It demonstrates: failure injection → alert → quarantine → correction review → replay → dashboard update.

## CI status

No CI workflow is configured yet. When CI is introduced, run the three commands above in a validation job and add a Docker-backed smoke test with a limited source batch.

## Follow-up test coverage

- Add Docker-backed integration tests that verify the MinIO Delta path, a small end-to-end batch, PostgreSQL UPSERT behavior, and rerun idempotency.
- Add a regression test whenever a change fixes a data-quality, late-event, correction, or replay defect.

When the project adds linting, formatting, type checking, or Docker integration tests, document the verified command here and add it to `AGENTS.md` and the validator where appropriate.
