# Testing and Validation

## Validated commands

The repository currently contains harness and source-contract tests. They use Python's standard-library `unittest` and need no dependency installation.

Run them from the repository root:

```powershell
python -m unittest discover -s tests -v
python scripts/validate_project.py
docker compose config --quiet
```

`validate_project.py` checks the harness documentation contract, repository-local Skill metadata, and committed-secret hygiene. `docker compose config --quiet` checks Compose interpolation and topology.

## Docker smoke test

Start the stack with `docker compose up --build -d`, then trigger `pipeline_daily` with this Airflow run configuration:

```json
{
  "match_limit": 1,
  "inject_chaos": true
}
```

The expected result is Delta tables in MinIO for Bronze, Silver, Quarantine, Gold, and `ops/quality_metrics`, plus rows in PostgreSQL's `gold_match_summary`, `gold_team_performance`, and `gold_event_distribution` tables. A validated smoke run processed 3,393 event rows, retained 3,389 valid events, deduplicated 1, and quarantined 3 deliberate invalid/correction variants.

## CI status

No CI workflow is configured yet. When CI is introduced, run the three commands above in a validation job and add a Docker-backed smoke test with a limited source batch.

## Follow-up test coverage

- Put focused Python tests under `tests/` using the selected test framework.
- Add unit tests for ingestion metadata, transformation rules, deduplication, and quality-rule routing.
- Add Docker-backed integration tests that verify the MinIO Delta path, a small end-to-end batch, PostgreSQL UPSERT behavior, and rerun idempotency.
- Add a regression test whenever a change fixes a data-quality, late-event, correction, or replay defect.

When the project adds linting, formatting, type checking, or Docker integration tests, document the verified command here and add it to `AGENTS.md` and the validator where appropriate.
