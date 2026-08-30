# Testing and Validation

## Current support

The repository currently contains harness and source-contract tests. They use Python's standard-library `unittest` and need no dependency installation.

Run them from the repository root:

```powershell
python -m unittest discover -s tests -v
python scripts/validate_project.py
docker compose config --quiet
```

`validate_project.py` checks the harness documentation contract, repository-local Skill metadata, and committed-secret hygiene. `docker compose config --quiet` checks Compose interpolation and topology. Neither substitutes for a full Spark/MinIO pipeline run.

## CI status

No CI workflow is configured yet, which is appropriate while the repository has no runtime stack. When CI is introduced, run the two commands above in a validation job before adding Docker-backed integration tests.

## Expectations for future implementation

- Put focused Python tests under `tests/` using the selected test framework.
- Add unit tests for ingestion metadata, transformation rules, deduplication, and quality-rule routing.
- Add integration tests only once Docker services exist. They should verify the MinIO Delta path, a small end-to-end batch, PostgreSQL UPSERT behavior, and rerun idempotency.
- Add a regression test whenever a change fixes a data-quality, late-event, correction, or replay defect.

When the project adds linting, formatting, type checking, or Docker integration tests, document the verified command here and add it to `AGENTS.md` and the validator where appropriate.
