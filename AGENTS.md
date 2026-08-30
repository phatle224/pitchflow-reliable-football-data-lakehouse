# PitchFlow Agent Guide

## Project overview

PitchFlow is a design-first football data lakehouse project. The repository currently contains the approved product requirements and V1 implementation plan; no runtime pipeline code, Docker configuration, or dependency manifest exists yet.

## Repository map

- `docs/PitchFlow_PRD.md` — product requirements and long-term architecture.
- `docs/IMPLEMENTATION_PLAN.md` — approved, implementable V1 decisions.
- `docs/architecture.md` — component boundaries and V1 data flow.
- `docs/data-sources.md` — source manifest, provenance, and attribution rules.
- `docs/dq_rules.md` — executable V1 data-quality policy.
- `docs/engineering-conventions.md` — repository conventions and invariants.
- `docs/testing.md` — current validation and future test expectations.
- `docs/decisions/` — concise records of approved architecture decisions.
- `.agents/skills/` — repeatable, project-specific Codex workflows.
- `scripts/validate_project.py` — repository harness validation entry point.
- `tests/` — tests for repository tooling and, later, product code.

## Source of truth

Read `docs/PitchFlow_PRD.md` before changing product behavior. For V1 implementation details, also read `docs/IMPLEMENTATION_PLAN.md`; its locked V1 choices take precedence where the PRD has not yet been synchronized. Read the relevant architecture, convention, testing, and decision documents before implementation.

## Engineering invariants

- Delta tables in MinIO own Bronze, Silver, Gold, and Quarantine data; PostgreSQL receives only selected Gold datasets for Metabase.
- The V1 external source is a version-pinned StatsBomb Open Data FIFA World Cup 2022 snapshot (`competition_id=43`, `season_id=106`). Preserve its source URI and commit SHA in Bronze lineage.
- Bronze is append-only and retains raw payload plus ingestion lineage. Business validation happens after Bronze.
- Airflow orchestrates work; Spark performs transformations and data-quality checks.
- Every batch propagates `pipeline_run_id`. Retrying the same logical batch must not duplicate Silver, Gold, or serving-layer business rows.
- Invalid data is quarantined with a `bronze_record_id` reference; it is not silently discarded.
- Do not commit credentials or local runtime state. Use documented environment variables and an untracked `.env`; commit only `.env.example`.

## Development workflow

1. Inspect the relevant implementation and read its linked documentation.
2. Use a repository Skill when its trigger applies.
3. Make the smallest change that satisfies the task and update docs when an approved decision changes.
4. Add or update focused tests for changed behavior.
5. Run tests and `python scripts/validate_project.py`.
6. Inspect `git diff --check` and `git diff` for unintended changes.
7. Fix issues and rerun validation before handoff.

## Commands

```powershell
python -m unittest discover -s tests -v
python scripts/validate_project.py
docker compose config --quiet
git diff --check
git diff
```

No application lint, formatter, type checker, or integration-test command exists yet. Add them to this guide only after they are configured and verified.
