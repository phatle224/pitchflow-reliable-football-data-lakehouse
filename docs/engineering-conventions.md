# Engineering Conventions

## Scope of current conventions

No application source code exists yet, so this document records only approved architecture and repository conventions. Do not infer a formatter, framework, package manager, logging library, or Python style rule until the implementation introduces and documents one.

## Data and layer conventions

- Use lowercase snake_case for dataset, job, configuration, and Python module names, matching the approved plan.
- Use business keys (`event_id`, `match_id`, and equivalent entity IDs) for deduplication and idempotent writes.
- Preserve raw input and lineage in Bronze. Perform business validation, normalization, and deduplication only after Bronze.
- Record the external source URI, resolved source commit SHA, and retrieval timestamp for every StatsBomb snapshot. Credit StatsBomb in user-facing data provenance documentation.
- Write valid transformed records to Silver and invalid records to Quarantine with actionable rule/error metadata and a `bronze_record_id` reference.
- Keep Gold business-ready and publish only dashboard-required Gold data to PostgreSQL.
- Propagate `pipeline_run_id` through each data-processing stage.

## Configuration and secrets

- Read runtime configuration from environment variables. Commit `.env.example` when configuration is introduced; never commit `.env` or credentials.
- Keep MinIO endpoint/credentials, PostgreSQL connection settings, data-quality thresholds, and scheduling settings outside source code.
- Use Docker Compose as the supported local runtime once the runtime stack is added.

## Change discipline

- Keep orchestration, ingestion, transformation, quality, serving, and dashboard concerns separated as described in `architecture.md`.
- Add tests with behavior changes. Test data-quality and idempotency behavior rather than only happy-path output.
- Update the PRD, implementation plan, or decision record when changing an approved architecture decision.
