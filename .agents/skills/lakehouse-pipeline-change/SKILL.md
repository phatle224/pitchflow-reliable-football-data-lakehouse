---
name: lakehouse-pipeline-change
description: Implement or modify PitchFlow ingestion, Spark transformations, data-quality, orchestration, or serving behavior while preserving Delta Lakehouse invariants.
---

# Lakehouse Pipeline Change

Use this Skill for changes that affect data movement, data contracts, Airflow jobs, Delta tables, or PostgreSQL publishing. Do not use it for documentation-only or generic repository-tooling changes.

1. Read `AGENTS.md`, `docs/architecture.md`, `docs/engineering-conventions.md`, and the relevant section of `docs/IMPLEMENTATION_PLAN.md`.
2. Identify the affected layer and preserve its boundary: Bronze retains raw data, Spark handles transformations/DQ, Airflow orchestrates, and PostgreSQL serves selected Gold data.
3. Define the business key, `pipeline_run_id` behavior, retry behavior, and Quarantine action before changing data writes.
4. Add focused tests for the changed success and failure behavior. Include idempotency or replay coverage whenever a write path changes.
5. Run the documented tests and `python scripts/validate_project.py`; inspect the diff before handoff.
