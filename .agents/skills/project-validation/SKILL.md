---
name: project-validation
description: Validate a PitchFlow change before handoff, including repository harness checks, tests, and an intentional diff review.
---

# Project Validation

Use this Skill when finishing a PitchFlow implementation, reviewing a change, or diagnosing a validation failure.

1. Read `docs/testing.md` for the currently supported checks.
2. Run `python -m unittest discover -s tests -v` and `python scripts/validate_project.py` from the repository root.
3. Run `git diff --check` and inspect `git diff` and `git status --short` for unrelated, generated, or secret-bearing files.
4. Repair failures in the changed scope and rerun the failed checks. Report any check that cannot run, why, and the smallest next step needed to enable it.
