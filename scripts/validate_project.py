"""Validate the lightweight PitchFlow repository harness without extra dependencies."""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILES = (
    "AGENTS.md",
    "docs/PitchFlow_PRD.md",
    "docs/IMPLEMENTATION_PLAN.md",
    "docs/architecture.md",
    "docs/engineering-conventions.md",
    "docs/testing.md",
)
SKILL_DIRECTORY = ".agents/skills"


@dataclass(frozen=True)
class Finding:
    """A validation result with remediation suitable for a coding agent."""

    level: str
    message: str


def is_disallowed_secret_path(path: str) -> bool:
    """Return whether a tracked file is an environment file that may hold secrets."""

    normalized = path.replace("\\", "/")
    name = Path(normalized).name
    return name == ".env" or (name.startswith(".env.") and name != ".env.example")


def read_skill_metadata(skill_file: Path) -> dict[str, str] | None:
    """Read the small required YAML frontmatter subset without a YAML dependency."""

    try:
        lines = skill_file.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None

    if not lines or lines[0].strip() != "---":
        return None

    metadata: dict[str, str] = {}
    for line in lines[1:]:
        if line.strip() == "---":
            return metadata
        if ":" not in line:
            continue
        key, value = line.split(":", maxsplit=1)
        metadata[key.strip()] = value.strip().strip('"')
    return None


def tracked_files(repository_root: Path) -> set[str] | None:
    """Return tracked paths when Git is available; otherwise leave the check unavailable."""

    try:
        result = subprocess.run(
            ["git", "ls-files"],
            cwd=repository_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return {line for line in result.stdout.splitlines() if line}


def validate_repository(repository_root: Path = REPOSITORY_ROOT) -> list[Finding]:
    """Return all harness findings for a repository root."""

    findings: list[Finding] = []

    for relative_path in REQUIRED_FILES:
        path = repository_root / relative_path
        if not path.is_file() or not path.read_text(encoding="utf-8").strip():
            findings.append(
                Finding(
                    "ERROR",
                    f"Required harness document is missing or empty: {relative_path}. "
                    "Restore it and see AGENTS.md for the repository navigation map.",
                )
            )

    skills_root = repository_root / SKILL_DIRECTORY
    skill_files = sorted(skills_root.glob("*/SKILL.md")) if skills_root.is_dir() else []
    if len(skill_files) < 2:
        findings.append(
            Finding(
                "ERROR",
                "At least two repository Skills are required under .agents/skills/. "
                "Add focused workflows rather than generic instructions.",
            )
        )

    for skill_file in skill_files:
        metadata = read_skill_metadata(skill_file)
        expected_name = skill_file.parent.name
        if metadata is None:
            findings.append(
                Finding(
                    "ERROR",
                    f"Skill frontmatter is invalid: {skill_file.relative_to(repository_root)}. "
                    "Start the file with YAML frontmatter containing name and description.",
                )
            )
            continue
        if metadata.get("name") != expected_name or not metadata.get("description"):
            findings.append(
                Finding(
                    "ERROR",
                    f"Skill metadata is incomplete: {skill_file.relative_to(repository_root)}. "
                    f"Set name to '{expected_name}' and provide a discriminating description.",
                )
            )

    tracked = tracked_files(repository_root)
    if tracked is None:
        findings.append(
            Finding(
                "WARNING",
                "Git tracked-file check was skipped because Git is unavailable. "
                "Do not commit .env files; see docs/engineering-conventions.md#configuration-and-secrets.",
            )
        )
    else:
        secret_paths = sorted(path for path in tracked if is_disallowed_secret_path(path))
        for path in secret_paths:
            findings.append(
                Finding(
                    "ERROR",
                    f"Tracked secret-bearing environment file: {path}. "
                    "Remove it from version control, keep it local, and commit a redacted .env.example instead. "
                    "See docs/engineering-conventions.md#configuration-and-secrets.",
                )
            )

    return findings


def main() -> int:
    findings = validate_repository()
    errors = [finding for finding in findings if finding.level == "ERROR"]

    if findings:
        for finding in findings:
            print(f"{finding.level}: {finding.message}")
    else:
        print("PASS: PitchFlow harness validation completed successfully.")

    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
