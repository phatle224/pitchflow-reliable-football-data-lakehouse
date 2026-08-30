"""Focused tests for the repository harness validator."""

import unittest
from pathlib import Path

from scripts.validate_project import is_disallowed_secret_path, read_skill_metadata, validate_repository


class SecretPathTests(unittest.TestCase):
    def test_blocks_real_environment_files_but_allows_example(self) -> None:
        self.assertTrue(is_disallowed_secret_path(".env"))
        self.assertTrue(is_disallowed_secret_path("deploy/.env.production"))
        self.assertFalse(is_disallowed_secret_path(".env.example"))
        self.assertFalse(is_disallowed_secret_path("config/example.env"))


class SkillMetadataTests(unittest.TestCase):
    def test_reads_repository_skill_metadata(self) -> None:
        skill_file = Path(".agents/skills/lakehouse-pipeline-change/SKILL.md")
        metadata = read_skill_metadata(skill_file)

        self.assertIsNotNone(metadata)
        self.assertEqual("lakehouse-pipeline-change", metadata["name"])
        self.assertTrue(metadata["description"])


class RepositoryValidationTests(unittest.TestCase):
    def test_current_repository_harness_has_no_errors(self) -> None:
        findings = validate_repository(Path.cwd())
        errors = [finding for finding in findings if finding.level == "ERROR"]

        self.assertEqual([], errors)


if __name__ == "__main__":
    unittest.main()
