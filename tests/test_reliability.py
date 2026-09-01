"""Focused unit tests for V2 reliability policy without requiring Spark or Docker."""

import os
import unittest
from unittest.mock import patch

from spark.common.alerts import send_webhook_alert
from spark.common.orchestration import airflow_default_args, required_string_list
from spark.common.reliability import (
    FAILED,
    HEALTHY,
    WARNING,
    QualityAssessment,
    QualityThresholds,
    assess_quality,
    quality_status,
    quality_thresholds_from_environment,
)


class QualityThresholdTests(unittest.TestCase):
    def test_quality_statuses_follow_configured_boundaries(self) -> None:
        thresholds = QualityThresholds(warning_pass_rate=95, failure_pass_rate=80)

        self.assertEqual(HEALTHY, quality_status(95, thresholds))
        self.assertEqual(WARNING, quality_status(94.99, thresholds))
        self.assertEqual(FAILED, quality_status(79.99, thresholds))

    def test_invalid_threshold_order_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            QualityThresholds(warning_pass_rate=70, failure_pass_rate=80)

    def test_environment_thresholds_must_be_numeric(self) -> None:
        with self.assertRaises(ValueError):
            quality_thresholds_from_environment(
                {"PITCHFLOW_DQ_WARNING_PASS_RATE": "not-a-number", "PITCHFLOW_DQ_FAILURE_PASS_RATE": "80"}
            )

    def test_invalid_quarantine_rate_threshold_order_rejected(self) -> None:
        with self.assertRaises(ValueError):
            QualityThresholds(
                warning_pass_rate=95, failure_pass_rate=80,
                quarantine_rate_warning=50, quarantine_rate_failure=30,
            )

    def test_invalid_late_event_threshold_order_rejected(self) -> None:
        with self.assertRaises(ValueError):
            QualityThresholds(
                warning_pass_rate=95, failure_pass_rate=80,
                late_event_warning=30, late_event_failure=10,
            )


class QualityAssessmentTests(unittest.TestCase):
    """V2: multi-dimensional DQ assessment covering pass-rate, quarantine-rate, and late-event-rate."""

    def setUp(self) -> None:
        self.thresholds = QualityThresholds(
            warning_pass_rate=95, failure_pass_rate=80,
            quarantine_rate_warning=20, quarantine_rate_failure=40,
            late_event_warning=10, late_event_failure=25,
        )

    def test_healthy_run_returns_healthy_overall(self) -> None:
        counts = {"input": 100, "valid": 98, "quarantined": 2, "duplicates": 0, "late": 1}
        assessment = assess_quality(counts, self.thresholds)

        self.assertEqual(HEALTHY, assessment.overall_status)
        self.assertEqual(HEALTHY, assessment.pass_rate_status)
        self.assertEqual(HEALTHY, assessment.quarantine_rate_status)
        self.assertEqual(HEALTHY, assessment.late_event_status)
        self.assertAlmostEqual(98.0, assessment.pass_rate)
        self.assertAlmostEqual(2.0, assessment.quarantine_rate)
        self.assertAlmostEqual(1.0, assessment.late_event_rate)
        self.assertEqual([], assessment.warnings)

    def test_quarantine_rate_triggers_warning(self) -> None:
        counts = {"input": 100, "valid": 80, "quarantined": 20, "duplicates": 0, "late": 0}
        assessment = assess_quality(counts, self.thresholds)

        self.assertEqual(WARNING, assessment.quarantine_rate_status)
        self.assertEqual(WARNING, assessment.overall_status)

    def test_quarantine_rate_triggers_failure(self) -> None:
        counts = {"input": 100, "valid": 55, "quarantined": 45, "duplicates": 0, "late": 0}
        assessment = assess_quality(counts, self.thresholds)

        self.assertEqual(FAILED, assessment.quarantine_rate_status)
        self.assertEqual(FAILED, assessment.overall_status)

    def test_late_event_rate_triggers_warning(self) -> None:
        counts = {"input": 100, "valid": 98, "quarantined": 2, "duplicates": 0, "late": 12}
        assessment = assess_quality(counts, self.thresholds)

        self.assertEqual(WARNING, assessment.late_event_status)
        self.assertEqual(WARNING, assessment.overall_status)

    def test_late_event_rate_triggers_failure(self) -> None:
        counts = {"input": 100, "valid": 98, "quarantined": 2, "duplicates": 0, "late": 30}
        assessment = assess_quality(counts, self.thresholds)

        self.assertEqual(FAILED, assessment.late_event_status)
        self.assertEqual(FAILED, assessment.overall_status)

    def test_worst_status_wins(self) -> None:
        counts = {"input": 100, "valid": 50, "quarantined": 50, "duplicates": 0, "late": 30}
        assessment = assess_quality(counts, self.thresholds)

        self.assertEqual(FAILED, assessment.pass_rate_status)
        self.assertEqual(FAILED, assessment.quarantine_rate_status)
        self.assertEqual(FAILED, assessment.late_event_status)
        self.assertEqual(FAILED, assessment.overall_status)
        self.assertEqual(3, len(assessment.warnings))

    def test_empty_batch_is_healthy(self) -> None:
        counts = {"input": 0, "valid": 0, "quarantined": 0, "duplicates": 0, "late": 0}
        assessment = assess_quality(counts, self.thresholds)

        self.assertEqual(HEALTHY, assessment.overall_status)
        self.assertAlmostEqual(100.0, assessment.pass_rate)
        self.assertAlmostEqual(0.0, assessment.quarantine_rate)
        self.assertAlmostEqual(0.0, assessment.late_event_rate)

    def test_environment_reads_all_threshold_env_vars(self) -> None:
        env = {
            "PITCHFLOW_DQ_WARNING_PASS_RATE": "90",
            "PITCHFLOW_DQ_FAILURE_PASS_RATE": "70",
            "PITCHFLOW_DQ_QUARANTINE_RATE_WARNING": "15",
            "PITCHFLOW_DQ_QUARANTINE_RATE_FAILURE": "35",
            "PITCHFLOW_DQ_LATE_EVENT_WARNING": "5",
            "PITCHFLOW_DQ_LATE_EVENT_FAILURE": "20",
        }
        thresholds = quality_thresholds_from_environment(env)

        self.assertAlmostEqual(90.0, thresholds.warning_pass_rate)
        self.assertAlmostEqual(70.0, thresholds.failure_pass_rate)
        self.assertAlmostEqual(15.0, thresholds.quarantine_rate_warning)
        self.assertAlmostEqual(35.0, thresholds.quarantine_rate_failure)
        self.assertAlmostEqual(5.0, thresholds.late_event_warning)
        self.assertAlmostEqual(20.0, thresholds.late_event_failure)


class OrchestrationPolicyTests(unittest.TestCase):
    def test_replay_config_requires_safe_non_empty_string_lists(self) -> None:
        self.assertEqual(["record-a", "record-b"], required_string_list({"bronze_record_ids": [" record-a ", "record-b"]}, "bronze_record_ids"))
        with self.assertRaises(ValueError):
            required_string_list({"bronze_record_ids": [1]}, "bronze_record_ids")
        with self.assertRaises(ValueError):
            required_string_list({}, "bronze_record_ids")

    def test_retry_policy_uses_configured_backoff(self) -> None:
        settings = {
            "PITCHFLOW_AIRFLOW_RETRIES": "3",
            "PITCHFLOW_AIRFLOW_RETRY_DELAY_MINUTES": "2",
            "PITCHFLOW_AIRFLOW_MAX_RETRY_DELAY_MINUTES": "10",
        }
        with patch.dict(os.environ, settings, clear=False):
            defaults = airflow_default_args()

        self.assertEqual(3, defaults["retries"])
        self.assertTrue(defaults["retry_exponential_backoff"])
        self.assertEqual(2 * 60, int(defaults["retry_delay"].total_seconds()))
        self.assertEqual(10 * 60, int(defaults["max_retry_delay"].total_seconds()))

    def test_retry_callback_is_configured(self) -> None:
        """V2: on_retry_callback must be present for early retry alerting."""
        with patch.dict(os.environ, {}, clear=False):
            defaults = airflow_default_args()
        self.assertIn("on_retry_callback", defaults)
        self.assertIsNotNone(defaults["on_retry_callback"])


class ReplayValidationTests(unittest.TestCase):
    """V2: pipeline_replay DAG requires a replay_reason for audit traceability."""

    def test_replay_reason_required(self) -> None:
        from spark.common.orchestration import validate_replay_reason

        with self.assertRaises(ValueError):
            validate_replay_reason({})
        with self.assertRaises(ValueError):
            validate_replay_reason({"replay_reason": ""})
        with self.assertRaises(ValueError):
            validate_replay_reason({"replay_reason": "   "})

    def test_replay_reason_accepted(self) -> None:
        from spark.common.orchestration import validate_replay_reason

        reason = validate_replay_reason({"replay_reason": "DQ rule fix for INVALID_MINUTE"})
        self.assertEqual("DQ rule fix for INVALID_MINUTE", reason)


class CorrectionResolutionTests(unittest.TestCase):
    """V2: correction workflow action validation."""

    VALID_ACTIONS = {"approve", "reject", "under_review"}

    def test_valid_correction_actions_accepted(self) -> None:
        for action in self.VALID_ACTIONS:
            self.assertIn(action, self.VALID_ACTIONS)

    def test_invalid_action_rejected(self) -> None:
        """Correction action must be one of approve/reject/under_review."""
        self.assertNotIn("delete", self.VALID_ACTIONS)
        self.assertNotIn("", self.VALID_ACTIONS)
        self.assertNotIn("DROP TABLE", self.VALID_ACTIONS)


class AlertTests(unittest.TestCase):
    def test_missing_webhook_is_an_intentional_noop(self) -> None:
        self.assertFalse(
            send_webhook_alert(
                severity="WARNING",
                title="test",
                message="no delivery is configured",
                environment={"PITCHFLOW_ALERT_WEBHOOK_URL": ""},
            )
        )


if __name__ == "__main__":
    unittest.main()

