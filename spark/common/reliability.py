"""Small, dependency-free policies shared by the V2 reliability jobs."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


HEALTHY = "HEALTHY"
WARNING = "WARNING"
FAILED = "FAILED"


@dataclass(frozen=True)
class QualityThresholds:
    """Pass-rate and secondary metric boundaries expressed as percentages (0–100)."""

    warning_pass_rate: float
    failure_pass_rate: float
    quarantine_rate_warning: float = 20.0
    quarantine_rate_failure: float = 40.0
    late_event_warning: float = 10.0
    late_event_failure: float = 25.0

    def __post_init__(self) -> None:
        if not 0 <= self.failure_pass_rate <= self.warning_pass_rate <= 100:
            raise ValueError(
                "Quality thresholds must satisfy 0 <= failure_pass_rate <= warning_pass_rate <= 100."
            )
        if not 0 <= self.quarantine_rate_warning <= self.quarantine_rate_failure <= 100:
            raise ValueError(
                "Quarantine-rate thresholds must satisfy 0 <= warning <= failure <= 100."
            )
        if not 0 <= self.late_event_warning <= self.late_event_failure <= 100:
            raise ValueError(
                "Late-event thresholds must satisfy 0 <= warning <= failure <= 100."
            )


def quality_thresholds_from_environment(environment: Mapping[str, str] | None = None) -> QualityThresholds:
    """Read the documented DQ gate configuration without importing Spark."""

    values = environment if environment is not None else os.environ
    try:
        return QualityThresholds(
            warning_pass_rate=float(values.get("PITCHFLOW_DQ_WARNING_PASS_RATE", "95")),
            failure_pass_rate=float(values.get("PITCHFLOW_DQ_FAILURE_PASS_RATE", "80")),
            quarantine_rate_warning=float(values.get("PITCHFLOW_DQ_QUARANTINE_RATE_WARNING", "20")),
            quarantine_rate_failure=float(values.get("PITCHFLOW_DQ_QUARANTINE_RATE_FAILURE", "40")),
            late_event_warning=float(values.get("PITCHFLOW_DQ_LATE_EVENT_WARNING", "10")),
            late_event_failure=float(values.get("PITCHFLOW_DQ_LATE_EVENT_FAILURE", "25")),
        )
    except ValueError as error:
        raise ValueError("PITCHFLOW DQ thresholds must be numeric percentages.") from error


def quality_status(pass_rate: float, thresholds: QualityThresholds) -> str:
    """Classify a run without hiding warning-quality data from downstream layers."""

    if not 0 <= pass_rate <= 100:
        raise ValueError("DQ pass rate must be between 0 and 100.")
    if pass_rate < thresholds.failure_pass_rate:
        return FAILED
    if pass_rate < thresholds.warning_pass_rate:
        return WARNING
    return HEALTHY


@dataclass(frozen=True)
class QualityAssessment:
    """Result of evaluating all DQ dimensions for a single pipeline run."""

    pass_rate: float
    quarantine_rate: float
    late_event_rate: float
    pass_rate_status: str
    quarantine_rate_status: str
    late_event_status: str

    @property
    def overall_status(self) -> str:
        """Return the worst status across all dimensions."""
        statuses = (self.pass_rate_status, self.quarantine_rate_status, self.late_event_status)
        if FAILED in statuses:
            return FAILED
        if WARNING in statuses:
            return WARNING
        return HEALTHY

    @property
    def warnings(self) -> list[str]:
        """Return human-readable messages for every non-healthy dimension."""
        messages: list[str] = []
        if self.pass_rate_status != HEALTHY:
            messages.append(f"DQ pass rate {self.pass_rate:.2f}% is {self.pass_rate_status}")
        if self.quarantine_rate_status != HEALTHY:
            messages.append(f"Quarantine rate {self.quarantine_rate:.2f}% is {self.quarantine_rate_status}")
        if self.late_event_status != HEALTHY:
            messages.append(f"Late event rate {self.late_event_rate:.2f}% is {self.late_event_status}")
        return messages


def _rate_status(rate: float, warning: float, failure: float) -> str:
    """Classify a rate where *exceeding* the threshold is bad (quarantine, late)."""

    if rate >= failure:
        return FAILED
    if rate >= warning:
        return WARNING
    return HEALTHY


def assess_quality(counts: dict[str, int], thresholds: QualityThresholds) -> QualityAssessment:
    """Evaluate pass-rate, quarantine-rate, and late-event-rate in one call."""

    total = counts.get("input", 0)
    valid = counts.get("valid", 0)
    quarantined = counts.get("quarantined", 0)
    late = counts.get("late", 0)

    pass_rate = (valid / total * 100) if total else 100.0
    quarantine_rate = (quarantined / total * 100) if total else 0.0
    late_rate = (late / total * 100) if total else 0.0

    return QualityAssessment(
        pass_rate=pass_rate,
        quarantine_rate=quarantine_rate,
        late_event_rate=late_rate,
        pass_rate_status=quality_status(pass_rate, thresholds),
        quarantine_rate_status=_rate_status(quarantine_rate, thresholds.quarantine_rate_warning, thresholds.quarantine_rate_failure),
        late_event_status=_rate_status(late_rate, thresholds.late_event_warning, thresholds.late_event_failure),
    )
