"""Create deterministic, small failure variants from valid StatsBomb events."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, TYPE_CHECKING

from ingestion.common.records import SourceRecord, canonical_json

if TYPE_CHECKING:
    from ingestion.statsbomb.client import StatsBombSource


SYNTHETIC_SOURCE = "synthetic_statsbomb"


def _select_events(events: list[dict[str, Any]], count: int = 5) -> list[dict[str, Any]]:
    selected = [event for event in events if event.get("id") and event.get("team") and event.get("minute") is not None]
    if len(selected) < count:
        raise ValueError("StatsBomb event batch does not contain enough valid events for controlled variants.")
    return selected[:count]


def _variant_record(
    source: "StatsBombSource",
    match_id: str,
    variant_type: str,
    payload: dict[str, Any],
) -> SourceRecord:
    event_id = payload.get("id", "missing-event-id")
    locator = f"synthetic/{source.competition_id}/{source.season_id}/{match_id}/{variant_type}/{event_id}"
    return SourceRecord(
        source=SYNTHETIC_SOURCE,
        source_object="synthetic_event",
        source_uri=f"{source.repository_url}/tree/{source.commit_sha}/data/events/{match_id}.json",
        source_commit_sha=source.commit_sha,
        source_record_locator=locator,
        raw_payload=canonical_json(payload),
        source_match_id=match_id,
        variant_type=variant_type,
    )


def generate_controlled_variants(
    *,
    source: "StatsBombSource",
    match_id: str,
    events: list[dict[str, Any]],
) -> list[SourceRecord]:
    """Build exact duplicate, invalid, correction, and late-event examples.

    The original external source objects are never changed. Each generated record
    carries the `synthetic_statsbomb` source label and a distinct locator.
    """

    duplicate, missing_id, invalid_minute, correction, late = map(deepcopy, _select_events(events))

    missing_id.pop("id", None)
    invalid_minute["minute"] = 200
    correction["type"] = {"id": 30, "name": "Pass"}
    late["id"] = f"{late['id']}-late"
    late["minute"] = 1

    return [
        _variant_record(source, match_id, "exact_duplicate", duplicate),
        _variant_record(source, match_id, "missing_event_id", missing_id),
        _variant_record(source, match_id, "invalid_minute", invalid_minute),
        _variant_record(source, match_id, "event_correction", correction),
        _variant_record(source, match_id, "late_event", late),
    ]
