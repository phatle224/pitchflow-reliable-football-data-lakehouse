"""Download a pinned StatsBomb snapshot into raw Bronze envelopes."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.request import Request, urlopen

from ingestion.common.records import SourceRecord, canonical_json
from ingestion.generator.chaos import generate_controlled_variants


JsonFetcher = Callable[[str], Any]


def fetch_json(url: str) -> Any:
    """Fetch JSON with a descriptive user agent and no hidden retries."""

    request = Request(url, headers={"User-Agent": "PitchFlow/1.0 data-ingestion"})
    with urlopen(request, timeout=60) as response:  # noqa: S310 - URL is source-manifest controlled.
        return json.loads(response.read().decode("utf-8"))


@dataclass(frozen=True)
class StatsBombSource:
    source: str
    repository_url: str
    raw_base_url: str
    commit_sha: str
    competition_id: int
    season_id: int
    competition_name: str
    season_name: str

    @classmethod
    def from_file(cls, path: str | Path) -> "StatsBombSource":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        source = cls(**data)
        if len(source.commit_sha) != 40 or any(character not in "0123456789abcdef" for character in source.commit_sha):
            raise ValueError("StatsBomb source manifest must contain a 40-character lowercase commit SHA.")
        return source

    def url_for(self, relative_path: str) -> str:
        return f"{self.raw_base_url.rstrip('/')}/{relative_path.lstrip('/')}"


def _snapshot_record(
    source: StatsBombSource,
    *,
    source_object: str,
    relative_path: str,
    payload: Any,
    source_match_id: str | None = None,
    source_timestamp: str | None = None,
) -> SourceRecord:
    return SourceRecord(
        source=source.source,
        source_object=source_object,
        source_uri=source.url_for(relative_path),
        source_commit_sha=source.commit_sha,
        source_record_locator=relative_path,
        raw_payload=canonical_json(payload),
        source_match_id=source_match_id,
        source_timestamp=source_timestamp,
    )


def fetch_snapshot_records(
    source: StatsBombSource,
    *,
    match_limit: int | None = None,
    inject_chaos: bool = False,
    fetcher: JsonFetcher = fetch_json,
) -> list[SourceRecord]:
    """Return one immutable Bronze envelope per downloaded source file.

    Events and lineups remain file-level raw payloads. This preserves the source
    representation while avoiding one Bronze row per event before Spark parsing.
    """

    competition_path = "competitions.json"
    matches_path = f"matches/{source.competition_id}/{source.season_id}.json"
    competitions = fetcher(source.url_for(competition_path))
    matches = fetcher(source.url_for(matches_path))
    selected_matches = matches[:match_limit] if match_limit else matches

    records = [
        _snapshot_record(
            source,
            source_object="competitions",
            relative_path=competition_path,
            payload=competitions,
        ),
        _snapshot_record(
            source,
            source_object="matches",
            relative_path=matches_path,
            payload=selected_matches,
        ),
    ]

    first_event_batch: tuple[str, list[dict[str, Any]]] | None = None
    for match in selected_matches:
        match_id = str(match["match_id"])
        match_updated = match.get("last_updated")
        lineup_path = f"lineups/{match_id}.json"
        event_path = f"events/{match_id}.json"
        lineups = fetcher(source.url_for(lineup_path))
        events = fetcher(source.url_for(event_path))
        records.extend(
            (
                _snapshot_record(
                    source,
                    source_object="lineups",
                    relative_path=lineup_path,
                    payload=lineups,
                    source_match_id=match_id,
                    source_timestamp=match_updated,
                ),
                _snapshot_record(
                    source,
                    source_object="events",
                    relative_path=event_path,
                    payload=events,
                    source_match_id=match_id,
                    source_timestamp=match_updated,
                ),
            )
        )
        if first_event_batch is None:
            first_event_batch = (match_id, events)

    if inject_chaos and first_event_batch:
        match_id, events = first_event_batch
        records.extend(
            generate_controlled_variants(
                source=source,
                match_id=match_id,
                events=events,
            )
        )

    return records
