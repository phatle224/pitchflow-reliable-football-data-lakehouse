"""Tests for deterministic source provenance and controlled variants."""

import json
import tempfile
import unittest
from pathlib import Path

from ingestion.common.records import SourceRecord, canonical_json
from ingestion.statsbomb.client import StatsBombSource, fetch_snapshot_records


def event(event_id: str, minute: int) -> dict:
    return {
        "id": event_id,
        "index": minute,
        "minute": minute,
        "second": 0,
        "team": {"id": 10, "name": "Home"},
        "player": {"id": 99, "name": "Player"},
        "type": {"id": 30, "name": "Pass"},
    }


class SourceRecordTests(unittest.TestCase):
    def test_bronze_identity_is_deterministic_and_record_locator_sensitive(self) -> None:
        payload = canonical_json({"b": 2, "a": 1})
        common = {
            "source": "statsbomb_open_data",
            "source_object": "events",
            "source_uri": "https://example.test/events/1.json",
            "source_commit_sha": "a" * 40,
            "raw_payload": payload,
        }
        first = SourceRecord(source_record_locator="events/1.json", **common)
        second = SourceRecord(source_record_locator="events/1.json", **common)
        distinct = SourceRecord(source_record_locator="events/2.json", **common)

        self.assertEqual(first.bronze_record_id, second.bronze_record_id)
        self.assertNotEqual(first.bronze_record_id, distinct.bronze_record_id)
        self.assertEqual(payload, '{"a":1,"b":2}')


class StatsBombSnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source = StatsBombSource(
            source="statsbomb_open_data",
            repository_url="https://github.com/hudl/open-data",
            raw_base_url="https://raw.example.test/data",
            commit_sha="b" * 40,
            competition_id=43,
            season_id=106,
            competition_name="FIFA World Cup",
            season_name="2022",
        )
        self.match = {"match_id": 1, "last_updated": "2022-12-18T18:00:00.000Z"}
        self.responses = {
            self.source.url_for("competitions.json"): [{"competition_id": 43}],
            self.source.url_for("matches/43/106.json"): [self.match],
            self.source.url_for("lineups/1.json"): [{"team_id": 10, "lineup": []}],
            self.source.url_for("events/1.json"): [event(f"event-{number}", number) for number in range(1, 6)],
        }

    def test_snapshot_keeps_source_files_raw_and_labels_variants(self) -> None:
        records = fetch_snapshot_records(
            self.source,
            match_limit=1,
            inject_chaos=True,
            fetcher=self.responses.__getitem__,
        )

        self.assertEqual(9, len(records))
        self.assertEqual({"competitions", "matches", "lineups", "events", "synthetic_event"}, {record.source_object for record in records})
        self.assertEqual(5, len([record for record in records if record.source == "synthetic_statsbomb"]))
        event_file = next(record for record in records if record.source_object == "events")
        self.assertEqual("1", event_file.source_match_id)
        self.assertEqual("b" * 40, event_file.source_commit_sha)

    def test_manifest_rejects_unpinned_commit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "source.json"
            manifest.write_text(json.dumps({**self.source.__dict__, "commit_sha": "main"}), encoding="utf-8")
            with self.assertRaises(ValueError):
                StatsBombSource.from_file(manifest)


if __name__ == "__main__":
    unittest.main()
