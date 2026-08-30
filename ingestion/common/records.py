"""Immutable raw-record envelope used before writing Bronze."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    """Return a timezone-aware ISO timestamp suitable for ingestion metadata."""

    return datetime.now(timezone.utc).isoformat()


def canonical_json(payload: Any) -> str:
    """Serialize a source payload deterministically without changing its meaning."""

    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SourceRecord:
    """A raw source object plus the lineage required by the Bronze contract."""

    source: str
    source_object: str
    source_uri: str
    source_commit_sha: str
    source_record_locator: str
    raw_payload: str
    source_timestamp: str | None = None
    source_match_id: str | None = None
    variant_type: str | None = None

    @property
    def payload_hash(self) -> str:
        return sha256_hex(self.raw_payload)

    @property
    def bronze_record_id(self) -> str:
        identity = "\x1f".join(
            (
                self.source,
                self.source_object,
                self.source_record_locator,
                self.payload_hash,
            )
        )
        return sha256_hex(identity)

    def as_bronze_row(self, pipeline_run_id: str, ingestion_timestamp: str | None = None) -> dict[str, str | None]:
        """Build the Delta Bronze row without mutating the upstream payload."""

        ingested_at = ingestion_timestamp or utc_now_iso()
        return {
            "bronze_record_id": self.bronze_record_id,
            "source": self.source,
            "source_object": self.source_object,
            "source_uri": self.source_uri,
            "source_commit_sha": self.source_commit_sha,
            "source_record_locator": self.source_record_locator,
            "source_timestamp": self.source_timestamp,
            "source_match_id": self.source_match_id,
            "variant_type": self.variant_type,
            "payload_hash": self.payload_hash,
            "raw_payload": self.raw_payload,
            "ingestion_timestamp": ingested_at,
            "pipeline_run_id": pipeline_run_id,
            "partition_date": ingested_at[:10],
        }
