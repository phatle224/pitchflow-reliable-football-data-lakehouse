# 0003 — Use StatsBomb Premier League 2015/16 as the active source

## Decision

The active PitchFlow source is the version-pinned StatsBomb Open Data Premier League 2015/16 snapshot: `competition_id=2`, `season_id=27`, commit `b0bc9f22dd77c206ddedc1d742893b3bbe64baec`.

The existing ingestion contract already supports the competition manifest, match list, lineup files, and event files. This snapshot contains 380 matches with event-level JSON, so it increases dashboard breadth without introducing a second schema or weakening the Bronze lineage contract.

## Alternatives considered

- Football-Data.co.uk provides useful season-level CSV results and match statistics, but not the event and lineup objects required by the current Silver and Gold event model. It remains a possible future match-results adapter.
- A live football API would provide current seasons but introduces credentials, rate limits, mutable source data, and a separate reconciliation policy. It is out of scope for the reproducible local project.

## Migration and reproducibility

The previous World Cup manifest is retained at `config/statsbomb_world_cup_2022.json`. A fresh local lakehouse should start with the active Premier League manifest. Existing MinIO/PostgreSQL volumes are not automatically deleted; reset them only when a clean single-competition dataset is desired.

StatsBomb attribution and the pinned commit/source URI remain mandatory in Bronze and user-facing documentation.
