# Data Sources and Provenance

## Active source

PitchFlow reads a pinned Premier League 2015/16 snapshot from [StatsBomb Open Data Repository](https://github.com/statsbomb/open-data) (or mirror [hudl/open-data](https://github.com/hudl/open-data)). Direct match dataset manifest: [`data/matches/2/27.json`](https://github.com/statsbomb/open-data/blob/master/data/matches/2/27.json). The active manifest is `config/statsbomb_source.json`:

```text
competition_id: 2
season_id: 27
competition_name: Premier League
season_name: 2015/2016
commit_sha: b0bc9f22dd77c206ddedc1d742893b3bbe64baec
```

The selected snapshot contains 380 matches. The ingestion job downloads `competitions.json`, the competition match manifest, and one lineup and event file per selected match. It retains each downloaded source file as an immutable Bronze payload rather than committing the bulk dataset to Git. Use `PITCHFLOW_SOURCE_CONFIG` or `--source-config` to select the active pinned manifest.

## Provenance contract

Every Bronze record stores source name, source URI, commit SHA, source-object path, payload hash, deterministic `bronze_record_id`, ingestion time, and `pipeline_run_id`. A repeat of the same source file is skipped by an insert-only Delta merge; a different snapshot or source-file payload produces a new Bronze record.

## Attribution

Published analyses or insights derived from this data must credit StatsBomb and follow its Open Data terms. Do not add data from another source without documenting its license, fields, refresh behavior, and key-matching policy.

## Controlled variants

`--inject-chaos` creates a small deterministic set of records derived from valid events: an exact duplicate, missing event ID, invalid minute, event correction, and late event. They have `source=synthetic_statsbomb`; the original StatsBomb payload stays untouched.

The Premier League 2015/16 manifest is the active dataset for PitchFlow. It provides a comprehensive 380-match dataset with event-level data telemetry. A run must use one complete pinned manifest per lakehouse profile.
