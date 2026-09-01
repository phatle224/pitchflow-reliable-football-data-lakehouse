# Data Sources and Provenance

## Active source

PitchFlow reads a pinned [StatsBomb Open Data](https://github.com/hudl/open-data) Premier League 2015/16 snapshot. The active manifest is `config/statsbomb_source.json`:

```text
competition_id: 2
season_id: 27
competition_name: Premier League
season_name: 2015/2016
commit_sha: b0bc9f22dd77c206ddedc1d742893b3bbe64baec
```

The selected snapshot contains 380 matches. The ingestion job downloads `competitions.json`, the competition match manifest, and one lineup and event file per selected match. It retains each downloaded source file as an immutable Bronze payload rather than committing the bulk dataset to Git. Use `PITCHFLOW_SOURCE_CONFIG` or `--source-config` to select another pinned manifest, including the retained World Cup manifest at `config/statsbomb_world_cup_2022.json`.

## Provenance contract

Every Bronze record stores source name, source URI, commit SHA, source-object path, payload hash, deterministic `bronze_record_id`, ingestion time, and `pipeline_run_id`. A repeat of the same source file is skipped by an insert-only Delta merge; a different snapshot or source-file payload produces a new Bronze record.

## Attribution

Published analyses or insights derived from this data must credit StatsBomb and follow its Open Data terms. Do not add data from another source without documenting its license, fields, refresh behavior, and key-matching policy.

## Controlled variants

`--inject-chaos` creates a small deterministic set of records derived from valid events: an exact duplicate, missing event ID, invalid minute, event correction, and late event. They have `source=synthetic_statsbomb`; the original StatsBomb payload stays untouched.

## Source selection policy

The default is the Premier League manifest because it provides a substantially broader dashboard dataset while retaining the same event-level contract. A run must use one complete pinned manifest per lakehouse profile; do not mix World Cup and Premier League records in the same serving tables unless a multi-competition model and dashboard filters have been explicitly enabled. For a clean source switch, recreate the local volumes as documented in `docs/RUN_GUIDE.md`.
