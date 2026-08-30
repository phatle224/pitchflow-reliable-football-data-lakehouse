# Data Sources and Provenance

## V1 source

PitchFlow V1 reads a pinned [StatsBomb Open Data](https://github.com/hudl/open-data) FIFA World Cup 2022 snapshot. The active manifest is `config/statsbomb_source.json`:

```text
competition_id: 43
season_id: 106
commit_sha: b0bc9f22dd77c206ddedc1d742893b3bbe64baec
```

The ingestion job downloads `competitions.json`, the selected competition match manifest, and one lineup and event file per match. It retains each downloaded source file as an immutable Bronze payload rather than committing the bulk dataset to Git.

## Provenance contract

Every Bronze record stores source name, source URI, commit SHA, source-object path, payload hash, deterministic `bronze_record_id`, ingestion time, and `pipeline_run_id`. A repeat of the same source file is skipped by an insert-only Delta merge; a different snapshot or source-file payload produces a new Bronze record.

## Attribution

Published analyses or insights derived from this data must credit StatsBomb and follow its Open Data terms. Do not add data from another source without documenting its license, fields, refresh behavior, and key-matching policy.

## Controlled variants

`--inject-chaos` creates a small deterministic set of records derived from valid events: an exact duplicate, missing event ID, invalid minute, event correction, and late event. They have `source=synthetic_statsbomb`; the original StatsBomb payload stays untouched.
