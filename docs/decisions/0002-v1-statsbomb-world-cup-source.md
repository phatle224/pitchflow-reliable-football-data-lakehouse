# 0002 — Use StatsBomb World Cup 2022 as the V1 source

**Status:** Accepted

## Context

PitchFlow needs real, internally consistent match, team, player, stadium, lineup, and event data to demonstrate Bronze–Silver–Gold processing and reliability behavior. Independently maintained CSV master data plus fully fabricated events would weaken referential-integrity and replay demonstrations.

## Decision

V1 ingests a version-pinned snapshot of the StatsBomb Open Data FIFA World Cup 2022 dataset: `competition_id=43`, `season_id=106`. It uses the competition, match, lineup, and event JSON objects available in the repository. Ingestion resolves and records the Git commit SHA, source URI, and retrieval timestamp in Bronze metadata.

Teams, players, stadiums, matches, lineups, and events are derived from the snapshot. Controlled synthetic records are derived from valid events only to exercise duplicate, malformed, correction, and late-arrival behavior. They carry a distinct synthetic source label.

## Consequences

The V1 pipeline has no dependency on a live API key or rate limit. It must preserve the raw JSON payload and document StatsBomb attribution when publishing data-derived analysis or insights, in line with the source terms. Football-data.org and football-data.co.uk remain optional future sources, not V1 dependencies.
