"""StatsBomb schemas used when parsing immutable raw JSON Bronze payloads."""

from __future__ import annotations

from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType, StructField, StructType


country_schema = StructType([StructField("id", LongType()), StructField("name", StringType())])

team_schema = StructType(
    [
        StructField("home_team_id", LongType()),
        StructField("home_team_name", StringType()),
        StructField("home_team_gender", StringType()),
        StructField("home_team_group", StringType()),
        StructField("away_team_id", LongType()),
        StructField("away_team_name", StringType()),
        StructField("away_team_gender", StringType()),
        StructField("away_team_group", StringType()),
        StructField("country", country_schema),
    ]
)

match_schema = StructType(
    [
        StructField("match_id", LongType()),
        StructField("match_date", StringType()),
        StructField("kick_off", StringType()),
        StructField("competition", StructType([StructField("competition_id", LongType()), StructField("competition_name", StringType())])),
        StructField("season", StructType([StructField("season_id", LongType()), StructField("season_name", StringType())])),
        StructField("home_team", team_schema),
        StructField("away_team", team_schema),
        StructField("home_score", IntegerType()),
        StructField("away_score", IntegerType()),
        StructField("match_status", StringType()),
        StructField("last_updated", StringType()),
        StructField("stadium", StructType([StructField("id", LongType()), StructField("name", StringType()), StructField("country", country_schema)])),
    ]
)

event_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("index", LongType()),
        StructField("period", IntegerType()),
        StructField("timestamp", StringType()),
        StructField("minute", IntegerType()),
        StructField("second", IntegerType()),
        StructField("type", StructType([StructField("id", LongType()), StructField("name", StringType())])),
        StructField("team", StructType([StructField("id", LongType()), StructField("name", StringType())])),
        StructField("player", StructType([StructField("id", LongType()), StructField("name", StringType())])),
        StructField("shot", StructType([StructField("outcome", StructType([StructField("id", LongType()), StructField("name", StringType())]))])),
        StructField("foul_committed", StructType([StructField("card", StructType([StructField("id", LongType()), StructField("name", StringType())]))])),
        StructField("bad_behaviour", StructType([StructField("card", StructType([StructField("id", LongType()), StructField("name", StringType())]))])),
    ]
)

lineup_player_schema = StructType(
    [
        StructField("player_id", LongType()),
        StructField("player_name", StringType()),
        StructField("positions", ArrayType(StructType([StructField("position", StringType())]))),
    ]
)

lineup_schema = ArrayType(
    StructType(
        [
            StructField("team_id", LongType()),
            StructField("team_name", StringType()),
            StructField("lineup", ArrayType(lineup_player_schema)),
        ]
    )
)
