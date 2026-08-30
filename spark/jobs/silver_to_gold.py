"""Build analytics-ready Gold Delta tables from conformed Silver data."""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import functions as F

from spark.common.delta import merge_by_keys, read_delta_or_none
from spark.common.runtime import build_spark_session, table_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--match-id", action="append", default=[])
    return parser.parse_args()


def _affected_match_ids(matches, events, pipeline_run_id: str, requested_match_ids: list[str]):
    if requested_match_ids:
        return matches.filter(F.col("match_id").isin([int(match_id) for match_id in requested_match_ids])).select("match_id")
    event_matches = events.filter(F.col("pipeline_run_id") == pipeline_run_id).select("match_id")
    match_updates = matches.filter(F.col("pipeline_run_id") == pipeline_run_id).select("match_id")
    return event_matches.unionByName(match_updates).dropDuplicates()


def _match_summary(matches, events):
    event_counts = events.groupBy("match_id").agg(
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("event_type") == "SHOT", 1).otherwise(0)).cast("long").alias("shot_count"),
        F.sum(F.when(F.col("card_type") == "YELLOW CARD", 1).otherwise(0)).cast("long").alias("yellow_card_count"),
        F.sum(F.when(F.col("card_type").isin("RED CARD", "SECOND YELLOW"), 1).otherwise(0)).cast("long").alias("red_card_count"),
    )
    return (
        matches.join(event_counts, "match_id", "left")
        .withColumn("winner", F.when(F.col("home_score") > F.col("away_score"), F.col("home_team_name")).when(F.col("away_score") > F.col("home_score"), F.col("away_team_name")).otherwise(F.lit("DRAW")))
        .select(
            "match_id",
            "competition_id",
            "competition_name",
            "season_id",
            "season_name",
            "match_date",
            "home_team_id",
            "home_team_name",
            "away_team_id",
            "away_team_name",
            "home_score",
            "away_score",
            "winner",
            (F.coalesce(F.col("home_score"), F.lit(0)) + F.coalesce(F.col("away_score"), F.lit(0))).alias("goal_count"),
            F.coalesce(F.col("yellow_card_count"), F.lit(0)).alias("yellow_card_count"),
            F.coalesce(F.col("red_card_count"), F.lit(0)).alias("red_card_count"),
            F.coalesce(F.col("shot_count"), F.lit(0)).alias("shot_count"),
            F.coalesce(F.col("event_count"), F.lit(0)).alias("event_count"),
            "pipeline_run_id",
        )
    )


def _team_performance(matches):
    home = matches.select(
        F.col("home_team_id").alias("team_id"),
        F.col("home_team_name").alias("team_name"),
        F.col("home_score").alias("goals_for"),
        F.col("away_score").alias("goals_against"),
    )
    away = matches.select(
        F.col("away_team_id").alias("team_id"),
        F.col("away_team_name").alias("team_name"),
        F.col("away_score").alias("goals_for"),
        F.col("home_score").alias("goals_against"),
    )
    return (
        home.unionByName(away)
        .groupBy("team_id", "team_name")
        .agg(
            F.count("*").alias("matches_played"),
            F.sum(F.when(F.col("goals_for") > F.col("goals_against"), 1).otherwise(0)).cast("long").alias("wins"),
            F.sum(F.when(F.col("goals_for") == F.col("goals_against"), 1).otherwise(0)).cast("long").alias("draws"),
            F.sum(F.when(F.col("goals_for") < F.col("goals_against"), 1).otherwise(0)).cast("long").alias("losses"),
            F.sum("goals_for").cast("long").alias("goals_for"),
            F.sum("goals_against").cast("long").alias("goals_against"),
        )
        .withColumn("goal_difference", F.col("goals_for") - F.col("goals_against"))
        .withColumn("points", (F.col("wins") * 3) + F.col("draws"))
    )


def _event_distribution(events):
    return (
        events.withColumn("minute_bucket", (F.floor(F.col("minute") / 15) * 15).cast("int"))
        .groupBy("event_type", "minute_bucket")
        .agg(F.count("*").alias("event_count"))
    )


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_spark_session("pitchflow-silver-to-gold")
    try:
        matches = read_delta_or_none(spark, table_path("silver", "matches"))
        events = read_delta_or_none(spark, table_path("silver", "match_events"))
        if matches is None or events is None:
            logging.info("Gold build skipped: Silver matches or events is not initialized.")
            return

        affected_matches = _affected_match_ids(matches, events, args.pipeline_run_id, args.match_id)
        if affected_matches.rdd.isEmpty():
            logging.info("Gold build skipped: no affected matches for run %s.", args.pipeline_run_id)
            return

        summary = _match_summary(matches, events)
        affected_summary = summary.join(affected_matches, "match_id", "inner")
        merge_by_keys(affected_summary, table_path("gold", "match_summary"), ["match_id"])

        affected_teams = affected_summary.select(F.col("home_team_id").alias("team_id")).unionByName(
            affected_summary.select(F.col("away_team_id").alias("team_id"))
        ).dropDuplicates()
        performance = _team_performance(matches).join(affected_teams, "team_id", "inner").withColumn("pipeline_run_id", F.lit(args.pipeline_run_id))
        merge_by_keys(performance, table_path("gold", "team_performance"), ["team_id"])

        # This aggregate has a small fixed grain. Rebuilding it transactionally avoids stale buckets after corrections.
        distribution = _event_distribution(events).withColumn("pipeline_run_id", F.lit(args.pipeline_run_id))
        distribution.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path("gold", "event_distribution"))
        logging.info("Gold build complete for %s.", args.pipeline_run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
