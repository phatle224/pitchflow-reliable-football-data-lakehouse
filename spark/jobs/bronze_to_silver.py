"""Parse Bronze source files, enforce data quality, and update Silver/Quarantine."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

from spark.common.alerts import send_webhook_alert
from spark.common.delta import merge_by_keys, read_delta_or_none
from spark.common.reliability import FAILED, WARNING, QualityAssessment, assess_quality, quality_thresholds_from_environment
from spark.common.runtime import build_spark_session, table_path
from spark.common.schemas import event_schema, lineup_schema, match_schema


QUALITY_METRICS_SCHEMA = StructType(
    [
        StructField("pipeline_run_id", StringType(), nullable=False),
        StructField("input_event_rows", LongType(), nullable=False),
        StructField("valid_event_rows", LongType(), nullable=False),
        StructField("quarantine_event_rows", LongType(), nullable=False),
        StructField("duplicate_event_rows", LongType(), nullable=False),
        StructField("late_event_count", LongType(), nullable=False),
        StructField("dq_pass_rate", DoubleType(), nullable=False),
        StructField("dq_quarantine_rate", DoubleType(), nullable=False),
        StructField("dq_late_event_rate", DoubleType(), nullable=False),
        StructField("dq_status", StringType(), nullable=False),
        StructField("measured_at", TimestampType(), nullable=False),
    ]
)

WATERMARK_SCHEMA = StructType(
    [
        StructField("source", StringType(), nullable=False),
        StructField("entity", StringType(), nullable=False),
        StructField("watermark_timestamp", TimestampType(), nullable=False),
        StructField("pipeline_run_id", StringType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

EVENT_WATERMARK_ENTITY = "match_events"
DEFAULT_WATERMARK_SOURCE = "all"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--bronze-record-id", action="append", default=[])
    return parser.parse_args()


def _run_bronze(spark, pipeline_run_id: str, bronze_record_ids: list[str]) -> DataFrame | None:
    bronze = read_delta_or_none(spark, table_path("bronze", "source_records"))
    if bronze is None:
        return None
    if bronze_record_ids:
        return bronze.filter(F.col("bronze_record_id").isin(bronze_record_ids))
    # V2: prefer watermark-based incremental read with lookback, fall back to run_id
    lookback_hours = int(os.environ.get("PITCHFLOW_WATERMARK_LOOKBACK_HOURS", "0"))
    if lookback_hours > 0:
        watermark = _read_event_watermark(spark)
        if watermark is not None:
            from datetime import timedelta
            cutoff = watermark - timedelta(hours=lookback_hours)
            ingestion_filtered = bronze.filter(
                F.to_timestamp(F.col("ingestion_timestamp")) >= F.lit(cutoff)
            )
            if not ingestion_filtered.rdd.isEmpty():
                return ingestion_filtered
    return bronze.filter(F.col("pipeline_run_id") == pipeline_run_id)


def _parse_matches(bronze: DataFrame) -> DataFrame:
    parsed = (
        bronze.filter(F.col("source_object") == "matches")
        .select(
            "bronze_record_id",
            "pipeline_run_id",
            F.explode(F.from_json("raw_payload", ArrayType(match_schema))).alias("match"),
        )
        .select(
            F.col("match.match_id").alias("match_id"),
            F.col("match.competition.competition_id").alias("competition_id"),
            F.col("match.competition.competition_name").alias("competition_name"),
            F.col("match.season.season_id").alias("season_id"),
            F.col("match.season.season_name").alias("season_name"),
            F.to_date(F.col("match.match_date")).alias("match_date"),
            F.to_timestamp(F.concat_ws(" ", F.col("match.match_date"), F.col("match.kick_off")), "yyyy-MM-dd HH:mm:ss.SSS").alias("kickoff_timestamp"),
            F.col("match.home_team.home_team_id").alias("home_team_id"),
            F.col("match.home_team.home_team_name").alias("home_team_name"),
            F.col("match.home_team.country.name").alias("home_team_country"),
            F.col("match.away_team.away_team_id").alias("away_team_id"),
            F.col("match.away_team.away_team_name").alias("away_team_name"),
            F.col("match.away_team.country.name").alias("away_team_country"),
            F.col("match.stadium.id").alias("stadium_id"),
            F.col("match.stadium.name").alias("stadium_name"),
            F.col("match.stadium.country.name").alias("stadium_country"),
            F.col("match.home_score").alias("home_score"),
            F.col("match.away_score").alias("away_score"),
            F.col("match.match_status").alias("status"),
            F.col("bronze_record_id"),
            F.col("pipeline_run_id"),
        )
    )
    return parsed.filter(F.col("match_id").isNotNull())


def _write_match_dimensions(matches: DataFrame) -> None:
    if matches.rdd.isEmpty():
        return
    merge_by_keys(matches, table_path("silver", "matches"), ["match_id"])

    home_teams = matches.select(
        F.col("home_team_id").alias("team_id"),
        F.col("home_team_name").alias("team_name"),
        F.col("home_team_country").alias("country"),
        "pipeline_run_id",
    )
    away_teams = matches.select(
        F.col("away_team_id").alias("team_id"),
        F.col("away_team_name").alias("team_name"),
        F.col("away_team_country").alias("country"),
        "pipeline_run_id",
    )
    teams = home_teams.unionByName(away_teams).filter(F.col("team_id").isNotNull()).dropDuplicates(["team_id"])
    merge_by_keys(teams, table_path("silver", "teams"), ["team_id"])

    stadiums = matches.select(
        "stadium_id",
        "stadium_name",
        F.col("stadium_country").alias("country"),
        "pipeline_run_id",
    ).filter(F.col("stadium_id").isNotNull()).dropDuplicates(["stadium_id"])
    if not stadiums.rdd.isEmpty():
        merge_by_keys(stadiums, table_path("silver", "stadiums"), ["stadium_id"])


def _parse_lineup_players(bronze: DataFrame) -> DataFrame | None:
    lineups = bronze.filter(F.col("source_object") == "lineups")
    if lineups.rdd.isEmpty():
        return None
    return (
        lineups.select("pipeline_run_id", F.explode(F.from_json("raw_payload", lineup_schema)).alias("team_lineup"))
        .select("pipeline_run_id", "team_lineup.team_id", F.explode("team_lineup.lineup").alias("player"))
        .select(
            F.col("player.player_id").alias("player_id"),
            F.col("player.player_name").alias("player_name"),
            F.col("team_id"),
            F.element_at(F.col("player.positions"), 1).getField("position").alias("position"),
            "pipeline_run_id",
        )
        .filter(F.col("player_id").isNotNull())
        .dropDuplicates(["player_id"])
    )


def _parse_events(bronze: DataFrame) -> DataFrame | None:
    event_files = bronze.filter(F.col("source_object") == "events")
    synthetic_events = bronze.filter(F.col("source_object") == "synthetic_event")
    if event_files.rdd.isEmpty() and synthetic_events.rdd.isEmpty():
        return None

    selected_columns = [
        "bronze_record_id",
        "source",
        "source_match_id",
        "variant_type",
        "pipeline_run_id",
    ]
    normal = event_files.select(*selected_columns, F.explode(F.from_json("raw_payload", ArrayType(event_schema))).alias("event"))
    synthetic = synthetic_events.select(*selected_columns, F.from_json("raw_payload", event_schema).alias("event"))
    parsed = normal.unionByName(synthetic)
    return parsed.select(
        "bronze_record_id",
        "source",
        "source_match_id",
        "variant_type",
        "pipeline_run_id",
        F.col("event.id").alias("event_id"),
        F.col("event.index").alias("event_index"),
        F.col("event.period").alias("period"),
        F.col("event.timestamp").alias("event_clock"),
        F.col("event.minute").alias("minute"),
        F.col("event.second").alias("second"),
        F.upper(F.col("event.type.name")).alias("event_type"),
        F.col("event.team.id").alias("team_id"),
        F.col("event.team.name").alias("team_name"),
        F.col("event.player.id").alias("player_id"),
        F.col("event.player.name").alias("player_name"),
        F.upper(F.col("event.shot.outcome.name")).alias("shot_outcome"),
        F.coalesce(F.upper(F.col("event.foul_committed.card.name")), F.upper(F.col("event.bad_behaviour.card.name"))).alias("card_type"),
        F.sha2(F.to_json(F.col("event")), 256).alias("event_payload_hash"),
    ).withColumn("match_id", F.col("source_match_id").cast("long"))


def _with_core_quality(events: DataFrame) -> DataFrame:
    return events.withColumn(
        "core_error",
        F.when(F.col("event_id").isNull(), F.lit("MISSING_EVENT_ID"))
        .when(F.col("match_id").isNull(), F.lit("MISSING_MATCH_ID"))
        .when(F.col("team_id").isNull(), F.lit("MISSING_TEAM_ID"))
        .when(F.col("minute").isNull() | (F.col("minute") < 0) | (F.col("minute") > 130), F.lit("INVALID_MATCH_MINUTE")),
    )


def _quarantine_frame(dataframe: DataFrame, error_column: str) -> DataFrame:
    return dataframe.select(
        F.sha2(F.concat_ws("|", F.col("bronze_record_id"), F.col(error_column)), 256).alias("quarantine_id"),
        "bronze_record_id",
        "source",
        F.col("event_id").alias("record_key"),
        "pipeline_run_id",
        F.col(error_column).alias("error_type"),
        F.concat(F.lit("Data quality rule failed: "), F.col(error_column)).alias("error_message"),
        F.col(error_column).alias("failed_rule"),
        F.current_timestamp().alias("detected_at"),
        F.lit(0).cast("int").alias("retry_count"),
        F.lit("NEW").alias("status"),
        F.lit(None).cast("timestamp").alias("reprocessed_at"),
        F.lit("v1").alias("rule_version"),
    )


def _read_event_watermark(spark, source: str = DEFAULT_WATERMARK_SOURCE):
    """Read the persisted high watermark without depending on Silver table layout."""

    watermarks = read_delta_or_none(spark, table_path("ops", "processing_watermarks"))
    if watermarks is None:
        return None
    row = (
        watermarks.filter(
            (F.col("entity") == EVENT_WATERMARK_ENTITY)
            & (F.col("source") == source)
        )
        .agg(F.max("watermark_timestamp").alias("watermark_timestamp"))
        .first()
    )
    ts = row["watermark_timestamp"]
    if ts is not None:
        return ts
    # Fallback: read any source watermark for this entity (V1 compatibility)
    row = (
        watermarks.filter(F.col("entity") == EVENT_WATERMARK_ENTITY)
        .agg(F.max("watermark_timestamp").alias("watermark_timestamp"))
        .first()
    )
    return row["watermark_timestamp"]


def _write_event_watermark(spark, pipeline_run_id: str, existing_watermark, new_events: DataFrame, source: str = DEFAULT_WATERMARK_SOURCE) -> None:
    """Persist a monotonic event watermark only after Silver has been written safely."""

    candidate = new_events.agg(F.max("event_timestamp").alias("watermark_timestamp")).first()["watermark_timestamp"]
    if candidate is None and existing_watermark is None:
        return
    watermark = max(value for value in (existing_watermark, candidate) if value is not None)
    dataframe = spark.createDataFrame(
        [(source, EVENT_WATERMARK_ENTITY, watermark, pipeline_run_id, datetime.now(timezone.utc))],
        schema=WATERMARK_SCHEMA,
    )
    merge_by_keys(dataframe, table_path("ops", "processing_watermarks"), ["source", "entity"])


def _process_events(spark, bronze: DataFrame, pipeline_run_id: str) -> dict[str, int]:
    events = _parse_events(bronze)
    if events is None:
        return {"input": 0, "valid": 0, "quarantined": 0, "duplicates": 0, "late": 0}

    input_count = events.count()
    core_checked = _with_core_quality(events)
    core_invalid = core_checked.filter(F.col("core_error").isNotNull())
    core_valid = core_checked.filter(F.col("core_error").isNull())

    matches = read_delta_or_none(spark, table_path("silver", "matches"))
    if matches is None:
        raise RuntimeError("Cannot validate events because silver_matches is not available.")
    match_reference = matches.select("match_id", "home_team_id", "away_team_id", "kickoff_timestamp")
    referential_checked = core_valid.join(match_reference, "match_id", "left").withColumn(
        "reference_error",
        F.when(F.col("home_team_id").isNull(), F.lit("UNKNOWN_MATCH"))
        .when(
            (F.col("team_id") != F.col("home_team_id")) & (F.col("team_id") != F.col("away_team_id")),
            F.lit("INVALID_TEAM_MATCH_RELATIONSHIP"),
        ),
    )
    reference_invalid = referential_checked.filter(F.col("reference_error").isNotNull())
    candidate_events = referential_checked.filter(F.col("reference_error").isNull())

    stats = candidate_events.groupBy("event_id").agg(
        F.count("*").alias("event_record_count"),
        F.countDistinct("event_payload_hash").alias("payload_variant_count"),
    )
    ordering = Window.partitionBy("event_id").orderBy(F.when(F.col("source") == "statsbomb_open_data", 0).otherwise(1), "bronze_record_id")
    dedupe_checked = (
        candidate_events.join(stats, "event_id", "left")
        .withColumn("event_row_number", F.row_number().over(ordering))
        .withColumn(
            "duplicate_error",
            F.when((F.col("event_record_count") > 1) & (F.col("payload_variant_count") > 1) & (F.col("source") != "statsbomb_open_data"), F.lit("EVENT_CORRECTION_REQUIRES_REVIEW")),
        )
        .withColumn(
            "is_exact_duplicate",
            (F.col("event_record_count") > 1)
            & (F.col("payload_variant_count") == 1)
            & (F.col("event_row_number") > 1),
        )
    )
    correction_invalid = dedupe_checked.filter(F.col("duplicate_error").isNotNull())
    valid_events = dedupe_checked.filter(F.col("duplicate_error").isNull() & ~F.col("is_exact_duplicate"))
    duplicate_count = dedupe_checked.filter(F.col("is_exact_duplicate")).count()

    event_timestamp = F.to_timestamp(
        F.from_unixtime(F.unix_timestamp("kickoff_timestamp") + (F.col("minute") * F.lit(60)) + F.coalesce(F.col("second"), F.lit(0)))
    )
    prepared_events = valid_events.withColumn("event_timestamp", event_timestamp).select(
        "event_id",
        "match_id",
        "team_id",
        "team_name",
        "player_id",
        "player_name",
        "event_index",
        "period",
        "event_clock",
        "minute",
        "second",
        "event_type",
        "shot_outcome",
        "card_type",
        "event_timestamp",
        "source",
        "variant_type",
        "bronze_record_id",
        "event_payload_hash",
        "pipeline_run_id",
    )

    existing = read_delta_or_none(spark, table_path("silver", "match_events"))
    watermark = _read_event_watermark(spark)
    if watermark is None and existing is not None:
        # Compatibility fallback for a V1 table before the V2 watermark state exists.
        watermark = existing.agg(F.max("event_timestamp").alias("watermark")).first()["watermark"]
    new_events = (
        prepared_events
        if existing is None
        else prepared_events.join(existing.select("event_id").dropDuplicates(), "event_id", "left_anti")
    )
    silver_events = new_events.withColumn(
        "is_late",
        F.lit(False) if watermark is None else F.col("event_timestamp") < F.lit(watermark),
    )
    valid_count = prepared_events.count()
    late_count = silver_events.filter(F.col("is_late")).count()
    if not silver_events.rdd.isEmpty():
        merge_by_keys(silver_events, table_path("silver", "match_events"), ["event_id"], insert_only=True)
    _write_event_watermark(spark, pipeline_run_id, watermark, silver_events)

    event_players = silver_events.filter(F.col("player_id").isNotNull()).select(
        "player_id", "player_name", "team_id", F.lit(None).cast("string").alias("position"), "pipeline_run_id"
    ).dropDuplicates(["player_id"])
    if not event_players.rdd.isEmpty():
        merge_by_keys(event_players, table_path("silver", "players"), ["player_id"])

    quarantine = _quarantine_frame(core_invalid, "core_error").unionByName(
        _quarantine_frame(reference_invalid, "reference_error")
    ).unionByName(_quarantine_frame(correction_invalid, "duplicate_error"))
    quarantined_count = quarantine.count()
    if quarantined_count:
        merge_by_keys(quarantine, table_path("quarantine", "match_events"), ["quarantine_id"], insert_only=True)

    return {
        "input": input_count,
        "valid": valid_count,
        "quarantined": quarantined_count,
        "duplicates": duplicate_count,
        "late": late_count,
    }


def _write_quality_metrics(spark, pipeline_run_id: str, counts: dict[str, int], assessment: QualityAssessment) -> None:
    row = [
        (
            pipeline_run_id,
            counts["input"],
            counts["valid"],
            counts["quarantined"],
            counts["duplicates"],
            counts["late"],
            assessment.pass_rate,
            assessment.quarantine_rate,
            assessment.late_event_rate,
            assessment.overall_status,
            datetime.now(timezone.utc),
        )
    ]
    dataframe = spark.createDataFrame(row, schema=QUALITY_METRICS_SCHEMA)
    merge_by_keys(dataframe, table_path("ops", "quality_metrics"), ["pipeline_run_id"])


def _enforce_quality_gate(pipeline_run_id: str, assessment: QualityAssessment) -> None:
    status = assessment.overall_status
    detail = (
        f"pipeline_run_id={pipeline_run_id}; "
        f"dq_pass_rate={assessment.pass_rate:.2f}%; "
        f"quarantine_rate={assessment.quarantine_rate:.2f}%; "
        f"late_event_rate={assessment.late_event_rate:.2f}%"
    )
    for warning in assessment.warnings:
        logging.warning("DQ gate: %s", warning)
    if status == WARNING:
        send_webhook_alert(severity=WARNING, title="PitchFlow data quality", message=detail)
    elif status == FAILED:
        # The metric and Quarantine data are already persisted. Raising makes
        # Airflow retry/alert the unhealthy batch without losing evidence.
        raise RuntimeError(f"Data-quality gate failed: {detail}")


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_spark_session("pitchflow-bronze-to-silver")
    try:
        bronze = _run_bronze(spark, args.pipeline_run_id, args.bronze_record_id)
        if bronze is None or bronze.rdd.isEmpty():
            logging.info("No Bronze records selected for run %s.", args.pipeline_run_id)
            return
        matches = _parse_matches(bronze)
        _write_match_dimensions(matches)
        lineup_players = _parse_lineup_players(bronze)
        if lineup_players is not None and not lineup_players.rdd.isEmpty():
            merge_by_keys(lineup_players, table_path("silver", "players"), ["player_id"])
        counts = _process_events(spark, bronze, args.pipeline_run_id)
        thresholds = quality_thresholds_from_environment()
        assessment = assess_quality(counts, thresholds)
        _write_quality_metrics(spark, args.pipeline_run_id, counts, assessment)
        _enforce_quality_gate(args.pipeline_run_id, assessment)
        logging.info("Bronze-to-Silver complete for %s: %s; dq_status=%s", args.pipeline_run_id, counts, assessment.overall_status)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
