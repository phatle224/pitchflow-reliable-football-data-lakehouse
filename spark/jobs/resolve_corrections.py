"""Apply an explicitly approved event correction while preserving Bronze and Quarantine audit history."""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.common.delta import merge_by_keys, read_delta_or_none
from spark.common.runtime import build_spark_session, table_path
from spark.jobs.bronze_to_silver import (
    _parse_events,
    _read_event_watermark,
    _with_core_quality,
    _write_event_watermark,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--quarantine-id", action="append", required=True)
    parser.add_argument("--action", choices=("approve", "reject", "under_review"), required=True)
    parser.add_argument("--resolution-note", default="")
    return parser.parse_args()


def _write_resolution(
    dataframe: DataFrame,
    *,
    status: str,
    action: str,
    pipeline_run_id: str,
    note_column: str | None = None,
    operator_note: str = "",
    increment_retry: bool = False,
) -> None:
    note = (
        F.concat_ws(" | ", F.col(note_column), F.lit(operator_note))
        if note_column
        else F.lit(operator_note or None).cast("string")
    )
    resolved = (
        dataframe.withColumn("status", F.lit(status))
        .withColumn(
            "retry_count",
            F.coalesce(F.col("retry_count"), F.lit(0)) + F.lit(1 if increment_retry else 0),
        )
        .withColumn("reprocessed_at", F.current_timestamp() if status == "REPROCESSED" else F.col("reprocessed_at"))
        .withColumn("resolution_action", F.lit(action))
        .withColumn("resolution_note", note)
        .withColumn("resolution_run_id", F.lit(pipeline_run_id))
        .withColumn("resolved_at", F.current_timestamp())
    )
    merge_by_keys(resolved, table_path("quarantine", "match_events"), ["quarantine_id"])


def _prepare_approved_events(spark, selected: DataFrame) -> tuple[DataFrame | None, DataFrame | None]:
    """Revalidate approved records and return (applicable corrections, failures)."""

    bronze = read_delta_or_none(spark, table_path("bronze", "source_records"))
    matches = read_delta_or_none(spark, table_path("silver", "matches"))
    existing_events = read_delta_or_none(spark, table_path("silver", "match_events"))
    if bronze is None or matches is None or existing_events is None:
        raise RuntimeError("Cannot resolve a correction before Bronze, Silver matches, and Silver events exist.")

    selected_ids = selected.select("quarantine_id", "bronze_record_id")
    selected_bronze = bronze.join(selected_ids, "bronze_record_id", "inner")
    parsed = _parse_events(selected_bronze)
    if parsed is None:
        raise RuntimeError("Selected correction has no parseable event payload in Bronze.")
    parsed = parsed.join(selected_ids, "bronze_record_id", "inner")
    checked = _with_core_quality(parsed)
    core_invalid = checked.filter(F.col("core_error").isNotNull()).select(
        "quarantine_id", F.col("core_error").alias("resolution_error")
    )
    core_valid = checked.filter(F.col("core_error").isNull())
    references = matches.select("match_id", "home_team_id", "away_team_id", "kickoff_timestamp")
    reference_checked = core_valid.join(references, "match_id", "left").withColumn(
        "reference_error",
        F.when(F.col("home_team_id").isNull(), F.lit("UNKNOWN_MATCH"))
        .when(
            (F.col("team_id") != F.col("home_team_id")) & (F.col("team_id") != F.col("away_team_id")),
            F.lit("INVALID_TEAM_MATCH_RELATIONSHIP"),
        ),
    )
    reference_invalid = reference_checked.filter(F.col("reference_error").isNotNull()).select(
        "quarantine_id", F.col("reference_error").alias("resolution_error")
    )
    candidate = reference_checked.filter(F.col("reference_error").isNull())
    existing_ids = existing_events.select("event_id").dropDuplicates()
    applicable = candidate.join(existing_ids, "event_id", "inner")
    original_missing = candidate.join(existing_ids, "event_id", "left_anti").select(
        "quarantine_id", F.lit("ORIGINAL_EVENT_NOT_FOUND").alias("resolution_error")
    )
    failures = core_invalid.unionByName(reference_invalid).unionByName(original_missing).dropDuplicates(["quarantine_id"])
    return applicable, failures


def _apply_approved_events(spark, pipeline_run_id: str, events: DataFrame) -> None:
    if events.rdd.isEmpty():
        return
    watermark = _read_event_watermark(spark)
    event_timestamp = F.to_timestamp(
        F.from_unixtime(
            F.unix_timestamp("kickoff_timestamp") + (F.col("minute") * F.lit(60)) + F.coalesce(F.col("second"), F.lit(0))
        )
    )
    prepared = events.withColumn("event_timestamp", event_timestamp).select(
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
        F.lit(pipeline_run_id).alias("pipeline_run_id"),
        (F.lit(False) if watermark is None else F.col("event_timestamp") < F.lit(watermark)).alias("is_late"),
    )
    merge_by_keys(prepared, table_path("silver", "match_events"), ["event_id"])
    _write_event_watermark(spark, pipeline_run_id, watermark, prepared)


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_spark_session("pitchflow-resolve-corrections")
    try:
        quarantine = read_delta_or_none(spark, table_path("quarantine", "match_events"))
        if quarantine is None:
            raise RuntimeError("No Quarantine table exists to resolve.")
        selected = quarantine.filter(F.col("quarantine_id").isin(args.quarantine_id)).filter(
            F.col("error_type") == "EVENT_CORRECTION_REQUIRES_REVIEW"
        ).filter(~F.col("status").isin("REPROCESSED", "REJECTED"))
        if selected.count() != len(set(args.quarantine_id)):
            raise ValueError("Every --quarantine-id must identify one event correction awaiting resolution.")

        if args.action == "under_review":
            _write_resolution(
                selected,
                status="UNDER_REVIEW",
                action="MARKED_UNDER_REVIEW",
                pipeline_run_id=args.pipeline_run_id,
                operator_note=args.resolution_note,
                increment_retry=False,
            )
            return
        if args.action == "reject":
            _write_resolution(
                selected,
                status="REJECTED",
                action="REJECTED",
                pipeline_run_id=args.pipeline_run_id,
                operator_note=args.resolution_note,
                increment_retry=False,
            )
            return

        approved, failures = _prepare_approved_events(spark, selected)
        if failures is not None and not failures.rdd.isEmpty():
            failed_records = selected.join(failures, "quarantine_id", "inner")
            _write_resolution(
                failed_records,
                status="FAILED",
                action="APPROVE_FAILED",
                pipeline_run_id=args.pipeline_run_id,
                note_column="resolution_error",
                operator_note=args.resolution_note,
                increment_retry=True,
            )
        successful = selected if failures is None else selected.join(failures.select("quarantine_id"), "quarantine_id", "left_anti")
        if approved is not None and not approved.rdd.isEmpty():
            _apply_approved_events(spark, args.pipeline_run_id, approved)
            _write_resolution(
                successful,
                status="REPROCESSED",
                action="APPROVED",
                pipeline_run_id=args.pipeline_run_id,
                operator_note=args.resolution_note,
                increment_retry=True,
            )
        logging.info("Correction resolution complete for %s.", args.pipeline_run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
