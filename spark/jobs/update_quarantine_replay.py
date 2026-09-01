"""Mark quarantine records as REPROCESSED after a successful replay."""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import functions as F

from spark.common.delta import merge_by_keys, read_delta_or_none
from spark.common.runtime import build_spark_session, table_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--bronze-record-id", action="append", required=True)
    parser.add_argument("--replay-reason", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_spark_session("pitchflow-update-quarantine-replay")
    try:
        quarantine = read_delta_or_none(spark, table_path("quarantine", "match_events"))
        if quarantine is None:
            logging.info("No quarantine table exists; skipping replay update.")
            return

        matching = quarantine.filter(
            F.col("bronze_record_id").isin(args.bronze_record_id)
            & F.col("status").isin("NEW", "UNDER_REVIEW", "FIXED")
        )

        if matching.rdd.isEmpty():
            logging.info("No quarantine records match the replayed bronze_record_ids.")
            return

        updated = (
            matching.withColumn("status", F.lit("REPROCESSED"))
            .withColumn("retry_count", F.coalesce(F.col("retry_count"), F.lit(0)) + F.lit(1))
            .withColumn("reprocessed_at", F.current_timestamp())
            .withColumn("resolution_action", F.lit("REPLAY"))
            .withColumn("resolution_note", F.lit(args.replay_reason))
            .withColumn("resolution_run_id", F.lit(args.pipeline_run_id))
            .withColumn("resolved_at", F.current_timestamp())
        )
        merge_by_keys(updated, table_path("quarantine", "match_events"), ["quarantine_id"])
        logging.info(
            "Marked %d quarantine records as REPROCESSED for replay run %s.",
            updated.count(),
            args.pipeline_run_id,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
