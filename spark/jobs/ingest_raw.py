"""Fetch the approved source snapshot and persist raw payloads to Bronze Delta."""

from __future__ import annotations

import argparse
import logging

from ingestion.statsbomb.client import StatsBombSource, fetch_snapshot_records
from spark.common.bronze import write_bronze_records
from spark.common.runtime import build_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--source-config", default="config/statsbomb_source.json")
    parser.add_argument("--match-limit", type=int)
    parser.add_argument("--inject-chaos", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    source = StatsBombSource.from_file(args.source_config)
    records = fetch_snapshot_records(source, match_limit=args.match_limit, inject_chaos=args.inject_chaos)
    rows = [record.as_bronze_row(args.pipeline_run_id) for record in records]

    spark = build_spark_session("pitchflow-ingest-raw")
    try:
        inserted_candidates = write_bronze_records(spark, rows)
        logging.info("Prepared %s raw source envelopes for Bronze run %s.", inserted_candidates, args.pipeline_run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
