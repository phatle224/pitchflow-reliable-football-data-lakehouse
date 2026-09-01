"""Fetch the approved source snapshot and persist raw payloads to Bronze Delta."""

from __future__ import annotations

import argparse
import logging
import os

from ingestion.statsbomb.client import StatsBombSource, iter_snapshot_records
from spark.common.bronze import write_bronze_records
from spark.common.runtime import build_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument(
        "--source-config",
        default=os.getenv("PITCHFLOW_SOURCE_CONFIG", "config/statsbomb_source.json"),
    )
    parser.add_argument("--match-limit", type=int)
    parser.add_argument("--inject-chaos", action="store_true")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=int(os.getenv("PITCHFLOW_INGEST_CHUNK_SIZE", "25")),
        help="Number of raw source files to commit per Bronze transaction.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.chunk_size < 1:
        raise ValueError("--chunk-size must be greater than zero.")
    source = StatsBombSource.from_file(args.source_config)

    spark = build_spark_session("pitchflow-ingest-raw")
    try:
        total_candidates = 0
        chunk: list[dict[str, str | None]] = []
        for record in iter_snapshot_records(
            source,
            match_limit=args.match_limit,
            inject_chaos=args.inject_chaos,
        ):
            chunk.append(record.as_bronze_row(args.pipeline_run_id))
            if len(chunk) >= args.chunk_size:
                total_candidates += write_bronze_records(spark, chunk)
                logging.info("Committed Bronze chunk of %s source envelopes.", len(chunk))
                chunk = []
        if chunk:
            total_candidates += write_bronze_records(spark, chunk)
            logging.info("Committed final Bronze chunk of %s source envelopes.", len(chunk))
        logging.info("Prepared %s raw source envelopes for Bronze run %s.", total_candidates, args.pipeline_run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
