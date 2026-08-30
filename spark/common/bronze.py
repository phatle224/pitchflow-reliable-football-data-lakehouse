"""Bronze Delta write operations."""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

from spark.common.delta import merge_by_keys
from spark.common.runtime import table_path


BRONZE_SCHEMA = StructType(
    [
        StructField("bronze_record_id", StringType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("source_object", StringType(), nullable=False),
        StructField("source_uri", StringType(), nullable=False),
        StructField("source_commit_sha", StringType(), nullable=False),
        StructField("source_record_locator", StringType(), nullable=False),
        StructField("source_timestamp", StringType()),
        StructField("source_match_id", StringType()),
        StructField("variant_type", StringType()),
        StructField("payload_hash", StringType(), nullable=False),
        StructField("raw_payload", StringType(), nullable=False),
        StructField("ingestion_timestamp", StringType(), nullable=False),
        StructField("pipeline_run_id", StringType(), nullable=False),
        StructField("partition_date", StringType(), nullable=False),
    ]
)


def write_bronze_records(spark, rows: list[dict[str, str | None]]) -> int:
    """Insert immutable source envelopes by deterministic Bronze record ID."""

    if not rows:
        return 0
    dataframe = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
    merge_by_keys(dataframe, table_path("bronze", "source_records"), ["bronze_record_id"], insert_only=True)
    return dataframe.count()
