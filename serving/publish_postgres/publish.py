"""UPSERT selected Gold Delta tables into PostgreSQL for Metabase."""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Iterable

from spark.common.delta import read_delta_or_none
from spark.common.runtime import build_spark_session, table_path


TABLES = {
    "gold_match_summary": {
        "path": table_path("gold", "match_summary"),
        "key_columns": ("match_id",),
        "ddl": """
            CREATE TABLE IF NOT EXISTS gold_match_summary (
                match_id BIGINT PRIMARY KEY,
                competition_id BIGINT,
                competition_name TEXT,
                season_id BIGINT,
                season_name TEXT,
                match_date DATE,
                home_team_id BIGINT,
                home_team_name TEXT,
                away_team_id BIGINT,
                away_team_name TEXT,
                home_score INTEGER,
                away_score INTEGER,
                winner TEXT,
                goal_count BIGINT,
                yellow_card_count BIGINT,
                red_card_count BIGINT,
                shot_count BIGINT,
                event_count BIGINT,
                pipeline_run_id TEXT
            )
        """,
    },
    "gold_team_performance": {
        "path": table_path("gold", "team_performance"),
        "key_columns": ("team_id",),
        "ddl": """
            CREATE TABLE IF NOT EXISTS gold_team_performance (
                team_id BIGINT PRIMARY KEY,
                team_name TEXT,
                matches_played BIGINT,
                wins BIGINT,
                draws BIGINT,
                losses BIGINT,
                goals_for BIGINT,
                goals_against BIGINT,
                goal_difference BIGINT,
                points BIGINT,
                pipeline_run_id TEXT
            )
        """,
    },
    "gold_event_distribution": {
        "path": table_path("gold", "event_distribution"),
        "key_columns": ("event_type", "minute_bucket"),
        "ddl": """
            CREATE TABLE IF NOT EXISTS gold_event_distribution (
                event_type TEXT NOT NULL,
                minute_bucket INTEGER NOT NULL,
                event_count BIGINT,
                pipeline_run_id TEXT,
                PRIMARY KEY (event_type, minute_bucket)
            )
        """,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-run-id", required=True)
    return parser.parse_args()


def postgres_connection():
    """Open a PostgreSQL connection using the documented environment contract."""

    import psycopg

    return psycopg.connect(
        host=os.getenv("PITCHFLOW_POSTGRES_HOST", "postgres"),
        port=os.getenv("PITCHFLOW_POSTGRES_PORT", "5432"),
        dbname=os.getenv("PITCHFLOW_POSTGRES_DB", "pitchflow"),
        user=os.getenv("PITCHFLOW_POSTGRES_USER", "pitchflow"),
        password=os.getenv("PITCHFLOW_POSTGRES_PASSWORD", "pitchflow"),
    )


def _upsert_rows(cursor, table: str, key_columns: Iterable[str], rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    from psycopg import sql

    columns = tuple(rows[0].keys())
    keys = tuple(key_columns)
    update_columns = tuple(column for column in columns if column not in keys)
    statement = sql.SQL(
        "INSERT INTO {table} ({columns}) VALUES ({values}) "
        "ON CONFLICT ({keys}) DO UPDATE SET {updates}"
    ).format(
        table=sql.Identifier(table),
        columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
        values=sql.SQL(", ").join(sql.Placeholder(column) for column in columns),
        keys=sql.SQL(", ").join(map(sql.Identifier, keys)),
        updates=sql.SQL(", ").join(
            sql.SQL("{column} = EXCLUDED.{column}").format(column=sql.Identifier(column)) for column in update_columns
        ),
    )
    cursor.executemany(statement, rows)


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_spark_session("pitchflow-publish-serving")
    try:
        with postgres_connection() as connection:
            with connection.cursor() as cursor:
                for table_name, table in TABLES.items():
                    dataframe = read_delta_or_none(spark, table["path"])
                    if dataframe is None:
                        logging.info("Serving table %s skipped because its Gold source does not exist.", table_name)
                        continue
                    cursor.execute(table["ddl"])
                    rows = [row.asDict(recursive=True) for row in dataframe.toLocalIterator()]
                    _upsert_rows(cursor, table_name, table["key_columns"], rows)
                    cursor.execute(f"SELECT count(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    logging.info("Published %s rows to %s for run %s.", count, table_name, args.pipeline_run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
