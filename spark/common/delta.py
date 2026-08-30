"""Small Delta Lake write helpers that preserve idempotent table semantics."""

from __future__ import annotations

from typing import Iterable


def delta_exists(spark, path: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def merge_by_keys(dataframe, path: str, keys: Iterable[str], *, insert_only: bool = False) -> None:
    """Merge a DataFrame by business keys, creating the Delta table on first write."""

    from delta.tables import DeltaTable

    key_names = tuple(keys)
    if not key_names:
        raise ValueError("At least one merge key is required.")

    spark = dataframe.sparkSession
    if not delta_exists(spark, path):
        dataframe.write.format("delta").mode("overwrite").save(path)
        return

    condition = " AND ".join(f"target.{key} = source.{key}" for key in key_names)
    merge = DeltaTable.forPath(spark, path).alias("target").merge(dataframe.alias("source"), condition)
    if not insert_only:
        merge = merge.whenMatchedUpdateAll()
    merge.whenNotMatchedInsertAll().execute()


def read_delta_or_none(spark, path: str):
    """Read an existing Delta table or return None when a layer is not initialized."""

    if not delta_exists(spark, path):
        return None
    return spark.read.format("delta").load(path)
