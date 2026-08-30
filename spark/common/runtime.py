"""Runtime configuration shared by every Spark job."""

from __future__ import annotations

import os


def setting(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or not value:
        raise RuntimeError(f"Required environment variable is not set: {name}")
    return value


def table_path(layer: str, table: str) -> str:
    bucket = setting("PITCHFLOW_MINIO_BUCKET", "pitchflow")
    return f"s3a://{bucket}/{layer}/{table}"


def build_spark_session(app_name: str):
    """Create a Delta-enabled Spark session pointed at the local MinIO service."""

    from pyspark.sql import SparkSession

    endpoint = setting("PITCHFLOW_MINIO_ENDPOINT", "http://minio:9000")
    access_key = setting("MINIO_ROOT_USER", "minioadmin")
    secret_key = setting("MINIO_ROOT_PASSWORD", "minioadmin")

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.delta.logStore.class", "io.delta.storage.S3SingleDriverLogStore")
        .getOrCreate()
    )
