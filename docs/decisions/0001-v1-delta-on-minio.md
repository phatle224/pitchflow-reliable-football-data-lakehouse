# 0001 — Store V1 Delta tables on MinIO

**Status:** Accepted

## Context

PitchFlow needs an S3-like, reproducible local storage layer for Bronze, Silver, Gold, and Quarantine Delta tables. The original PRD listed local-disk Delta as the initial option and MinIO as optional.

## Decision

V1 uses MinIO as the mandatory S3-compatible object store. Delta paths use the `pitchflow` bucket and the `bronze`, `silver`, `gold`, and `quarantine` prefixes. PostgreSQL stores only dashboard-serving Gold datasets.

## Consequences

The Docker Compose stack must configure Spark's S3A client for MinIO and initialize the bucket. The storage layout maps directly to S3 later, but MinIO adds a service and configuration surface to the local environment.
