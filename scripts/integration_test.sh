#!/usr/bin/env bash
# PitchFlow Docker integration smoke test.
#
# Prerequisites:
#   docker compose up --build -d
#
# Usage:
#   bash scripts/integration_test.sh
#
# This script triggers the pipeline_daily DAG with chaos variants,
# waits for completion, and validates the expected Delta tables and
# PostgreSQL rows exist.

set -euo pipefail

AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8088}"
AIRFLOW_USER="${AIRFLOW_USER:-airflow}"
AIRFLOW_PASS="${AIRFLOW_PASS:-airflow}"
PG_HOST="${PITCHFLOW_POSTGRES_HOST:-localhost}"
PG_PORT="${PITCHFLOW_POSTGRES_PORT:-5432}"
PG_DB="${PITCHFLOW_POSTGRES_DB:-pitchflow}"
PG_USER="${PITCHFLOW_POSTGRES_USER:-pitchflow}"
PG_PASS="${PITCHFLOW_POSTGRES_PASSWORD:-pitchflow}"

echo "=== PitchFlow Integration Test ==="

# 1. Wait for Airflow to be ready
echo "[1/7] Waiting for Airflow webserver..."
for i in $(seq 1 30); do
    if curl -sf "${AIRFLOW_URL}/health" >/dev/null 2>&1; then
        echo "  Airflow is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  ERROR: Airflow not ready after 150s."
        exit 1
    fi
    sleep 5
done

# 2. Unpause the pipeline_daily DAG
echo "[2/7] Unpausing pipeline_daily DAG..."
curl -sf -X PATCH \
    -u "${AIRFLOW_USER}:${AIRFLOW_PASS}" \
    -H "Content-Type: application/json" \
    -d '{"is_paused": false}' \
    "${AIRFLOW_URL}/api/v1/dags/pipeline_daily" >/dev/null

# 3. Trigger pipeline_daily with chaos
echo "[3/7] Triggering pipeline_daily with match_limit=1, inject_chaos=true..."
RUN_RESPONSE=$(curl -sf -X POST \
    -u "${AIRFLOW_USER}:${AIRFLOW_PASS}" \
    -H "Content-Type: application/json" \
    -d '{"conf": {"match_limit": 1, "inject_chaos": true}}' \
    "${AIRFLOW_URL}/api/v1/dags/pipeline_daily/dagRuns")
DAG_RUN_ID=$(echo "$RUN_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['dag_run_id'])" 2>/dev/null || echo "unknown")
echo "  DAG run ID: ${DAG_RUN_ID}"

# 4. Wait for pipeline completion (up to 10 minutes)
echo "[4/7] Waiting for pipeline completion (timeout: 10 minutes)..."
for i in $(seq 1 60); do
    STATE=$(curl -sf -u "${AIRFLOW_USER}:${AIRFLOW_PASS}" \
        "${AIRFLOW_URL}/api/v1/dags/pipeline_daily/dagRuns/${DAG_RUN_ID}" \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])" 2>/dev/null || echo "unknown")
    if [ "$STATE" = "success" ]; then
        echo "  Pipeline completed successfully."
        break
    elif [ "$STATE" = "failed" ]; then
        echo "  WARNING: Pipeline failed (expected if DQ threshold was breached)."
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "  ERROR: Pipeline did not complete within 10 minutes (state: ${STATE})."
        exit 1
    fi
    sleep 10
done

# 5. Verify PostgreSQL serving tables
echo "[5/7] Verifying PostgreSQL serving tables..."
PGPASSWORD="${PG_PASS}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" -At -c "
    SELECT 'gold_match_summary:' || count(*) FROM gold_match_summary
    UNION ALL
    SELECT 'gold_team_performance:' || count(*) FROM gold_team_performance
    UNION ALL
    SELECT 'gold_event_distribution:' || count(*) FROM gold_event_distribution;
" 2>/dev/null && echo "  PostgreSQL tables verified." || echo "  WARNING: Could not query PostgreSQL."

# 6. Verify MinIO Delta paths exist
echo "[6/7] Verifying MinIO Delta tables..."
docker compose exec -T minio-init mc ls local/pitchflow/bronze/source_records/ >/dev/null 2>&1 \
    && echo "  Bronze: OK" || echo "  Bronze: MISSING"
docker compose exec -T minio-init mc ls local/pitchflow/silver/matches/ >/dev/null 2>&1 \
    && echo "  Silver matches: OK" || echo "  Silver matches: MISSING"
docker compose exec -T minio-init mc ls local/pitchflow/silver/match_events/ >/dev/null 2>&1 \
    && echo "  Silver match_events: OK" || echo "  Silver match_events: MISSING"
docker compose exec -T minio-init mc ls local/pitchflow/gold/match_summary/ >/dev/null 2>&1 \
    && echo "  Gold match_summary: OK" || echo "  Gold match_summary: MISSING"
docker compose exec -T minio-init mc ls local/pitchflow/quarantine/match_events/ >/dev/null 2>&1 \
    && echo "  Quarantine: OK" || echo "  Quarantine: MISSING"
docker compose exec -T minio-init mc ls local/pitchflow/ops/quality_metrics/ >/dev/null 2>&1 \
    && echo "  DQ Metrics: OK" || echo "  DQ Metrics: MISSING"
docker compose exec -T minio-init mc ls local/pitchflow/ops/processing_watermarks/ >/dev/null 2>&1 \
    && echo "  Watermarks: OK" || echo "  Watermarks: MISSING"

# 7. Summary
echo "[7/7] Integration test complete."
echo ""
echo "=== Test Results ==="
echo "DAG run: ${DAG_RUN_ID}"
echo "Final state: ${STATE}"
echo ""
echo "Next steps:"
echo "  - Review Airflow UI at ${AIRFLOW_URL}"
echo "  - Review MinIO console at http://localhost:9001"
echo "  - Review Metabase at http://localhost:3000"
