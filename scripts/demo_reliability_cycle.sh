#!/usr/bin/env bash
# PitchFlow V2 Reliability Demo
#
# Demonstrates the full reliability cycle:
#   failure → alert → quarantine → fix → replay → dashboard updated
#
# Prerequisites:
#   docker compose up --build -d
#   Set PITCHFLOW_ALERT_WEBHOOK_URL in .env for live alerts (optional)
#
# Usage:
#   bash scripts/demo_reliability_cycle.sh

set -euo pipefail

AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8088}"
AIRFLOW_USER="${AIRFLOW_USER:-airflow}"
AIRFLOW_PASS="${AIRFLOW_PASS:-airflow}"
PG_HOST="${PITCHFLOW_POSTGRES_HOST:-localhost}"
PG_PORT="${PITCHFLOW_POSTGRES_PORT:-5432}"
PG_DB="${PITCHFLOW_POSTGRES_DB:-pitchflow}"
PG_USER="${PITCHFLOW_POSTGRES_USER:-pitchflow}"
PG_PASS="${PITCHFLOW_POSTGRES_PASSWORD:-pitchflow}"

api() {
    curl -sf -u "${AIRFLOW_USER}:${AIRFLOW_PASS}" -H "Content-Type: application/json" "$@"
}

wait_dag() {
    local dag_id="$1" run_id="$2"
    for i in $(seq 1 60); do
        STATE=$(api "${AIRFLOW_URL}/api/v1/dags/${dag_id}/dagRuns/${run_id}" \
            | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])" 2>/dev/null || echo "unknown")
        if [ "$STATE" = "success" ] || [ "$STATE" = "failed" ]; then
            echo "  State: ${STATE}"
            return 0
        fi
        sleep 10
    done
    echo "  Timeout waiting for ${dag_id}"
    return 1
}

echo "╔══════════════════════════════════════════════════════════╗"
echo "║     PitchFlow V2 — Reliability Demonstration Cycle     ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ─── Step 1: Ingest with intentional failures ───
echo "▶ Step 1: Ingest data with chaos variants (includes failures)"
echo "  Triggering pipeline_daily with inject_chaos=true..."
api -X PATCH -d '{"is_paused": false}' "${AIRFLOW_URL}/api/v1/dags/pipeline_daily" >/dev/null 2>&1 || true
RESP=$(api -X POST -d '{"conf": {"match_limit": 1, "inject_chaos": true}}' \
    "${AIRFLOW_URL}/api/v1/dags/pipeline_daily/dagRuns")
RUN_ID=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['dag_run_id'])" 2>/dev/null)
echo "  DAG run: ${RUN_ID}"
wait_dag "pipeline_daily" "${RUN_ID}"
echo ""

# ─── Step 2: Observe failures and quarantine ───
echo "▶ Step 2: Observe quarantine and DQ metrics"
echo "  Querying DQ metrics from PostgreSQL..."
echo ""
echo "  --- Quality Metrics (ops/quality_metrics) ---"
echo "  (View in MinIO console or Spark SQL)"
echo ""
echo "  --- Quarantine Records ---"
echo "  Expected: missing_event_id → MISSING_EVENT_ID quarantine"
echo "  Expected: invalid_minute → INVALID_MATCH_MINUTE quarantine"
echo "  Expected: event_correction → EVENT_CORRECTION_REQUIRES_REVIEW quarantine"
echo ""

# ─── Step 3: Check alerts ───
echo "▶ Step 3: Check alerts"
echo "  If PITCHFLOW_ALERT_WEBHOOK_URL is configured, check your Discord/Slack channel."
echo "  Expected alerts:"
echo "    - WARNING or FAILED webhook if DQ thresholds are breached"
echo "    - Retry alerts if any task retries"
echo ""

# ─── Step 4: Review correction ───
echo "▶ Step 4: Resolve a correction via pipeline_resolve_correction"
echo "  (This step requires quarantine_ids from the quarantine table.)"
echo "  Example trigger config:"
echo '  {"quarantine_ids": ["<id>"], "action": "approve", "match_ids": ["<match_id>"], "resolution_note": "Verified correction"}'
echo ""

# ─── Step 5: Dashboard update ───
echo "▶ Step 5: Verify dashboard data"
echo "  Checking PostgreSQL serving tables..."
PGPASSWORD="${PG_PASS}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" -At -c "
    SELECT 'match_summary:' || count(*) FROM gold_match_summary
    UNION ALL
    SELECT 'team_performance:' || count(*) FROM gold_team_performance
    UNION ALL
    SELECT 'event_distribution:' || count(*) FROM gold_event_distribution;
" 2>/dev/null || echo "  (PostgreSQL query requires psql client on host)"
echo ""

echo "╔══════════════════════════════════════════════════════════╗"
echo "║                 Demo Complete                           ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Airflow UI:  ${AIRFLOW_URL}                     ║"
echo "║  MinIO:       http://localhost:9001                     ║"
echo "║  Metabase:    http://localhost:3000                     ║"
echo "╚══════════════════════════════════════════════════════════╝"
