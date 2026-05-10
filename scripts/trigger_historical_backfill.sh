#!/usr/bin/env bash
# Trigger the full historical backfill in 25-day chunks.
# Each chunk uses ~350 ENTSO-E API calls (25 days × 14 tasks).
# The 400-call/day limit means one chunk per day is the maximum safe rate.
#
# Usage:
#   ./trigger_historical_backfill.sh                   # interactive: one chunk, then pause
#   ./trigger_historical_backfill.sh --auto            # fire all chunks back-to-back (use with cron)
#   ./trigger_historical_backfill.sh --from 2024-03-01 # resume from a given date
#
# Cron (one chunk per day at 04:00, after the nightly backup at 03:00):
#   0 4 * * * /home/john/BigDataSmallPrice/scripts/trigger_historical_backfill.sh --auto >> /home/john/BigDataSmallPrice/backups/db/backfill.log 2>&1

set -euo pipefail

API="http://localhost:8001"
CHUNK_DAYS=730   # fetch_tasks now batches 30 days per API call → 14×25=350 calls for full 2-year range
LOG_DIR="$(cd "$(dirname "$0")/.." && pwd)/backups/db"
LOG="$LOG_DIR/backfill_history.log"
STATE_FILE="$LOG_DIR/backfill_state.txt"
POLL_INTERVAL=60
MAX_WAIT=14400  # 4 hours max per chunk (730 days × 14 tasks × 2s sleep = ~6h worst case)

mkdir -p "$LOG_DIR"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$LOG"; }

# Determine start date (resume from state file, or use --from, or default)
START_DEFAULT="2024-01-01"
END_TARGET="2026-04-02"   # earliest data currently in DB
AUTO=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --auto)   AUTO=true; shift ;;
        --from)   START_DEFAULT="$2"; shift 2 ;;
        *)        echo "Unknown arg: $1"; exit 1 ;;
    esac
done

if [[ -f "$STATE_FILE" ]]; then
    CHUNK_START=$(cat "$STATE_FILE")
    log "Resuming from state file: $CHUNK_START"
else
    CHUNK_START="$START_DEFAULT"
    log "Starting fresh from $CHUNK_START"
fi

# Advance one chunk
CHUNK_END=$(date -d "$CHUNK_START + ${CHUNK_DAYS} days" +%Y-%m-%d)
if [[ "$CHUNK_END" > "$END_TARGET" ]]; then
    CHUNK_END="$END_TARGET"
fi

if [[ "$CHUNK_START" > "$END_TARGET" || "$CHUNK_START" == "$END_TARGET" ]]; then
    log "Backfill complete — all data up to $END_TARGET is in the DB."
    rm -f "$STATE_FILE"
    exit 0
fi

log "Triggering backfill: $CHUNK_START → $CHUNK_END"
RESP=$(curl -sf -X POST "$API/api/backfill/trigger" \
    -H "Content-Type: application/json" \
    -d "{\"start_date\": \"$CHUNK_START\", \"end_date\": \"$CHUNK_END\"}")

RUN_ID=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['dag_run_id'])")
log "DAG run: $RUN_ID"

# Poll until done
WAITED=0
while true; do
    sleep "$POLL_INTERVAL"
    WAITED=$((WAITED + POLL_INTERVAL))
    STATUS=$(curl -sf "$API/api/backfill/status/$RUN_ID" | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])")
    log "  state=$STATUS (waited ${WAITED}s)"
    if [[ "$STATUS" == "success" ]]; then
        log "Chunk complete: $CHUNK_START → $CHUNK_END"
        # Advance state to next chunk
        echo "$CHUNK_END" > "$STATE_FILE"
        break
    elif [[ "$STATUS" == "failed" ]]; then
        log "ERROR: DAG run failed. Fix errors in Airflow UI, then re-run this script."
        exit 1
    fi
    if [[ $WAITED -ge $MAX_WAIT ]]; then
        log "ERROR: Timed out after ${MAX_WAIT}s waiting for $RUN_ID"
        exit 1
    fi
done

NEXT_START="$CHUNK_END"
if [[ "$NEXT_START" > "$END_TARGET" || "$NEXT_START" == "$END_TARGET" ]]; then
    log "All historical data recovered through $END_TARGET."
    rm -f "$STATE_FILE"
else
    REMAINING=$(( ($(date -d "$END_TARGET" +%s) - $(date -d "$NEXT_START" +%s)) / 86400 ))
    CHUNKS_LEFT=$(( (REMAINING + CHUNK_DAYS - 1) / CHUNK_DAYS ))
    log "Next chunk starts: $NEXT_START — ~$CHUNKS_LEFT chunks remaining (~$CHUNKS_LEFT days at one chunk/day)."
    if $AUTO; then
        log "Running next chunk immediately (--auto mode)."
        echo "$NEXT_START" > "$STATE_FILE"
        exec "$0" --auto
    else
        log "Run this script again tomorrow (or add to cron with --auto) to continue."
    fi
fi
