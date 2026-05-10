#!/usr/bin/env bash
# Nightly pg_dump for the BigDataSmallPrice TimescaleDB.
# Runs inside the container so no external pg_dump install is needed.
# Keeps the last 14 backups; older ones are removed automatically.

set -euo pipefail

BACKUP_DIR="$(cd "$(dirname "$0")/.." && pwd)/backups/db"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/bdsp_${TIMESTAMP}.sql.gz"
KEEP_DAYS=14
CONTAINER="bigdatasmallprice-timescaledb-1"
LOG="$BACKUP_DIR/backup.log"

mkdir -p "$BACKUP_DIR"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Starting backup → $BACKUP_FILE" | tee -a "$LOG"

docker exec "$CONTAINER" \
    pg_dump -U bdsp -d bdsp --no-password --clean --if-exists \
    | gzip > "$BACKUP_FILE"

SIZE=$(du -sh "$BACKUP_FILE" | cut -f1)
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Backup complete: $SIZE" | tee -a "$LOG"

# Remove backups older than KEEP_DAYS
find "$BACKUP_DIR" -name "bdsp_*.sql.gz" -mtime +"$KEEP_DAYS" -delete
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Retention cleanup done (>${KEEP_DAYS}d removed)" | tee -a "$LOG"
