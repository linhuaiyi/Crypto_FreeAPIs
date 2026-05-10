#!/bin/bash
# prune_cloud_data.sh — Cloud data lifecycle management
#
# Runs on the cloud collection node (4vCPU/8GB/75GB NVMe).
# Syncs data to local archive via rclone, then deletes files older than KEEP_DAYS.
#
# Crontab: 0 3 * * * /opt/crypto-data/scripts/prune_cloud_data.sh
#
# Exit codes:
#   0 — success (sync + prune completed)
#   1 — rclone sync failed (prune NOT executed, data preserved)
#   2 — prune failed (partial cleanup)

set -euo pipefail

KEEP_DAYS=14
DATA_DIR="/opt/crypto-data"
LOG_FILE="/var/log/data-prune.log"
ARCHIVE_REMOTE="local-archive"
ARCHIVE_PATH="/archive/crypto-data/"

timestamp() { date '+%Y-%m-%d %H:%M:%S'; }

log() { echo "[$(timestamp)] $*" | tee -a "$LOG_FILE"; }

# ── Step 1: Disk usage check ──
log "=== Prune started ==="
log "Disk usage before: $(df -h "$DATA_DIR" | awk 'NR==2{print $5}')"

# ── Step 2: Sync to local archive via rclone ──
log "Syncing to local archive..."
if rclone sync "$DATA_DIR" "$ARCHIVE_REMOTE:$ARCHIVE_PATH" \
    --transfers 4 \
    --checkers 8 \
    --contimeout 60s \
    --timeout 300s \
    --retries 3 \
    --log-level INFO \
    --log-file "$LOG_FILE"; then
    log "rclone sync completed successfully"
else
    log "ERROR: rclone sync failed — aborting prune to preserve data"
    exit 1
fi

# ── Step 3: Delete Parquet files older than KEEP_DAYS ──
log "Pruning files older than ${KEEP_DAYS} days..."
DELETED=$(find "$DATA_DIR" -name "*.parquet" -mtime +"$KEEP_DAYS" -print -delete | wc -l)
log "Deleted ${DELETED} parquet files"

# ── Step 4: Remove empty date= directories ──
find "$DATA_DIR" -type d -name "date=*" -empty -delete 2>/dev/null || true
find "$DATA_DIR" -type d -empty -not -path "$DATA_DIR" -delete 2>/dev/null || true

# ── Step 5: Report ──
log "Disk usage after: $(df -h "$DATA_DIR" | awk 'NR==2{print $5}')"
log "=== Prune completed ==="
