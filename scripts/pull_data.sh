#!/usr/bin/env bash
# Pull yesterday's parquet data from remote server to local, then clean from server.
#
# Usage:
#   bash scripts/pull_data.sh              # pull yesterday
#   bash scripts/pull_data.sh 2026-05-10   # pull specific date
#
# Prerequisites:
#   SSH key configured: ssh -i ~/.ssh/id_rsa root@217.76.63.39

set -euo pipefail

REMOTE_HOST="root@217.76.63.39"
REMOTE_DIR="/opt/Crypto_FreeAPIs/data"
LOCAL_DIR="./data"
SSH_KEY="$HOME/.ssh/id_rsa"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"

# Resolve target date
if [[ $# -ge 1 ]]; then
    TARGET_DATE="$1"
else
    TARGET_DATE=$(date -d "yesterday" +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d)
fi

echo "========================================="
echo "  Pull data for: ${TARGET_DATE}"
echo "  Remote: ${REMOTE_HOST}:${REMOTE_DIR}"
echo "  Local:  ${LOCAL_DIR}"
echo "========================================="

PATTERN="*_${TARGET_DATE}.parquet"

# Step 1: Find remote files
echo ""
echo "[1/3] Scanning remote files..."

FILE_LIST=$(ssh $SSH_OPTS "$REMOTE_HOST" \
    "find ${REMOTE_DIR} -name '${PATTERN}' -type f" 2>/dev/null || true)

FOUND=$(echo "$FILE_LIST" | grep -c '.' || true)

if [[ "$FOUND" -eq 0 ]]; then
    echo "No files found for ${TARGET_DATE}. Nothing to do."
    exit 0
fi

echo "Found ${FOUND} files."

# Step 2: Download each file via scp
echo ""
echo "[2/3] Downloading files..."
mkdir -p "$LOCAL_DIR"

PULLED=0
while IFS= read -r remote_path; do
    [[ -z "$remote_path" ]] && continue
    rel_path="${remote_path#${REMOTE_DIR}/}"
    local_path="${LOCAL_DIR}/${rel_path}"

    mkdir -p "$(dirname "$local_path")"

    if scp $SSH_OPTS "${REMOTE_HOST}:${remote_path}" "$local_path" 2>/dev/null; then
        echo "  OK: ${rel_path}"
        PULLED=$((PULLED + 1))
    else
        echo "  FAIL: ${rel_path}"
    fi
done <<< "$FILE_LIST"

echo "Downloaded: ${PULLED}/${FOUND} files"

if [[ $PULLED -eq 0 ]]; then
    echo "No files downloaded. Skipping cleanup."
    exit 1
fi

# Step 3: Verify local files, then clean remote in batch
echo ""
echo "[3/3] Cleaning remote files..."
CLEANED=0
RM_LIST=""

while IFS= read -r remote_path; do
    [[ -z "$remote_path" ]] && continue
    rel_path="${remote_path#${REMOTE_DIR}/}"
    local_path="${LOCAL_DIR}/${rel_path}"

    # Safety: only delete if local copy exists and is non-empty
    if [[ -f "$local_path" && -s "$local_path" ]]; then
        RM_LIST="${RM_LIST} ${remote_path}"
        echo "  DEL: ${rel_path}"
        CLEANED=$((CLEANED + 1))
    else
        echo "  SKIP: ${rel_path} (local missing/empty)"
    fi
done <<< "$FILE_LIST"

# Batch delete + clean empty dirs in one SSH call
if [[ $CLEANED -gt 0 ]]; then
    ssh $SSH_OPTS "$REMOTE_HOST" "rm -f ${RM_LIST} && find ${REMOTE_DIR} -type d -empty -delete" 2>/dev/null
fi

echo ""
echo "========================================="
echo "  Done!"
echo "  Date:     ${TARGET_DATE}"
echo "  Pulled:   ${PULLED} files"
echo "  Cleaned:  ${CLEANED} files"
echo "========================================="
