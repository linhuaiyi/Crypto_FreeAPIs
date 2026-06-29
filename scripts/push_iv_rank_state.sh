#!/usr/bin/env bash
# Push local IV rank state JSON files to the remote collector server.
#
# The state file is loaded by IVRankTracker on launch.py startup, giving
# the server a real baseline even when its data/ dir has been cleared by
# previous pulls. The server then continues appending new days on its own.
#
# This OVERWRITES the remote state file. Only run when seeding a fresh
# server or recovering from state loss — routine pulls do not need this.
#
# Usage:
#   bash scripts/push_iv_rank_state.sh

set -euo pipefail

SSH_KEY="${HOME}/.ssh/id_rsa"
HOST="root@217.76.63.39"
REMOTE_BASE="/opt/Crypto_FreeAPIs/deribit-options-data-collector"
REMOTE_STATE_DIR="$REMOTE_BASE/state"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOCAL_STATE_DIR="$PROJECT_ROOT/deribit-options-data-collector/state"

if [ ! -d "$LOCAL_STATE_DIR" ]; then
    echo "ERROR: local state dir not found: $LOCAL_STATE_DIR" >&2
    echo "Run scripts/export_iv_rank_state.py first." >&2
    exit 1
fi

shopt -s nullglob
files=("$LOCAL_STATE_DIR"/iv_rank_*.json)
if [ ${#files[@]} -eq 0 ]; then
    echo "ERROR: no iv_rank_*.json in $LOCAL_STATE_DIR" >&2
    echo "Run scripts/export_iv_rank_state.py first." >&2
    exit 1
fi

echo "=== Pushing IV rank state to $HOST ==="
echo "  local:  $LOCAL_STATE_DIR"
echo "  remote: $REMOTE_STATE_DIR"
echo ""

ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$HOST" "mkdir -p $REMOTE_STATE_DIR"

for f in "${files[@]}"; do
    name="$(basename "$f")"
    # Back up the existing remote state (if any) before overwriting.
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$HOST" \
        "[ -f $REMOTE_STATE_DIR/$name ] && cp $REMOTE_STATE_DIR/$name $REMOTE_STATE_DIR/$name.bak || true"
    scp -i "$SSH_KEY" -o StrictHostKeyChecking=no "$f" "$HOST:$REMOTE_STATE_DIR/$name"
    echo "  pushed $name (previous version backed up as $name.bak if it existed)"
done

echo ""
echo "=== Done ==="
echo "Remote launch.py will load the new state on next restart."
echo "To apply now (graceful SIGTERM + tmux relaunch):"
echo "  ssh -i $SSH_KEY $HOST \"pgrep -f 'launch.py' | xargs -r kill -TERM\""
echo "  # wait ~15s for flush_all + IVRank state save"
echo "  ssh -i $SSH_KEY $HOST \"tmux send-keys -t crypto 'cd /opt/Crypto_FreeAPIs && python deribit-options-data-collector/launch.py --mode live' C-m\""
