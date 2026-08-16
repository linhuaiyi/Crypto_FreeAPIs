#!/usr/bin/env bash
# Redeploy the live collector on 217.76.63.39 with clean data.
#
# Standard procedure (per ~/.claude/.../feedback_deploy_cleanup.md):
#   1. SIGINT the live collector (flush_all + save IVRank state)
#   2. Wipe affected stream parquet files + _validation_sample/
#   3. rsync local code -> remote
#   4. Restart in tmux session `crypto`
#   5. Wait for ChunkedBuffer flush (5 min default for low-volume streams)
#   6. Verify via verify_post_restart.py
#
# Usage:
#   bash scripts/redeploy_collector.sh                    # full redeploy, clean ALL streams
#   bash scripts/redeploy_collector.sh --streams binance/funding_rate,binance/ohlcv
#                                                          # clean only listed streams
#   bash scripts/redeploy_collector.sh --skip-clean       # keep existing data (not recommended)
#   bash scripts/redeploy_collector.sh --skip-verify      # skip post-restart verification
#   bash scripts/redeploy_collector.sh --dry-run          # print actions, execute nothing
#
# Prerequisites:
#   - SSH key at ~/.ssh/id_rsa
#   - Local venv activated if editing code before deploy
#   - Remote dir: /opt/Crypto_FreeAPIs (deribit-options-data-collector/ subdir)

set -euo pipefail

REMOTE_HOST="root@217.76.63.39"
REMOTE_ROOT="/opt/Crypto_FreeAPIs"
REMOTE_PROJECT="${REMOTE_ROOT}/deribit-options-data-collector"
REMOTE_DATA="${REMOTE_PROJECT}/data"
REMOTE_VENV="${REMOTE_ROOT}/venv/bin/python"
SSH_KEY="$HOME/.ssh/id_rsa"
SSH_OPTS="-i ${SSH_KEY} -o StrictHostKeyChecking=no -o ServerAliveInterval=30"
TMUX_SESSION="crypto"
FLUSH_WAIT_SEC=330   # 5 min ChunkedBuffer default + 30s buffer

STREAMS=""
SKIP_CLEAN=0
SKIP_VERIFY=0
DRY_RUN=0

for arg in "$@"; do
    case "$arg" in
        --streams=*) STREAMS="${arg#--streams=}";;
        --skip-clean) SKIP_CLEAN=1;;
        --skip-verify) SKIP_VERIFY=1;;
        --dry-run) DRY_RUN=1;;
        -h|--help)
            grep -E '^#( |$)' "$0" | sed 's/^# //'
            exit 0;;
        *) echo "Unknown arg: $arg" >&2; exit 2;;
    esac
done

run_local() { echo "+ $*"; [[ $DRY_RUN -eq 1 ]] || "$@"; }
run_remote() { echo "ssh> $*"; [[ $DRY_RUN -eq 1 ]] || ssh ${SSH_OPTS} "${REMOTE_HOST}" "$*"; }

echo "========================================="
echo "  Redeploy collector"
echo "  Remote:  ${REMOTE_HOST}:${REMOTE_PROJECT}"
echo "  Streams: ${STREAMS:-ALL}"
echo "  Dry-run: ${DRY_RUN}"
echo "========================================="

# --- 1. Graceful stop ---------------------------------------------------------
echo ""
echo "[1/6] Stopping collector (SIGINT)..."
PID=$(ssh ${SSH_OPTS} "${REMOTE_HOST}" "pgrep -f 'launch.py' | head -1" 2>/dev/null || true)
if [[ -z "$PID" ]]; then
    echo "  No running launch.py process."
else
    echo "  PID=${PID}; sending SIGINT"
    run_remote "kill -INT ${PID}"
    # Wait up to 30s for graceful shutdown (flush_all + IVRank save)
    for i in $(seq 1 30); do
        REMAINING=$(ssh ${SSH_OPTS} "${REMOTE_HOST}" "pgrep -f 'launch.py' | head -1" 2>/dev/null || true)
        [[ -z "$REMAINING" ]] && break
        sleep 1
    done
    REMAINING=$(ssh ${SSH_OPTS} "${REMOTE_HOST}" "pgrep -f 'launch.py' | head -1" 2>/dev/null || true)
    if [[ -n "$REMAINING" ]]; then
        echo "  Still alive after 30s; sending SIGTERM"
        run_remote "kill -TERM ${REMAINING}"
        sleep 5
    fi
fi

# --- 2. Clean data -----------------------------------------------------------
echo ""
echo "[2/6] Cleaning pre-existing data..."
if [[ $SKIP_CLEAN -eq 1 ]]; then
    echo "  Skipped (--skip-clean)."
elif [[ -z "$STREAMS" ]]; then
    echo "  Removing ALL parquet under ${REMOTE_DATA}/*/*/"
    run_remote "find ${REMOTE_DATA} -name '*.parquet' -delete"
else
    IFS=',' read -ra STREAM_LIST <<< "$STREAMS"
    for s in "${STREAM_LIST[@]}"; do
        # stream format: {exchange}/{stream}, e.g. binance/funding_rate
        echo "  Removing ${REMOTE_DATA}/${s}/*.parquet"
        run_remote "find ${REMOTE_DATA}/${s} -name '*.parquet' -delete 2>/dev/null || true"
    done
fi
echo "  Removing ${REMOTE_DATA}/_validation_sample/"
run_remote "rm -rf ${REMOTE_DATA}/_validation_sample"

# --- 3. Sync code ------------------------------------------------------------
echo ""
echo "[3/6] Syncing code to remote..."
# Use --filter to exclude local data/, logs/, __pycache__, .venv
run_local rsync -avz --delete \
    -e "ssh ${SSH_OPTS}" \
    --exclude='.venv/' \
    --exclude='__pycache__/' \
    --exclude='*.pyc' \
    --exclude='deribit-options-data-collector/data/' \
    --exclude='deribit-options-data-collector/logs/' \
    --exclude='deribit-options-data-collector/*.zip' \
    --exclude='.git/' \
    ./ "${REMOTE_HOST}:${REMOTE_ROOT}/"

# --- 4. Restart in tmux ------------------------------------------------------
echo ""
echo "[4/6] Restarting in tmux session '${TMUX_SESSION}'..."
# Ensure session exists (single-window session dies when process exits)
run_remote "tmux has-session -t ${TMUX_SESSION} 2>/dev/null || tmux new-session -d -s ${TMUX_SESSION}"
# Launch collector
run_remote "tmux send-keys -t ${TMUX_SESSION} 'cd ${REMOTE_PROJECT} && ${REMOTE_VENV} launch.py --mode live 2>&1 | tee -a logs/collector.console.log' C-m"

# --- 5. Wait for first flush -------------------------------------------------
echo ""
echo "[5/6] Waiting ${FLUSH_WAIT_SEC}s for first ChunkedBuffer flush..."
if [[ $DRY_RUN -eq 0 ]]; then
    sleep ${FLUSH_WAIT_SEC}
else
    echo "  (dry-run: skipping sleep)"
fi

# --- 6. Verify ---------------------------------------------------------------
echo ""
echo "[6/6] Verifying..."
if [[ $SKIP_VERIFY -eq 1 ]]; then
    echo "  Skipped (--skip-verify)."
elif [[ $DRY_RUN -eq 0 ]]; then
    ssh ${SSH_OPTS} "${REMOTE_HOST}" "${REMOTE_VENV} ${REMOTE_PROJECT}/../scripts/verify_post_restart.py --root ${REMOTE_DATA} 2>&1 | tail -40" || true
fi

echo ""
echo "========================================="
echo "  Done."
echo "  Tail logs: ssh ${SSH_OPTS} ${REMOTE_HOST} \"tail -f ${REMOTE_PROJECT}/logs/collector.log\""
echo "========================================="
