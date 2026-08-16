#!/bin/bash
# V3.0 Options+Perp Data Collector — Docker/Local entrypoint
# Compatible: Linux, macOS, Windows Git Bash / MSYS2
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load .env (try script dir first, then project root)
for env_file in "$SCRIPT_DIR/.env" "$PROJECT_ROOT/.env"; do
    if [ -f "$env_file" ]; then
        set -a
        source "$env_file"
        set +a
        break
    fi
done

MODE="${MODE:-live}"
STRATEGIES="${STRATEGIES:-all}"
PYTHON="${PYTHON:-python}"
MAX_RETRIES="${MAX_RETRIES:-0}"    # 0 = unlimited in live mode
RETRY_DELAY="${RETRY_DELAY:-30}"   # seconds between restart attempts

echo "========================================"
echo " V3.0 Options+Perp Data Collector"
echo " Mode:       $MODE"
echo " Strategies: $STRATEGIES"
echo "========================================"

cd "$PROJECT_ROOT"

if [ "$MODE" = "live" ]; then
    # Live mode: auto-restart on abnormal exit
    attempt=0
    while true; do
        attempt=$((attempt + 1))
        echo "[entrypoint] Starting collector (attempt $attempt)..."

        $PYTHON "$SCRIPT_DIR/launch.py" --mode live --strategies "$STRATEGIES" "$@" \
            && exit_code=$? || exit_code=$?

        if [ "$exit_code" -eq 0 ]; then
            echo "[entrypoint] Clean shutdown (exit 0). Not restarting."
            exit 0
        fi

        echo "[entrypoint] Collector crashed (exit $exit_code)."

        if [ "$MAX_RETRIES" -gt 0 ] && [ "$attempt" -ge "$MAX_RETRIES" ]; then
            echo "[entrypoint] Max retries ($MAX_RETRIES) reached. Exiting."
            exit "$exit_code"
        fi

        echo "[entrypoint] Restarting in ${RETRY_DELAY}s..."
        sleep "$RETRY_DELAY"
    done
else
    # Non-live mode: single run, no restart
    exec $PYTHON "$SCRIPT_DIR/launch.py" --mode "$MODE" --strategies "$STRATEGIES" "$@"
fi
