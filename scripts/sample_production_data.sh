#!/usr/bin/env bash
# Copy current production parquet files to data/_validation_sample/ for review.
#
# This is the ONLY correct way to generate a "sample" for the user.
# Per ~/.claude/.../feedback_sample_generation.md:
#   - NEVER call historical fetch endpoints
#   - NEVER backfill a time range
#   - Just copy the current production files (they reflect new code output)
#
# Usage:
#   bash scripts/sample_production_data.sh                                # all streams, latest file per stream
#   bash scripts/sample_production_data.sh binance/funding_rate            # one stream
#   bash scripts/sample_production_data.sh binance/funding_rate fred/risk_free_rate
#   bash scripts/sample_production_data.sh --remote                        # sample from remote server
#   bash scripts/sample_production_data.sh --remote --streams binance/funding_rate
#
# Output goes to: data/_validation_sample/{exchange}/{stream}/<latest-file>.parquet

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL_DATA="${PROJECT_ROOT}/deribit-options-data-collector/data"
SAMPLE_DIR="${LOCAL_DATA}/_validation_sample"

REMOTE_HOST="root@217.76.63.39"
REMOTE_DATA="/opt/Crypto_FreeAPIs/deribit-options-data-collector/data"
SSH_KEY="$HOME/.ssh/id_rsa"
SSH_OPTS="-i ${SSH_KEY} -o StrictHostKeyChecking=no"

SOURCE=local
STREAMS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --remote) SOURCE=remote; shift;;
        --streams) shift; while [[ $# -gt 0 && "$1" != --* ]]; do STREAMS+=("$1"); shift; done;;
        -h|--help)
            grep -E '^#( |$)' "$0" | sed 's/^# //'
            exit 0;;
        *) STREAMS+=("$1"); shift;;
    esac
done

collect_latest_local() {
    local stream_path="$1"   # e.g. data/binance/funding_rate
    local pattern="*.parquet"
    # Pick the newest file by mtime
    local f
    f=$(ls -t "${stream_path}"/${pattern} 2>/dev/null | head -1 || true)
    echo "$f"
}

collect_latest_remote() {
    local stream_rel="$1"    # e.g. binance/funding_rate
    ssh ${SSH_OPTS} "${REMOTE_HOST}" \
        "latest=\$(ls -t ${REMOTE_DATA}/${stream_rel}/*.parquet 2>/dev/null | head -1); echo \"\$latest\""
}

sample_one() {
    local stream_rel="$1"   # {exchange}/{stream}
    local out_dir="${SAMPLE_DIR}/${stream_rel}"
    mkdir -p "$out_dir"

    local src=""
    if [[ "$SOURCE" == "remote" ]]; then
        src=$(collect_latest_remote "$stream_rel")
        if [[ -z "$src" ]]; then
            echo "  [skip] ${stream_rel}: no remote parquet yet"
            return
        fi
        echo "  [remote] ${stream_rel} <- $(basename "$src")"
        scp ${SSH_OPTS} "${REMOTE_HOST}:${src}" "${out_dir}/$(basename "$src")"
    else
        src=$(collect_latest_local "${LOCAL_DATA}/${stream_rel}")
        if [[ -z "$src" ]]; then
            echo "  [skip] ${stream_rel}: no local parquet yet"
            return
        fi
        echo "  [local]  ${stream_rel} <- $(basename "$src")"
        cp "$src" "${out_dir}/$(basename "$src")"
    fi
}

echo "========================================="
echo "  Sample source: ${SOURCE}"
echo "  Streams: ${STREAMS[*]:-ALL}"
echo "  Output:  ${SAMPLE_DIR}"
echo "========================================="

# Default stream list when none specified
if [[ ${#STREAMS[@]} -eq 0 ]]; then
    STREAMS=(
        binance/funding_rate
        binance/ohlcv
        fred/risk_free_rate
        deribit/options_greeks
        deribit/option_chain
        deribit/index_price
        deribit/dvol
        hyperliquid/funding_rate
    )
fi

for s in "${STREAMS[@]}"; do
    sample_one "$s"
done

echo ""
echo "Done. Sample files at:"
echo "  ${SAMPLE_DIR}/"
echo ""
echo "Inspect with:"
echo "  python -c \"import pandas as pd, glob; \\
f=sorted(glob.glob('${SAMPLE_DIR}/**/*.parquet', recursive=True)); \\
import pprint; pprint.pprint({p: pd.read_parquet(p).head().to_dict('records') for p in f})\""
