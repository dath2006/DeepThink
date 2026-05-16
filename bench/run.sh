#!/usr/bin/env bash
# bench/run.sh
#
# Canonical benchmark runner for Anvil P-02 — Persistent Context Engine.
#
# Usage:
#   ./bench/run.sh                        # full run, report to stdout
#   ./bench/run.sh --out report.json      # write JSON report to file
#   ./bench/run.sh --mode deep            # deep reconstruction mode
#   ./bench/run.sh --quick                # fast iteration (2 seeds, small dataset)
#
# The script must be run from the repository root, or any directory,
# as long as REPO_ROOT is resolvable.
#
# Requirements: Python 3.11+, pip (all Python deps are installed automatically).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="$REPO_ROOT/Anvil-P-E/bench-p02-context"
ENGINE_DEPS="$REPO_ROOT/persistent_context_engine/requirements.txt"
ADAPTER="adapters.myteam:Engine"

# L3 canonical seeds — do NOT change without a council release
L3_SEEDS="314159 271828 161803 141421 173205"

# ---------- argument parsing ----------
MODE="fast"
OUT="-"
QUICK=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)    MODE="$2";  shift 2 ;;
        --out)     OUT="$2";   shift 2 ;;
        --quick)   QUICK="1";  shift ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

# ---------- dependency check / install ----------
echo "[bench/run.sh] Installing engine dependencies from $ENGINE_DEPS ..."
pip install --quiet -r "$ENGINE_DEPS"

# ---------- run ----------
cd "$BENCH_DIR"

if [[ -n "$QUICK" ]]; then
    # Quick: single seed, L3 stretch config (30 svc, 21 days) — ~30s
    echo "[bench/run.sh] Running quick L3 check (1 seed, 30 services, 21 days) ..."
    if [[ "$OUT" == "-" ]]; then
        python run.py \
            --adapter "$ADAPTER" \
            --mode "$MODE" \
            --seeds 314159
    else
        python run.py \
            --adapter "$ADAPTER" \
            --mode "$MODE" \
            --seeds 314159 \
            --out "$OUT"
        echo "[bench/run.sh] Report written to $OUT"
    fi
else
    echo "[bench/run.sh] Running canonical L3 benchmark (5 seeds, 30 services, 21 days) ..."
    if [[ "$OUT" == "-" ]]; then
        python run.py \
            --adapter "$ADAPTER" \
            --mode "$MODE" \
            --seeds $L3_SEEDS
    else
        python run.py \
            --adapter "$ADAPTER" \
            --mode "$MODE" \
            --seeds $L3_SEEDS \
            --out "$OUT"
        echo "[bench/run.sh] Report written to $OUT"
    fi
fi
