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
ADAPTER="adapters.myteam:PersistentContextAdapter"

# ---------- argument parsing ----------
MODE="fast"
OUT="-"
QUICK=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)    MODE="$2";  shift 2 ;;
        --out)     OUT="$2";   shift 2 ;;
        --quick)   QUICK="--quick"; shift ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

# ---------- dependency check / install ----------
echo "[bench/run.sh] Installing engine dependencies from $ENGINE_DEPS ..."
pip install --quiet -r "$ENGINE_DEPS"

# ---------- run ----------
cd "$BENCH_DIR"

if [[ -n "$QUICK" ]]; then
    echo "[bench/run.sh] Running quick self-check (2 seeds, 6 services) ..."
    python self_check.py \
        --adapter "$ADAPTER" \
        --mode "$MODE" \
        --quick
else
    echo "[bench/run.sh] Running canonical benchmark (5 seeds, 12 services, 7 days) ..."
    if [[ "$OUT" == "-" ]]; then
        python run.py \
            --adapter "$ADAPTER" \
            --mode "$MODE" \
            --seeds 42 101 202 303 404
    else
        python run.py \
            --adapter "$ADAPTER" \
            --mode "$MODE" \
            --seeds 42 101 202 303 404 \
            --out "$OUT"
        echo "[bench/run.sh] Report written to $OUT"
    fi
fi
