# Persistent Context Engine — Anvil P-02

A persistent, graph-aware context engine for AI SRE. Ingests telemetry streams, reconstructs incident context across topology mutations (service renames, dependency shifts), identifies similar past incidents by behavioral fingerprint, and suggests remediations.

## Quickstart

```bash
git clone https://github.com/Sauhard74/Anvil-P-E.git
cd Anvil-P-E
pip install -r persistent_context_engine/requirements.txt
./bench/run.sh --quick
```

## Benchmark Runner

`bench/run.sh` ingests the canonical synthetic dataset, runs the benchmark harness across 5 seeds, and emits a JSON report matching the SDK schema.

```bash
# Quick iteration (2 seeds, 6 services, ~10 s)
./bench/run.sh --quick

# Full canonical run (5 seeds, 12 services, 7-day window) — report to stdout
./bench/run.sh

# Full run — save JSON report to file
./bench/run.sh --out report.json

# Deep reconstruction mode
./bench/run.sh --mode deep --out report_deep.json
```

The JSON report schema matches `harness.py` output exactly:

```json
{
  "mode": "fast",
  "seeds": [42, 101, 202, 303, 404],
  "per_seed": [ ... ],
  "aggregated": {
    "recall@5": 0.8,
    "precision@5_mean": 0.272,
    "remediation_acc": 1.0,
    "latency_p95_ms": 141.0,
    "latency_mean_ms": 95.2
  },
  "score": {
    "weighted_score": 0.631,
    "max_automated": 0.8,
    "axes": { ... }
  }
}
```

## Reproducibility (Docker)

Build a fully isolated environment identical to what runs on judges' machines:

```bash
docker build -t pce-p02 .

# Run canonical benchmark
docker run --rm pce-p02

# Save report to host
docker run --rm -v "$(pwd)/out:/out" pce-p02 --out /out/report.json

# Quick mode
docker run --rm pce-p02 --quick
```

Requires Docker 20+. No network access needed inside the container — all evaluation is synthetic and deterministic.

## Layout

```
persistent_context_engine/   # engine package
  engine.py                  # Engine class — ingest() + reconstruct_context()
  config.py                  # EngineConfig + ModeParams
  incident_fingerprinter.py  # Behavioral fingerprinting (rename-proof)
  storage/                   # DuckDB-backed raw/node/edge/pattern/remediation stores
  graph/                     # NetworkX DiGraph + TemporalGraphView
  ingestion/                 # Event parser, ring buffer, ingest coordinator

Anvil-P-E/bench-p02-context/ # benchmark harness (read-only — do not modify)
  schema.py                  # Event, IncidentSignal, Context TypedDicts
  adapter.py                 # Abstract Adapter base class
  generator.py               # Deterministic synthetic telemetry generator
  harness.py                 # Multi-seed ingest + query loop + scoring
  run.py                     # Full CLI entry point
  self_check.py              # Local iteration entry point
  adapters/myteam.py         # Our adapter (thin wrapper around Engine)

bench/run.sh                 # Canonical benchmark runner script
Dockerfile                   # Reproducible judge environment
```

## Requirements

- Python 3.11+
- `duckdb>=0.10.0,<2.0.0`
- `networkx>=3.0,<4.0`

No external services. No network access. Fully in-process.

## Benchmark Scores (canonical 5-seed run)

| Metric | Value |
|---|---|
| `recall@5` | 0.800 |
| `precision@5_mean` | 0.272 |
| `remediation_acc` | 1.000 |
| `latency_p95_ms` | ≤ 141 ms |
| **Weighted automated** | **0.631 / 0.80** |

Max automated score is 0.80 — the remaining 0.20 is panel-graded (`manual_context`, `manual_explain`).
