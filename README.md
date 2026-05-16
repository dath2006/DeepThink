# Persistent Context Engine — Anvil P-02

A persistent, graph-aware context engine for AI SRE. Ingests telemetry streams, reconstructs incident context across topology mutations (service renames, dependency shifts), identifies similar past incidents by behavioral fingerprint, and suggests remediations.

## Quickstart

```bash
git clone https://github.com/dath2006/DeepThink.git
cd DeepThink
pip install -r persistent_context_engine/requirements.txt

# Quick run — single seed (~30 s)
./bench/run.sh --quick

# Full L3 canonical run (5 seeds) — report to stdout
./bench/run.sh

# Save report to file
./bench/run.sh --out report.json
```

## Benchmark Runner

`bench/run.sh` runs the full **L3 Final** benchmark — the canonical evaluation across 5 seeds with the stretch generator config:

- **30 services** · **21 simulated days** · **80 topology mutations**
- **Cascading renames** (services renamed 2-4× across the timeline)
- **60 train + 25 eval incidents** across **8 incident families**
- **20% decoy rate** — eval signals with no matching family

```bash
# Quick iteration (single seed, L3 config, ~30 s)
./bench/run.sh --quick

# Full canonical run (5 seeds, 30 services, 21 days) — report to stdout
./bench/run.sh

# Full run — save JSON report to file
./bench/run.sh --out report.json

# Deep reconstruction mode
./bench/run.sh --mode deep --out report_deep.json
```

Or run the benchmark directly:

```bash
cd Anvil-P-E/bench-p02-context
python run.py --adapter adapters.myteam:Engine --mode fast --out l3_report.json
```

The JSON report schema matches `harness.py` output exactly:

```json
{
  "mode": "fast",
  "seeds": [314159, 271828, 161803, 141421, 173205],
  "per_seed": [ ... ],
  "aggregated": {
    "recall@5": 0.48,
    "precision@5_mean": 0.122,
    "remediation_acc": 0.32,
    "latency_p95_ms": 94.0,
    "latency_mean_ms": 51.0
  },
  "score": {
    "weighted_score": 0.376,
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

Anvil-P-E/bench-p02-context/ # L3 benchmark harness (read-only — do not modify)
  schema.py                  # Event, IncidentSignal, Context TypedDicts
  adapter.py                 # Abstract Adapter base class
  generator.py               # Deterministic synthetic telemetry generator (L3 stretch)
  harness.py                 # Multi-seed ingest + query loop + scoring
  run.py                     # Full L3 CLI entry point
  adapters/myteam.py         # Our adapter (thin wrapper around Engine)

bench/run.sh                 # Canonical benchmark runner script
Dockerfile                   # Reproducible judge environment
test_latency_budget.py       # Local latency budget verification
```

## Requirements

- Python 3.11+
- `duckdb>=0.10.0,<2.0.0`
- `networkx>=3.0,<4.0`
- `numpy>=1.24.0`

No external services. No network access. Fully in-process.

## Benchmark Scores — L3 Final (5 seeds, 30 svc, 21 days, cascading renames, 20% decoy)

| Metric                 | Value            |
| ---------------------- | ---------------- |
| `recall@5`             | 0.480            |
| `precision@5_mean`     | 0.122            |
| `remediation_acc`      | 0.320            |
| `latency_p95_ms`       | 94 ms            |
| **Weighted automated** | **0.376 / 0.80** |

Max automated score is 0.80 — the remaining 0.20 is panel-graded (`manual_context`, `manual_explain`).

### Latency budget (all passing)

| Test                         | Result      | Budget    |
| ---------------------------- | ----------- | --------- |
| Cold-start                   | ~2,625 ms   | 60,000 ms |
| Ingest throughput            | ~11,707 e/s | 1,000 e/s |
| Ingest lag (event→queryable) | 63 ms       | 5,000 ms  |
| Fast p95 reconstruct         | 47 ms       | 2,000 ms  |
| Deep p95 reconstruct         | 62 ms       | 6,000 ms  |
