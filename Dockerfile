# Dockerfile — Persistent Context Engine (Anvil P-02 · L3 Final)
#
# Produces a fully reproducible environment for L3 benchmark evaluation.
# Judges can build and run this on any machine with Docker installed.
#
# Build:
#   docker build -t pce-p02 .
#
# Run canonical L3 benchmark (5 seeds, JSON report to stdout):
#   docker run --rm pce-p02
#
# Run and save report to host:
#   docker run --rm -v "$(pwd)/out:/out" pce-p02 --out /out/report.json
#
# Quick single-seed run:
#   docker run --rm pce-p02 --quick
#
# Deep mode:
#   docker run --rm pce-p02 --mode deep --out /out/report_deep.json

FROM python:3.11-slim

LABEL description="Anvil P-02 · Persistent Context Engine · L3 Final benchmark"

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy engine package and its requirements
COPY persistent_context_engine/ ./persistent_context_engine/
COPY persistent_context_engine/requirements.txt ./requirements.txt

# Copy L3 benchmark harness (run.py, harness.py, generator.py, schema.py, metrics.py, adapter.py)
COPY Anvil-P-E/bench-p02-context/ ./Anvil-P-E/bench-p02-context/

# Copy runner script
COPY bench/run.sh ./bench/run.sh

# Install Python dependencies (duckdb, networkx, numpy)
RUN pip install --no-cache-dir -r requirements.txt

# Make runner executable
RUN chmod +x ./bench/run.sh

# bench/run.sh resolves REPO_ROOT relative to its own location (/app/bench/run.sh → /app)
ENTRYPOINT ["./bench/run.sh"]
CMD []
