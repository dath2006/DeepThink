# Dockerfile — Persistent Context Engine (Anvil P-02)
#
# Produces a fully reproducible environment for benchmark evaluation.
# Judges can build and run this on any machine with Docker installed.
#
# Build:
#   docker build -t pce-p02 .
#
# Run canonical benchmark (JSON report to stdout):
#   docker run --rm pce-p02
#
# Run and save report to host:
#   docker run --rm -v "$(pwd)/out:/out" pce-p02 --out /out/report.json
#
# Quick iteration:
#   docker run --rm pce-p02 --quick

FROM python:3.11-slim

LABEL description="Anvil P-02 · Persistent Context Engine benchmark"

WORKDIR /app

# Copy engine package and its dependencies
COPY persistent_context_engine/ ./persistent_context_engine/
COPY persistent_context_engine/requirements.txt ./requirements.txt

# Copy benchmark harness
COPY Anvil-P-E/bench-p02-context/ ./Anvil-P-E/bench-p02-context/

# Copy runner script
COPY bench/run.sh ./bench/run.sh

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Make runner executable
RUN chmod +x ./bench/run.sh

# bench/run.sh resolves REPO_ROOT relative to script location — works from /app
ENTRYPOINT ["./bench/run.sh"]
CMD []
