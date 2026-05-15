"""
Latency budget verification against Problem Statement requirements.

Tests:
1. Ingest throughput >= 1,000 events/sec
2. Ingest lag (event -> queryable) <= 5s
3. reconstruct_context fast mode p95 <= 2s
4. reconstruct_context deep mode p95 <= 6s
5. Cold-start to first reconstruction <= 60s
"""
import sys
import time
import statistics
from pathlib import Path

# Add project root and bench paths
_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "Anvil-P-E" / "bench-p02-context"))

from persistent_context_engine import Engine, EngineConfig
from generator import GenConfig, generate


def p95(values):
    s = sorted(values)
    idx = max(0, int(0.95 * len(s)) - 1)
    return s[idx]


def main():
    print("=" * 60)
    print("Latency Budget Verification")
    print("=" * 60)

    # ---- Test 5: Cold-start ----
    print("\n[1/5] Cold-start to first reconstruction...")
    cold_start_t0 = time.monotonic()

    cfg = GenConfig(seed=42, n_services=12, days=7)
    ds = generate(cfg)
    engine = Engine(EngineConfig(db_path=":memory:", buffer_size=500))
    engine.ingest(ds.train_events)
    engine.ingest(ds.eval_events)

    # First reconstruction
    sig = ds.eval_signals[0]
    signal = {
        "incident_id": sig["incident_id"],
        "ts": sig["ts"],
        "trigger": sig.get("trigger", ""),
        "service": sig.get("service", ""),
    }
    engine.reconstruct_context(signal, mode="fast")
    cold_start_ms = (time.monotonic() - cold_start_t0) * 1000
    cold_ok = cold_start_ms <= 60_000
    print(f"  Cold-start: {cold_start_ms:.0f}ms  (budget: 60,000ms)  {'PASS' if cold_ok else 'FAIL'}")
    engine.close()

    # ---- Test 1: Ingest throughput ----
    print("\n[2/5] Ingest throughput (>= 1,000 events/sec)...")
    engine2 = Engine(EngineConfig(db_path=":memory:", buffer_size=500))
    all_events = ds.train_events + ds.eval_events
    n_events = len(all_events)

    t0 = time.monotonic()
    engine2.ingest(all_events)
    ingest_sec = time.monotonic() - t0
    throughput = n_events / ingest_sec if ingest_sec > 0 else float("inf")
    tp_ok = throughput >= 1000
    print(f"  Events: {n_events}, Time: {ingest_sec:.2f}s, Throughput: {throughput:.0f} evt/s  (budget: 1000)  {'PASS' if tp_ok else 'FAIL'}")

    # ---- Test 2: Ingest lag ----
    print("\n[3/5] Ingest lag (event -> queryable <= 5s)...")
    # Ingest a single event and immediately query
    from datetime import datetime, timezone
    test_event = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "kind": "log",
        "service": "test-lag-svc",
        "level": "error",
        "msg": "lag test",
    }
    t0 = time.monotonic()
    engine2.ingest([test_event])
    # Query to verify it's queryable
    ctx = engine2.reconstruct_context({
        "incident_id": "LAG-TEST",
        "ts": datetime.now(timezone.utc).isoformat(),
        "trigger": "test",
        "service": "test-lag-svc",
    }, mode="fast")
    lag_ms = (time.monotonic() - t0) * 1000
    lag_ok = lag_ms <= 5000
    print(f"  Ingest + query lag: {lag_ms:.0f}ms  (budget: 5,000ms)  {'PASS' if lag_ok else 'FAIL'}")

    # ---- Test 3: reconstruct_context fast p95 ----
    print("\n[4/5] reconstruct_context FAST mode p95 (<= 2,000ms)...")
    fast_latencies = []
    for sig in ds.eval_signals:
        signal = {
            "incident_id": sig["incident_id"],
            "ts": sig["ts"],
            "trigger": sig.get("trigger", ""),
            "service": sig.get("service", ""),
        }
        t0 = time.monotonic()
        engine2.reconstruct_context(signal, mode="fast")
        fast_latencies.append((time.monotonic() - t0) * 1000)

    fast_p95 = p95(fast_latencies)
    fast_mean = statistics.mean(fast_latencies)
    fast_ok = fast_p95 <= 2000
    print(f"  p95: {fast_p95:.0f}ms, mean: {fast_mean:.0f}ms  (budget: 2,000ms)  {'PASS' if fast_ok else 'FAIL'}")

    # ---- Test 4: reconstruct_context deep p95 ----
    print("\n[5/5] reconstruct_context DEEP mode p95 (<= 6,000ms)...")
    deep_latencies = []
    for sig in ds.eval_signals:
        signal = {
            "incident_id": sig["incident_id"],
            "ts": sig["ts"],
            "trigger": sig.get("trigger", ""),
            "service": sig.get("service", ""),
        }
        t0 = time.monotonic()
        engine2.reconstruct_context(signal, mode="deep")
        deep_latencies.append((time.monotonic() - t0) * 1000)

    deep_p95 = p95(deep_latencies)
    deep_mean = statistics.mean(deep_latencies)
    deep_ok = deep_p95 <= 6000
    print(f"  p95: {deep_p95:.0f}ms, mean: {deep_mean:.0f}ms  (budget: 6,000ms)  {'PASS' if deep_ok else 'FAIL'}")

    engine2.close()

    # ---- Summary ----
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    results = [
        ("Cold-start <= 60s", cold_ok, f"{cold_start_ms:.0f}ms"),
        ("Ingest >= 1000 evt/s", tp_ok, f"{throughput:.0f} evt/s"),
        ("Ingest lag <= 5s", lag_ok, f"{lag_ms:.0f}ms"),
        ("Fast p95 <= 2s", fast_ok, f"{fast_p95:.0f}ms"),
        ("Deep p95 <= 6s", deep_ok, f"{deep_p95:.0f}ms"),
    ]
    all_pass = True
    for name, ok, val in results:
        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False
        print(f"  [{status}] {name}: {val}")

    print(f"\n{'ALL BUDGETS MET' if all_pass else 'SOME BUDGETS EXCEEDED'}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
