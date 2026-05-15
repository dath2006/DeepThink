"""
Performance tests for reconstruct_context latency budgets.

Budgets from spec:
- Fast mode: p95 <= 2 seconds
- Deep mode: p95 <= 6 seconds
- Cold-start to first reconstruction: <= 60 seconds
"""

import sys
import time
import statistics
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "d:/Agents/DeepThink")

from persistent_context_engine import Engine, EngineConfig


class TestPerformance(unittest.TestCase):
    """Verify latency budgets are met."""

    def setUp(self):
        """Create engine with test data."""
        self.engine = Engine(EngineConfig(db_path=":memory:"))
        self.base_ts = datetime.fromisoformat("2026-05-10T14:00:00+00:00")
        
        # Ingest some test data for context
        self._ingest_test_data()

    def tearDown(self):
        """Clean up."""
        self.engine.close()

    def _ingest_test_data(self):
        """Create test incidents for reconstruction."""
        import random
        
        services = [f"svc-{i:02d}" for i in range(10)]
        
        for i in range(20):
            svc = random.choice(services)
            ts = self.base_ts + timedelta(hours=i)
            
            # Create incident pattern: deploy -> metric -> signal
            events = [
                {
                    "ts": ts.isoformat(),
                    "kind": "deploy",
                    "service": svc,
                    "version": f"v1.{i}.0",
                },
                {
                    "ts": (ts + timedelta(minutes=2)).isoformat(),
                    "kind": "metric",
                    "service": svc,
                    "name": "latency_p99_ms",
                    "value": 5000 + random.randint(-1000, 1000),
                },
                {
                    "ts": (ts + timedelta(minutes=5)).isoformat(),
                    "kind": "incident_signal",
                    "incident_id": f"INC-PERF-{i:03d}",
                    "trigger": f"alert:{svc}/latency>4s",
                    "service": svc,
                },
                {
                    "ts": (ts + timedelta(minutes=15)).isoformat(),
                    "kind": "remediation",
                    "incident_id": f"INC-PERF-{i:03d}",
                    "action": "rollback",
                    "target": svc,
                    "outcome": "resolved",
                },
            ]
            self.engine.ingest(events)
        
        # Add a rename for rename-proof testing
        self.engine.ingest([{
            "ts": (self.base_ts + timedelta(hours=10)).isoformat(),
            "kind": "topology",
            "change": "rename",
            "from": "svc-00",
            "to": "svc-00-renamed",
        }])

    def test_fast_mode_latency(self):
        """
        Fast mode p95 must be <= 2 seconds.
        
        Run 20 reconstructions and measure wall-clock time.
        """
        signal = {
            "ts": (self.base_ts + timedelta(hours=25)).isoformat(),
            "kind": "incident_signal",
            "incident_id": "INC-PERF-TEST",
            "trigger": "alert:svc-05/latency>4s",
            "service": "svc-05",
        }
        
        # Warm-up (first call may be slower due to caching)
        self.engine.reconstruct_context(signal, mode="fast")
        
        # Timed runs
        times = []
        for _ in range(20):
            start = time.perf_counter()
            ctx = self.engine.reconstruct_context(signal, mode="fast")
            end = time.perf_counter()
            times.append(end - start)
        
        p95 = statistics.quantiles(times, n=20)[18]  # Approx p95
        mean_time = statistics.mean(times)
        max_time = max(times)
        
        print(f"\nFast mode latency results:")
        print(f"  Mean: {mean_time*1000:.1f}ms")
        print(f"  Max: {max_time*1000:.1f}ms")
        print(f"  P95 (approx): {p95*1000:.1f}ms")
        print(f"  All times: {[f'{t*1000:.1f}ms' for t in times]}")
        
        # Budget: p95 <= 2s
        self.assertLess(max_time, 2.0, 
            f"Fast mode max latency {max_time*1000:.1f}ms exceeds 2000ms budget")

    def test_deep_mode_latency(self):
        """
        Deep mode p95 must be <= 6 seconds.
        
        Run 10 reconstructions (deep mode is slower).
        """
        signal = {
            "ts": (self.base_ts + timedelta(hours=25)).isoformat(),
            "kind": "incident_signal",
            "incident_id": "INC-PERF-TEST-DEEP",
            "trigger": "alert:svc-05/latency>4s",
            "service": "svc-05",
        }
        
        # Warm-up
        self.engine.reconstruct_context(signal, mode="deep")
        
        # Timed runs
        times = []
        for _ in range(10):
            start = time.perf_counter()
            ctx = self.engine.reconstruct_context(signal, mode="deep")
            end = time.perf_counter()
            times.append(end - start)
        
        p95 = statistics.quantiles(times, n=20)[18] if len(times) >= 20 else max(times)
        mean_time = statistics.mean(times)
        max_time = max(times)
        
        print(f"\nDeep mode latency results:")
        print(f"  Mean: {mean_time*1000:.1f}ms")
        print(f"  Max: {max_time*1000:.1f}ms")
        print(f"  P95 (approx): {p95*1000:.1f}ms")
        
        # Budget: p95 <= 6s
        self.assertLess(max_time, 6.0,
            f"Deep mode max latency {max_time*1000:.1f}ms exceeds 6000ms budget")

    def test_cold_start_latency(self):
        """
        Cold-start to first reconstruction must be <= 60 seconds.
        
        This tests engine initialization + first reconstruction.
        """
        start = time.perf_counter()
        
        # Create fresh engine (cold start)
        cold_engine = Engine(EngineConfig(db_path=":memory:"))
        
        # Ingest minimal data
        cold_engine.ingest([
            {
                "ts": self.base_ts.isoformat(),
                "kind": "deploy",
                "service": "test-svc",
                "version": "v1.0.0",
            },
            {
                "ts": (self.base_ts + timedelta(minutes=5)).isoformat(),
                "kind": "incident_signal",
                "incident_id": "INC-COLD-001",
                "trigger": "alert:test-svc/latency>1s",
                "service": "test-svc",
            },
        ])
        
        # First reconstruction
        signal = {
            "ts": (self.base_ts + timedelta(minutes=5)).isoformat(),
            "kind": "incident_signal",
            "incident_id": "INC-COLD-TEST",
            "trigger": "alert:test-svc/latency>1s",
            "service": "test-svc",
        }
        ctx = cold_engine.reconstruct_context(signal, mode="fast")
        
        end = time.perf_counter()
        elapsed = end - start
        
        cold_engine.close()
        
        print(f"\nCold start latency: {elapsed*1000:.1f}ms")
        
        # Budget: <= 60s
        self.assertLess(elapsed, 60.0,
            f"Cold start latency {elapsed:.1f}s exceeds 60s budget")


if __name__ == "__main__":
    unittest.main()
