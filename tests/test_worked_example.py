"""
End-to-end test for the worked example from the problem statement.

This test verifies that reconstruct_context correctly handles:
1. The payments-svc deploy → latency spike → error cascade
2. The rename to billing-svc
3. Topology-aware context reconstruction at incident time

The critical test: INC-714 reconstruction must find the deploy on 
payments-svc (now renamed) via temporal graph traversal.
"""

import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "d:/Agents/DeepThink")

from persistent_context_engine import Engine, EngineConfig


class TestWorkedExample(unittest.TestCase):
    """Test the canonical worked example from the spec."""

    def setUp(self):
        self.engine = Engine(EngineConfig(db_path=":memory:"))
        # Base timestamp from spec example
        self.base_ts = datetime.fromisoformat("2026-05-10T14:00:00+00:00")

    def tearDown(self):
        self.engine.close()

    def test_worked_example_full_sequence(self):
        """
        Ingest the complete worked example from the spec.
        
        Event sequence:
        14:21:30 - deploy payments-svc v2.14.0
        14:22:01 - log checkout-api error (timeout calling payments-svc)
        14:22:01 - metric payments-svc latency_p99_ms=4820
        14:22:08 - trace abc123 (checkout-api → payments-svc)
        14:30:00 - topology rename payments-svc → billing-svc
        14:32:11 - incident_signal INC-714 (checkout-api error-rate>5%)
        15:10:00 - remediation rollback billing-svc to v2.13.4, resolved
        """
        events = [
            {
                "ts": "2026-05-10T14:21:30Z",
                "kind": "deploy",
                "service": "payments-svc",
                "version": "v2.14.0",
                "actor": "ci",
            },
            {
                "ts": "2026-05-10T14:22:01Z",
                "kind": "log",
                "service": "checkout-api",
                "level": "error",
                "msg": "timeout calling payments-svc",
                "trace_id": "abc123",
            },
            {
                "ts": "2026-05-10T14:22:01Z",
                "kind": "metric",
                "service": "payments-svc",
                "name": "latency_p99_ms",
                "value": 4820,
            },
            {
                "ts": "2026-05-10T14:22:08Z",
                "kind": "trace",
                "trace_id": "abc123",
                "spans": [
                    {"svc": "checkout-api", "dur_ms": 5012},
                    {"svc": "payments-svc", "dur_ms": 4980},
                ],
            },
            {
                "ts": "2026-05-10T14:30:00Z",
                "kind": "topology",
                "change": "rename",
                "from": "payments-svc",
                "to": "billing-svc",
            },
            {
                "ts": "2026-05-10T14:32:11Z",
                "kind": "incident_signal",
                "incident_id": "INC-714",
                "trigger": "alert:checkout-api/error-rate>5%",
                "service": "checkout-api",
            },
            {
                "ts": "2026-05-10T15:10:00Z",
                "kind": "remediation",
                "incident_id": "INC-714",
                "action": "rollback",
                "target": "billing-svc",
                "version": "v2.13.4",
                "outcome": "resolved",
            },
        ]
        
        self.engine.ingest(events)
        
        # Verify stats
        stats = self.engine.stats()
        self.assertEqual(stats["raw_events"], 7)
        
        # Both names resolve to same UUID (rename-proof identity)
        uuid_payments = self.engine._node_store.resolve("payments-svc")
        uuid_billing = self.engine._node_store.resolve("billing-svc")
        self.assertEqual(uuid_payments, uuid_billing)
        
        # Reconstruct context for INC-714
        signal = {
            "ts": "2026-05-10T14:32:11Z",
            "kind": "incident_signal",
            "incident_id": "INC-714",
            "trigger": "alert:checkout-api/error-rate>5%",
            "service": "checkout-api",
        }
        
        context = self.engine.reconstruct_context(signal, mode="fast")
        
        # Assertions per spec
        # 1. Related events should include deploy, metric, log, trace
        self.assertGreaterEqual(len(context["related_events"]), 4,
            "Should include deploy, metric, log, trace")
        
        kinds = {e.get("kind") for e in context["related_events"]}
        self.assertIn("deploy", kinds)
        self.assertIn("metric", kinds)
        self.assertIn("log", kinds)
        
        # 2. Causal chain should have at least one edge with confidence >= 0.5
        if context["causal_chain"]:
            top_conf = max(e["confidence"] for e in context["causal_chain"])
            self.assertGreaterEqual(top_conf, 0.5,
                "Causal chain should have edge with confidence >= 0.5")
        
        # 3. Context should be returned (pattern store may or may not have matches)
        self.assertIsNotNone(context["confidence"])
        self.assertGreaterEqual(context["confidence"], 0.0)
        
        # 4. Explain should be populated
        self.assertTrue(context["explain"])
        self.assertIn("INC-714", context["explain"])
        
        print(f"\nContext reconstruction results:")
        print(f"  Related events: {len(context['related_events'])}")
        print(f"  Causal chain: {len(context['causal_chain'])} edges")
        print(f"  Similar incidents: {len(context['similar_past_incidents'])}")
        print(f"  Suggested remediations: {len(context['suggested_remediations'])}")
        print(f"  Confidence: {context['confidence']}")
        print(f"  Explain: {context['explain'][:100]}...")

    def test_topology_aware_expansion_at_incident_time(self):
        """
        Test that service expansion uses topology at incident time,
        not current topology.
        
        Scenario:
        - T=0: svc-a depends on svc-b
        - T=1: svc-b depends on svc-c (added)
        - T=2: Incident on svc-a
        
        At incident time (T=2), expansion should find svc-a → svc-b → svc-c
        because those edges were all active at T=2.
        """
        events = [
            # Initial topology: svc-a -> svc-b
            {
                "ts": (self.base_ts).isoformat(),
                "kind": "deploy",
                "service": "svc-a",
                "version": "v1.0.0",
            },
            {
                "ts": (self.base_ts).isoformat(),
                "kind": "topology",
                "change": "dep_add",
                "from": "svc-a",
                "to": "svc-b",
            },
            # Later: svc-b -> svc-c added
            {
                "ts": (self.base_ts + timedelta(minutes=5)).isoformat(),
                "kind": "topology",
                "change": "dep_add",
                "from": "svc-b",
                "to": "svc-c",
            },
            # Incident on svc-a at T=10
            {
                "ts": (self.base_ts + timedelta(minutes=10)).isoformat(),
                "kind": "incident_signal",
                "incident_id": "INC-TOPO-001",
                "trigger": "alert:svc-a/latency>1s",
                "service": "svc-a",
            },
        ]
        
        self.engine.ingest(events)
        
        # At incident time (T=10), the topology should include:
        # svc-a -> svc-b -> svc-c
        
        # Test temporal expansion directly
        node_a = self.engine._node_store.resolve("svc-a")
        incident_ts = self.base_ts + timedelta(minutes=10)
        
        neighborhood, subgraph = self.engine._temporal_view.get_neighborhood_at_timestamp(
            node_a, incident_ts, max_hops=2
        )
        
        # Should include all 3 services
        self.assertEqual(len(neighborhood), 3)
        
        node_b = self.engine._node_store.resolve("svc-b")
        node_c = self.engine._node_store.resolve("svc-c")
        
        self.assertIn(node_a, neighborhood)
        self.assertIn(node_b, neighborhood)
        self.assertIn(node_c, neighborhood)
        
        # Subgraph should have the chain
        self.assertTrue(subgraph.has_edge(node_a, node_b))
        self.assertTrue(subgraph.has_edge(node_b, node_c))

    def test_topology_drift_handling(self):
        """
        Test that context reconstruction handles topology drift.
        
        Scenario:
        - T=0: svc-a depends on svc-b
        - T=1: svc-a no longer depends on svc-b (removed)
        - T=2: Incident on svc-a
        
        At incident time (T=2), expansion should NOT find svc-b
        because the edge was already retired.
        """
        events = [
            {
                "ts": (self.base_ts).isoformat(),
                "kind": "deploy",
                "service": "svc-a",
                "version": "v1.0.0",
            },
            {
                "ts": (self.base_ts).isoformat(),
                "kind": "topology",
                "change": "dep_add",
                "from": "svc-a",
                "to": "svc-b",
            },
            # Remove dependency
            {
                "ts": (self.base_ts + timedelta(minutes=3)).isoformat(),
                "kind": "topology",
                "change": "dep_remove",
                "from": "svc-a",
                "to": "svc-b",
            },
            # Incident after removal
            {
                "ts": (self.base_ts + timedelta(minutes=10)).isoformat(),
                "kind": "incident_signal",
                "incident_id": "INC-DRIFT-001",
                "trigger": "alert:svc-a/error-rate>5%",
                "service": "svc-a",
            },
        ]
        
        self.engine.ingest(events)
        
        # At incident time (T=10), svc-b should NOT be in neighborhood
        node_a = self.engine._node_store.resolve("svc-a")
        incident_ts = self.base_ts + timedelta(minutes=10)
        
        neighborhood, _ = self.engine._temporal_view.get_neighborhood_at_timestamp(
            node_a, incident_ts, max_hops=2
        )
        
        # Should only have svc-a
        self.assertEqual(len(neighborhood), 1)
        self.assertIn(node_a, neighborhood)


if __name__ == "__main__":
    unittest.main()
