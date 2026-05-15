"""
Tests for TemporalGraphView — point-in-time graph reconstruction.

These tests verify that the engine correctly reconstructs graph topology
at arbitrary points in time, handling:
- Edges added after T (should not appear)
- Edges retired before T (should not appear)
- Edges active at T (should appear)
"""

import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "d:/Agents/DeepThink")

from persistent_context_engine import Engine, EngineConfig
from persistent_context_engine.graph.temporal_view import TemporalGraphView


class TestTemporalGraphView(unittest.TestCase):
    """Test point-in-time graph reconstruction."""

    def setUp(self):
        """Create fresh engine for each test."""
        self.engine = Engine(EngineConfig(db_path=":memory:"))
        self.base_ts = datetime.fromisoformat("2026-05-10T14:00:00+00:00")
        
        # Build temporal view
        self.temporal = TemporalGraphView(
            self.engine._db,
            self.engine._node_store,
        )

    def tearDown(self):
        """Clean up."""
        self.engine.close()

    def test_empty_graph_at_timestamp(self):
        """Graph at any timestamp should be empty initially."""
        g = self.temporal.at_timestamp(self.base_ts)
        self.assertEqual(g.number_of_nodes(), 0)
        self.assertEqual(g.number_of_edges(), 0)

    def test_node_created_before_timestamp_visible(self):
        """Nodes created before T should be visible at T."""
        # Create a node via event ingestion
        self.engine.ingest([{
            "ts": (self.base_ts - timedelta(hours=1)).isoformat(),
            "kind": "deploy",
            "service": "svc-a",
            "version": "v1.0.0",
        }])
        
        # Graph at T should show the node
        g = self.temporal.at_timestamp(self.base_ts)
        
        # Get the node ID
        node_id = self.engine._node_store.resolve("svc-a")
        self.assertIsNotNone(node_id)
        
        # Should be in the graph
        self.assertIn(node_id, g.nodes)
        self.assertEqual(g.nodes[node_id]["canonical_name"], "svc-a")

    def test_node_created_after_timestamp_not_visible(self):
        """Nodes created after T should NOT be visible at T."""
        # Create a node AFTER the query timestamp
        self.engine.ingest([{
            "ts": (self.base_ts + timedelta(hours=1)).isoformat(),
            "kind": "deploy",
            "service": "svc-b",
            "version": "v1.0.0",
        }])
        
        # Graph at T should NOT show the node
        g = self.temporal.at_timestamp(self.base_ts)
        
        node_id = self.engine._node_store.resolve("svc-b")
        self.assertIsNotNone(node_id)
        
        # Should NOT be in the graph at T
        self.assertNotIn(node_id, g.nodes)

    def test_edge_active_at_timestamp_visible(self):
        """Edges active at T should be visible."""
        # Create two nodes and a dependency
        self.engine.ingest([
            {
                "ts": (self.base_ts - timedelta(hours=2)).isoformat(),
                "kind": "deploy",
                "service": "svc-a",
                "version": "v1.0.0",
            },
            {
                "ts": (self.base_ts - timedelta(hours=1)).isoformat(),
                "kind": "topology",
                "change": "dep_add",
                "from": "svc-a",
                "to": "svc-b",
            },
        ])
        
        # Get node IDs
        node_a = self.engine._node_store.resolve("svc-a")
        node_b = self.engine._node_store.resolve("svc-b")
        
        # Edge should be visible at T
        g = self.temporal.at_timestamp(self.base_ts)
        self.assertTrue(g.has_edge(node_a, node_b))

    def test_edge_retired_before_timestamp_not_visible(self):
        """Edges retired before T should NOT be visible."""
        # Create dependency, then retire it
        self.engine.ingest([
            {
                "ts": (self.base_ts - timedelta(hours=3)).isoformat(),
                "kind": "deploy",
                "service": "svc-a",
                "version": "v1.0.0",
            },
            {
                "ts": (self.base_ts - timedelta(hours=2)).isoformat(),
                "kind": "topology",
                "change": "dep_add",
                "from": "svc-a",
                "to": "svc-b",
            },
            {
                "ts": (self.base_ts - timedelta(hours=1)).isoformat(),
                "kind": "topology",
                "change": "dep_remove",
                "from": "svc-a",
                "to": "svc-b",
            },
        ])
        
        # Get node IDs
        node_a = self.engine._node_store.resolve("svc-a")
        node_b = self.engine._node_store.resolve("svc-b")
        
        # Edge should NOT be visible at T (retired 1 hour ago)
        g = self.temporal.at_timestamp(self.base_ts)
        self.assertFalse(g.has_edge(node_a, node_b))

    def test_rename_preserved_across_timestamps(self):
        """
        Node UUID identity preserved across renames.
        
        NOTE: Names are current-only (not historical). The important
        invariant is that both payments-svc and billing-svc resolve
        to the same UUID, enabling rename-proof incident matching.
        """
        # Create service, then rename it
        self.engine.ingest([
            {
                "ts": (self.base_ts - timedelta(hours=2)).isoformat(),
                "kind": "deploy",
                "service": "payments-svc",
                "version": "v1.0.0",
            },
            {
                "ts": self.base_ts.isoformat(),
                "kind": "topology",
                "change": "rename",
                "from": "payments-svc",
                "to": "billing-svc",
            },
        ])
        
        # CRITICAL INVARIANT: Same UUID for both names (rename-proof identity)
        uuid_old = self.engine._node_store.resolve("payments-svc")
        uuid_new = self.engine._node_store.resolve("billing-svc")
        self.assertEqual(uuid_old, uuid_new)
        
        # Node exists in graph at both times
        g_before = self.temporal.at_timestamp(
            self.base_ts - timedelta(hours=1)
        )
        g_after = self.temporal.at_timestamp(
            self.base_ts + timedelta(hours=1)
        )
        
        self.assertIn(uuid_old, g_before.nodes)
        self.assertIn(uuid_new, g_after.nodes)
        
        # Names are current-only (shows current canonical name)
        # This is acceptable because UUID is the topology driver
        current_name = self.engine._node_store.get_canonical_name(uuid_old)
        self.assertEqual(g_before.nodes[uuid_old]["canonical_name"], current_name)
        self.assertEqual(g_after.nodes[uuid_new]["canonical_name"], current_name)

    def test_neighborhood_expansion_at_timestamp(self):
        """Neighborhood expansion respects topology at specific timestamp."""
        # Create topology: svc-a -> svc-b -> svc-c
        self.engine.ingest([
            {
                "ts": (self.base_ts - timedelta(hours=2)).isoformat(),
                "kind": "deploy",
                "service": "svc-a",
                "version": "v1.0.0",
            },
            {
                "ts": (self.base_ts - timedelta(hours=2)).isoformat(),
                "kind": "topology",
                "change": "dep_add",
                "from": "svc-a",
                "to": "svc-b",
            },
            {
                "ts": (self.base_ts - timedelta(hours=1)).isoformat(),
                "kind": "topology",
                "change": "dep_add",
                "from": "svc-b",
                "to": "svc-c",
            },
        ])
        
        node_a = self.engine._node_store.resolve("svc-a")
        
        # Get 2-hop neighborhood at T
        neighborhood, subgraph = self.temporal.get_neighborhood_at_timestamp(
            node_a, self.base_ts, max_hops=2
        )
        
        # Should include svc-a, svc-b, svc-c
        self.assertEqual(len(neighborhood), 3)
        
        # Subgraph should have the chain
        node_b = self.engine._node_store.resolve("svc-b")
        node_c = self.engine._node_store.resolve("svc-c")
        self.assertTrue(subgraph.has_edge(node_a, node_b))
        self.assertTrue(subgraph.has_edge(node_b, node_c))

    def test_neighborhood_respects_edge_retirement(self):
        """Neighborhood doesn't traverse retired edges."""
        # Create then break the chain
        self.engine.ingest([
            {
                "ts": (self.base_ts - timedelta(hours=3)).isoformat(),
                "kind": "deploy",
                "service": "svc-a",
                "version": "v1.0.0",
            },
            {
                "ts": (self.base_ts - timedelta(hours=2)).isoformat(),
                "kind": "topology",
                "change": "dep_add",
                "from": "svc-a",
                "to": "svc-b",
            },
            {
                "ts": (self.base_ts - timedelta(hours=1)).isoformat(),
                "kind": "topology",
                "change": "dep_remove",
                "from": "svc-a",
                "to": "svc-b",
            },
        ])
        
        node_a = self.engine._node_store.resolve("svc-a")
        
        # Get 2-hop neighborhood at T
        neighborhood, _ = self.temporal.get_neighborhood_at_timestamp(
            node_a, self.base_ts, max_hops=2
        )
        
        # Should only include svc-a (edge to svc-b is retired)
        self.assertEqual(len(neighborhood), 1)
        self.assertIn(node_a, neighborhood)


if __name__ == "__main__":
    unittest.main()
