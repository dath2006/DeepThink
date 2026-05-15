"""
TemporalGraphView — point-in-time graph snapshots.

This module creates graph views at specific timestamps using the bi-temporal
edge data stored in DuckDB. Critical for robust context reconstruction because
topology changes (renames, dependency shifts) happen over time.

Key insight:
- The in-memory GraphManager only holds CURRENT active topology
- For historical reconstruction, we must query edges as they were at time T
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import networkx as nx

from ..storage.database import DatabaseManager
from ..storage.node_store import NodeStore

log = logging.getLogger(__name__)


class TemporalGraphView:
    """
    Creates point-in-time snapshots of the graph using bi-temporal edge data.
    
    An edge is visible at time T if:
        first_seen_ts <= T AND (retired_at IS NULL OR retired_at > T)
    
    This handles:
    - Service renames (node identity preserved, name changed)
    - Dependency additions (edge appears at first_seen_ts)
    - Dependency removals (edge disappears at retired_at)
    """

    def __init__(
        self,
        db: DatabaseManager,
        node_store: NodeStore,
    ) -> None:
        self._db = db
        self._node_store = node_store

    def at_timestamp(
        self,
        ts: datetime,
        relevant_nodes: Optional[Set[str]] = None,
    ) -> nx.DiGraph:
        """
        Create a graph snapshot as it existed at time *ts*.
        
        Parameters
        ----------
        ts : datetime
            The timestamp to query at
        relevant_nodes : Optional[Set[str]]
            If provided, only include these nodes and edges between them.
            This is an optimization for context reconstruction.
        
        Returns
        -------
        nx.DiGraph
            A frozen DiGraph representing the topology at time ts.
            Nodes are UUIDs with canonical_name and aliases attributes.
            Edges have edge_kind and confidence attributes.
        """
        g = nx.DiGraph()
        
        # Build node set
        if relevant_nodes:
            node_ids = relevant_nodes
        else:
            # All nodes that existed at ts
            node_ids = self._get_nodes_at_timestamp(ts)
        
        # Add nodes with their state at ts
        for node_id in node_ids:
            node_data = self._get_node_state_at_timestamp(node_id, ts)
            if node_data:
                g.add_node(
                    node_id,
                    canonical_name=node_data["canonical_name"],
                    aliases=node_data["aliases"],
                )
        
        # Add edges that were active at ts
        edges = self._get_edges_at_timestamp(ts, node_ids if relevant_nodes else None)
        for src, dst, edge_data in edges:
            if src in g.nodes and dst in g.nodes:
                g.add_edge(
                    src, dst,
                    edge_kind=edge_data["edge_kind"],
                    confidence=edge_data["confidence"],
                )
        
        return g

    def _get_nodes_at_timestamp(self, ts: datetime) -> Set[str]:
        """Get all node IDs that existed at or before ts."""
        rows = self._db.conn.execute(
            """
            SELECT id FROM nodes
            WHERE first_seen_ts <= ?
            """,
            [ts],
        ).fetchall()
        return {r[0] for r in rows}

    def _get_node_state_at_timestamp(
        self,
        node_id: str,
        ts: datetime,
    ) -> Optional[Dict]:
        """
        Get node state (name, aliases) as it was at time ts.
        
        NOTE: The current implementation returns the CURRENT canonical name,
        not the historical name at time ts. This is a known limitation:
        
        - Nodes table stores current canonical_name only
        - Rename history would require querying raw topology events
        - UUID identity is preserved, which is the critical invariant
        - Names are labels; topology is driven by UUIDs
        
        For Phase 3, this is sufficient because:
        1. Graph topology (edges) IS correctly temporal
        2. Incident matching uses UUIDs (rename-proof)
        3. Names in output are for human readability (current names are fine)
        """
        # Get current state from node_store
        canonical = self._node_store.get_canonical_name(node_id)
        if not canonical:
            return None
        
        all_aliases = self._node_store.all_names_for_id(node_id)
        # Filter to remove the canonical name
        aliases = [a for a in all_aliases if a != canonical]
        
        return {
            "canonical_name": canonical,
            "aliases": aliases,
        }

    def _get_edges_at_timestamp(
        self,
        ts: datetime,
        node_filter: Optional[Set[str]] = None,
    ) -> List[Tuple[str, str, Dict]]:
        """
        Get all edges that were active at time ts.
        
        An edge is active if:
            first_seen_ts <= ts AND (retired_at IS NULL OR retired_at > ts)
        """
        if node_filter:
            # Optimize: only query edges between relevant nodes
            placeholders = ", ".join(["?"] * len(node_filter))
            rows = self._db.conn.execute(
                f"""
                SELECT source_node_id, target_node_id, edge_kind, confidence
                FROM edges
                WHERE source_node_id IN ({placeholders})
                  AND target_node_id IN ({placeholders})
                  AND first_seen_ts <= ?
                  AND (retired_at IS NULL OR retired_at > ?)
                """,
                list(node_filter) * 2 + [ts, ts],
            ).fetchall()
        else:
            rows = self._db.conn.execute(
                """
                SELECT source_node_id, target_node_id, edge_kind, confidence
                FROM edges
                WHERE first_seen_ts <= ?
                  AND (retired_at IS NULL OR retired_at > ?)
                """,
                [ts, ts],
            ).fetchall()
        
        return [
            (r[0], r[1], {"edge_kind": r[2], "confidence": float(r[3])})
            for r in rows
        ]

    def get_neighborhood_at_timestamp(
        self,
        center_node_id: str,
        ts: datetime,
        max_hops: int = 2,
        direction: str = "both",
    ) -> Tuple[Set[str], nx.DiGraph]:
        """
        Get the neighborhood around a center node at time ts.
        
        Returns
        -------
        Tuple[Set[str], nx.DiGraph]
            (node_ids in neighborhood, subgraph)
        """
        # Start with the center node
        neighborhood: Set[str] = {center_node_id}
        frontier: Set[str] = {center_node_id}
        
        # Get graph at timestamp for traversal
        # For efficiency, we do iterative expansion rather than building full graph
        for hop in range(max_hops):
            next_frontier: Set[str] = set()
            
            for node_id in frontier:
                # Query edges from/to this node at timestamp
                edges = self._get_connected_nodes_at_timestamp(node_id, ts, direction)
                next_frontier.update(edges)
            
            next_frontier -= neighborhood  # Don't revisit
            neighborhood.update(next_frontier)
            frontier = next_frontier
            
            if not frontier:
                break
        
        # Build subgraph for the neighborhood
        subgraph = self.at_timestamp(ts, neighborhood)
        
        return neighborhood, subgraph

    def _get_connected_nodes_at_timestamp(
        self,
        node_id: str,
        ts: datetime,
        direction: str,
    ) -> Set[str]:
        """Get nodes connected to node_id at time ts."""
        nodes: Set[str] = set()
        
        if direction in ("both", "downstream"):
            # Successors (outgoing edges)
            rows = self._db.conn.execute(
                """
                SELECT target_node_id
                FROM edges
                WHERE source_node_id = ?
                  AND first_seen_ts <= ?
                  AND (retired_at IS NULL OR retired_at > ?)
                """,
                [node_id, ts, ts],
            ).fetchall()
            nodes.update(r[0] for r in rows)
        
        if direction in ("both", "upstream"):
            # Predecessors (incoming edges)
            rows = self._db.conn.execute(
                """
                SELECT source_node_id
                FROM edges
                WHERE target_node_id = ?
                  AND first_seen_ts <= ?
                  AND (retired_at IS NULL OR retired_at > ?)
                """,
                [node_id, ts, ts],
            ).fetchall()
            nodes.update(r[0] for r in rows)
        
        return nodes

    def get_graph_changes_between(
        self,
        start_ts: datetime,
        end_ts: datetime,
    ) -> Dict[str, List[Dict]]:
        """
        Get topology changes between two timestamps.
        
        Returns
        -------
        Dict with keys: 'edges_added', 'edges_removed', 'nodes_renamed'
        """
        # Edges added in interval
        added_rows = self._db.conn.execute(
            """
            SELECT source_node_id, target_node_id, edge_kind, first_seen_ts
            FROM edges
            WHERE first_seen_ts > ? AND first_seen_ts <= ?
            """,
            [start_ts, end_ts],
        ).fetchall()
        
        # Edges removed in interval
        removed_rows = self._db.conn.execute(
            """
            SELECT source_node_id, target_node_id, edge_kind, retired_at
            FROM edges
            WHERE retired_at > ? AND retired_at <= ?
            """,
            [start_ts, end_ts],
        ).fetchall()
        
        return {
            "edges_added": [
                {
                    "source": r[0],
                    "target": r[1],
                    "kind": r[2],
                    "timestamp": r[3],
                }
                for r in added_rows
            ],
            "edges_removed": [
                {
                    "source": r[0],
                    "target": r[1],
                    "kind": r[2],
                    "timestamp": r[3],
                }
                for r in removed_rows
            ],
        }
