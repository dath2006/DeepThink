"""
NetworkX DiGraph manager — in-memory graph for fast traversal.

Architecture contract
---------------------
* Nodes are keyed by UUID string, NOT by service name.
  A rename therefore requires ZERO structural changes — only a label
  attribute update.  This is the payoff for the UUID-as-identity decision.

* This graph holds the CURRENT state (active topology only).
  Historical / retired edges live in DuckDB exclusively.

* GraphManager is a *derived view* of DuckDB.  It can always be rebuilt
  from scratch by the Engine on startup.  If a write to NetworkX fails
  after a DuckDB commit, the engine just calls ``rebuild_from_db()``.

* All public methods are intentionally simple; complex graph algorithms
  (shortest path, community detection, etc.) belong in Phase 2+ modules
  that receive this graph as input.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx

log = logging.getLogger(__name__)


class GraphManager:
    """
    Thin wrapper around a ``networkx.DiGraph`` with UUID node keys.

    Node attributes stored per node:
        canonical_name : str   — current service name
        aliases        : list  — historical names

    Edge attributes stored per edge:
        edge_kind  : str   — EdgeKind value
        confidence : float — [0, 1]
    """

    def __init__(self) -> None:
        self._g: nx.DiGraph = nx.DiGraph()

    # ------------------------------------------------------------------
    # Node operations
    # ------------------------------------------------------------------

    def add_or_update_node(
        self,
        node_id: str,
        canonical_name: str,
        aliases: Optional[List[str]] = None,
    ) -> None:
        """Insert a new node or update its label attributes."""
        if node_id not in self._g:
            self._g.add_node(
                node_id,
                canonical_name=canonical_name,
                aliases=list(aliases or []),
            )
        else:
            self._g.nodes[node_id]["canonical_name"] = canonical_name
            if aliases is not None:
                self._g.nodes[node_id]["aliases"] = aliases

    def handle_rename(
        self,
        node_id: str,
        new_name: str,
        old_name: str,
    ) -> None:
        """
        Update label for an existing node after a topology/rename event.
        The node UUID key is unchanged — zero structural change.
        """
        if node_id not in self._g:
            self._g.add_node(node_id, canonical_name=new_name, aliases=[old_name])
            return

        node_data = self._g.nodes[node_id]
        aliases: List[str] = list(node_data.get("aliases", []))
        if old_name not in aliases:
            aliases.append(old_name)
        node_data["canonical_name"] = new_name
        node_data["aliases"]        = aliases

    # ------------------------------------------------------------------
    # Edge operations
    # ------------------------------------------------------------------

    def add_or_update_edge(
        self,
        source_id: str,
        target_id: str,
        edge_kind: str,
        confidence: float = 1.0,
    ) -> None:
        """Add a new directed edge or update confidence on an existing one."""
        if self._g.has_edge(source_id, target_id):
            existing_conf = self._g[source_id][target_id].get("confidence", 0.0)
            self._g[source_id][target_id]["confidence"] = max(existing_conf, confidence)
        else:
            self._g.add_edge(
                source_id,
                target_id,
                edge_kind=edge_kind,
                confidence=confidence,
            )

    def retire_edge(self, source_id: str, target_id: str) -> None:
        """Remove an edge from the active graph (retirement, not deletion)."""
        if self._g.has_edge(source_id, target_id):
            self._g.remove_edge(source_id, target_id)

    # ------------------------------------------------------------------
    # Traversal helpers  (used by context reconstruction, Phase 2+)
    # ------------------------------------------------------------------

    def neighbors_at_hops(
        self,
        node_id: str,
        max_hops: int = 2,
    ) -> List[Tuple[str, str]]:
        """
        BFS returning (neighbor_id, direction) where direction is 'upstream'
        (predecessor) or 'downstream' (successor) relative to the start node.

        Used by fingerprinter to compute service roles (upstream-1, downstream-2, etc.)
        """
        result: List[Tuple[str, str]] = []
        visited: Set[str] = {node_id}

        # Track nodes at each hop level with their direction
        current_level: Dict[str, str] = {node_id: "trigger"}

        for hop in range(1, max_hops + 1):
            next_level: Dict[str, str] = {}

            for current_id, _ in current_level.items():
                # Successors = downstream
                for succ in self._g.successors(current_id):
                    if succ not in visited:
                        visited.add(succ)
                        next_level[succ] = "downstream"
                        result.append((succ, "downstream"))

                # Predecessors = upstream
                for pred in self._g.predecessors(current_id):
                    if pred not in visited:
                        visited.add(pred)
                        next_level[pred] = "upstream"
                        result.append((pred, "upstream"))

            current_level = next_level
            if not current_level:
                break

        return result

    def neighbors_within_hops(
        self,
        node_id: str,
        hops: int = 2,
    ) -> Set[str]:
        """
        BFS to collect all node IDs reachable within *hops* from *node_id*,
        traversing edges in both directions (predecessor and successor).
        """
        visited: Set[str] = set()
        frontier: Set[str] = {node_id}
        for _ in range(hops):
            next_frontier: Set[str] = set()
            for n in frontier:
                if n in visited:
                    continue
                next_frontier.update(self._g.successors(n))
                next_frontier.update(self._g.predecessors(n))
            visited.update(frontier)
            frontier = next_frontier - visited
        visited.discard(node_id)
        return visited

    def get_node_attribute(
        self,
        node_id: str,
        attr: str,
        default: Any = None,
    ) -> Any:
        if node_id not in self._g:
            return default
        return self._g.nodes[node_id].get(attr, default)

    def has_node(self, node_id: str) -> bool:
        return node_id in self._g

    def node_count(self) -> int:
        return self._g.number_of_nodes()

    def edge_count(self) -> int:
        return self._g.number_of_edges()

    # ------------------------------------------------------------------
    # Graph rebuild  (called on Engine startup)
    # ------------------------------------------------------------------

    def rebuild(
        self,
        node_rows: List[Tuple[str, str, List[str]]],
        edge_rows: List[Tuple[str, str, str, float]],
    ) -> None:
        """
        Replace the current graph with a snapshot from DuckDB.

        Parameters
        ----------
        node_rows : list of (node_id, canonical_name, aliases)
        edge_rows : list of (source_id, target_id, edge_kind, confidence)
        """
        self._g.clear()
        for node_id, canonical, aliases in node_rows:
            self._g.add_node(node_id, canonical_name=canonical, aliases=aliases)
        for src, dst, kind, conf in edge_rows:
            self._g.add_edge(src, dst, edge_kind=kind, confidence=conf)
        log.debug(
            "Graph rebuilt: %d nodes, %d edges",
            self._g.number_of_nodes(),
            self._g.number_of_edges(),
        )

    # ------------------------------------------------------------------
    # Raw access  (for algorithms in later phases)
    # ------------------------------------------------------------------

    @property
    def graph(self) -> nx.DiGraph:
        """Direct access to the underlying DiGraph."""
        return self._g
