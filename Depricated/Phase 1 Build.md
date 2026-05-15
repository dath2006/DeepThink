Build
1.2 — Core ingest + raw storage + basic graph construction
Write an event parser that reads JSONL and normalises each event into a typed dict — timestamp as a datetime object, kind as an enum, and the remaining fields preserved verbatim.
Write a raw event store (append-only table, one row per event) with columns: id, ts, kind, service, raw_json. This is your provenance anchor — every later inference points back here.
Write a node table: id, canonical_name, aliases (JSON list), first_seen_ts, last_seen_ts. When a deploy event arrives for "payments-svc", upsert a node for it.
Write an edge table: id, source_node_id, target_node_id, edge_kind, first_seen_ts, last_seen_ts, confidence, evidence_event_ids (JSON list). Edges can be inferred (not just from explicit topology events).
Handle the topology/rename event: when you see it, do NOT delete the old node. Instead, add the new name as an alias on the same node record and update canonical_name. This is the key to topology-independence.
Handle the topology/dependency-shift event: create or retire edges, but mark retired edges with a retired_at timestamp — never delete them.
Write an ingest coordinator that calls the parser, then dispatches to the raw store and the graph updater in sequence, inside a transaction so they never diverge.
Add a simple in-memory recent-events buffer (last 500 events) to avoid hitting disk for every lookup during context reconstruction.

-DuckDB is the source of truth (raw events, bi-temporal edges, aliases, remediation history)
NetworkX holds the current graph in memory for fast traversal
On a topology event (rename, dependency shift), you update both
The key insight: because NetworkX nodes are keyed by UUID (not service name), a rename requires zero structural changes to the NetworkX graph. You just update a label attribute. This is why the UUID-as-identity decision in Phase 1 pays off.
Use NetworkX for traversal (neighbourhood lookup, hop counting, path finding) and DuckDB for everything that needs history (point-in-time queries, provenance, remediation store). The interface between them is small — you only sync on topology events, not on every ingest.
One thing to avoid: don't use nx.DiGraph keyed by service name. Key it by the UUID you assign at first observation. Every other design decision flows from that.

