# Phase 1 — Implementation Report
## Persistent Context Engine for Autonomous SRE

---

## 1. What Was Built

Phase 1 delivers the complete **ingest + raw storage + graph construction** foundation. Every later phase (causal inference, incident matching, remediation suggestion) builds directly on top of the primitives produced here.

---

## 2. Directory Structure

```
persistent_context_engine/
├── __init__.py              # Package exports: Engine, EngineConfig, all TypedDicts
├── config.py                # EngineConfig dataclass — all tuneable parameters
├── schema.py                # TypedDicts, Enums, DuckDB DDL
├── engine.py                # Top-level Engine class (benchmark interface)
├── requirements.txt         # duckdb, networkx
│
├── storage/                 # DuckDB persistence layer
│   ├── __init__.py
│   ├── database.py          # Single-connection DuckDB manager + transactions
│   ├── raw_store.py         # Append-only raw_events table (provenance anchor)
│   ├── node_store.py        # nodes table + in-memory name→UUID index
│   └── edge_store.py        # edges table — bi-temporal (retired_at, never deleted)
│
├── graph/                   # In-memory graph layer
│   ├── __init__.py
│   ├── manager.py           # NetworkX DiGraph keyed by UUID
│   └── topology.py          # Topology event handler (DB ops → pending NX ops)
│
└── ingestion/               # Ingest pipeline
    ├── __init__.py
    ├── parser.py            # EventParser: normalises dict|JSONL → typed event
    ├── buffer.py            # RecentEventsBuffer: deque(maxlen=500)
    └── coordinator.py       # IngestCoordinator: transaction + NX update
```

> **Dead files at root** (`event_parser.py`, `raw_event_store.py`): artefacts from a prior incomplete attempt; superseded by the subpackage structure and not imported by anything.

---

## 3. Component Deep-Dives

### 3.1 `schema.py` — Contracts

| Symbol | Purpose |
|--------|---------|
| `EventKind(str, Enum)` | 7 guaranteed kinds + unknown-kind tolerance. `str` mixin means `kind == EventKind.DEPLOY` works without `.value`. |
| `EdgeKind(str, Enum)` | CALLS, DEPENDENCY, DEPLOYMENT, ERROR_PROPAGATION, CAUSAL, REMEDIATION_LINK |
| `Event(TypedDict, total=False)` | Benchmark-compatible. `total=False` because every kind carries different keys. |
| `IncidentSignal`, `Context`, `CausalEdge`, `IncidentMatch`, `Remediation` | Match the benchmark harness interface exactly. |
| `NodeRecord`, `EdgeRecord` | Internal storage TypedDicts (not exposed to benchmark). |
| `SCHEMA_DDL`, `INDEX_DDL` | DuckDB-compatible DDL. Uses `VARCHAR` (not `UUID`/`JSON`), `TIMESTAMPTZ`, no unenforced FK constraints. |

---

### 3.2 `storage/database.py` — Single DuckDB Connection

**Critical rule (from DuckDB docs):** one read-write connection per process. The previous implementation opened a new connection per operation — this is incorrect for file-based DuckDB and was the primary architectural flaw corrected here.

```
DatabaseManager
  ├── connect()         — opens once, initialises schema + indexes
  ├── conn (property)   — lazy accessor, auto-connects
  ├── begin()           — explicit transaction start
  ├── commit()
  └── rollback()
```

---

### 3.3 `storage/raw_store.py` — Provenance Anchor

- **Append-only** `raw_events` table: `id (PK), ts (TIMESTAMPTZ), kind, service, raw_json (VARCHAR)`
- Every downstream inference stores `evidence_event_ids` pointing back here
- Uses a `_json_default` encoder so normalised events (where `ts` is a `datetime`) serialise cleanly to JSON without `TypeError`
- Read helpers use `conn.cursor()` (DuckDB idiom for concurrent reads without blocking the write path)

---

### 3.4 `storage/node_store.py` — UUID-as-Identity

**The key insight from the Phase 1 build plan:**

> *"Key it by the UUID you assign at first observation. Every other design decision flows from that."*

```
NodeStore
  ├── _name_to_id: Dict[str, str]    — in-memory name/alias → UUID index
  ├── _load_index()                  — rebuilds from DuckDB on startup (crash recovery)
  ├── get_or_create(service, ts)     — O(1) lookup via _name_to_id, INSERT only if unseen
  ├── handle_rename(from, to, ts)    — appends from_name to aliases[], updates canonical_name,
  │                                    maps BOTH names to SAME UUID in _name_to_id
  ├── resolve(name)                  — O(1) name→UUID lookup (works for any alias)
  └── all_names_for_id(node_id)     — returns canonical + all historical aliases
```

**Topology-independence mechanism:** After `payments-svc → billing-svc` rename, `_name_to_id["payments-svc"]` and `_name_to_id["billing-svc"]` both return the same UUID. Graph traversal, edge lookups, and event correlation all go through this index, so they remain correct across any rename chain.

---

### 3.5 `storage/edge_store.py` — Bi-temporal Edges

| Column | Purpose |
|--------|---------|
| `id` | UUID |
| `source_node_id / target_node_id` | UUID references (topology-independent) |
| `edge_kind` | EdgeKind value |
| `first_seen_ts / last_seen_ts` | Bi-temporal tracking |
| `retired_at` | `NULL` = active. Set on `retire()` — **edges are never deleted** |
| `confidence` | `[0.0, 1.0]`, takes `max(existing, new)` on merge |
| `evidence_event_ids` | JSON list of raw_event UUIDs — provenance chain |
| `metadata` | JSON blob for Phase 2+ annotations |

`get_or_create()` **merges** into an existing active edge rather than inserting a duplicate: evidence is unioned, confidence takes the higher value, `last_seen_ts` is bumped.

---

### 3.6 `graph/manager.py` — NetworkX DiGraph

- `nx.DiGraph` keyed by UUID strings, **not** service names
- **Rename = zero structural change**: only the `canonical_name` and `aliases` node attributes are updated. No edges move, no nodes are re-keyed.
- `retire_edge(src, dst)` removes from the active graph; the historical record remains in DuckDB
- `rebuild(node_rows, edge_rows)` re-creates the entire graph from DuckDB snapshots — used on Engine startup for crash recovery
- `neighbors_within_hops(node_id, hops=2)` — BFS in both directions; used by context reconstruction to expand the service blast radius

---

### 3.7 `graph/topology.py` — Topology Handler

**Design contract:**
- Performs **DuckDB operations only** (inside an open transaction)
- Returns a `List[_NxOp]` of pending NetworkX operations
- The coordinator applies those ops **after `db.commit()`**
- This guarantees: if the DB transaction rolls back → NetworkX is never mutated

Handles `dependency-shift` defensively across multiple field-name conventions:

| Field role | Accepted keys |
|-----------|--------------|
| Source service | `"source"` \| `"from"` \| `"src"` |
| Target service | `"target"` \| `"to"` \| `"dst"` |
| Operation | `"op"` \| `"action"` \| `"type"` (default: `"add"`) |

---

### 3.8 `ingestion/parser.py` — EventParser

- Accepts **both** pre-parsed `dict` (benchmark path) and raw JSONL `str` (file path)
- Parses `ts` to UTC-aware `datetime` (handles `Z` suffix, `+HH:MM` offset, naive strings)
- **Lenient on unknown event kinds** — logs a warning and continues (forward-compatible with held-out L3 kinds)
- Tracks `parsed_count` / `error_count` for observability

---

### 3.9 `ingestion/buffer.py` — RecentEventsBuffer

- `collections.deque(maxlen=500)` — O(1) append, O(1) auto-eviction of oldest event
- `in_window(start_ts, end_ts)` — datetime comparison against normalised `ts`
- `for_services(names, start_ts, end_ts)` — multi-service filtered scan
- Completely ephemeral; rebuilt from DuckDB if needed

---

### 3.10 `ingestion/coordinator.py` — IngestCoordinator

**The transaction model (per event):**

```
begin()
  │
  ├─ raw_store.insert(event, ts, kind)          → event_id (UUID)
  │
  ├─ [deploy / log / metric / remediation]
  │    └─ node_store.get_or_create(service, ts) → node_id
  │
  ├─ [trace]
  │    ├─ node_store.get_or_create(svc, ts) for each span
  │    └─ edge_store.get_or_create(span[i] → span[i+1], CALLS, evidence=[event_id])
  │
  └─ [topology]
       └─ topology_handler.handle_*(raw, ts, event_id)  → DB ops only, returns NX ops
commit()

_apply_nx_ops([...])    ← NetworkX updates AFTER commit
buffer.push(event)
```

If the DB transaction throws, `rollback()` is called and NetworkX is never touched, keeping both stores consistent.

**Per-kind routing summary:**

| `kind` | DB mutations | NX ops |
|--------|-------------|--------|
| `deploy` | upsert node | add/update node |
| `log` | upsert node (if service present) | add/update node |
| `metric` | upsert node | add/update node |
| `trace` | upsert nodes for all spans; get_or_create CALLS edges | add nodes + edges |
| `topology/rename` | handle_rename (alias + canonical update) | rename_node |
| `topology/dependency-shift` | get_or_create or retire DEPENDENCY edge | add/update or retire_edge |
| `incident_signal` | raw store only | none |
| `remediation` | upsert node for target | add/update node |
| unknown | raw store only | none |

---

### 3.11 `engine.py` — Engine

Public interface matching the benchmark adapter contract:

```python
Engine.ingest(events: Iterable[dict]) -> None
Engine.reconstruct_context(signal: dict, mode: str) -> Context
Engine.close() -> None
```

**Phase 1 `reconstruct_context`:**
1. Parse incident signal timestamp
2. Open symmetric window: `[incident_ts − 60min, incident_ts + 60min]`
3. Extract anchor services from trigger string (e.g. `"alert:checkout-api/error-rate>5%"` → `["checkout-api"]`)
4. Expand via `_expand_services()` using 2-hop graph neighbourhood + all node aliases
5. Search buffer first (hot path), supplement with full window scan, DuckDB fallback if cold
6. Deduplicate, sort by `ts`, normalise `ts` back to ISO string for benchmark compatibility
7. Returns valid `Context` with populated `related_events`; `causal_chain`, `similar_past_incidents`, `suggested_remediations` are `[]` (Phase 2+)

---

## 4. Testing

### 4.1 Test: Worked Example from Problem Statement

File: `d:\Agents\DeepThink\smoke_test.py`

The test ingests all 7 events from the problem statement's canonical worked example:

```json
{"ts":"2026-05-10T14:21:30Z","kind":"deploy",   "service":"payments-svc","version":"v2.14.0","actor":"ci"}
{"ts":"2026-05-10T14:22:01Z","kind":"log",       "service":"checkout-api","level":"error","msg":"timeout calling payments-svc","trace_id":"abc123"}
{"ts":"2026-05-10T14:22:01Z","kind":"metric",    "service":"payments-svc","name":"latency_p99_ms","value":4820}
{"ts":"2026-05-10T14:22:08Z","kind":"trace",     "trace_id":"abc123","spans":[{"svc":"checkout-api","dur_ms":5012},{"svc":"payments-svc","dur_ms":4980}]}
{"ts":"2026-05-10T14:30:00Z","kind":"topology",  "change":"rename","from":"payments-svc","to":"billing-svc"}
{"ts":"2026-05-10T14:32:11Z","kind":"incident_signal","incident_id":"INC-714","trigger":"alert:checkout-api/error-rate>5%"}
{"ts":"2026-05-10T15:10:00Z","kind":"remediation","incident_id":"INC-714","action":"rollback","target":"billing-svc","version":"v2.13.4","outcome":"resolved"}
```

### 4.2 Assertions Checked

| # | Assertion | Check |
|---|-----------|-------|
| 1 | All 7 events ingested | `raw_events == 7` |
| 2 | 2 unique service nodes created | `nodes == 2` (checkout-api, payments/billing-svc) |
| 3 | 1 active CALLS edge in DuckDB | `active_edges == 1` |
| 4 | NetworkX graph in sync | `graph_nodes == 2, graph_edges == 1` |
| 5 | Rename preserves UUID identity | `resolve("payments-svc") == resolve("billing-svc")` |
| 6 | Old name still resolves | `resolve("payments-svc") is not None` |
| 7 | CALLS edge exists on checkout-api | `get_active_edges_for_node(checkout-api)` returns 1 CALLS edge |
| 8 | Context reconstruction finds events | `len(related_events) > 0` |

### 4.3 Test Results

```
Creating engine...
Ingesting events...

=== Engine Stats ===
  raw_events:    7
  nodes:         2
  active_edges:  1
  graph_nodes:   2
  graph_edges:   1
  buffer_size:   7
  parser_stats:  {'parsed': 7, 'errors': 0}

=== Topology-Independence Check ===
  payments-svc UUID : 6d67f109-77af-4216-a312-8417f88bea7e
  billing-svc  UUID : 6d67f109-77af-4216-a312-8417f88bea7e
  -> Same UUID confirmed (rename preserved identity)

=== Graph Edges ===
  Active CALLS edges for checkout-api: 1
  -> CALLS edge found: checkout-api → payments/billing-svc

=== Context Reconstruction ===
  related_events:         7 events
  causal_chain:           []
  similar_past_incidents: []
  suggested_remediations: []
  confidence:             0.5
  explain: [Phase 1] Incident INC-714 triggered by 'alert:checkout-api/error-rate>5%'.
           Retrieved 7 related events in window
           [2026-05-10T13:32:11+00:00, 2026-05-10T15:32:11+00:00].
           Event breakdown: 1×deploy, 1×incident_signal, 1×log, 1×metric,
           1×remediation, 1×topology, 1×trace.
           Causal inference and incident matching require Phase 2.

==================================================
SMOKE TEST PASSED
==================================================
```

**All 8 assertions passed. Exit code: 0.**

---

## 5. Key Invariants Verified

### Topology Independence (The Critical Test)

After the `topology/rename` event (`payments-svc → billing-svc`):

- `NodeStore._name_to_id["payments-svc"]` = `6d67f109-...`
- `NodeStore._name_to_id["billing-svc"]`  = `6d67f109-...` ← **same UUID**
- DuckDB `nodes` row: `canonical_name="billing-svc"`, `aliases=["payments-svc"]`
- NetworkX node `6d67f109-...`: `canonical_name="billing-svc"`, `aliases=["payments-svc"]`
- **Zero structural changes to NetworkX** (no node re-keying, no edge updates)

This is the foundation for the problem statement's non-negotiable requirement:
> *"If the engine has previously ingested a payments-svc deploy → rollback pattern, that match must surface despite the payments-svc → billing-svc rename."*

### Edge Bi-temporality

Retired edges are never deleted — they receive `retired_at` timestamp. This preserves the full history of dependency changes for temporal reasoning in Phase 2+.

### Transaction Safety

Every event is processed in a single DuckDB transaction covering `raw_events + nodes + edges`. If the DB transaction rolls back, the NetworkX graph is never modified (NX ops are applied only after `commit()`).

---

## 6. What Phase 2 Builds On

| Phase 2 Need | Phase 1 Foundation |
|---|---|
| Causal chain inference | `edges` table with `evidence_event_ids` + `EdgeKind.CAUSAL` | 
| Deploy → latency → error causality | `raw_events` provenance + NetworkX graph traversal |
| Incident matching across renames | `nodes.aliases` + `_name_to_id` — same UUID despite name changes |
| Remediation suggestion | `edges` with `EdgeKind.REMEDIATION_LINK` + historical `outcome` in raw_events |
| Behavioural fingerprinting | `raw_events` full JSON + `edge.confidence` + `edge.evidence_event_ids` |
| Point-in-time topology queries | `edges.first_seen_ts / last_seen_ts / retired_at` |

---

## 7. Dependencies

| Package | Version | Role |
|---------|---------|------|
| `duckdb` | `>=0.10.0, <2.0.0` | Source of truth: raw events, nodes, edges (bi-temporal) |
| `networkx` | `>=3.0, <4.0` | In-memory DiGraph for fast traversal; UUID-keyed |

No other external dependencies. Pure Python stdlib for everything else.
