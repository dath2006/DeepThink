"""Quick smoke test for Phase 1 — uses the exact worked example from the problem statement."""
import sys
import json

sys.path.insert(0, "d:/Agents/DeepThink")

from persistent_context_engine import Engine, EngineConfig

SAMPLE_EVENTS = [
    {"ts": "2026-05-10T14:21:30Z", "kind": "deploy",   "service": "payments-svc", "version": "v2.14.0", "actor": "ci"},
    {"ts": "2026-05-10T14:22:01Z", "kind": "log",      "service": "checkout-api", "level": "error", "msg": "timeout calling payments-svc", "trace_id": "abc123"},
    {"ts": "2026-05-10T14:22:01Z", "kind": "metric",   "service": "payments-svc", "name": "latency_p99_ms", "value": 4820},
    {"ts": "2026-05-10T14:22:08Z", "kind": "trace",    "trace_id": "abc123", "spans": [{"svc": "checkout-api", "dur_ms": 5012}, {"svc": "payments-svc", "dur_ms": 4980}]},
    {"ts": "2026-05-10T14:30:00Z", "kind": "topology", "change": "rename", "from": "payments-svc", "to": "billing-svc"},
    {"ts": "2026-05-10T14:32:11Z", "kind": "incident_signal", "incident_id": "INC-714", "trigger": "alert:checkout-api/error-rate>5%"},
    {"ts": "2026-05-10T15:10:00Z", "kind": "remediation", "incident_id": "INC-714", "action": "rollback", "target": "billing-svc", "version": "v2.13.4", "outcome": "resolved"},
]

print("Creating engine...")
engine = Engine(EngineConfig(db_path=":memory:"))

print("Ingesting events...")
engine.ingest(SAMPLE_EVENTS)

stats = engine.stats()
print("\n=== Engine Stats ===")
for k, v in stats.items():
    print(f"  {k}: {v}")

# Verify the rename worked: both names should resolve to same UUID
from persistent_context_engine.storage.node_store import NodeStore
ns = engine._node_store
uuid_old = ns.resolve("payments-svc")
uuid_new = ns.resolve("billing-svc")
print(f"\n=== Topology-Independence Check ===")
print(f"  payments-svc UUID : {uuid_old}")
print(f"  billing-svc  UUID : {uuid_new}")
assert uuid_old == uuid_new, "FAIL: rename did not preserve UUID identity"
assert uuid_old is not None,  "FAIL: payments-svc was never registered"
print("  -> Same UUID confirmed (rename preserved identity)")

# Verify CALLS edge was created from the trace spans
from persistent_context_engine.storage.edge_store import EdgeStore
checkout_id = ns.resolve("checkout-api")
payments_id = ns.resolve("payments-svc")  # same as billing-svc
edges = engine._edge_store.get_active_edges_for_node(checkout_id)
calls_edges = [e for e in edges if e["edge_kind"] == "calls"]
print(f"\n=== Graph Edges ===")
print(f"  Active CALLS edges for checkout-api: {len(calls_edges)}")
assert len(calls_edges) >= 1, "FAIL: CALLS edge from trace was not created"
print("  -> CALLS edge found: checkout-api -> payments/billing-svc")

# Verify context reconstruction returns related events
signal = {
    "ts": "2026-05-10T14:32:11Z",
    "kind": "incident_signal",
    "incident_id": "INC-714",
    "trigger": "alert:checkout-api/error-rate>5%",
}
ctx = engine.reconstruct_context(signal, mode="fast")
print(f"\n=== Context Reconstruction ===")
print(f"  related_events:         {len(ctx['related_events'])} events")
print(f"  causal_chain:           {ctx['causal_chain']}")
print(f"  similar_past_incidents: {ctx['similar_past_incidents']}")
print(f"  suggested_remediations: {ctx['suggested_remediations']}")
print(f"  confidence:             {ctx['confidence']}")
print(f"  explain:                {ctx['explain']}")
assert len(ctx["related_events"]) > 0, "FAIL: No related events found"

engine.close()

print("\n" + "="*50)
print("SMOKE TEST PASSED")
print("="*50)
