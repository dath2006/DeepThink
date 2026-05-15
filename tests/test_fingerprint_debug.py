"""
Debug test for fingerprint consistency.
"""
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "d:/Agents/DeepThink")

from persistent_context_engine import Engine, EngineConfig

def test_fingerprint_consistency():
    """Test that same pattern type produces same fingerprint hash."""
    engine = Engine(EngineConfig(db_path=":memory:"))
    base_ts = datetime.fromisoformat("2026-05-10T14:00:00+00:00")
    
    # Create two incidents of same pattern type on same service
    for i, offset in enumerate([0, 4]):  # 0 hours and 4 hours later
        ts = base_ts + timedelta(hours=offset)
        events = [
            {"ts": ts.isoformat(), "kind": "deploy", "service": "svc-mixed", "version": "v1.0.0", "actor": "ci"},
            {"ts": (ts + timedelta(minutes=2)).isoformat(), "kind": "metric", "service": "svc-mixed",
             "name": "latency_p99_ms", "value": 5000},
            {"ts": (ts + timedelta(minutes=5)).isoformat(), "kind": "incident_signal",
             "incident_id": f"TEST-{i+1}", "trigger": "alert:svc-mixed/latency>4s", "service": "svc-mixed"},
            {"ts": (ts + timedelta(minutes=15)).isoformat(), "kind": "remediation",
             "incident_id": f"TEST-{i+1}", "action": "rollback", "target": "svc-mixed", "outcome": "resolved"},
        ]
        engine.ingest(events)
    
    # Check patterns
    p1 = engine._pattern_store.get_pattern_by_incident("TEST-1")
    p2 = engine._pattern_store.get_pattern_by_incident("TEST-2")
    
    print(f"Pattern 1 hash: {p1['fingerprint_hash']}")
    print(f"Pattern 2 hash: {p2['fingerprint_hash']}")
    print(f"Same hash: {p1['fingerprint_hash'] == p2['fingerprint_hash']}")
    print(f"Family 1: {p1['family_id']}")
    print(f"Family 2: {p2['family_id']}")
    print(f"Same family: {p1['family_id'] == p2['family_id']}")
    
    # Check tuple contents
    import json
    t1 = json.loads(p1['fingerprint_tuple'])
    t2 = json.loads(p2['fingerprint_tuple'])
    print(f"\nTuple 1: {t1}")
    print(f"Tuple 2: {t2}")
    
    engine.close()

if __name__ == "__main__":
    test_fingerprint_consistency()
