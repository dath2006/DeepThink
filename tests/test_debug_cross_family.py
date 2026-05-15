"""
Debug test 4 scenario.
"""
import sys
import random
from datetime import datetime, timedelta

sys.path.insert(0, "d:/Agents/DeepThink")

from persistent_context_engine import Engine, EngineConfig

def create_incident_pattern(engine, base_ts, service, incident_id, pattern_type):
    """Create an incident with a specific pattern type."""
    
    events = []
    
    if pattern_type == "deploy_latency_rollback":
        events = [
            {"ts": base_ts, "kind": "deploy", "service": service, "version": "v1.0.0", "actor": "ci"},
            {"ts": base_ts + timedelta(minutes=2), "kind": "metric", "service": service,
             "name": "latency_p99_ms", "value": 5000 + random.randint(-500, 500)},
            {"ts": base_ts + timedelta(minutes=5), "kind": "incident_signal",
             "incident_id": incident_id, "trigger": f"alert:{service}/latency>4s", "service": service},
            {"ts": base_ts + timedelta(minutes=15), "kind": "remediation",
             "incident_id": incident_id, "action": "rollback", "target": service, "outcome": "resolved"},
        ]
    elif pattern_type == "config_memory_scaleup":
        events = [
            {"ts": base_ts, "kind": "deploy", "service": service, "version": "v1.0.0", "actor": "config-bot"},
            {"ts": base_ts + timedelta(minutes=3), "kind": "metric", "service": service,
             "name": "memory_usage", "value": 92},
            {"ts": base_ts + timedelta(minutes=5), "kind": "incident_signal",
             "incident_id": incident_id, "trigger": f"alert:{service}/memory>90%", "service": service},
            {"ts": base_ts + timedelta(minutes=10), "kind": "remediation",
             "incident_id": incident_id, "action": "scale_up", "target": service, "outcome": "resolved"},
        ]
    
    # Format timestamps
    for e in events:
        if isinstance(e["ts"], datetime):
            e["ts"] = e["ts"].isoformat().replace("+00:00", "Z")
    
    engine.ingest(events)
    return {"signal_ts": events[-2]["ts"] if len(events) >= 2 else events[-1]["ts"]}


def test_cross_family():
    """Exact replica of test 4."""
    engine = Engine(EngineConfig(db_path=":memory:"))
    base_ts = datetime.fromisoformat("2026-05-10T00:00:00+00:00")
    
    # Create two completely different patterns on the same service
    print("\n[4.1] Creating Pattern A: Deploy -> Latency -> Rollback")
    create_incident_pattern(engine, base_ts, "svc-mixed", "MIX-A-001", 
                           "deploy_latency_rollback")
    
    print("[4.2] Creating Pattern B: Config Change -> Memory Spike -> Scale Up")
    create_incident_pattern(engine, base_ts + timedelta(hours=2), "svc-mixed", "MIX-B-001",
                           "config_memory_scaleup")
    
    p_a = engine._pattern_store.get_pattern_by_incident("MIX-A-001")
    p_b = engine._pattern_store.get_pattern_by_incident("MIX-B-001")
    
    print(f"\n[4.3] Family assignment:")
    print(f"  Pattern A family: {p_a['family_id']}")
    print(f"  Pattern B family: {p_b['family_id']}")
    print(f"  A hash: {p_a['fingerprint_hash']}")
    print(f"  B hash: {p_b['fingerprint_hash']}")
    print(f"  Different families: {p_a['family_id'] != p_b['family_id']}")
    
    # Second incident of each type
    print("\n[4.4] Creating second incidents...")
    create_incident_pattern(engine, base_ts + timedelta(hours=4), "svc-mixed", "MIX-A-002",
                           "deploy_latency_rollback")
    create_incident_pattern(engine, base_ts + timedelta(hours=6), "svc-mixed", "MIX-B-002",
                           "config_memory_scaleup")
    
    p_a2 = engine._pattern_store.get_pattern_by_incident("MIX-A-002")
    p_b2 = engine._pattern_store.get_pattern_by_incident("MIX-B-002")
    
    print(f"\n[4.5] Consistency check:")
    print(f"  MIX-A-001 family: {p_a['family_id']}, MIX-A-002 family: {p_a2['family_id']}")
    print(f"  MIX-B-001 family: {p_b['family_id']}, MIX-B-002 family: {p_b2['family_id']}")
    print(f"  A2 hash: {p_a2['fingerprint_hash']}")
    print(f"  B2 hash: {p_b2['fingerprint_hash']}")
    
    print(f"\n  A1 == A2: {p_a['family_id'] == p_a2['family_id']}")
    print(f"  B1 == B2: {p_b['family_id'] == p_b2['family_id']}")
    
    engine.close()

if __name__ == "__main__":
    test_cross_family()
