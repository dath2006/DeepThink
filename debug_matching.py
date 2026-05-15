"""Debug script to understand family matching behavior."""
import sys
sys.path.insert(0, "Anvil-P-E/bench-p02-context")

from adapters.persistent_context import PersistentContextAdapter
from generator import generate, GenConfig, Dataset
from metrics import score_match, score_remediation, _family_from_incident_id
import json

ds = generate(GenConfig(seed=42, n_services=6, days=7))
adapter = PersistentContextAdapter()
adapter.ingest(ds.train_events)
adapter.ingest(ds.eval_events)

stats = adapter._engine.stats()
print(f"Patterns: {stats['patterns']}, Families: {stats['families']}")

# Show how engine families map to benchmark families
cursor = adapter._engine._pattern_store._cursor()
rows = cursor.execute("""
    SELECT p.family_id, p.incident_id
    FROM incident_patterns p
    ORDER BY p.family_id, p.incident_id
""").fetchall()

# Build mapping: engine_family_id -> set of benchmark_family_tags
from collections import defaultdict
engine_to_bench = defaultdict(set)
for fam_id, inc_id in rows:
    bench_fam = _family_from_incident_id(inc_id)
    engine_to_bench[fam_id].add(bench_fam)

print(f"\nEngine family -> Benchmark families:")
for efam, bfams in sorted(engine_to_bench.items(), key=lambda x: min(x[1])):
    print(f"  {efam[:12]}... -> bench families {sorted(bfams)}")

# Test each signal
signals = ds.eval_signals
gt = ds.ground_truth
print(f"\nEval: {len(signals)} signals, {len(gt)} ground truth entries")

for i, (sig, truth) in enumerate(zip(signals, gt)):
    signal = {
        "incident_id": sig["incident_id"],
        "ts": sig["ts"],
        "trigger": sig.get("trigger", ""),
        "service": sig.get("service", ""),
    }
    ctx = adapter.reconstruct_context(signal, mode="fast")
    in_top_k, precision = score_match(ctx, truth, k=5)
    
    matches = ctx["similar_past_incidents"]
    match_bench_fams = [_family_from_incident_id(m["incident_id"]) for m in matches]
    target_fam = truth["family"]
    
    status = "HIT" if in_top_k else "MISS"
    print(f"  [{status}] sig={sig['incident_id']} target_fam={target_fam} "
          f"match_fams={match_bench_fams} prec={precision:.2f}")
    
    if not in_top_k:
        # Show engine families of matches
        match_engine_fams = set()
        for m in matches:
            pat = adapter._engine._pattern_store.get_pattern_by_incident(m["incident_id"])
            if pat:
                match_engine_fams.add(pat["family_id"][:8])
        print(f"    engine families in output: {match_engine_fams}")
        print(f"    need bench family {target_fam}")

# Show trigger service distribution
trig_rows = cursor.execute("""
    SELECT trigger_node_id, COUNT(*) as cnt, GROUP_CONCAT(incident_id, ', ')
    FROM incident_patterns
    GROUP BY trigger_node_id
    ORDER BY cnt DESC
""").fetchall()
print(f"\nTrigger service distribution ({len(trig_rows)} distinct triggers):")
for trig_id, cnt, inc_ids in trig_rows:
    trig_name = adapter._engine._node_store.get_canonical_name(trig_id) or "?"
    bench_fams = set()
    for iid in inc_ids.split(", "):
        bf = _family_from_incident_id(iid.strip())
        bench_fams.add(bf)
    print(f"  {trig_name} ({trig_id[:8]}...) : {cnt} patterns, bench_fams={sorted(bench_fams)}")

# Show fingerprint hash distribution
rows2 = cursor.execute("""
    SELECT fingerprint_hash, COUNT(*) as cnt, GROUP_CONCAT(incident_id, ', ')
    FROM incident_patterns
    GROUP BY fingerprint_hash
    ORDER BY cnt DESC
""").fetchall()
print(f"\nFingerprint hash distribution ({len(rows2)} distinct hashes):")
for fhash, cnt, inc_ids in rows2:
    bench_fams = set()
    for iid in inc_ids.split(", "):
        bf = _family_from_incident_id(iid.strip())
        bench_fams.add(bf)
    print(f"  {fhash[:16]}... : {cnt} patterns, bench_fams={sorted(bench_fams)}")

# Show a sample fingerprint tuple
sample = cursor.execute("""
    SELECT incident_id, fingerprint_tuple, fingerprint_hash
    FROM incident_patterns LIMIT 3
""").fetchall()
for inc_id, fp_tuple, fp_hash in sample:
    elements = json.loads(fp_tuple) if fp_tuple else []
    print(f"\n  {inc_id} hash={fp_hash[:16]} elements({len(elements)}):")
    for el in elements[:5]:
        print(f"    {el}")

adapter.close()
