"""Diagnose why T1.1 still fails after family-scoping fix."""
import sys
sys.path.insert(0, '.')
sys.path.insert(0, 'Anvil-P-E/bench-p02-context')

from persistent_context_engine import Engine, EngineConfig
from generator import GenConfig, generate
from metrics import score_match

cfg = GenConfig(seed=777, n_services=30, days=14, deploys=80,
                topology_mutations=25, incidents_train=60,
                incidents_eval=20, incident_families=20,
                background_density=100)
ds = generate(cfg)

engine = Engine(EngineConfig(db_path=":memory:", buffer_size=1000))
engine.ingest(ds.train_events)
engine.ingest(ds.eval_events)

# Run first 5 eval signals and show what matches came back
hits = 0
for sig, gt in list(zip(ds.eval_signals, ds.ground_truth))[:5]:
    signal = {
        "incident_id": sig["incident_id"],
        "ts": sig["ts"],
        "trigger": sig.get("trigger", ""),
        "service": sig.get("service", ""),
    }
    ctx = engine.reconstruct_context(signal, mode="fast")
    matched_ids = [m["incident_id"] for m in ctx.get("similar_incidents", [])]
    expected_fam = gt["family"]
    # What families do the matches belong to?
    cur = engine._pattern_store._cursor()
    match_fams = []
    for mid in matched_ids:
        row = cur.execute(
            "SELECT family_id FROM incident_patterns WHERE incident_id = ? LIMIT 1", [mid]
        ).fetchone()
        match_fams.append(str(row[0])[:8] if row else "?")

    # What is the correct family UUID?
    # Find training incidents with the same generator family number
    train_same_fam = [e["incident_id"] for e in ds.train_events
                      if e.get("kind") == "incident_signal"
                      and e["incident_id"].rsplit("-", 1)[-1] == str(expected_fam)]
    correct_fam_uuids = set()
    for tid in train_same_fam:
        row = cur.execute(
            "SELECT family_id FROM incident_patterns WHERE incident_id = ? LIMIT 1", [tid]
        ).fetchone()
        if row:
            correct_fam_uuids.add(str(row[0])[:8])

    in_top5, prec = score_match(ctx, gt, k=5)
    print(f"\nSignal: {sig['incident_id']}  gen_family={expected_fam}  correct_fam_uuids={correct_fam_uuids}")
    print(f"  top5_matches: {list(zip(matched_ids, match_fams))}")
    print(f"  recall_hit={in_top5}  precision={prec:.3f}")

engine.close()
