"""
Diagnostic: run one seed of L3, intercept _find_similar_incidents and _should_abstain
to understand why genuine incidents are returning empty results.
"""
import sys, json, logging
sys.path.insert(0, 'D:/Agents/DeepThink')
sys.path.insert(0, 'D:/Agents/DeepThink/Anvil-P-E/bench-p02-context')

from persistent_context_engine import Engine, EngineConfig
from generator import generate, stretch_config, Dataset

logging.disable(logging.CRITICAL)

SEED = 314159
cfg = stretch_config(SEED)

engine = Engine(EngineConfig(db_path=":memory:", buffer_size=5000, ingest_batch_size=1000))
dataset = generate(cfg)
train_events = dataset.train_events
eval_signals = dataset.eval_signals
engine.ingest(train_events)
engine.ingest(dataset.eval_events)
print(f"Ingested {len(train_events)} train + {len(dataset.eval_events)} eval events")

# Check how many patterns were stored
cur = engine._pattern_store._cursor()
n_patterns = cur.execute("SELECT COUNT(*) FROM incident_patterns").fetchone()[0]
n_families = cur.execute("SELECT COUNT(*) FROM incident_families").fetchone()[0]
n_with_fam = cur.execute("SELECT COUNT(*) FROM incident_patterns WHERE family_id IS NOT NULL").fetchone()[0]
n_remeds = cur.execute("SELECT COUNT(*) FROM remediation_history").fetchone()[0]
print(f"Patterns: {n_patterns}  Families: {n_families}  With family_id: {n_with_fam}  Remediations: {n_remeds}")

# Sample fingerprints from a few patterns
rows = cur.execute(
    "SELECT incident_id, fingerprint_tuple, trigger_node_id, family_id FROM incident_patterns LIMIT 5"
).fetchall()
print("\nSample stored patterns:")
for inc_id, fp_tuple, trig_id, fam_id in rows:
    elements = json.loads(fp_tuple)
    print(f"  {inc_id}  elements={len(elements)}  family={str(fam_id)[:8] if fam_id else None}  fp={elements[:2]}")

# Now run eval and log similarity scores for first 5 genuine missed incidents
print("\n=== Eval signal similarity scores ===")
genuine_missed = 0
for sig in eval_signals[:30]:
    inc_id = sig.get("incident_id","")
    if inc_id.startswith("DEC-"):
        continue

    ctx = engine.reconstruct_context(sig, mode="fast")
    matches = ctx["similar_past_incidents"]

    # Also get raw scores BEFORE abstention by calling internal methods
    from persistent_context_engine.ingestion.parser import EventParser
    from datetime import datetime, timezone, timedelta
    parser = EventParser()
    try:
        norm = parser.normalise(sig)
    except Exception:
        norm = dict(sig)
    inc_ts = norm.get("ts", datetime.now(timezone.utc))

    svc = norm.get("service") or ""
    trig_nid = engine._node_store.resolve(svc) if svc else None

    fp = None
    if trig_nid:
        mp = engine._cfg.for_mode("fast")
        expanded = engine._expand_services([svc], at_timestamp=inc_ts, max_hops=mp.graph_hops)
        from datetime import timedelta
        start_ts = inc_ts - timedelta(minutes=mp.context_window_minutes)
        end_ts = inc_ts + timedelta(minutes=5)
        db_rows = engine._raw_store.get_by_services(list(expanded), start_ts, end_ts)
        related = [r["raw"] for r in db_rows]
        fp = engine._fingerprinter.fingerprint(
            trigger_node_id=trig_nid,
            trigger_service=svc,
            incident_ts=inc_ts,
            events=related,
            graph_manager=engine._graph,
            node_store=engine._node_store,
            temporal_view=engine._temporal_view,
            time_bucket_minutes=mp.time_bucket_minutes,
        )

    n_elements = len(fp.elements) if fp else 0
    n_returned = len(matches)

    if n_returned == 0 and genuine_missed < 2:
        genuine_missed += 1
        print(f"\n  MISSED: {inc_id}  fp_elements={n_elements}  svc={svc}  trig_nid={str(trig_nid)[:8] if trig_nid else None}")
        if fp is not None:
            print(f"    fp elements={fp.elements}")
            # Check what raw events exist in the window for this service
            from datetime import timedelta
            mp2 = engine._cfg.for_mode('fast')
            st = inc_ts - timedelta(minutes=mp2.context_window_minutes)
            et = inc_ts + timedelta(minutes=5)
            db_rows2 = engine._raw_store.get_by_services([svc], st, et)
            print(f"    raw_store events in window for svc={svc}: {len(db_rows2)}")
            for rr in db_rows2[:5]:
                rv = rr.get('raw', {})
                ev_svc = rv.get('service','')
                resolved = engine._node_store.resolve(ev_svc)
                print(f"      kind={rv.get('kind')} svc={ev_svc} resolved_nid={str(resolved)[:8] if resolved else None} ts={str(rv.get('ts',''))[:16]}")
            # Also check buffer
            from datetime import timezone
            buf_evs = engine._buffer.for_services([svc], st, et)
            print(f"    buffer events for svc={svc}: {len(buf_evs)}")
            # Get raw candidates with scores
            all_rows = engine._pattern_store._cursor().execute(
                "SELECT incident_id, trigger_node_id, family_id, fingerprint_hash, fingerprint_tuple, created_at FROM incident_patterns WHERE created_at < ? ORDER BY created_at DESC",
                [inc_ts]
            ).fetchall()
            print(f"    candidate_patterns={len(all_rows)}")
            scored = []
            for r_inc_id, r_trig, r_fam, r_hash, r_tuple, _ in all_rows[:50]:
                sim = engine._compute_pattern_similarity(fp, r_hash, r_tuple)
                scored.append((sim, r_inc_id, r_fam))
            scored.sort(reverse=True)
            print(f"    top-5 sim: {[(round(s,3), fid[:8] if fid else None) for s,_,fid in scored[:5]]}")
