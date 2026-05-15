import sys
sys.path.insert(0, '.')
sys.path.insert(0, 'Anvil-P-E/bench-p02-context')

from persistent_context_engine import Engine, EngineConfig
from generator import GenConfig, generate

cfg = GenConfig(seed=777, n_services=30, days=14, deploys=80,
                topology_mutations=25, incidents_train=60,
                incidents_eval=20, incident_families=20,
                background_density=100)
ds = generate(cfg)

train_sigs = [e for e in ds.train_events if e.get("kind") == "incident_signal"]
print(f"train incident signals: {len(train_sigs)}")

engine = Engine(EngineConfig(db_path=":memory:", buffer_size=1000))
engine.ingest(ds.train_events)

cur = engine._pattern_store._cursor()
n_pat   = cur.execute("SELECT COUNT(*) FROM incident_patterns").fetchone()[0]
n_fam   = cur.execute("SELECT COUNT(*) FROM incident_families").fetchone()[0]
n_hash  = cur.execute("SELECT COUNT(DISTINCT fingerprint_hash) FROM incident_patterns").fetchone()[0]
n_trig  = cur.execute("SELECT COUNT(DISTINCT trigger_node_id) FROM incident_patterns").fetchone()[0]
n_null_trig = cur.execute("SELECT COUNT(*) FROM incident_patterns WHERE trigger_node_id IS NULL OR trigger_node_id = ''").fetchone()[0]

print(f"patterns stored:             {n_pat}")
print(f"distinct fingerprint hashes: {n_hash}")
print(f"distinct trigger_node_ids:   {n_trig}")
print(f"patterns with null trigger:  {n_null_trig}")
print(f"families created:            {n_fam}")

# What do families look like?
fam_rows = cur.execute(
    "SELECT id, family_hash, trigger_node_id, incident_count FROM incident_families ORDER BY incident_count DESC LIMIT 10"
).fetchall()
print("\nTop families:")
for fid, fhash, tnode, cnt in fam_rows:
    print(f"  fam={fid[:8]}... hash={fhash[:12]}... trig={str(tnode)[:12]}... count={cnt}")

# Check if trigger_node_id is actually being set on patterns
pat_sample = cur.execute(
    "SELECT incident_id, fingerprint_hash, trigger_node_id, family_id FROM incident_patterns LIMIT 8"
).fetchall()
print("\nPattern samples:")
for inc, fhash, tnode, fam in pat_sample:
    print(f"  inc={inc} hash={fhash[:10]}... trig={str(tnode)[:12]}... fam={str(fam)[:8]}...")

engine.close()
