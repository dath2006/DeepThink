"""Quick perf test: time ingestion and reconstruction."""
import sys, time
sys.path.insert(0, "d:/Agents/DeepThink/Anvil-P-E/bench-p02-context")
sys.path.insert(0, "d:/Agents/DeepThink")
from generator import GenConfig, generate
from persistent_context_engine import Engine, EngineConfig

# Full benchmark config
cfg = GenConfig(seed=42, n_services=12, days=7)
print(f"Generating dataset...")
t0 = time.time()
ds = generate(cfg)
print(f"  Generated in {time.time()-t0:.1f}s")
print(f"  train={len(ds.train_events)}, eval={len(ds.eval_events)}, signals={len(ds.eval_signals)}")

engine = Engine(EngineConfig(db_path=":memory:"))

print(f"Ingesting train events ({len(ds.train_events)})...")
t0 = time.time()
# Ingest in batches to see progress
batch = 1000
for i in range(0, len(ds.train_events), batch):
    engine.ingest(ds.train_events[i:i+batch])
    elapsed = time.time() - t0
    rate = (i+batch) / elapsed if elapsed > 0 else 0
    print(f"  {min(i+batch, len(ds.train_events)):>6d}/{len(ds.train_events)}  {elapsed:.1f}s  ({rate:.0f} evt/s)")
print(f"  Train done in {time.time()-t0:.1f}s")

print(f"Ingesting eval events ({len(ds.eval_events)})...")
t1 = time.time()
for i in range(0, len(ds.eval_events), batch):
    engine.ingest(ds.eval_events[i:i+batch])
    elapsed = time.time() - t1
    rate = (i+batch) / elapsed if elapsed > 0 else 0
    print(f"  {min(i+batch, len(ds.eval_events)):>6d}/{len(ds.eval_events)}  {elapsed:.1f}s  ({rate:.0f} evt/s)")
print(f"  Eval done in {time.time()-t1:.1f}s")

print(f"Reconstructing context for {len(ds.eval_signals)} signals...")
t2 = time.time()
for i, sig in enumerate(ds.eval_signals):
    signal = {"incident_id": sig["incident_id"], "ts": sig["ts"],
              "trigger": sig.get("trigger",""), "service": sig.get("service","")}
    ctx = engine.reconstruct_context(signal, mode="fast")
    print(f"  Signal {i+1}/{len(ds.eval_signals)}: {time.time()-t2:.1f}s")
print(f"  Reconstruction done in {time.time()-t2:.1f}s")

engine.close()
print(f"TOTAL: {time.time()-t0:.1f}s")
