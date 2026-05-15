"""Verify benchmark topology events are processed correctly."""
import sys
sys.path.insert(0, 'd:/Agents/DeepThink')
sys.path.insert(0, 'd:/Agents/DeepThink/Anvil-P-E/bench-p02-context')

from generator import GenConfig, generate
from persistent_context_engine import Engine, EngineConfig

cfg = GenConfig(seed=42, n_services=6, days=2)
ds = generate(cfg)

engine = Engine(EngineConfig(db_path=':memory:'))
engine.ingest(ds.train_events)
engine.ingest(ds.eval_events)

stats = engine.stats()
print('=== After benchmark ingest ===')
for k, v in stats.items():
    print(f'  {k}: {v}')

ns = engine._node_store
print()
print(f'=== Nodes: {ns.count()} ===')
renamed = []
for rec in ns.get_all():
    if rec['aliases']:
        renamed.append(f"  {rec['canonical_name']} (aliases: {rec['aliases']})")
print('Renamed nodes:')
for r in renamed:
    print(r)
if not renamed:
    print('  (none — topology mutations may not have been processed)')

engine.close()
