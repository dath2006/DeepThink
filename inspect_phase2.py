from datetime import datetime, timedelta
import json
import random
from persistent_context_engine import Engine, EngineConfig
from phase2_strain_test import create_incident_pattern
from persistent_context_engine.incident_fingerprinter import IncidentFingerprint

engine = Engine(EngineConfig(db_path=':memory:'))
base_ts = datetime.fromisoformat('2026-05-10T00:00:00+00:00')
inc1 = create_incident_pattern(engine, base_ts, 'payments-svc', 'INC-001', 'deploy_latency_rollback')
renames = [
    (timedelta(hours=1), 'payments-svc', 'billing-svc'),
    (timedelta(hours=2), 'billing-svc', 'transactions-svc'),
    (timedelta(hours=3), 'transactions-svc', 'payment-gateway'),
]
for offset, f, t in renames:
    engine.ingest([
        {'ts': (base_ts+offset).isoformat().replace('+00:00','Z'), 'kind': 'topology', 'change': 'rename', 'from': f, 'to': t}
    ])
inc2 = create_incident_pattern(engine, base_ts+timedelta(hours=4), 'payment-gateway', 'INC-002', 'deploy_latency_rollback')

print('=== TEST 1 PATTERNS ===')
p1 = engine._pattern_store.get_pattern_by_incident('INC-001')
p2 = engine._pattern_store.get_pattern_by_incident('INC-002')
print('p1 hash', p1['fingerprint_hash'])
print('p2 hash', p2['fingerprint_hash'])
print('p1 family', p1['family_id'])
print('p2 family', p2['family_id'])
print('p1 tuple', p1['fingerprint_tuple'])
print('p2 tuple', p2['fingerprint_tuple'])
print('p1 node payments-svc', engine._node_store.resolve('payments-svc'))
print('p2 node payment-gateway', engine._node_store.resolve('payment-gateway'))

# inspect reconstruction fingerprint
test_signal = {
    'ts': inc2['signal_ts'],
    'kind': 'incident_signal',
    'incident_id': 'INC-002-TEST',
    'trigger': 'alert:payment-gateway/latency>4s',
    'service': 'payment-gateway',
}
ctx = engine.reconstruct_context(test_signal, mode='fast')
print('reconstruct similar count', len(ctx['similar_past_incidents']))
print('similar incidents', [s['incident_id'] for s in ctx['similar_past_incidents']])

# Compute fingerprint for reconstruction loop by stepping through engine internals
from persistent_context_engine.ingestion.parser import EventParser
parser = EventParser()
trigger_node_id = engine._node_store.resolve('payment-gateway')
window_start = parser._parse_timestamp(inc2['signal_ts']) - timedelta(minutes=30)
window_end = parser._parse_timestamp(inc2['signal_ts']) + timedelta(minutes=5)
rows = engine._raw_store.get_by_timerange(window_start, window_end, limit=100)
raw_events = []
for row in rows:
    raw = row['raw']
    if isinstance(raw.get('ts'), str):
        raw['ts'] = parser._parse_timestamp(raw['ts'])
    raw_events.append(raw)
reconstructed_fp = engine._fingerprinter.fingerprint(
    trigger_node_id=trigger_node_id,
    trigger_service='payment-gateway',
    incident_ts=parser._parse_timestamp(inc2['signal_ts']),
    events=raw_events,
    graph_manager=engine._graph,
    node_store=engine._node_store,
)
print('reconstructed hash', reconstructed_fp.structural_hash)
print('stored INC-002 hash', p2['fingerprint_hash'])

# TEST 2 inspect
print('\n=== TEST 2 PATTERNS ===')
engine2 = Engine(EngineConfig(db_path=':memory:'))
base_ts = datetime.fromisoformat('2026-05-10T00:00:00+00:00')
families = {
    'latency_rollback': {
        'pattern': 'deploy_latency_rollback',
        'services': ['svc-a', 'svc-b'],
        'rename': ('svc-a', 'svc-a-renamed')
    },
}
config = families['latency_rollback']
inc1 = create_incident_pattern(engine2, base_ts + timedelta(hours=2), 'svc-a', 'INC-F1-001', config['pattern'])
engine2.ingest([{
    'ts': (base_ts + timedelta(hours=3)).isoformat().replace('+00:00','Z'),
    'kind': 'topology', 'change': 'rename', 'from': config['rename'][0], 'to': config['rename'][1]
}])
inc2 = create_incident_pattern(engine2, base_ts + timedelta(hours=4), config['rename'][1], 'INC-F1-002', config['pattern'])
p1 = engine2._pattern_store.get_pattern_by_incident('INC-F1-001')
p2 = engine2._pattern_store.get_pattern_by_incident('INC-F1-002')
print('p1 hash', p1['fingerprint_hash'])
print('p2 hash', p2['fingerprint_hash'])
print('p1 tuple', p1['fingerprint_tuple'])
print('p2 tuple', p2['fingerprint_tuple'])
print('p1 family', p1['family_id'])
print('p2 family', p2['family_id'])
p1_elements = json.loads(p1['fingerprint_tuple'])
p2_elements = json.loads(p2['fingerprint_tuple'])
p1_fp = IncidentFingerprint(
    elements=p1_elements,
    structural_hash=p1['fingerprint_hash'],
    trigger_node_id='',
    window_start=datetime.min.replace(tzinfo=datetime.utcnow().astimezone().tzinfo),
    window_end=datetime.min.replace(tzinfo=datetime.utcnow().astimezone().tzinfo),
    event_count=len(p1_elements),
)
p2_fp = IncidentFingerprint(
    elements=p2_elements,
    structural_hash=p2['fingerprint_hash'],
    trigger_node_id='',
    window_start=datetime.min.replace(tzinfo=datetime.utcnow().astimezone().tzinfo),
    window_end=datetime.min.replace(tzinfo=datetime.utcnow().astimezone().tzinfo),
    event_count=len(p2_elements),
)
print('edit distance', p1_fp.edit_distance(p2_fp))
engine.close()
engine2.close()
