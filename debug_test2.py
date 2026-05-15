from datetime import datetime, timedelta
import json
from persistent_context_engine import Engine, EngineConfig
from phase2_strain_test import create_incident_pattern

engine = Engine(EngineConfig(db_path=':memory:'))
base_ts = datetime.fromisoformat('2026-05-10T00:00:00+00:00')

families = {
    'latency_rollback': {
        'pattern': 'deploy_latency_rollback',
        'services': ['svc-a', 'svc-b'],
        'rename': ('svc-a', 'svc-a-renamed')
    },
    'error_restart': {
        'pattern': 'deploy_error_restart',
        'services': ['svc-c', 'svc-d'],
        'rename': ('svc-c', 'svc-c-renamed')
    }
}

for i, (family_name, config) in enumerate(families.items(), 1):
    print('=== family', family_name)
    svc1, svc2 = config['services']
    rename_from, rename_to = config['rename']
    create_incident_pattern(engine, base_ts + timedelta(hours=i*2), svc1, f'INC-F{i}-001', config['pattern'])
    engine.ingest([{
        'ts': (base_ts + timedelta(hours=i*2+1)).isoformat().replace('+00:00','Z'),
        'kind': 'topology', 'change': 'rename', 'from': rename_from, 'to': rename_to
    }])
    create_incident_pattern(engine, base_ts + timedelta(hours=i*2+2), rename_to if svc1 == rename_from else svc2, f'INC-F{i}-002', config['pattern'])
    p1 = engine._pattern_store.get_pattern_by_incident(f'INC-F{i}-001')
    p2 = engine._pattern_store.get_pattern_by_incident(f'INC-F{i}-002')
    print('p1', p1['id'], p1['family_id'], p1['fingerprint_hash'], p1['fingerprint_tuple'])
    print('p2', p2['id'], p2['family_id'], p2['fingerprint_hash'], p2['fingerprint_tuple'])
    if family_name == 'error_restart':
        from persistent_context_engine.incident_fingerprinter import IncidentFingerprint
        a = [tuple(x) for x in json.loads(p1['fingerprint_tuple'])]
        b = [tuple(x) for x in json.loads(p2['fingerprint_tuple'])]
        fp1 = IncidentFingerprint(a, p1['fingerprint_hash'], p1['trigger_node_id'], p1['window_start_ts'], p1['window_end_ts'], p1['event_count'])
        fp2 = IncidentFingerprint(b, p2['fingerprint_hash'], p2['trigger_node_id'], p2['window_start_ts'], p2['window_end_ts'], p2['event_count'])
        print('distance', fp1.edit_distance(fp2))
    print('all patterns so far:')
    for row in engine._pattern_store._cursor().execute('SELECT id, incident_id, fingerprint_hash, family_id FROM incident_patterns').fetchall():
        print('  row', row)
    print('all families:')
    for row in engine._pattern_store._cursor().execute('SELECT id, family_hash FROM incident_families').fetchall():
        print('  fam', row)
