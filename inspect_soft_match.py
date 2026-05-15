from datetime import datetime, timedelta
import json
from persistent_context_engine import Engine, EngineConfig
from phase2_strain_test import create_incident_pattern
from persistent_context_engine.incident_fingerprinter import Fingerprinter

engine = Engine(EngineConfig(db_path=':memory:'))
base_ts = datetime.fromisoformat('2026-05-10T00:00:00+00:00')
create_incident_pattern(engine, base_ts, 'svc-a', 'INC-F1-001', 'deploy_latency_rollback')
print('raw events after first incident', engine._raw_store.get_by_timerange(base_ts - timedelta(minutes=1), base_ts + timedelta(minutes=6), limit=10))
engine.ingest([{'ts': (base_ts + timedelta(hours=1)).isoformat().replace('+00:00','Z'), 'kind':'topology', 'change':'rename', 'from':'svc-a', 'to':'svc-a-renamed'}])

# Build a second incident fingerprint manually.
raw_events = [
    {'ts': base_ts + timedelta(hours=4), 'kind': 'deploy', 'service': 'svc-a-renamed', 'version': 'v1.0.0', 'actor': 'ci'},
    {'ts': base_ts + timedelta(hours=4, minutes=2), 'kind': 'metric', 'service': 'svc-a-renamed', 'name': 'latency_p99_ms', 'value': 5200},
    {'ts': base_ts + timedelta(hours=4, minutes=5), 'kind': 'incident_signal', 'incident_id': 'INC-F1-002', 'trigger': 'alert:svc-a-renamed/latency>4s', 'service': 'svc-a-renamed'},
    {'ts': base_ts + timedelta(hours=4, minutes=15), 'kind': 'remediation', 'incident_id': 'INC-F1-002', 'action': 'rollback', 'target': 'svc-a-renamed', 'outcome': 'resolved'},
]
trigger_node_id = engine._node_store.resolve('svc-a-renamed')
second_fp = Fingerprinter().fingerprint(
    trigger_node_id=trigger_node_id,
    trigger_service='svc-a-renamed',
    incident_ts=raw_events[2]['ts'],
    events=raw_events,
    graph_manager=engine._graph,
    node_store=engine._node_store,
)
pattern_id = engine._pattern_store.insert_pattern(
    incident_id='INC-F1-002',
    fingerprint_hash=second_fp.structural_hash,
    fingerprint_tuple=second_fp.to_tuple_string(),
    trigger_node_id=trigger_node_id,
    window_start=second_fp.window_start,
    window_end=second_fp.window_end,
    event_count=second_fp.event_count,
)
print('inserted pattern id', pattern_id)
existing_rows = engine._pattern_store._cursor().execute("SELECT id,fingerprint_tuple,family_id FROM incident_patterns WHERE family_id IS NOT NULL").fetchall()
print('existing rows', existing_rows)
print('new tuple', second_fp.to_tuple_string())
if existing_rows:
    existing_elements = json.loads(existing_rows[0][1])
    existing_fp = Fingerprinter().fingerprint(
        trigger_node_id=trigger_node_id,
        trigger_service='svc-a-renamed',
        incident_ts=raw_events[2]['ts'],
        events=[{'ts': raw_events[0]['ts'], 'kind': 'deploy', 'service': 'svc-a-renamed', 'version': 'v1.0.0', 'actor': 'ci'}, {'ts': raw_events[1]['ts'], 'kind': 'metric', 'service': 'svc-a-renamed', 'name': 'latency_p99_ms', 'value': 5200}, {'ts': raw_events[2]['ts'], 'kind': 'incident_signal', 'incident_id': 'INC-F1-002', 'trigger': 'alert:svc-a-renamed/latency>4s', 'service': 'svc-a-renamed'},],
        graph_manager=engine._graph,
        node_store=engine._node_store,
    )
    print('existing_fp hash', existing_fp.structural_hash)
try:
    from persistent_context_engine.incident_fingerprinter import IncidentFingerprint as IF
    new_fp = IF(elements=json.loads(second_fp.to_tuple_string()), structural_hash='', trigger_node_id='', window_start=datetime.min.replace(tzinfo=datetime.utcfromtimestamp(0).astimezone().tzinfo), window_end=datetime.min.replace(tzinfo=datetime.utcfromtimestamp(0).astimezone().tzinfo), event_count=len(json.loads(second_fp.to_tuple_string())))
    old_fp = IF(elements=json.loads(existing_rows[0][1]), structural_hash='', trigger_node_id='', window_start=datetime.min.replace(tzinfo=datetime.utcfromtimestamp(0).astimezone().tzinfo), window_end=datetime.min.replace(tzinfo=datetime.utcfromtimestamp(0).astimezone().tzinfo), event_count=len(json.loads(existing_rows[0][1])))
    print('distance', new_fp.edit_distance(old_fp))
except Exception as e:
    print('distance error', e)
returned = engine._pattern_store.find_or_create_family(second_fp, pattern_id, raw_events[2]['ts'])
print('returned from family lookup', returned)
print('all rows', engine._pattern_store._cursor().execute('SELECT id,incident_id,fingerprint_hash,family_id,similarity_score FROM incident_patterns').fetchall())
engine.close()
