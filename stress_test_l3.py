"""
L3-Style Adversarial Stress Test for Persistent Context Engine.
 
Simulates what the held-out L3 evaluation might look like:
1. Extreme generator parameters (more services, denser drift, more families)
2. Hand-crafted adversarial scenarios the generator can't produce:
   - Cascading rename chains (A→B→C→D)
   - Correlated multi-service outages
   - Families morphed across BOTH rename AND dependency shifts
   - Rapid topology changes right before incident
   - Services with 5+ aliases
   - Back-to-back incidents on same service
   - Interleaved families on shared services
 
Usage:
    python stress_test_l3.py
"""
from __future__ import annotations
 
import sys
import time
import statistics
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple
from collections import Counter
from dataclasses import dataclass
 
_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))
 
# Try multiple locations for the benchmark code
_BENCH = _ROOT / "Anvil-P-E" / "bench-p02-context"
if not _BENCH.exists():
    _BENCH = Path("/home/ubuntu/Anvil-P-E/bench-p02-context")
sys.path.insert(0, str(_BENCH))
 
from persistent_context_engine import Engine, EngineConfig
from generator import GenConfig, generate
from metrics import score_match, score_remediation, IncidentScore, aggregate
from schema import Context, Event, IncidentSignal
 
 
# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
 
def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
 
 
def _now() -> datetime:
    return datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
 
 
@dataclass
class ScenarioResult:
    name: str
    recall_at_5: float
    precision_at_5: float
    remediation_acc: float
    latency_p95_ms: float
    n_incidents: int
    details: str = ""
 
 
def run_scenario(
    name: str,
    train_events: List[Event],
    eval_events: List[Event],
    eval_signals: List[Dict[str, Any]],
    ground_truth: List[Dict[str, Any]],
    mode: str = "fast",
) -> ScenarioResult:
    """Run a single adversarial scenario and return metrics."""
    engine = Engine(EngineConfig(db_path=":memory:", buffer_size=1000))
    try:
        engine.ingest(train_events)
        engine.ingest(eval_events)
 
        scores: List[IncidentScore] = []
        for sig, gt in zip(eval_signals, ground_truth):
            signal: IncidentSignal = {
                "incident_id": sig["incident_id"],
                "ts": sig["ts"],
                "trigger": sig.get("trigger", ""),
                "service": sig.get("service", ""),
            }
            t0 = time.monotonic()
            ctx: Context = engine.reconstruct_context(signal, mode=mode)
            latency = (time.monotonic() - t0) * 1000.0
 
            in_top_k, precision = score_match(ctx, gt, k=5)
            rem_ok = score_remediation(ctx, gt)
 
            scores.append(IncidentScore(
                incident_id=sig["incident_id"],
                correct_family_in_top_k=in_top_k,
                precision_at_k=precision,
                remediation_matches=rem_ok,
                latency_ms=latency,
            ))
 
        if not scores:
            return ScenarioResult(name, 0, 0, 0, 0, 0, "No eval incidents")
 
        summary = aggregate(scores)
        return ScenarioResult(
            name=name,
            recall_at_5=summary["recall@5"],
            precision_at_5=summary["precision@5_mean"],
            remediation_acc=summary["remediation_acc"],
            latency_p95_ms=summary["latency_p95_ms"],
            n_incidents=len(scores),
        )
    finally:
        engine.close()
 
 
# ──────────────────────────────────────────────────────────────────────
# TIER 1: Extreme Generator Parameters
# ──────────────────────────────────────────────────────────────────────
 
def tier1_extreme_params() -> List[ScenarioResult]:
    """Push the generator to L3-style extremes."""
    results = []
 
    configs = [
        ("T1.1: 30 services, 14 days, 20 families",
         GenConfig(seed=777, n_services=30, days=14, deploys=80,
                   topology_mutations=25, incidents_train=60,
                   incidents_eval=20, incident_families=20,
                   background_density=100)),
        ("T1.2: Dense drift (50 topology mutations)",
         GenConfig(seed=888, n_services=15, days=7, deploys=50,
                   topology_mutations=50, incidents_train=30,
                   incidents_eval=12, incident_families=8,
                   background_density=150)),
        ("T1.3: High noise (500 bg events/svc-day)",
         GenConfig(seed=999, n_services=12, days=7, deploys=30,
                   topology_mutations=10, incidents_train=24,
                   incidents_eval=10, incident_families=5,
                   background_density=500)),
        ("T1.4: Many families, few incidents each",
         GenConfig(seed=1234, n_services=20, days=10, deploys=40,
                   topology_mutations=15, incidents_train=30,
                   incidents_eval=15, incident_families=15,
                   background_density=100)),
        ("T1.5: Long horizon (30 days)",
         GenConfig(seed=5555, n_services=15, days=30, deploys=100,
                   topology_mutations=30, incidents_train=50,
                   incidents_eval=20, incident_families=10,
                   background_density=50)),
    ]
 
    for name, cfg in configs:
        print(f"  Running {name}...")
        ds = generate(cfg)
        result = run_scenario(
            name, ds.train_events, ds.eval_events,
            ds.eval_signals, ds.ground_truth, mode="fast"
        )
        results.append(result)
 
    return results
 
 
# ──────────────────────────────────────────────────────────────────────
# TIER 2: Hand-Crafted Adversarial Scenarios
# ──────────────────────────────────────────────────────────────────────
 
def _make_incident_events(
    service: str,
    t: datetime,
    incident_id: str,
    upstream: str = "upstream-svc",
    remediation_action: str = "rollback",
    include_remediation: bool = True,
) -> Tuple[List[Event], Event, Event]:
    """Generate a standard incident pattern (pre-events, signal, remediation)."""
    pre = [
        {"ts": _iso(t - timedelta(minutes=30)), "kind": "deploy",
         "service": service, "version": "v2.1.0", "actor": "ci"},
        {"ts": _iso(t - timedelta(minutes=10)), "kind": "metric",
         "service": service, "name": "latency_p99_ms", "value": 5000.0},
        {"ts": _iso(t - timedelta(seconds=30)), "kind": "log",
         "service": upstream, "level": "error",
         "msg": f"timeout calling {service}"},
    ]
    signal: Event = {
        "ts": _iso(t), "kind": "incident_signal",
        "incident_id": incident_id,
        "trigger": f"alert:{service}/latency_p99_ms>3000",
        "service": service,
    }
    remediation: Event = {
        "ts": _iso(t + timedelta(minutes=20)), "kind": "remediation",
        "incident_id": incident_id, "action": remediation_action,
        "target": service, "outcome": "resolved",
    }
    return pre, signal, remediation
 
 
def scenario_cascading_rename_chain() -> ScenarioResult:
    """
    ADVERSARIAL: Service renamed 5 times (A→B→C→D→E→F).
    Training incidents use names A, B, C. Eval incident uses name F.
    Engine must trace through the full rename chain to find matches.
    """
    base = _now() - timedelta(days=14)
    train_events: List[Event] = []
    eval_events: List[Event] = []
    eval_signals: List[Event] = []
    ground_truth: List[Dict[str, Any]] = []
 
    names = ["alpha-svc", "beta-svc", "gamma-svc", "delta-svc", "epsilon-svc", "zeta-svc"]
 
    # Rename chain over time
    for i in range(len(names) - 1):
        t = base + timedelta(days=i * 2)
        train_events.append({
            "ts": _iso(t), "kind": "topology", "change": "rename",
            "from_": names[i], "to": names[i + 1],
        })
 
    # Training incidents using early names (family 0)
    for i, name in enumerate(names[:3]):
        t = base + timedelta(days=i * 2, hours=6)
        pre, sig, rem = _make_incident_events(
            name, t, f"INC-CHAIN-{i}-0", upstream="observer-svc"
        )
        train_events.extend(pre)
        train_events.append(sig)
        train_events.append(rem)
 
    # A different family (family 1) on a completely different service
    for i in range(3):
        t = base + timedelta(days=i * 3, hours=12)
        pre, sig, rem = _make_incident_events(
            "other-svc", t, f"INC-OTHER-{i}-1", upstream="monitor-svc"
        )
        train_events.extend(pre)
        train_events.append(sig)
        train_events.append(rem)
 
    # Eval: incident on the LATEST name in the chain (zeta-svc)
    eval_t = base + timedelta(days=12)
    pre, sig, rem = _make_incident_events(
        names[-1], eval_t, f"INC-EVAL-0-0", upstream="observer-svc"
    )
    eval_events.extend(pre)
    eval_events.append(sig)
    eval_signals.append(sig)
    ground_truth.append({
        "incident_id": "INC-EVAL-0-0",
        "family": 0,
        "trigger_service_live": names[-1],
        "trigger_service_canonical": names[0],
        "expected_remediation": "rollback",
    })
 
    train_events.sort(key=lambda e: e["ts"])
    eval_events.sort(key=lambda e: e["ts"])
 
    return run_scenario(
        "T2.1: Cascading rename chain (A→B→C→D→E→F)",
        train_events, eval_events, eval_signals, ground_truth
    )
 
 
def scenario_rename_plus_dep_shift() -> ScenarioResult:
    """
    ADVERSARIAL: Service is renamed AND its dependency graph changes.
    Family's signature is morphed across both rename and topology shift.
    """
    base = _now() - timedelta(days=10)
    train_events: List[Event] = []
    eval_events: List[Event] = []
    eval_signals: List[Event] = []
    ground_truth: List[Dict[str, Any]] = []
 
    # Phase 1: training incidents on "payment-svc" with dep on "db-primary"
    train_events.append({
        "ts": _iso(base), "kind": "topology", "change": "dep_add",
        "from_": "payment-svc", "to": "db-primary",
    })
    for i in range(4):
        t = base + timedelta(days=i, hours=3)
        pre, sig, rem = _make_incident_events(
            "payment-svc", t, f"INC-PAY-{i}-0",
            upstream="gateway-svc", remediation_action="rollback"
        )
        train_events.extend(pre)
        train_events.append(sig)
        train_events.append(rem)
 
    # Phase 2: rename payment-svc → billing-engine
    rename_t = base + timedelta(days=5)
    train_events.append({
        "ts": _iso(rename_t), "kind": "topology", "change": "rename",
        "from_": "payment-svc", "to": "billing-engine",
    })
 
    # Phase 3: dependency shift (old dep removed, new dep added)
    train_events.append({
        "ts": _iso(rename_t + timedelta(hours=1)), "kind": "topology",
        "change": "dep_remove", "from_": "billing-engine", "to": "db-primary",
    })
    train_events.append({
        "ts": _iso(rename_t + timedelta(hours=2)), "kind": "topology",
        "change": "dep_add", "from_": "billing-engine", "to": "db-replica-pool",
    })
 
    # Distractor family on a different service
    for i in range(4):
        t = base + timedelta(days=i + 1, hours=8)
        pre, sig, rem = _make_incident_events(
            "search-svc", t, f"INC-SEARCH-{i}-1",
            upstream="cache-svc", remediation_action="restart"
        )
        train_events.extend(pre)
        train_events.append(sig)
        train_events.append(rem)
 
    # Eval: incident on "billing-engine" (renamed) with new dep graph
    eval_t = base + timedelta(days=8)
    pre, sig, rem = _make_incident_events(
        "billing-engine", eval_t, f"INC-EVAL-1-0",
        upstream="gateway-svc", remediation_action="rollback"
    )
    eval_events.extend(pre)
    eval_events.append(sig)
    eval_signals.append(sig)
    ground_truth.append({
        "incident_id": "INC-EVAL-1-0",
        "family": 0,
        "trigger_service_live": "billing-engine",
        "trigger_service_canonical": "payment-svc",
        "expected_remediation": "rollback",
    })
 
    train_events.sort(key=lambda e: e["ts"])
    eval_events.sort(key=lambda e: e["ts"])
 
    return run_scenario(
        "T2.2: Rename + dependency shift (payment-svc→billing-engine, dep db-primary→db-replica-pool)",
        train_events, eval_events, eval_signals, ground_truth
    )
 
 
def scenario_multi_service_correlated_outage() -> ScenarioResult:
    """
    ADVERSARIAL: Correlated outage affecting 4 services simultaneously.
    The engine must not confuse the multi-service blast radius with
    unrelated single-service incidents.
    """
    base = _now() - timedelta(days=7)
    train_events: List[Event] = []
    eval_events: List[Event] = []
    eval_signals: List[Event] = []
    ground_truth: List[Dict[str, Any]] = []
 
    services = ["api-gateway", "auth-svc", "user-svc", "order-svc"]
 
    # Add topology (connected services)
    for i in range(len(services) - 1):
        train_events.append({
            "ts": _iso(base - timedelta(days=1)), "kind": "topology",
            "change": "dep_add", "from_": services[i], "to": services[i + 1],
        })
 
    # Training: correlated outage pattern (family 0) — deploy on api-gateway
    # cascades to all downstream services
    for episode in range(3):
        t = base + timedelta(days=episode * 2)
        # Deploy on api-gateway triggers cascade
        train_events.append({
            "ts": _iso(t - timedelta(minutes=30)), "kind": "deploy",
            "service": "api-gateway", "version": f"v1.{episode}.0", "actor": "ci",
        })
        # Metrics spike on ALL services
        for svc in services:
            train_events.append({
                "ts": _iso(t - timedelta(minutes=10 - services.index(svc))),
                "kind": "metric", "service": svc,
                "name": "error_rate", "value": float(50 + episode * 10),
            })
        # Error logs cascade downstream
        for j, svc in enumerate(services[1:], 1):
            train_events.append({
                "ts": _iso(t - timedelta(seconds=30 - j * 5)),
                "kind": "log", "service": svc, "level": "error",
                "msg": f"upstream {services[j-1]} unavailable",
            })
        # Signal on api-gateway
        sig: Event = {
            "ts": _iso(t), "kind": "incident_signal",
            "incident_id": f"INC-CASCADE-{episode}-0",
            "trigger": "alert:api-gateway/error_rate>50",
            "service": "api-gateway",
        }
        train_events.append(sig)
        train_events.append({
            "ts": _iso(t + timedelta(minutes=15)), "kind": "remediation",
            "incident_id": f"INC-CASCADE-{episode}-0",
            "action": "rollback", "target": "api-gateway", "outcome": "resolved",
        })
 
    # Distractor: single-service incidents (family 1) on auth-svc
    for i in range(3):
        t = base + timedelta(days=i, hours=12)
        pre, sig, rem = _make_incident_events(
            "auth-svc", t, f"INC-AUTH-{i}-1", upstream="ldap-svc"
        )
        train_events.extend(pre)
        train_events.append(sig)
        train_events.append(rem)
 
    # Eval: new correlated outage (family 0)
    eval_t = base + timedelta(days=6)
    eval_events.append({
        "ts": _iso(eval_t - timedelta(minutes=30)), "kind": "deploy",
        "service": "api-gateway", "version": "v1.5.0", "actor": "ci",
    })
    for svc in services:
        eval_events.append({
            "ts": _iso(eval_t - timedelta(minutes=10 - services.index(svc))),
            "kind": "metric", "service": svc,
            "name": "error_rate", "value": 75.0,
        })
    for j, svc in enumerate(services[1:], 1):
        eval_events.append({
            "ts": _iso(eval_t - timedelta(seconds=30 - j * 5)),
            "kind": "log", "service": svc, "level": "error",
            "msg": f"upstream {services[j-1]} unavailable",
        })
    eval_sig: Event = {
        "ts": _iso(eval_t), "kind": "incident_signal",
        "incident_id": "INC-EVAL-CASCADE-0",
        "trigger": "alert:api-gateway/error_rate>50",
        "service": "api-gateway",
    }
    eval_events.append(eval_sig)
    eval_signals.append(eval_sig)
    ground_truth.append({
        "incident_id": "INC-EVAL-CASCADE-0",
        "family": 0,
        "trigger_service_live": "api-gateway",
        "trigger_service_canonical": "api-gateway",
        "expected_remediation": "rollback",
    })
 
    train_events.sort(key=lambda e: e["ts"])
    eval_events.sort(key=lambda e: e["ts"])
 
    return run_scenario(
        "T2.3: Correlated multi-service outage (4-service cascade)",
        train_events, eval_events, eval_signals, ground_truth
    )
 
 
def scenario_rapid_topology_before_incident() -> ScenarioResult:
    """
    ADVERSARIAL: Multiple topology changes happen within minutes of the incident.
    Tests whether the engine's temporal view correctly snapshots the graph.
    """
    base = _now() - timedelta(days=5)
    train_events: List[Event] = []
    eval_events: List[Event] = []
    eval_signals: List[Event] = []
    ground_truth: List[Dict[str, Any]] = []
 
    # Training: stable topology, incidents on "worker-svc"
    for i in range(4):
        t = base + timedelta(days=i)
        pre, sig, rem = _make_incident_events(
            "worker-svc", t, f"INC-WORKER-{i}-0", upstream="queue-svc"
        )
        train_events.extend(pre)
        train_events.append(sig)
        train_events.append(rem)
 
    # Right before eval: rapid topology mutations
    eval_t = base + timedelta(days=4, hours=12)
 
    # Rename worker-svc → processor-v2
    eval_events.append({
        "ts": _iso(eval_t - timedelta(minutes=5)), "kind": "topology",
        "change": "rename", "from_": "worker-svc", "to": "processor-v2",
    })
    # Add new dependency
    eval_events.append({
        "ts": _iso(eval_t - timedelta(minutes=4)), "kind": "topology",
        "change": "dep_add", "from_": "processor-v2", "to": "redis-cluster",
    })
    # Remove old dependency
    eval_events.append({
        "ts": _iso(eval_t - timedelta(minutes=3)), "kind": "topology",
        "change": "dep_remove", "from_": "processor-v2", "to": "queue-svc",
    })
    # Another rename!
    eval_events.append({
        "ts": _iso(eval_t - timedelta(minutes=2)), "kind": "topology",
        "change": "rename", "from_": "processor-v2", "to": "task-engine",
    })
 
    # Eval incident on the LATEST name
    pre, sig, rem = _make_incident_events(
        "task-engine", eval_t, f"INC-EVAL-RAPID-0",
        upstream="queue-svc"
    )
    eval_events.extend(pre)
    eval_events.append(sig)
    eval_signals.append(sig)
    ground_truth.append({
        "incident_id": "INC-EVAL-RAPID-0",
        "family": 0,
        "trigger_service_live": "task-engine",
        "trigger_service_canonical": "worker-svc",
        "expected_remediation": "rollback",
    })
 
    train_events.sort(key=lambda e: e["ts"])
    eval_events.sort(key=lambda e: e["ts"])
 
    return run_scenario(
        "T2.4: Rapid topology changes (2 renames + dep shift within 5min of incident)",
        train_events, eval_events, eval_signals, ground_truth
    )
 
 
def scenario_different_remediation_actions() -> ScenarioResult:
    """
    ADVERSARIAL: Different families use different remediation actions.
    Tests whether the engine correctly associates remediations with families.
    """
    base = _now() - timedelta(days=7)
    train_events: List[Event] = []
    eval_events: List[Event] = []
    eval_signals: List[Event] = []
    ground_truth: List[Dict[str, Any]] = []
 
    # Family 0: always "rollback" on web-frontend
    for i in range(4):
        t = base + timedelta(days=i)
        pre, sig, rem = _make_incident_events(
            "web-frontend", t, f"INC-WEB-{i}-0",
            upstream="cdn-svc", remediation_action="rollback"
        )
        train_events.extend(pre)
        train_events.append(sig)
        train_events.append(rem)
 
    # Family 1: always "scale_up" on compute-pool
    for i in range(4):
        t = base + timedelta(days=i, hours=8)
        pre, sig, rem = _make_incident_events(
            "compute-pool", t, f"INC-COMPUTE-{i}-1",
            upstream="scheduler-svc", remediation_action="scale_up"
        )
        train_events.extend(pre)
        train_events.append(sig)
        train_events.append(rem)
 
    # Family 2: always "restart" on cache-layer
    for i in range(4):
        t = base + timedelta(days=i, hours=16)
        pre, sig, rem = _make_incident_events(
            "cache-layer", t, f"INC-CACHE-{i}-2",
            upstream="app-server", remediation_action="restart"
        )
        train_events.extend(pre)
        train_events.append(sig)
        train_events.append(rem)
 
    # Eval: one incident per family
    for fam, (svc, action, upstream) in enumerate([
        ("web-frontend", "rollback", "cdn-svc"),
        ("compute-pool", "scale_up", "scheduler-svc"),
        ("cache-layer", "restart", "app-server"),
    ]):
        eval_t = base + timedelta(days=6, hours=fam * 4)
        pre, sig, rem = _make_incident_events(
            svc, eval_t, f"INC-EVAL-REM-{fam}-{fam}",
            upstream=upstream, remediation_action=action
        )
        eval_events.extend(pre)
        eval_events.append(sig)
        eval_signals.append(sig)
        ground_truth.append({
            "incident_id": f"INC-EVAL-REM-{fam}-{fam}",
            "family": fam,
            "trigger_service_live": svc,
            "trigger_service_canonical": svc,
            "expected_remediation": action,
        })
 
    train_events.sort(key=lambda e: e["ts"])
    eval_events.sort(key=lambda e: e["ts"])
 
    return run_scenario(
        "T2.5: Different remediation actions per family (rollback/scale_up/restart)",
        train_events, eval_events, eval_signals, ground_truth
    )
 
 
def scenario_interleaved_incidents_same_service() -> ScenarioResult:
    """
    ADVERSARIAL: Two families alternate on the same service.
    Family 0 has deploys causing latency; Family 1 has memory leaks.
    Different behavioral signatures but same trigger service.
    """
    base = _now() - timedelta(days=7)
    train_events: List[Event] = []
    eval_events: List[Event] = []
    eval_signals: List[Event] = []
    ground_truth: List[Dict[str, Any]] = []
 
    svc = "shared-api"
 
    # Family 0: deploy → latency spike pattern
    for i in range(4):
        t = base + timedelta(days=i, hours=2)
        train_events.append({
            "ts": _iso(t - timedelta(minutes=30)), "kind": "deploy",
            "service": svc, "version": f"v1.{i}.0", "actor": "ci",
        })
        train_events.append({
            "ts": _iso(t - timedelta(minutes=10)), "kind": "metric",
            "service": svc, "name": "latency_p99_ms", "value": 6000.0,
        })
        train_events.append({
            "ts": _iso(t - timedelta(seconds=30)), "kind": "log",
            "service": "downstream-a", "level": "error",
            "msg": f"timeout calling {svc}",
        })
        sig: Event = {
            "ts": _iso(t), "kind": "incident_signal",
            "incident_id": f"INC-DEPLOY-{i}-0",
            "trigger": f"alert:{svc}/latency_p99_ms>3000",
            "service": svc,
        }
        train_events.append(sig)
        train_events.append({
            "ts": _iso(t + timedelta(minutes=20)), "kind": "remediation",
            "incident_id": f"INC-DEPLOY-{i}-0",
            "action": "rollback", "target": svc, "outcome": "resolved",
        })
 
    # Family 1: memory leak pattern (NO deploy, gradual memory rise)
    for i in range(4):
        t = base + timedelta(days=i, hours=14)
        # No deploy! Instead, gradual memory increase
        train_events.append({
            "ts": _iso(t - timedelta(minutes=60)), "kind": "metric",
            "service": svc, "name": "memory_used_pct", "value": 85.0,
        })
        train_events.append({
            "ts": _iso(t - timedelta(minutes=30)), "kind": "metric",
            "service": svc, "name": "memory_used_pct", "value": 92.0,
        })
        train_events.append({
            "ts": _iso(t - timedelta(minutes=10)), "kind": "metric",
            "service": svc, "name": "memory_used_pct", "value": 98.0,
        })
        train_events.append({
            "ts": _iso(t - timedelta(seconds=30)), "kind": "log",
            "service": svc, "level": "critical",
            "msg": "OOM killer invoked",
        })
        sig = {
            "ts": _iso(t), "kind": "incident_signal",
            "incident_id": f"INC-OOM-{i}-1",
            "trigger": f"alert:{svc}/memory_used_pct>95",
            "service": svc,
        }
        train_events.append(sig)
        train_events.append({
            "ts": _iso(t + timedelta(minutes=10)), "kind": "remediation",
            "incident_id": f"INC-OOM-{i}-1",
            "action": "restart", "target": svc, "outcome": "resolved",
        })
 
    # Eval: one of each type
    # Family 0 eval (deploy → latency)
    eval_t = base + timedelta(days=6)
    eval_events.append({
        "ts": _iso(eval_t - timedelta(minutes=30)), "kind": "deploy",
        "service": svc, "version": "v1.9.0", "actor": "ci",
    })
    eval_events.append({
        "ts": _iso(eval_t - timedelta(minutes=10)), "kind": "metric",
        "service": svc, "name": "latency_p99_ms", "value": 7000.0,
    })
    eval_events.append({
        "ts": _iso(eval_t - timedelta(seconds=30)), "kind": "log",
        "service": "downstream-a", "level": "error",
        "msg": f"timeout calling {svc}",
    })
    eval_sig: Event = {
        "ts": _iso(eval_t), "kind": "incident_signal",
        "incident_id": "INC-EVAL-DEPLOY-0",
        "trigger": f"alert:{svc}/latency_p99_ms>3000",
        "service": svc,
    }
    eval_events.append(eval_sig)
    eval_signals.append(eval_sig)
    ground_truth.append({
        "incident_id": "INC-EVAL-DEPLOY-0",
        "family": 0,
        "trigger_service_live": svc,
        "trigger_service_canonical": svc,
        "expected_remediation": "rollback",
    })
 
    # Family 1 eval (memory leak)
    eval_t2 = base + timedelta(days=6, hours=12)
    eval_events.append({
        "ts": _iso(eval_t2 - timedelta(minutes=60)), "kind": "metric",
        "service": svc, "name": "memory_used_pct", "value": 87.0,
    })
    eval_events.append({
        "ts": _iso(eval_t2 - timedelta(minutes=30)), "kind": "metric",
        "service": svc, "name": "memory_used_pct", "value": 94.0,
    })
    eval_events.append({
        "ts": _iso(eval_t2 - timedelta(minutes=10)), "kind": "metric",
        "service": svc, "name": "memory_used_pct", "value": 99.0,
    })
    eval_events.append({
        "ts": _iso(eval_t2 - timedelta(seconds=30)), "kind": "log",
        "service": svc, "level": "critical",
        "msg": "OOM killer invoked",
    })
    eval_sig2: Event = {
        "ts": _iso(eval_t2), "kind": "incident_signal",
        "incident_id": "INC-EVAL-OOM-1",
        "trigger": f"alert:{svc}/memory_used_pct>95",
        "service": svc,
    }
    eval_events.append(eval_sig2)
    eval_signals.append(eval_sig2)
    ground_truth.append({
        "incident_id": "INC-EVAL-OOM-1",
        "family": 1,
        "trigger_service_live": svc,
        "trigger_service_canonical": svc,
        "expected_remediation": "restart",
    })
 
    train_events.sort(key=lambda e: e["ts"])
    eval_events.sort(key=lambda e: e["ts"])
 
    return run_scenario(
        "T2.6: Interleaved families on same service (deploy-latency vs memory-leak)",
        train_events, eval_events, eval_signals, ground_truth
    )
 
 
def scenario_back_to_back_incidents() -> ScenarioResult:
    """
    ADVERSARIAL: Multiple incidents within minutes of each other.
    Context windows overlap — the engine must isolate each incident's signal.
    """
    base = _now() - timedelta(days=5)
    train_events: List[Event] = []
    eval_events: List[Event] = []
    eval_signals: List[Event] = []
    ground_truth: List[Dict[str, Any]] = []
 
    # Training: well-spaced incidents
    for i in range(4):
        t = base + timedelta(days=i)
        pre, sig, rem = _make_incident_events(
            "api-svc", t, f"INC-API-{i}-0", upstream="db-svc"
        )
        train_events.extend(pre)
        train_events.append(sig)
        train_events.append(rem)
 
    for i in range(4):
        t = base + timedelta(days=i, hours=12)
        pre, sig, rem = _make_incident_events(
            "worker-svc", t, f"INC-WORK-{i}-1", upstream="queue-svc"
        )
        train_events.extend(pre)
        train_events.append(sig)
        train_events.append(rem)
 
    # Eval: TWO incidents 5 minutes apart (overlapping context windows!)
    eval_t1 = base + timedelta(days=4, hours=6)
    eval_t2 = eval_t1 + timedelta(minutes=5)
 
    pre1, sig1, _ = _make_incident_events(
        "api-svc", eval_t1, "INC-EVAL-BACK1-0", upstream="db-svc"
    )
    pre2, sig2, _ = _make_incident_events(
        "worker-svc", eval_t2, "INC-EVAL-BACK2-1", upstream="queue-svc"
    )
    eval_events.extend(pre1 + pre2)
    eval_events.append(sig1)
    eval_events.append(sig2)
    eval_signals.extend([sig1, sig2])
    ground_truth.extend([
        {"incident_id": "INC-EVAL-BACK1-0", "family": 0,
         "trigger_service_live": "api-svc", "trigger_service_canonical": "api-svc",
         "expected_remediation": "rollback"},
        {"incident_id": "INC-EVAL-BACK2-1", "family": 1,
         "trigger_service_live": "worker-svc", "trigger_service_canonical": "worker-svc",
         "expected_remediation": "rollback"},
    ])
 
    train_events.sort(key=lambda e: e["ts"])
    eval_events.sort(key=lambda e: e["ts"])
 
    return run_scenario(
        "T2.7: Back-to-back incidents (5min apart, overlapping windows)",
        train_events, eval_events, eval_signals, ground_truth
    )
 
 
def scenario_cold_service_no_history() -> ScenarioResult:
    """
    ADVERSARIAL: Incident on a brand-new service with zero history.
    Engine must gracefully handle no matching patterns.
    """
    base = _now() - timedelta(days=5)
    train_events: List[Event] = []
    eval_events: List[Event] = []
    eval_signals: List[Event] = []
    ground_truth: List[Dict[str, Any]] = []
 
    # Training: incidents on known services (family 0, 1)
    for i in range(4):
        t = base + timedelta(days=i)
        pre, sig, rem = _make_incident_events(
            "known-svc", t, f"INC-KNOWN-{i}-0", upstream="helper-svc"
        )
        train_events.extend(pre)
        train_events.append(sig)
        train_events.append(rem)
 
    # Eval: incident on a BRAND NEW service never seen before
    eval_t = base + timedelta(days=4, hours=12)
    eval_events.append({
        "ts": _iso(eval_t - timedelta(minutes=30)), "kind": "deploy",
        "service": "brand-new-svc", "version": "v0.1.0", "actor": "ci",
    })
    eval_events.append({
        "ts": _iso(eval_t - timedelta(minutes=10)), "kind": "metric",
        "service": "brand-new-svc", "name": "latency_p99_ms", "value": 8000.0,
    })
    eval_sig: Event = {
        "ts": _iso(eval_t), "kind": "incident_signal",
        "incident_id": "INC-EVAL-NEW-0",
        "trigger": "alert:brand-new-svc/latency_p99_ms>3000",
        "service": "brand-new-svc",
    }
    eval_events.append(eval_sig)
    eval_signals.append(eval_sig)
    # Family 0 is the closest match (same behavioral pattern)
    ground_truth.append({
        "incident_id": "INC-EVAL-NEW-0",
        "family": 0,
        "trigger_service_live": "brand-new-svc",
        "trigger_service_canonical": "brand-new-svc",
        "expected_remediation": "rollback",
    })
 
    train_events.sort(key=lambda e: e["ts"])
    eval_events.sort(key=lambda e: e["ts"])
 
    return run_scenario(
        "T2.8: Cold service (brand new, zero history)",
        train_events, eval_events, eval_signals, ground_truth
    )
 
 
def scenario_latency_under_load() -> ScenarioResult:
    """
    STRESS: Large dataset to test latency budget under scale.
    30 services, 14 days, moderate background noise.
    """
    cfg = GenConfig(
        seed=31415,
        n_services=30,
        days=14,
        deploys=100,
        topology_mutations=30,
        incidents_train=50,
        incidents_eval=20,
        incident_families=12,
        background_density=150,
    )
    ds = generate(cfg)
 
    # Run in both modes
    result_fast = run_scenario(
        "T3.1: Scale stress (30 svcs, 14d, 150bg/svc-day) — FAST mode",
        ds.train_events, ds.eval_events,
        ds.eval_signals, ds.ground_truth, mode="fast"
    )
    result_deep = run_scenario(
        "T3.1: Scale stress (30 svcs, 14d, 150bg/svc-day) — DEEP mode",
        ds.train_events, ds.eval_events,
        ds.eval_signals, ds.ground_truth, mode="deep"
    )
    return result_fast, result_deep
 
 
# ──────────────────────────────────────────────────────────────────────
# Main runner
# ──────────────────────────────────────────────────────────────────────
 
def print_result(r: ScenarioResult, latency_budget: float = 2000.0):
    """Print a single scenario result with pass/fail indicators."""
    recall_ok = "PASS" if r.recall_at_5 >= 0.8 else "WARN" if r.recall_at_5 >= 0.5 else "FAIL"
    prec_ok = "PASS" if r.precision_at_5 >= 0.5 else "WARN" if r.precision_at_5 >= 0.3 else "FAIL"
    rem_ok = "PASS" if r.remediation_acc >= 0.7 else "WARN" if r.remediation_acc >= 0.4 else "FAIL"
    lat_ok = "PASS" if r.latency_p95_ms <= latency_budget else "WARN" if r.latency_p95_ms <= latency_budget * 2 else "FAIL"
 
    print(f"  {'─' * 70}")
    print(f"  {r.name}")
    print(f"  {'─' * 70}")
    print(f"    recall@5:        {r.recall_at_5:.3f}  [{recall_ok}]")
    print(f"    precision@5:     {r.precision_at_5:.3f}  [{prec_ok}]")
    print(f"    remediation_acc: {r.remediation_acc:.3f}  [{rem_ok}]")
    print(f"    latency_p95:     {r.latency_p95_ms:.1f}ms  [{lat_ok}] (budget: {latency_budget:.0f}ms)")
    print(f"    incidents:       {r.n_incidents}")
    if r.details:
        print(f"    notes:           {r.details}")
    print()
 
 
def main():
    print("=" * 72)
    print(" L3-STYLE ADVERSARIAL STRESS TEST")
    print(" Persistent Context Engine — Edge Case Discovery")
    print("=" * 72)
    print()
 
    all_results: List[ScenarioResult] = []
 
    # ── TIER 1: Extreme Generator Parameters ──
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║  TIER 1: Extreme Generator Parameters                              ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print()
    tier1_results = tier1_extreme_params()
    for r in tier1_results:
        print_result(r)
    all_results.extend(tier1_results)
 
    # ── TIER 2: Hand-Crafted Adversarial Scenarios ──
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║  TIER 2: Hand-Crafted Adversarial Scenarios                        ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print()
 
    tier2_scenarios = [
        scenario_cascading_rename_chain,
        scenario_rename_plus_dep_shift,
        scenario_multi_service_correlated_outage,
        scenario_rapid_topology_before_incident,
        scenario_different_remediation_actions,
        scenario_interleaved_incidents_same_service,
        scenario_back_to_back_incidents,
        scenario_cold_service_no_history,
    ]
 
    for scenario_fn in tier2_scenarios:
        print(f"  Running {scenario_fn.__name__}...")
        result = scenario_fn()
        print_result(result)
        all_results.append(result)
 
    # ── TIER 3: Scale Stress Test ──
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║  TIER 3: Scale / Latency Stress                                    ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print()
    print("  Running scale stress test (50 svcs, 14d, heavy noise)...")
    fast_result, deep_result = scenario_latency_under_load()
    print_result(fast_result, latency_budget=2000.0)
    print_result(deep_result, latency_budget=6000.0)
    all_results.extend([fast_result, deep_result])
 
    # ── SUMMARY ──
    print()
    print("=" * 72)
    print(" SUMMARY")
    print("=" * 72)
    print()
 
    fails = [r for r in all_results if r.recall_at_5 < 0.5 or r.remediation_acc < 0.4]
    warns = [r for r in all_results if r not in fails and (r.recall_at_5 < 0.8 or r.remediation_acc < 0.7)]
    passes = [r for r in all_results if r not in fails and r not in warns]
 
    print(f"  PASS: {len(passes)}  |  WARN: {len(warns)}  |  FAIL: {len(fails)}")
    print()
 
    if fails:
        print("  FAILURES (likely L3 vulnerabilities):")
        for r in fails:
            print(f"    ✗ {r.name}")
            print(f"      recall={r.recall_at_5:.3f} rem_acc={r.remediation_acc:.3f}")
        print()
 
    if warns:
        print("  WARNINGS (potential weaknesses):")
        for r in warns:
            print(f"    ⚠ {r.name}")
            print(f"      recall={r.recall_at_5:.3f} rem_acc={r.remediation_acc:.3f}")
        print()
 
    # Aggregate metrics
    avg_recall = statistics.mean(r.recall_at_5 for r in all_results)
    avg_prec = statistics.mean(r.precision_at_5 for r in all_results)
    avg_rem = statistics.mean(r.remediation_acc for r in all_results)
    max_lat = max(r.latency_p95_ms for r in all_results)
 
    print(f"  Aggregate across all scenarios:")
    print(f"    avg recall@5:        {avg_recall:.3f}")
    print(f"    avg precision@5:     {avg_prec:.3f}")
    print(f"    avg remediation_acc: {avg_rem:.3f}")
    print(f"    worst latency_p95:   {max_lat:.1f}ms")
    print()
 
 
if __name__ == "__main__":
    main()