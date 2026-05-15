<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Persistent Context Engine — Build Plan</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: system-ui, -apple-system, sans-serif;
    font-size: 15px;
    line-height: 1.7;
    color: #1a1a1a;
    background: #f8f7f4;
    padding: 40px 24px 80px;
    max-width: 860px;
    margin: 0 auto;
  }

h1 { font-size: 22px; font-weight: 600; margin-bottom: 6px; color: #111; }
.subtitle { font-size: 13px; color: #666; margin-bottom: 48px; }

.phase-header {
display: flex;
align-items: center;
gap: 12px;
margin-bottom: 24px;
margin-top: 52px;
}
.phase-tag {
font-size: 11px;
font-weight: 600;
letter-spacing: 0.08em;
text-transform: uppercase;
padding: 3px 10px;
border-radius: 20px;
white-space: nowrap;
}
.p1 .phase-tag { background: #e8effe; color: #2d56b8; }
.p2 .phase-tag { background: #edf7ee; color: #246b2c; }
.p3 .phase-tag { background: #fef3e2; color: #8a5a00; }

.phase-title { font-size: 19px; font-weight: 600; color: #111; }
.phase-desc { font-size: 14px; color: #555; margin-bottom: 28px; padding-left: 4px; }

.subphase {
border: 1px solid #e0ddd6;
border-radius: 12px;
background: #fff;
margin-bottom: 16px;
overflow: hidden;
}

.subphase-header {
display: flex;
align-items: center;
gap: 10px;
padding: 14px 20px;
border-bottom: 1px solid #f0ede8;
background: #fdfcfa;
}

.sp-badge {
font-size: 11px;
font-weight: 700;
padding: 2px 9px;
border-radius: 20px;
letter-spacing: 0.05em;
}
.research .sp-badge { background: #f0edfe; color: #5c47cc; }
.build .sp-badge { background: #e5f8f0; color: #1a7a4a; }
.test .sp-badge { background: #fff0e6; color: #b35600; }

.sp-title { font-size: 15px; font-weight: 600; color: #1a1a1a; }

.subphase-body { padding: 18px 20px; }

.item-list { list-style: none; display: flex; flex-direction: column; gap: 8px; }
.item-list li {
display: flex;
gap: 10px;
font-size: 14px;
color: #333;
line-height: 1.55;
}
.item-list li::before {
content: "–";
color: #999;
flex-shrink: 0;
margin-top: 1px;
}

.q-list { list-style: none; display: flex; flex-direction: column; gap: 7px; }
.q-list li {
font-size: 14px;
color: #333;
padding: 8px 12px;
background: #f9f7ff;
border-radius: 7px;
border-left: 2px solid #b8aeee;
line-height: 1.5;
}

.loop-note {
margin-top: 12px;
padding: 10px 14px;
background: #f2f2f2;
border-radius: 8px;
font-size: 13px;
color: #555;
}

/_ Design section _/
.design-section { margin-top: 64px; }
.design-section h2 { font-size: 19px; font-weight: 600; margin-bottom: 8px; color: #111; }
.design-section .intro { font-size: 14px; color: #555; margin-bottom: 28px; }

.design-block {
border: 1px solid #e0ddd6;
border-radius: 12px;
background: #fff;
margin-bottom: 20px;
overflow: hidden;
}
.design-block-header {
padding: 14px 20px;
background: #fdfcfa;
border-bottom: 1px solid #f0ede8;
}
.design-block-header h3 { font-size: 15px; font-weight: 600; color: #111; }
.design-block-header .tagline { font-size: 12px; color: #888; margin-top: 2px; }
.design-block-body { padding: 18px 20px; display: flex; flex-direction: column; gap: 10px; }

.design-row {
display: grid;
grid-template-columns: 130px 1fr;
gap: 8px;
font-size: 14px;
align-items: start;
padding-bottom: 10px;
border-bottom: 1px solid #f4f2ee;
}
.design-row:last-child { border-bottom: none; padding-bottom: 0; }
.dr-label { font-weight: 600; color: #444; font-size: 13px; padding-top: 1px; }
.dr-value { color: #333; line-height: 1.55; }

.oss-card {
border: 1px solid #e0ddd6;
border-radius: 12px;
background: #fff;
margin-bottom: 16px;
overflow: hidden;
}
.oss-card-header {
padding: 12px 20px;
background: #fdfcfa;
border-bottom: 1px solid #f0ede8;
display: flex;
align-items: center;
gap: 10px;
}
.oss-name { font-size: 15px; font-weight: 600; color: #111; }
.oss-link { font-size: 12px; color: #5c8fd6; text-decoration: none; }
.oss-body { padding: 14px 20px; display: flex; flex-direction: column; gap: 8px; font-size: 14px; color: #333; }
.oss-use { font-style: italic; color: #666; font-size: 13px; }

.arch-diagram {
margin: 24px 0;
border: 1px solid #e0ddd6;
border-radius: 12px;
padding: 24px;
background: #fdfcfa;
font-size: 13px;
font-family: monospace;
line-height: 1.8;
color: #333;
overflow-x: auto;
}

.warn-box {
margin-top: 28px;
padding: 14px 18px;
background: #fff8e6;
border-radius: 10px;
border-left: 3px solid #d4930a;
font-size: 13.5px;
color: #5a3e00;
line-height: 1.6;
}

.critical-insight {
padding: 12px 16px;
background: #f0edfe;
border-radius: 8px;
border-left: 3px solid #7c63e8;
font-size: 13.5px;
color: #2d1f6b;
margin-top: 12px;
line-height: 1.6;
}

hr.divider {
border: none;
border-top: 1px solid #e0ddd6;
margin: 48px 0;
}
</style>

</head>
<body>

<h1>Persistent Context Engine — Build Plan</h1>
<p class="subtitle">3 phases · each split into Research → Build → Test · then loop back</p>

<!-- ==================== PHASE 1 ==================== -->
<div class="phase-header p1">
  <span class="phase-tag">Phase 1</span>
  <span class="phase-title">Ingest pipeline + memory foundation</span>
</div>
<p class="phase-desc">
  Get events flowing in, stored with full provenance and time ordering, and converted into a basic graph structure.
  Nothing smart yet — just make sure you can replay history accurately and the graph can grow without breaking.
</p>

<!-- 1.1 Research -->
<div class="subphase">
  <div class="subphase-header research">
    <span class="sp-badge">Research</span>
    <span class="sp-title">1.1 — What does the ingestion layer actually need to handle?</span>
  </div>
  <div class="subphase-body">
    <ul class="q-list">
      <li>The benchmark sends events as JSONL. What is the fastest way to parse and classify each of the 6 event kinds without a switch statement that breaks when a new kind appears?</li>
      <li>Events arrive in timestamp order — but what happens when two events share the same timestamp? Which one "happened first" and does it matter for causal chain construction?</li>
      <li>The spec says "preserve provenance" — concretely, what does that mean? A source pointer back to the original raw event? A line number? A hash of the raw content?</li>
      <li>The spec says "support replayability" — does that mean storing raw events, or storing graph diffs, or both? What does replaying a graph diff look like?</li>
      <li>How do you store the graph so that a query at time T gives you the state of the graph as it was at T, not as it is now?</li>
      <li>What is the minimal schema for a node (service, component) and an edge (relationship) that lets you later add new fields without breaking old data?</li>
      <li>The benchmark has ~200 events per service per day across 12 services over 7 days — that is roughly 17,000 events. How much disk does that take if you store raw + graph separately?</li>
      <li>DuckDB and SQLite are both options. What is the difference in write throughput for 1,000 inserts per second?</li>
    </ul>
  </div>
</div>

<!-- 1.2 Build -->
<div class="subphase">
  <div class="subphase-header build">
    <span class="sp-badge">Build</span>
    <span class="sp-title">1.2 — Core ingest + raw storage + basic graph construction</span>
  </div>
  <div class="subphase-body">
    <ul class="item-list">
      <li>Write an event parser that reads JSONL and normalises each event into a typed dict — timestamp as a datetime object, kind as an enum, and the remaining fields preserved verbatim.</li>
      <li>Write a raw event store (append-only table, one row per event) with columns: id, ts, kind, service, raw_json. This is your provenance anchor — every later inference points back here.</li>
      <li>Write a node table: id, canonical_name, aliases (JSON list), first_seen_ts, last_seen_ts. When a deploy event arrives for "payments-svc", upsert a node for it.</li>
      <li>Write an edge table: id, source_node_id, target_node_id, edge_kind, first_seen_ts, last_seen_ts, confidence, evidence_event_ids (JSON list). Edges can be inferred (not just from explicit topology events).</li>
      <li>Handle the topology/rename event: when you see it, do NOT delete the old node. Instead, add the new name as an alias on the same node record and update canonical_name. This is the key to topology-independence.</li>
      <li>Handle the topology/dependency-shift event: create or retire edges, but mark retired edges with a retired_at timestamp — never delete them.</li>
      <li>Write an ingest coordinator that calls the parser, then dispatches to the raw store and the graph updater in sequence, inside a transaction so they never diverge.</li>
      <li>Add a simple in-memory recent-events buffer (last 500 events) to avoid hitting disk for every lookup during context reconstruction.</li>
    </ul>
  </div>
</div>

<!-- 1.3 Test -->
<div class="subphase">
  <div class="subphase-header test">
    <span class="sp-badge">Test</span>
    <span class="sp-title">1.3 — Verify provenance, ordering, and rename survival</span>
  </div>
  <div class="subphase-body">
    <ul class="item-list">
      <li>Feed the worked example JSONL from the spec. After ingestion, check that the raw store has exactly 7 rows (one per event line) and that each row's raw_json matches the original exactly.</li>
      <li>Query the node table. Confirm that after the rename event, there is exactly one node for payments-svc/billing-svc, and its aliases list contains both names.</li>
      <li>Query the edge table. Confirm that no edges were deleted — retired edges exist with a retired_at timestamp set.</li>
      <li>Replay test: feed the same events twice. Confirm the graph state is identical to a single feed (upsert semantics, not append).</li>
      <li>Point-in-time test: query the graph at ts = 14:25 (before the rename). Confirm the node's canonical name is still "payments-svc" at that point in time.</li>
      <li>Throughput check: feed 1,000 events in a tight loop. Measure wall-clock time. Must be under 1 second (target is ≥ 1,000 events/sec ingest).</li>
    </ul>
    <div class="loop-note">
      Loop back to 1.2 if the rename test fails or throughput is under target. The rename handling is load-bearing — everything in Phases 2 and 3 depends on a single node identity surviving across topology changes.
    </div>
  </div>
</div>

<!-- ==================== PHASE 2 ==================== -->
<div class="phase-header p2" style="margin-top:52px;">
  <span class="phase-tag">Phase 2</span>
  <span class="phase-title">Behavioral abstraction + pattern memory</span>
</div>
<p class="phase-desc">
  This is the hard part. Make the engine recognise that a "payments-svc deploy → latency spike → rollback" pattern
  and a "billing-svc deploy → latency spike → rollback" pattern are the same thing, despite the name change.
  This requires representing incidents by what happened, not by what the services were called.
</p>

<!-- 2.1 Research -->
<div class="subphase">
  <div class="subphase-header research">
    <span class="sp-badge">Research</span>
    <span class="sp-title">2.1 — How do you represent an incident as a shape, not a name?</span>
  </div>
  <div class="subphase-body">
    <ul class="q-list">
      <li>If you strip service names from an incident and keep only the sequence of event kinds and their timing gaps, what does the "payments-svc" incident look like? Write it out in plain English. Is it distinguishable from other incident types?</li>
      <li>What other signals should be part of the "incident shape"? Candidates: the metric names involved, the trace span structure (which service called which), the log level sequence, the time from first signal to remediation. Which of these survive a rename?</li>
      <li>How do you compare two incident shapes? Options: exact sequence match, edit distance over the event-kind sequence, vector embedding of the shape, graph isomorphism over the causal subgraph. Which is fast enough for 2-second p95 in fast mode?</li>
      <li>The benchmark has 5 "recurring incident families" each morphed across renames. What does "morphed" mean in practice — different service names, different metric values, different timing, or all of the above? What must stay constant within a family for pattern recognition to work?</li>
      <li>Confidence scoring: when you say "this new incident is 80% similar to INC-42 from 3 days ago," where does that 80% come from? What is the formula?</li>
      <li>How do you store incident patterns so that matching is fast? Options: precomputed shape fingerprint per incident (a hash or vector stored at ingest time), a separate pattern table that clusters incidents into families as new ones arrive, or on-demand computation at query time.</li>
      <li>The spec mentions "reinforcement and decay" — what does it mean for a pattern to decay? If you saw a certain failure pattern 6 months ago but it hasn't recurred, should it still surface with high confidence?</li>
    </ul>
  </div>
</div>

<!-- 2.2 Build -->
<div class="subphase">
  <div class="subphase-header build">
    <span class="sp-badge">Build</span>
    <span class="sp-title">2.2 — Behavioral fingerprinter + incident family tracker</span>
  </div>
  <div class="subphase-body">
    <ul class="item-list">
      <li>Write a "fingerprinter" function: given a window of events around an incident signal (say, 30 minutes before to the resolution), extract a canonical sequence of (event_kind, role, direction) tuples — where role is "upstream" or "downstream" relative to the trigger service, and direction is "before" or "after" the trigger.</li>
      <li>The role must be resolved using the graph's canonical node identity, not the service name in the event. This is what makes the fingerprint rename-proof: "billing-svc" and "payments-svc" are the same node, so they produce the same role in the fingerprint.</li>
      <li>Compute a structural hash of the fingerprint sequence. Store it in an incident_patterns table alongside the raw fingerprint and the incident_id.</li>
      <li>Write an incident family table: a grouping of incident_ids that share the same (or similar) structural hash. When a new incident is ingested and its hash matches an existing family, add it to that family. If no match, start a new family.</li>
      <li>Add soft matching: if hashes differ by one element (edit distance = 1), treat as "probable same family" with a confidence penalty. Store the similarity score alongside the family membership.</li>
      <li>Write a remediation history store: for each resolved incident, store what action was taken (rollback, restart, config change), what the target was (by canonical node id, not name), and whether the outcome was "resolved" or "worsened". This feeds the suggested_remediations output.</li>
      <li>Write a "temporal decay" scorer: a remediation pathway's confidence score decays as a function of how many days ago it was observed. Use a simple half-life formula — score × 0.95^days_since. Cap the floor at 0.1 so old remediations never disappear entirely.</li>
    </ul>
    <div class="critical-insight">
      The fingerprinter is the single most important piece of the whole system. If it leaks service names, every downstream comparison will fail across renames. Every reference to a service inside the fingerprint must go through the canonical node id lookup.
    </div>
  </div>
</div>

<!-- 2.3 Test -->
<div class="subphase">
  <div class="subphase-header test">
    <span class="sp-badge">Test</span>
    <span class="sp-title">2.3 — Verify rename-proof matching across topology drift</span>
  </div>
  <div class="subphase-body">
    <ul class="item-list">
      <li>Ingest a synthetic incident: payments-svc deploy at T=0, latency spike at T=1min, checkout-api error at T=2min, rollback of payments-svc at T=10min, outcome=resolved. Compute and store its fingerprint.</li>
      <li>Ingest the rename event: payments-svc → billing-svc.</li>
      <li>Ingest a second incident with billing-svc in the same role. Compute its fingerprint. Confirm the two fingerprints are identical (same hash).</li>
      <li>Confirm both incidents are placed in the same family in the incident_family table.</li>
      <li>Soft-match test: ingest a third incident where the sequence differs by one event (e.g. a metric event fires but no trace appears). Confirm it lands in the same family with a lower confidence score, not a new family.</li>
      <li>Remediation decay test: set one remediation's stored timestamp to 30 days ago. Confirm its surfaced confidence is noticeably lower than one from yesterday.</li>
    </ul>
    <div class="loop-note">
      This is the benchmark's central test. The "similar_past_incidents" field in the Context output must return a match despite the rename. If this test fails, do not move to Phase 3 — the whole system will fail the L2 evaluation.
    </div>
  </div>
</div>

<!-- ==================== PHASE 3 ==================== -->
<div class="phase-header p3" style="margin-top:52px;">
  <span class="phase-tag">Phase 3</span>
  <span class="phase-title">Context reconstruction + latency + evolution</span>
</div>
<p class="phase-desc">
  Wire everything together into the reconstruct_context method. Make fast mode return a useful answer in under 2 seconds.
  Make deep mode go further. Build the feedback loop that improves the engine over time.
</p>

<!-- 3.1 Research -->
<div class="subphase">
  <div class="subphase-header research">
    <span class="sp-badge">Research</span>
    <span class="sp-title">3.1 — How do you assemble a causal chain without making it up?</span>
  </div>
  <div class="subphase-body">
    <ul class="q-list">
      <li>A causal chain is a list of (cause_event_id, effect_event_id, evidence, confidence) tuples. What counts as evidence? A shared trace_id? A timestamp order within a 5-second window? An explicit topology edge between the two services?</li>
      <li>The spec requires "source-precedes-effect" under topology drift. What goes wrong if you use wall-clock time alone to order causes and effects? What if two events at the same millisecond are on different services?</li>
      <li>How do you prevent a causal chain from being just "everything that happened before the incident"? What is the cut-off rule — only events whose service is one hop away in the graph? Only events with a shared trace_id? Only events above a certain signal strength?</li>
      <li>The overall confidence field (0 to 1) on the Context output must aggregate across all the sub-components. What is the formula? Do you average the causal edge confidences? Weight by how many past incidents had the same pattern?</li>
      <li>Fast mode vs deep mode: what is the concrete difference in what each does? Fast mode: precomputed fingerprint lookup + cached remediation list. Deep mode: full graph traversal + re-scoring with current weights. Is that the right split?</li>
      <li>The explain field is free text. What should it contain? The causal chain in English? The matched family name? The remediation rationale? How long is too long?</li>
      <li>What is the biggest source of latency in context reconstruction? Is it graph traversal? The fingerprint lookup? Generating the explain text? Which one to optimize first?</li>
    </ul>
  </div>
</div>

<!-- 3.2 Build -->
<div class="subphase">
  <div class="subphase-header build">
    <span class="sp-badge">Build</span>
    <span class="sp-title">3.2 — reconstruct_context: fast mode + deep mode + explain</span>
  </div>
  <div class="subphase-body">
    <ul class="item-list">
      <li>Write a "context window extractor": given an incident_signal, retrieve all events from (signal.ts - 30 minutes) to (signal.ts + 5 minutes), filtered to services within 2 hops of the trigger service in the graph at signal time. These become related_events.</li>
      <li>Write a "causal chain builder": walk the related_events in timestamp order. For each pair (A, B) where A precedes B, add a causal edge if: they share a trace_id, OR the service of A has an edge to the service of B in the graph, OR A is a deploy event within 10 minutes of B being a metric/log spike. Set confidence based on which rule fired.</li>
      <li>Write the fast-mode path: fingerprint the current signal → lookup matching families → for each family, retrieve the top-3 past incidents by similarity score → retrieve their remediations sorted by confidence × recency. Return without re-traversing the full graph.</li>
      <li>Write the deep-mode path: same as fast mode but also re-traverse the full graph for the trigger service, look for non-obvious edges (services 3+ hops away that had correlated metric spikes), and re-score confidence using the current decay values.</li>
      <li>Write the explain generator: a template-based string builder (not an LLM call in fast mode) that says: "Incident triggered by [trigger]. Preceded by [deploy event] on [service] [N minutes] earlier. [M] similar past incidents found, most recent on [date]. Suggested action: [top remediation]." Keep it under 200 words.</li>
      <li>Build an in-memory cache for fingerprint→family lookups. Cache TTL = 30 seconds. This is the main latency optimization for fast mode.</li>
      <li>Write the feedback receiver: after an incident resolves, ingest a remediation event, update the remediation history store, reinforce the confidence of the matched family by 0.05 (additive), and update the family's "last confirmed" timestamp to reset decay.</li>
    </ul>
  </div>
</div>

<!-- 3.3 Test -->
<div class="subphase">
  <div class="subphase-header test">
    <span class="sp-badge">Test</span>
    <span class="sp-title">3.3 — End-to-end benchmark run + latency measurement</span>
  </div>
  <div class="subphase-body">
    <ul class="item-list">
      <li>Run the worked example from the spec end-to-end. Confirm the returned Context has all four required fields non-empty: related_events includes the deploy and the metric, causal_chain has at least one edge with confidence ≥ 0.5, similar_past_incidents surfaces a match despite the rename, suggested_remediations includes a rollback action.</li>
      <li>Latency test — fast mode: call reconstruct_context 20 times back to back, measure wall-clock per call. p95 must be under 2 seconds. If it fails, profile which function is slowest and cache it.</li>
      <li>Latency test — deep mode: same, p95 must be under 6 seconds.</li>
      <li>Run the self_check.py script from the benchmark harness. Look at recall@5 and precision@5. Target: recall@5 ≥ 0.6 before calling Phase 3 done.</li>
      <li>Memory evolution test: ingest the 24 training incidents. Record recall@5. Then ingest 5 remediation events for resolved incidents. Record recall@5 again. Confirm it improved (even slightly) — this proves the feedback loop works.</li>
      <li>Seed variance test: run the full benchmark with at least 3 different seeds. If recall@5 varies by more than 0.2 between seeds, the fingerprinter is probably leaking a service-name-specific signal. Find it and remove it.</li>
    </ul>
    <div class="loop-note">
      Loop back to Phase 2.2 if seed variance is high. Loop back to Phase 3.2 if latency is over budget. Do not start polishing the explain field until recall@5 is above 0.5 — accuracy before presentation.
    </div>
  </div>
</div>

<hr class="divider">

<!-- ==================== DESIGN APPROACH ==================== -->
<div class="design-section">
  <h2>Design approach</h2>
  <p class="intro">
    What the architecture looks like, why each piece was chosen, and where the real traps are.
    Informed by existing open source work on temporal graphs, operational memory, and incident management.
  </p>

  <div class="design-block">
    <div class="design-block-header">
      <h3>The core architectural decision: why a graph with bi-temporal edges, not a vector store</h3>
      <div class="tagline">The benchmark is specifically designed to break vector search — you need something different</div>
    </div>
    <div class="design-block-body">
      <div class="design-row">
        <div class="dr-label">The problem with vectors</div>
        <div class="dr-value">If you embed "payments-svc deploy caused timeout" and later see "billing-svc deploy caused timeout", these two sentences will get different embedding vectors because the service name is different. Cosine similarity will return a low match. The benchmark is explicitly testing for this failure mode.</div>
      </div>
      <div class="design-row">
        <div class="dr-label">What bi-temporal means</div>
        <div class="dr-value">Every edge in the graph has two timestamps: when the relationship started being true in the real world (valid_from), and when the system stopped believing it was true (valid_to, or null if still active). This lets you query "what did the graph look like at T=14:25" without losing history.</div>
      </div>
      <div class="design-row">
        <div class="dr-label">The rename fix</div>
        <div class="dr-value">A service rename is handled by adding an alias to an existing node — not by creating a new node. The node's identity is a UUID assigned at first observation, not the service name. Every comparison goes through UUID, so renames are invisible to the matcher.</div>
      </div>
      <div class="design-row">
        <div class="dr-label">Why not Neo4j</div>
        <div class="dr-value">Neo4j is great but heavyweight for a 24-hour build on a laptop. DuckDB gives you fast SQL joins over graph-shaped data (node table + edge table) without a separate server. For the L2 benchmark scale (17k events), DuckDB's in-process execution is more than fast enough.</div>
      </div>
    </div>
  </div>

  <div class="design-block">
    <div class="design-block-header">
      <h3>The behavioral fingerprint: what an incident "looks like" with names removed</h3>
      <div class="tagline">The critical transform that makes topology-independent matching possible</div>
    </div>
    <div class="design-block-body">
      <div class="design-row">
        <div class="dr-label">Inputs</div>
        <div class="dr-value">All events in a 30-minute window around the incident signal, filtered to services within 2 hops of the trigger service in the graph.</div>
      </div>
      <div class="design-row">
        <div class="dr-label">The transform</div>
        <div class="dr-value">Replace every service name with its role relative to the trigger: "trigger", "upstream-1", "downstream-1", "downstream-2", etc. Replace metric values with buckets: "latency-spike-high" instead of "4820ms". Replace log messages with just their level: "error". Keep event kinds and relative timing (rounded to the nearest minute).</div>
      </div>
      <div class="design-row">
        <div class="dr-label">Output</div>
        <div class="dr-value">A sorted tuple like: (deploy@-2min@trigger, metric-latency-spike-high@0min@trigger, log-error@0min@downstream-1, trace@+1min@trigger+downstream-1, remediation@+8min@trigger). Hash this tuple with SHA-256 for fast exact matching.</div>
      </div>
      <div class="design-row">
        <div class="dr-label">Soft matching</div>
        <div class="dr-value">For cases where timing differs or one event is missing, compute the edit distance between two fingerprint tuples. Treat edit distance ≤ 1 as "same family, lower confidence." This handles the L3 "morphed signatures" case.</div>
      </div>
    </div>
  </div>

  <div class="design-block">
    <div class="design-block-header">
      <h3>Storage layout (three tables, one DuckDB file)</h3>
      <div class="tagline">Keep it simple so it's debuggable at 3am</div>
    </div>
    <div class="design-block-body">
      <div class="design-row">
        <div class="dr-label">raw_events</div>
        <div class="dr-value">id, ts (datetime), kind, service, raw_json (string). Append-only. Every other table points back here by id. Never modified after insert.</div>
      </div>
      <div class="design-row">
        <div class="dr-label">nodes + edges</div>
        <div class="dr-value">nodes: node_id (UUID), canonical_name, aliases (JSON), first_seen, last_seen. edges: edge_id, source_node_id, target_node_id, edge_kind, valid_from, valid_to, confidence, evidence_ids (JSON). An edge with valid_to = null is currently active.</div>
      </div>
      <div class="design-row">
        <div class="dr-label">incident_patterns</div>
        <div class="dr-value">incident_id, fingerprint_hash, fingerprint_tuple (JSON), family_id, similarity_to_family_centroid, remediation_ids (JSON), resolved_at, outcome.</div>
      </div>
    </div>
  </div>

  <div class="design-block">
    <div class="design-block-header">
      <h3>Fast mode vs deep mode — the concrete difference</h3>
      <div class="tagline">Both must produce a valid Context — deep just does more work</div>
    </div>
    <div class="design-block-body">
      <div class="design-row">
        <div class="dr-label">Fast mode (≤2s)</div>
        <div class="dr-value">
          1. Extract 30-min event window from in-memory buffer or raw_events. 
          2. Build causal chain from trace_id matches and deploy-before-spike heuristic only. 
          3. Hash the fingerprint → lookup in incident_patterns → return top-3 family matches. 
          4. For each match, pull the best remediation from the remediation cache. 
          5. Fill the explain field from a template. 
          Total DB hits: ~2 queries.
        </div>
      </div>
      <div class="design-row">
        <div class="dr-label">Deep mode (≤6s)</div>
        <div class="dr-value">
          Same as fast mode, plus: 
          Re-traverse the full edge table for the trigger service's neighbourhood, including services 3+ hops away that had correlated metric changes. 
          Re-score all remediation confidences with current decay values rather than cached values. 
          Run soft-matching (edit distance) against the full family table, not just exact-hash matches. 
          Generate a richer explain string that includes the graph path taken.
        </div>
      </div>
    </div>
  </div>
</div>

<hr class="divider">

<!-- ==================== OSS REFERENCES ==================== -->
<div class="design-section">
  <h2>Open source references to study (not to copy)</h2>
  <p class="intro">
    These projects solve overlapping subproblems. Read their source for the specific reason listed — not their full API.
  </p>

  <div class="oss-card">
    <div class="oss-card-header">
      <span class="oss-name">getzep/graphiti</span>
      <a href="https://github.com/getzep/graphiti" class="oss-link" target="_blank">github.com/getzep/graphiti</a>
    </div>
    <div class="oss-body">
      <div><strong>What it does:</strong> Builds knowledge graphs where edges have temporal validity windows (valid_from / valid_to). When a fact changes, the old edge is invalidated — not deleted. Supports hybrid search: semantic + BM25 + graph traversal combined.</div>
      <div><strong>Why study it:</strong> Their bi-temporal edge model is exactly what you need for handling topology drift. Read how they invalidate an edge when new information contradicts it — that is the same logic as "payments-svc becomes billing-svc." Their node deduplication code (how they decide two entity mentions are the same node) is also directly applicable to your rename handling.</div>
      <div><strong>What to skip:</strong> Their LLM-based entity extraction pipeline — you have structured events, not free text. Their Neo4j integration — you're using DuckDB.</div>
      <div class="oss-use">Study: graphiti/graph/graph.py → the edge invalidation logic and the episode-to-graph conversion</div>
    </div>
  </div>

  <div class="oss-card">
    <div class="oss-card-header">
      <span class="oss-name">humemai/agent-room-env-v3</span>
      <a href="https://github.com/humemai/agent-room-env-v3" class="oss-link" target="_blank">github.com/humemai/agent-room-env-v3</a>
    </div>
    <div class="oss-body">
      <div><strong>What it does:</strong> An agent that maintains a temporal knowledge graph in a partially observable environment. Observations arrive as RDF triples; the agent extends them into a temporal KG with decay and reinforcement.</div>
      <div><strong>Why study it:</strong> Their memory scoring model — how they decide which facts to keep with high confidence vs let decay — is the academic backing for your remediation decay formula. Also useful: how they handle "I observed this fact at T1 and again at T2" — they merge, not duplicate. Their forget/reinforce cycle is a working implementation of the evolution mechanism the benchmark requires.</div>
      <div class="oss-use">Study: the TemporalKG class → strength scoring, decay rate per edge type, reinforcement on re-observation</div>
    </div>
  </div>

  <div class="oss-card">
    <div class="oss-card-header">
      <span class="oss-name">Netflix/Hollow (concept reference)</span>
      <a href="https://github.com/Netflix/hollow" class="oss-link" target="_blank">github.com/Netflix/hollow</a>
    </div>
    <div class="oss-body">
      <div><strong>What it does:</strong> An in-memory data store where every historical state is queryable. It snapshots the full dataset at each "generation" so you can query "what did the data look like at version N."</div>
      <div><strong>Why study it:</strong> You need the same property for your graph: "give me the graph state at T=14:25." Hollow's approach to storing deltas (only what changed between generations) rather than full snapshots is the efficient way to implement this. You don't need Hollow itself — just the idea of delta-chained snapshots indexed by timestamp.</div>
      <div class="oss-use">Study: the delta encoding strategy, not the Java implementation details</div>
    </div>
  </div>

  <div class="oss-card">
    <div class="oss-card-header">
      <span class="oss-name">abeimler/ecs_benchmark / flecs</span>
      <a href="https://github.com/SanderMertens/flecs" class="oss-link" target="_blank">github.com/SanderMertens/flecs</a>
    </div>
    <div class="oss-body">
      <div><strong>What it does:</strong> An Entity-Component-System engine used in games — entities have components, relationships are first-class, and the whole thing is designed for high-throughput update/query on evolving state.</div>
      <div><strong>Why study it:</strong> Not to use ECS directly, but because the "entity identity survives component changes" model is exactly analogous to "node identity survives service renames." Read how Flecs handles entity aliasing and relationship traversal. The mental model maps cleanly: Entity = service node, Component = current name/metric profile, Relationship = graph edge.</div>
      <div class="oss-use">Study: entity aliasing, relationship wildcards, and the archetype movement logic when a component changes</div>
    </div>
  </div>

  <div class="oss-card">
    <div class="oss-card-header">
      <span class="oss-name">Netflix/atlas (metric pattern library)</span>
      <a href="https://github.com/Netflix/atlas" class="oss-link" target="_blank">github.com/Netflix/atlas</a>
    </div>
    <div class="oss-body">
      <div><strong>What it does:</strong> Netflix's production time-series metric store. Designed from the ground up to handle millions of metric streams with changing label sets.</div>
      <div><strong>Why study it:</strong> Their approach to "metric identity under label drift" — when a metric is renamed or its tags change, how do you keep continuity — is directly applicable to your fingerprinter. Specifically, read how Atlas handles tag normalization and metric aliasing. Also useful: their spike detection heuristics (is this value anomalous relative to recent history?), which you can borrow for your "is this a latency spike" classifier in the fingerprinter.</div>
      <div class="oss-use">Study: the tag normalization layer and the anomaly detection in the query engine, not the full storage backend</div>
    </div>
  </div>

</div>

<div class="warn-box">
  <strong>The one thing that will sink the whole submission:</strong> letting a service name — any service name — leak into the behavioral fingerprint. Before submitting, add an assertion: serialize your fingerprint to JSON, then grep the result for every service name you ingested. If any appears, find it and remove it. The L3 adversarial scenarios will have cascading rename chains — if the fingerprinter is not completely name-free, it will fail all of them.
</div>

</body>
</html>
