Re-Architecture Plan
Based on the audit, here's a phased plan to replace the gaming approach with genuine behavioral intelligence. Each phase is self-contained, testable, and builds on the previous.

Phase A: Fix Mode Differentiation (Foundation)
Goal: Deep mode must do measurably more work than fast mode.

reconstruct_context — Thread mode through every sub-method
Fast mode: 1-hop graph expansion, top-3 similar incidents, simple causal pairs
Deep mode: 2-hop expansion, top-5 similar incidents, multi-hop causal chains, richer explain
Fingerprinter: Fast uses coarse time buckets (10min), deep uses fine buckets (2min)
Causal chain builder: Fast = pairwise only, deep = transitive chain construction (A→B→C)
Verification: Run self_check in both modes — scores should now differ.

Phase B: Genuine Fingerprint-Based Matching (Core Intelligence)
Goal: Remove incident_id parsing. Match incidents by behavioral similarity.

Delete \_incident_family_tag() — the gaming function
\_find_similar_incidents — Replace with:
Compute fingerprint for current incident
Query all stored patterns from incident_patterns table
Rank by IncidentFingerprint.edit_distance() (already implemented but unused)
Apply similarity threshold (e.g., ≥0.5)
Return top-K diverse matches (one per family to maintain recall)
Family assignment at ingest — Already exists via find_or_create_family() with soft matching. Verify it works without incident_id gaming.
Similarity score in output — Use actual 1.0 - (edit_distance / max_len) instead of hardcoded 0.9/0.75.
Verification: Run self_check — recall@5 should remain 1.0, precision@5 should remain ~0.200 (structural benchmark limit).

Phase C: Graph-Driven Causal Chains (Depth)
Goal: Build real causal chains using topology, not just pairwise event co-occurrence.

\_build_causal_chain — Rewrite to:
Start from trigger service
Walk graph edges (upstream predecessors first)
For each edge, check if both endpoints have correlated events in the window
Score edges by: temporal ordering + event severity + graph distance
Deep mode addition: Follow transitive paths (A→B→C) up to 3 hops
Use TemporalGraphView.at_timestamp() for historical topology correctness
Confidence scoring: Based on evidence count, temporal gap, and graph path length
Verification: Causal chain should contain actual service-to-service propagation paths. Manual inspect output.

Phase D: Remediation Accuracy Fix (Quick Win)
Goal: Close the remaining remediation_acc gap.

Fix applied_at timestamps — Use event ts instead of datetime.now()
Broaden remediation lookup: Query by family_id first (already works), then fallback to trigger_node_id across all families
Deep mode addition: In deep mode, also search for remediations on upstream/downstream services (not just trigger)
Temporal decay: Now functional since timestamps will be correct
Verification: remediation_acc should reach 1.0 consistently across seeds.

Phase E: Explain Narrative (Manual Score)
Goal: Improve manual_explain from ~1-2 to 3-4 on a 1-5 scale.

Structured explain sections:
Trigger: What happened, which service, what alert pattern
Topology context: Which services are related, any recent topology changes (renames/dep shifts)
Causal reasoning: Step-by-step propagation path from causal chain
Historical parallels: Which past incidents are similar and why (fingerprint comparison details)
Remediation rationale: Why this action is suggested, historical success rate
Deep mode addition: Include topology drift narrative ("service X was renamed from Y on date Z")
Use TemporalGraphView.get_graph_changes_between() to detect and describe topology changes in the incident window
Verification: Manual review of explain output quality.

Execution Order & Dependencies
Phase A (mode differentiation) ← no dependencies, do first
Phase B (fingerprint matching) ← requires A for mode-aware thresholds
Phase D (remediation fix) ← independent, can parallel with B
Phase C (causal chains) ← requires A for mode-aware depth
Phase E (explain) ← requires B + C for content to explain
Estimated scope: Phases A+B are the critical 80/20 — they fix the two fatal flaws (no mode differentiation, gaming-based matching). C/D/E are refinements.
