## Phase 2 Research: Soft Matching & Family Clustering

Based on your test output (INC-S1/S2 in different families), here's the targeted research needed:

### 2.1 Research Questions to Answer

| # | Question | Why It Matters |
|---|----------|--------------|
| 1 | **What exactly makes two fingerprints "similar enough" for the same family?** | Current test shows different families for edit distance 1. Need to define thresholds: exact hash match = 1.0 confidence, edit distance 1 = 0.8, distance 2 = 0.6? |
| 2 | **Should families be clusters or centroids?** | Current: exact hash → family. Alternative: maintain family centroid fingerprint, match new incidents to nearest centroid via edit distance. |
| 3 | **What operations should edit distance penalize?** | Options: event removal (missing metric), event replacement (different metric name), timing shift (±2min). Each should have different cost. |
| 4 | **How to prevent family explosion?** | If every slightly different incident creates a new family, you'll have N families for N incidents. Need merge threshold. |
| 5 | **Should we store precomputed similarity edges between families?** | Fast lookup vs compute-on-match tradeoff. |
| 6 | **What signals survive "morphed signatures" in L3 evaluation?** | The benchmark will add: dependency shifts + renames + timing variance. Need to identify invariant structure. |

### 2.2 Technical Deep-Dives Required

**A. Edit Distance Algorithm Selection**
```python
# Options to research:
- Levenshtein on event-kind sequence
- Optimal string alignment (restricted Damerau-Levenshtein)
- Custom: weighted by (event_kind + role + timing_bucket)
- Sequence embedding + cosine similarity
```

**B. Family Representation Strategy**
| Approach | Pros | Cons |
|----------|------|------|
| **Exact hash buckets** (current) | Fast O(1) lookup | Fragile to any change |
| **Centroid + radius** | Tolerant of variation | Centroid drift over time |
| **Hierarchical clustering** | Captures similarity levels | Slower O(n) search |
| **LSH (Locality Sensitive Hashing)** | Approximate fast match | False positives possible |

**C. Confidence Scoring Formula**
Research what formula to use:
```python
confidence = base_match_score * temporal_decay * reinforcement_boost

# Where base_match_score comes from:
- 1.0 for exact hash match
- 0.8 for edit distance 1
- 0.6 for edit distance 2
- 0.0 for distance > 2
```

### 2.3 Key Papers/References to Study

From `@d:\Agents\DeepThink\Build_plab.md`:

1. **getzep/graphiti** - Study their "edge invalidation logic" and "node deduplication code" — how they handle "two entity mentions are the same node"

2. **humemai/agent-room-env-v3** - Study `TemporalKG` class:
   - "strength scoring, decay rate per edge type"
   - "reinforcement on re-observation"
   - "merge, not duplicate" when re-observing facts

### 2.4 Critical Design Decision

**The fingerprint transform needs refinement:**

Current (from build plan):
```python
(deploy@-2min@trigger, metric-latency-spike-high@0min@trigger, ...)
```

Research: **What should be canonicalized vs preserved?**

| Signal | Current | Research Question |
|--------|---------|-------------------|
| Service names | Replaced with role | ✅ Correct |
| Metric names | Preserved verbatim | Should metric "latency_p99_ms" and "latency_p95_ms" be bucketed as "latency-metric"? |
| Metric values | Bucketed (high/medium/low) | Are the thresholds correct? |
| Timing | Rounded to minute | Should relative timing gaps be normalized? (deploy→incident always = "T-2min") |
| Log messages | Level only | Should error patterns be extracted? |

### 2.5 Immediate Experiments to Run

1. **Visualize fingerprint differences** between INC-S1 and INC-S2:
   ```python
   print(f"INC-S1: {p1['fingerprint_tuple']}")
   print(f"INC-S2: {p2['fingerprint_tuple']}")
   print(f"Edit distance: {compute_edit_distance(p1, p2)}")
   ```

2. **Test if edit distance ≤ 1 should force same family** or just "similar incident" with cross-family link

3. **Determine family merge threshold**: When should two families be merged into one?

Which area should we dive into first — the edit distance algorithm, the family clustering strategy, or the fingerprint canonicalization?