"""
Patch script: applies all 5 fixes from IMPROVEMENT.md
Run from: d:\\Agents\\DeepThink
"""
import re

# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────
def patch(path, old, new, label):
    with open(path, "rb") as f:
        raw = f.read()
    old_b = old.encode("utf-8")
    new_b = new.encode("utf-8")
    if old_b not in raw:
        print(f"  [SKIP] {label}: pattern not found in {path}")
        return False
    patched = raw.replace(old_b, new_b, 1)
    with open(path, "wb") as f:
        f.write(patched)
    print(f"  [OK]   {label}")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Files
# ─────────────────────────────────────────────────────────────────────────────
COORDINATOR = r"d:\Agents\DeepThink\persistent_context_engine\ingestion\coordinator.py"
ENGINE      = r"d:\Agents\DeepThink\persistent_context_engine\engine.py"
PATTERN     = r"d:\Agents\DeepThink\persistent_context_engine\storage\pattern_store.py"
MYTEAM      = r"d:\Agents\DeepThink\Anvil-P-E\bench-p02-context\adapters\myteam.py"


# ─────────────────────────────────────────────────────────────────────────────
# Fix A + C  (coordinator.py — _handle_incident_signal)
#   A: supplement window_events from buffer when DB has < 5 events
#   C: inject signal event itself into events_for_fp
# ─────────────────────────────────────────────────────────────────────────────
coord_old = (
    "        # Fetch events in window (from buffer or DB)\n"
    "        window_events = self._raw.get_by_timerange(\n"
    "            window_start, window_end, limit=1000\n"
    "        )\n"
    "\n"
    "        # Restrict fingerprint events to trigger service + immediate topology neighborhood.\n"
    "        # This prevents background noise from unrelated services from dominating hashes.\n"
    "        relevant_services = {service}\n"
    "        neighbor_ids = self._graph.neighbors_within_hops(trigger_node_id, hops=2)\n"
    "        for nid in neighbor_ids:\n"
    "            for name in self._nodes.all_names_for_id(nid):\n"
    "                relevant_services.add(name)\n"
    "\n"
    "        # Build event dicts for fingerprinter\n"
    "        events_for_fp = []\n"
    "        for row in window_events:\n"
    "            raw = row.get(\"raw\", {})\n"
    "            raw_svc = raw.get(\"service\")\n"
    "            if raw_svc and raw_svc not in relevant_services:\n"
    "                continue\n"
    "            # Parse ts back to datetime if needed\n"
    "            raw_ts = raw.get(\"ts\")\n"
    "            if isinstance(raw_ts, str):\n"
    "                try:\n"
    "                    raw[\"ts\"] = self._parser._parse_timestamp(raw_ts)\n"
    "                except Exception:\r\n"
    "                    pass\r\n"
    "            events_for_fp.append(raw)\r\n"
)

coord_new = (
    "        # Fetch events in window from DB\n"
    "        window_events = self._raw.get_by_timerange(\n"
    "            window_start, window_end, limit=1000\n"
    "        )\n"
    "\n"
    "        # Fix A: DB may not yet have recent events if the batch was not flushed.\n"
    "        # Supplement from the in-memory ring buffer to capture unflushed events.\n"
    "        db_event_ids = {row.get(\"id\") for row in window_events if row.get(\"id\")}\n"
    "        if len(window_events) < 5:\n"
    "            for buf_ev in self._buffer.in_window(window_start, window_end):\n"
    "                buf_ev_id = buf_ev.get(\"id\")\n"
    "                if buf_ev_id not in db_event_ids:\n"
    "                    window_events.append({\"raw\": buf_ev, \"id\": buf_ev_id})\n"
    "                    if buf_ev_id:\n"
    "                        db_event_ids.add(buf_ev_id)\n"
    "\n"
    "        # Restrict fingerprint events to trigger service + immediate topology neighborhood.\n"
    "        # This prevents background noise from unrelated services from dominating hashes.\n"
    "        relevant_services = {service}\n"
    "        neighbor_ids = self._graph.neighbors_within_hops(trigger_node_id, hops=2)\n"
    "        for nid in neighbor_ids:\n"
    "            for name in self._nodes.all_names_for_id(nid):\n"
    "                relevant_services.add(name)\n"
    "\n"
    "        # Fix C: Inject the incident signal event itself as the first fingerprint element.\n"
    "        # This guarantees every fingerprint has at least one incident_signal element,\n"
    "        # preventing empty-hash collisions that collapse all families into one bucket.\n"
    "        synthetic_signal = dict(event)\n"
    "        synthetic_signal[\"ts\"] = ts\n"
    "        events_for_fp = [synthetic_signal]\n"
    "        seen_ids_fp = {event.get(\"id\")} if event.get(\"id\") else set()\n"
    "\n"
    "        # Build event dicts for fingerprinter from DB+buffer window\n"
    "        for row in window_events:\n"
    "            raw = row.get(\"raw\", {})\n"
    "            raw_svc = raw.get(\"service\")\n"
    "            if raw_svc and raw_svc not in relevant_services:\n"
    "                continue\n"
    "            # Parse ts back to datetime if needed\n"
    "            raw_ts = raw.get(\"ts\")\n"
    "            if isinstance(raw_ts, str):\n"
    "                try:\n"
    "                    raw[\"ts\"] = self._parser._parse_timestamp(raw_ts)\n"
    "                except Exception:\n"
    "                    pass\n"
    "            raw_id = raw.get(\"id\")\n"
    "            if raw_id and raw_id in seen_ids_fp:\n"
    "                continue\n"
    "            if raw_id:\n"
    "                seen_ids_fp.add(raw_id)\n"
    "            events_for_fp.append(raw)\n"
)

patch(COORDINATOR, coord_old, coord_new, "Fix A+C: buffer fallback + signal injection")


# ─────────────────────────────────────────────────────────────────────────────
# Fix B  (engine.py — _should_abstain + threshold floor)
# ─────────────────────────────────────────────────────────────────────────────
engine_abstain_old = (
    "    @staticmethod\n"
    "    def _should_abstain(similar_incidents: List[IncidentMatch]) -> bool:\n"
    "        \"\"\"\n"
    "        Return True when retrieval confidence is too weak/ambiguous.\n"
    "\n"
    "        This is intentionally generic:\n"
    "        - no strong top match\n"
    "        - or top-2 are both low and close (no clear winner)\n"
    "        \"\"\"\n"
    "        if not similar_incidents:\n"
    "            return True\n"
    "\n"
    "        sims = [float(m.get(\"similarity\", 0.0)) for m in similar_incidents]\n"
    "        top = sims[0]\n"
    "        second = sims[1] if len(sims) > 1 else 0.0\n"
    "\n"
    "        # Strong nearest neighbor: keep results.\n"
    "        if top >= 0.62:\n"
    "            return False\n"
    "\n"
    "        # Weak top-1 and no clear separation from top-2: abstain.\n"
    "        if top < 0.5:\n"
    "            return True\n"
    "\n"
    "        return (top - second) < 0.08\n"
)

engine_abstain_new = (
    "    @staticmethod\n"
    "    def _should_abstain(similar_incidents: List[IncidentMatch]) -> bool:\n"
    "        \"\"\"\n"
    "        Return True when retrieval confidence is too weak/ambiguous.\n"
    "\n"
    "        Fix B: Lowered thresholds for L3 scale.  Under cascading renames the\n"
    "        fingerprint element role assignments can shift slightly, so genuine\n"
    "        family similarities drop to 0.6-0.75.  The old 0.5 floor and 0.62\n"
    "        keep-band were clipping many real matches.\n"
    "        \"\"\"\n"
    "        if not similar_incidents:\n"
    "            return True\n"
    "\n"
    "        sims = [float(m.get(\"similarity\", 0.0)) for m in similar_incidents]\n"
    "        top = sims[0]\n"
    "        second = sims[1] if len(sims) > 1 else 0.0\n"
    "\n"
    "        # Strong nearest neighbor: keep results (lowered from 0.62).\n"
    "        if top >= 0.55:\n"
    "            return False\n"
    "\n"
    "        # Weak top-1: abstain (lowered floor from 0.50 to 0.38).\n"
    "        if top < 0.38:\n"
    "            return True\n"
    "\n"
    "        # Ambiguous: abstain if top-2 are too close (relaxed gap from 0.08 to 0.06).\n"
    "        return (top - second) < 0.06\n"
)

patch(ENGINE, engine_abstain_old, engine_abstain_new, "Fix B: abstain threshold tuning")


# ─────────────────────────────────────────────────────────────────────────────
# Fix D  (engine.py — _find_similar_incidents: two-pass same-service query)
# ─────────────────────────────────────────────────────────────────────────────
engine_query_old = (
    "        all_trigger_ids = set(self._resolve_all_node_ids(trigger_node_id))\n"
    "\n"
    "        # Fetch all known patterns with fingerprint data\n"
    "        if reference_ts is not None:\n"
    "            all_rows = self._pattern_store._cursor().execute(\n"
    "                \"\"\"SELECT p.incident_id, p.trigger_node_id, p.family_id,\n"
    "                          p.fingerprint_hash, p.fingerprint_tuple, p.created_at\n"
    "                   FROM incident_patterns p\n"
    "                   WHERE EXISTS (\n"
    "                       SELECT 1 FROM remediation_history r\n"
    "                       WHERE r.incident_id = p.incident_id\n"
    "                   )\n"
    "                     AND\n"
    "                         p.created_at < ?\n"
    "                   ORDER BY p.created_at DESC\"\"\",\n"
    "                [reference_ts]\n"
    "            ).fetchall()\n"
    "        else:\n"
    "            all_rows = self._pattern_store._cursor().execute(\n"
    "                \"\"\"SELECT p.incident_id, p.trigger_node_id, p.family_id,\n"
    "                          p.fingerprint_hash, p.fingerprint_tuple, p.created_at\n"
    "                   FROM incident_patterns p\n"
    "                   WHERE EXISTS (\n"
    "                       SELECT 1 FROM remediation_history r\n"
    "                       WHERE r.incident_id = p.incident_id\n"
    "                   )\n"
    "                   ORDER BY p.created_at DESC\"\"\"\n"
    "            ).fetchall()\n"
)

engine_query_new = (
    "        all_trigger_ids = set(self._resolve_all_node_ids(trigger_node_id))\n"
    "        trigger_id_list = list(all_trigger_ids)\n"
    "        placeholders = \",\".join(\"?\" * len(trigger_id_list)) if trigger_id_list else \"''\"\n"
    "\n"
    "        # Fix D: Two-pass query — prefer same trigger service, fall back globally.\n"
    "        # This reduces false positives from coincidentally similar cross-service hashes\n"
    "        # while preserving rename-proof recall via the global fallback.\n"
    "        cursor = self._pattern_store._cursor()\n"
    "\n"
    "        def _fetch_rows(extra_where, params):\n"
    "            base = (\n"
    "                \"SELECT p.incident_id, p.trigger_node_id, p.family_id, \"\n"
    "                \"p.fingerprint_hash, p.fingerprint_tuple, p.created_at \"\n"
    "                \"FROM incident_patterns p \"\n"
    "                \"WHERE EXISTS (\"\n"
    "                \"  SELECT 1 FROM remediation_history r WHERE r.incident_id = p.incident_id\"\n"
    "                \") \"\n"
    "            )\n"
    "            sql = base + extra_where + \" ORDER BY p.created_at DESC\"\n"
    "            return cursor.execute(sql, params).fetchall()\n"
    "\n"
    "        if reference_ts is not None:\n"
    "            # Pass 1: same trigger service only\n"
    "            same_svc_rows = _fetch_rows(\n"
    "                f\"AND p.trigger_node_id IN ({placeholders}) AND p.created_at < ?\",\n"
    "                [*trigger_id_list, reference_ts],\n"
    "            ) if trigger_id_list else []\n"
    "\n"
    "            # Pass 2: global fallback when same-service results are sparse\n"
    "            if len(same_svc_rows) < mp.match_limit:\n"
    "                global_rows = _fetch_rows(\"AND p.created_at < ?\", [reference_ts])\n"
    "                # Merge: same-service rows first, then de-duplicated global rows\n"
    "                seen_inc = {r[0] for r in same_svc_rows}\n"
    "                extra = [r for r in global_rows if r[0] not in seen_inc]\n"
    "                all_rows = same_svc_rows + extra\n"
    "            else:\n"
    "                all_rows = same_svc_rows\n"
    "        else:\n"
    "            same_svc_rows = _fetch_rows(\n"
    "                f\"AND p.trigger_node_id IN ({placeholders})\",\n"
    "                trigger_id_list,\n"
    "            ) if trigger_id_list else []\n"
    "            if len(same_svc_rows) < mp.match_limit:\n"
    "                global_rows = _fetch_rows(\"\", [])\n"
    "                seen_inc = {r[0] for r in same_svc_rows}\n"
    "                extra = [r for r in global_rows if r[0] not in seen_inc]\n"
    "                all_rows = same_svc_rows + extra\n"
    "            else:\n"
    "                all_rows = same_svc_rows\n"
)

patch(ENGINE, engine_query_old, engine_query_new, "Fix D: two-pass same-service query")


# ─────────────────────────────────────────────────────────────────────────────
# Fix E  (pattern_store.py — Jaccard pre-filter before edit distance)
# ─────────────────────────────────────────────────────────────────────────────
ps_old = (
    "        best_match = None\n"
    "        best_distance = float('inf')\n"
    "        \n"
    "        for family_id, family_hash, confidence, rep_tuple in families:\n"
    "            try:\n"
    "                rep_elements = json.loads(rep_tuple)\n"
    "                rep_fp = IncidentFingerprint(\n"
    "                    elements=rep_elements,\n"
    "                    structural_hash=family_hash,\n"
    "                    trigger_node_id=\"\",\n"
    "                    window_start=created_at,\n"
    "                    window_end=created_at,\n"
    "                    event_count=len(rep_elements)\n"
    "                )\n"
    "                distance = new_fp.edit_distance(rep_fp)\n"
    "                if distance <= soft_match_threshold:\n"
)

ps_new = (
    "        best_match = None\n"
    "        best_distance = float('inf')\n"
    "\n"
    "        # Fix E: Jaccard pre-filter — skip edit distance when multiset overlap is\n"
    "        # too low.  Eliminates ~80% of comparisons at L3 scale (30 svcs, 85 signals).\n"
    "        from collections import Counter as _Counter\n"
    "        new_counter = _Counter(\n"
    "            tuple(e) if isinstance(e, list) else e for e in new_elements\n"
    "        )\n"
    "\n"
    "        for family_id, family_hash, confidence, rep_tuple in families:\n"
    "            try:\n"
    "                rep_elements = json.loads(rep_tuple)\n"
    "\n"
    "                # Jaccard pre-filter (cheap multiset comparison)\n"
    "                rep_counter = _Counter(\n"
    "                    tuple(e) if isinstance(e, list) else e for e in rep_elements\n"
    "                )\n"
    "                common = sum(\n"
    "                    min(new_counter[k], rep_counter[k])\n"
    "                    for k in new_counter.keys() | rep_counter.keys()\n"
    "                )\n"
    "                total = sum(\n"
    "                    max(new_counter[k], rep_counter[k])\n"
    "                    for k in new_counter.keys() | rep_counter.keys()\n"
    "                )\n"
    "                if total == 0 or (common / total) < 0.35:\n"
    "                    continue  # Skip edit distance — definitely not a soft match\n"
    "\n"
    "                rep_fp = IncidentFingerprint(\n"
    "                    elements=rep_elements,\n"
    "                    structural_hash=family_hash,\n"
    "                    trigger_node_id=\"\",\n"
    "                    window_start=created_at,\n"
    "                    window_end=created_at,\n"
    "                    event_count=len(rep_elements)\n"
    "                )\n"
    "                distance = new_fp.edit_distance(rep_fp)\n"
    "                if distance <= soft_match_threshold:\n"
)

patch(PATTERN, ps_old, ps_new, "Fix E: Jaccard pre-filter in find_or_create_family")


# ─────────────────────────────────────────────────────────────────────────────
# Fix E (part 2)  (myteam.py — larger buffer + batch size for L3 scale)
# ─────────────────────────────────────────────────────────────────────────────
myteam_old = (
    "        self._engine = PCEngine(EngineConfig(db_path=\":memory:\", buffer_size=2000))\n"
)
myteam_new = (
    "        # Fix E: Larger buffer (L3 has 75k background events, 2000 evicts too fast)\n"
    "        # and larger ingest_batch_size to reduce transaction round-trips.\n"
    "        self._engine = PCEngine(EngineConfig(\n"
    "            db_path=\":memory:\",\n"
    "            buffer_size=5000,\n"
    "            ingest_batch_size=1000,\n"
    "        ))\n"
)

patch(MYTEAM, myteam_old, myteam_new, "Fix E: buffer_size=5000, ingest_batch_size=1000 in myteam.py")

print("\nAll patches complete.")
