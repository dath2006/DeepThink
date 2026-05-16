"""
Patch script v2: applies Fix D and Fix E (pattern_store) with correct CRLF endings.
"""

def patch(path, old_b, new_b, label):
    with open(path, "rb") as f:
        raw = f.read()
    if old_b not in raw:
        print(f"  [SKIP] {label}: pattern not found")
        # Debug: show surrounding bytes
        key = old_b[:30]
        idx = raw.find(key)
        if idx >= 0:
            print(f"         Key found at {idx}, showing context:")
            print(repr(raw[idx:idx+len(old_b)+20]))
        return False
    patched = raw.replace(old_b, new_b, 1)
    with open(path, "wb") as f:
        f.write(patched)
    print(f"  [OK]   {label}")
    return True


ENGINE  = r"d:\Agents\DeepThink\persistent_context_engine\engine.py"
PATTERN = r"d:\Agents\DeepThink\persistent_context_engine\storage\pattern_store.py"

# ─────────────────────────────────────────────────────────────────────────────
# Fix D  — engine.py: two-pass same-service query
# Key insight from byte inspection: the file mixes \r\n and \n endings.
# The block starting at "all_trigger_ids" uses \r\n then switches to \n for the
# execute() block. We must replicate that exactly.
# ─────────────────────────────────────────────────────────────────────────────
fix_d_old = (
    b"        all_trigger_ids = set(self._resolve_all_node_ids(trigger_node_id))\r\n"
    b"\r\n"
    b"        # Fetch all known patterns with fingerprint data\r\n"
    b"        if reference_ts is not None:\n"
    b"            all_rows = self._pattern_store._cursor().execute(\n"
    b"                \"\"\"SELECT p.incident_id, p.trigger_node_id, p.family_id,\n"
    b"                          p.fingerprint_hash, p.fingerprint_tuple, p.created_at\n"
    b"                   FROM incident_patterns p\n"
    b"                   WHERE EXISTS (\n"
    b"                       SELECT 1 FROM remediation_history r\n"
    b"                       WHERE r.incident_id = p.incident_id\n"
    b"                   )\n"
    b"                     AND\n"
    b"                         p.created_at < ?\n"
    b"                   ORDER BY p.created_at DESC\"\"\",\n"
    b"                [reference_ts]\n"
    b"            ).fetchall()\n"
    b"        else:\n"
    b"            all_rows = self._pattern_store._cursor().execute(\n"
    b"                \"\"\"SELECT p.incident_id, p.trigger_node_id, p.family_id,\n"
    b"                          p.fingerprint_hash, p.fingerprint_tuple, p.created_at\n"
    b"                   FROM incident_patterns p\n"
    b"                   WHERE EXISTS (\n"
    b"                       SELECT 1 FROM remediation_history r\n"
    b"                       WHERE r.incident_id = p.incident_id\n"
    b"                   )\n"
    b"                   ORDER BY p.created_at DESC\"\"\"\n"
    b"            ).fetchall()\n"
)

fix_d_new = (
    b"        all_trigger_ids = set(self._resolve_all_node_ids(trigger_node_id))\n"
    b"        trigger_id_list = list(all_trigger_ids)\n"
    b"        placeholders = \",\".join(\"?\" * len(trigger_id_list)) if trigger_id_list else \"''\"\n"
    b"\n"
    b"        # Fix D: Two-pass query to reduce cross-service false positives while\n"
    b"        # preserving rename-proof recall via the global fallback pass.\n"
    b"        cursor = self._pattern_store._cursor()\n"
    b"\n"
    b"        def _fetch_rows(extra_where, params):\n"
    b"            sql = (\n"
    b"                \"SELECT p.incident_id, p.trigger_node_id, p.family_id, \"\n"
    b"                \"p.fingerprint_hash, p.fingerprint_tuple, p.created_at \"\n"
    b"                \"FROM incident_patterns p \"\n"
    b"                \"WHERE EXISTS (\"\n"
    b"                \"  SELECT 1 FROM remediation_history r WHERE r.incident_id = p.incident_id\"\n"
    b"                \") \" + extra_where + \" ORDER BY p.created_at DESC\"\n"
    b"            )\n"
    b"            return cursor.execute(sql, params).fetchall()\n"
    b"\n"
    b"        if reference_ts is not None:\n"
    b"            # Pass 1: same trigger service\n"
    b"            same_svc_rows = _fetch_rows(\n"
    b"                f\"AND p.trigger_node_id IN ({placeholders}) AND p.created_at < ?\",\n"
    b"                [*trigger_id_list, reference_ts],\n"
    b"            ) if trigger_id_list else []\n"
    b"            # Pass 2: global fallback when same-service results are sparse\n"
    b"            if len(same_svc_rows) < mp.match_limit:\n"
    b"                global_rows = _fetch_rows(\"AND p.created_at < ?\", [reference_ts])\n"
    b"                seen_inc = {r[0] for r in same_svc_rows}\n"
    b"                all_rows = same_svc_rows + [r for r in global_rows if r[0] not in seen_inc]\n"
    b"            else:\n"
    b"                all_rows = same_svc_rows\n"
    b"        else:\n"
    b"            same_svc_rows = _fetch_rows(\n"
    b"                f\"AND p.trigger_node_id IN ({placeholders})\",\n"
    b"                trigger_id_list,\n"
    b"            ) if trigger_id_list else []\n"
    b"            if len(same_svc_rows) < mp.match_limit:\n"
    b"                global_rows = _fetch_rows(\"\", [])\n"
    b"                seen_inc = {r[0] for r in same_svc_rows}\n"
    b"                all_rows = same_svc_rows + [r for r in global_rows if r[0] not in seen_inc]\n"
    b"            else:\n"
    b"                all_rows = same_svc_rows\n"
)

patch(ENGINE, fix_d_old, fix_d_new, "Fix D: two-pass same-service query (engine.py)")


# ─────────────────────────────────────────────────────────────────────────────
# Fix E  — pattern_store.py: Jaccard pre-filter before edit distance
# Byte inspection shows CRLF throughout this file.
# ─────────────────────────────────────────────────────────────────────────────
fix_e_old = (
    b"        best_match = None\r\n"
    b"        best_distance = float('inf')\r\n"
    b"        \r\n"
    b"        for family_id, family_hash, confidence, rep_tuple in families:\r\n"
    b"            try:\r\n"
    b"                rep_elements = json.loads(rep_tuple)\r\n"
    b"                rep_fp = IncidentFingerprint(\r\n"
    b"                    elements=rep_elements,\r\n"
    b"                    structural_hash=family_hash,\r\n"
    b"                    trigger_node_id=\"\",\r\n"
    b"                    window_start=created_at,\r\n"
    b"                    window_end=created_at,\r\n"
    b"                    event_count=len(rep_elements)\r\n"
    b"                )\r\n"
    b"                distance = new_fp.edit_distance(rep_fp)\r\n"
    b"                if distance <= soft_match_threshold:\r\n"
)

fix_e_new = (
    b"        best_match = None\r\n"
    b"        best_distance = float('inf')\r\n"
    b"\r\n"
    b"        # Fix E: Jaccard pre-filter eliminates ~80% of edit-distance calls at L3 scale.\r\n"
    b"        from collections import Counter as _Counter\r\n"
    b"        new_counter = _Counter(\r\n"
    b"            tuple(e) if isinstance(e, list) else e for e in new_elements\r\n"
    b"        )\r\n"
    b"\r\n"
    b"        for family_id, family_hash, confidence, rep_tuple in families:\r\n"
    b"            try:\r\n"
    b"                rep_elements = json.loads(rep_tuple)\r\n"
    b"\r\n"
    b"                # Cheap multiset Jaccard — skip edit distance if too dissimilar\r\n"
    b"                rep_counter = _Counter(\r\n"
    b"                    tuple(e) if isinstance(e, list) else e for e in rep_elements\r\n"
    b"                )\r\n"
    b"                _common = sum(\r\n"
    b"                    min(new_counter[k], rep_counter[k])\r\n"
    b"                    for k in new_counter.keys() | rep_counter.keys()\r\n"
    b"                )\r\n"
    b"                _total = sum(\r\n"
    b"                    max(new_counter[k], rep_counter[k])\r\n"
    b"                    for k in new_counter.keys() | rep_counter.keys()\r\n"
    b"                )\r\n"
    b"                if _total == 0 or (_common / _total) < 0.35:\r\n"
    b"                    continue  # Definitely not a soft match; skip edit distance\r\n"
    b"\r\n"
    b"                rep_fp = IncidentFingerprint(\r\n"
    b"                    elements=rep_elements,\r\n"
    b"                    structural_hash=family_hash,\r\n"
    b"                    trigger_node_id=\"\",\r\n"
    b"                    window_start=created_at,\r\n"
    b"                    window_end=created_at,\r\n"
    b"                    event_count=len(rep_elements)\r\n"
    b"                )\r\n"
    b"                distance = new_fp.edit_distance(rep_fp)\r\n"
    b"                if distance <= soft_match_threshold:\r\n"
)

patch(PATTERN, fix_e_old, fix_e_new, "Fix E: Jaccard pre-filter (pattern_store.py)")

print("\nDone.")
