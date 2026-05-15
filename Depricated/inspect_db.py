"""Inspector script for the Persistent Context Engine DuckDB database.

Usage:
    python inspect_db.py                    # inspect :memory: (run after engine starts)
    python inspect_db.py context_engine.duckdb   # inspect a file-based DB
"""
import sys
import json
import duckdb

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else ":memory:"

conn = duckdb.connect(database=DB_PATH)

print("=" * 60)
print(f"Database: {DB_PATH}")
print("=" * 60)

# --- raw_events ---
print("\n--- raw_events ---")
try:
    df = conn.execute("SELECT id, ts, kind, service FROM raw_events ORDER BY ts").fetchdf()
    print(df.to_string(index=False))
    print(f"Total raw_events: {conn.execute('SELECT COUNT(*) FROM raw_events').fetchone()[0]}")
except Exception as e:
    print(f"Error reading raw_events: {e}")

# --- nodes ---
print("\n--- nodes ---")
try:
    df = conn.execute(
        "SELECT id, canonical_name, aliases, first_seen_ts, last_seen_ts FROM nodes ORDER BY canonical_name"
    ).fetchdf()
    print(df.to_string(index=False))
    print(f"Total nodes: {conn.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]}")
except Exception as e:
    print(f"Error reading nodes: {e}")

# --- edges ---
print("\n--- edges ---")
try:
    df = conn.execute(
        """SELECT id, source_node_id, target_node_id, edge_kind,
                  retired_at, confidence, evidence_event_ids
           FROM edges ORDER BY edge_kind"""
    ).fetchdf()
    print(df.to_string(index=False))
    total = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
    active = conn.execute("SELECT COUNT(*) FROM edges WHERE retired_at IS NULL").fetchone()[0]
    print(f"Total edges: {total} | Active: {active} | Retired: {total - active}")
except Exception as e:
    print(f"Error reading edges: {e}")

# --- Sample raw_json pretty-print ---
print("\n--- Sample raw_json (first 3 events) ---")
try:
    rows = conn.execute("SELECT id, kind, raw_json FROM raw_events LIMIT 3").fetchall()
    for rid, kind, raw in rows:
        parsed = json.loads(raw)
        print(f"\n  [{rid}] {kind}:")
        for k, v in parsed.items():
            print(f"    {k}: {v}")
except Exception as e:
    print(f"Error reading raw_json: {e}")

conn.close()
print("\n" + "=" * 60)
