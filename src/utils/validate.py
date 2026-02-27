"""
ECARE Validation Report
Generates quality assessment of the entity resolution and relationship merge.

Usage:
    python src/utils/validate.py [--db-path data/output/ecare.db]
"""

import json
import os
import sys
import csv
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.utils.common import get_db_connection, DEFAULT_DB_PATH


def run_validation(db_path: str):
    conn = get_db_connection(db_path)

    print("=" * 60)
    print("ECARE Validation Report")
    print("=" * 60)

    # --- Entity Summary ---
    print("\n1. ENTITY SUMMARY")
    rows = conn.execute(
        "SELECT entity_type, COUNT(*) FROM canonical_entities GROUP BY entity_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    total = sum(r[1] for r in rows)
    print(f"   Total canonical entities: {total}")
    for row in rows:
        print(f"     {row[0]}: {row[1]}")

    # --- Resolution Summary ---
    print("\n2. ENTITY RESOLUTION SUMMARY")
    rows = conn.execute(
        "SELECT source_system, match_method, COUNT(*), ROUND(AVG(match_confidence), 3) "
        "FROM entity_resolution_log GROUP BY source_system, match_method "
        "ORDER BY source_system, match_method"
    ).fetchall()
    for row in rows:
        print(f"   {row[0]} / {row[1]}: {row[2]} entries (avg confidence: {row[3]})")

    # --- Fuzzy Matches (need review) ---
    print("\n3. FUZZY MATCHES (requires manual review)")
    fuzzy = conn.execute(
        """SELECT source_system, source_entity_name, canonical_id, match_confidence, match_details
           FROM entity_resolution_log
           WHERE match_method = 'fuzzy'
           ORDER BY match_confidence ASC
           LIMIT 50"""
    ).fetchall()

    # Get canonical names for display
    fuzzy_report = []
    for row in fuzzy:
        cname = conn.execute(
            "SELECT canonical_name FROM canonical_entities WHERE canonical_id = ?",
            (row[2],)
        ).fetchone()
        canonical_name = cname[0] if cname else "???"
        fuzzy_report.append({
            "source": row[0],
            "source_name": row[1],
            "canonical_name": canonical_name,
            "confidence": row[3],
            "canonical_id": row[2],
        })
        print(f"   [{row[3]:.2f}] \"{row[1]}\" → \"{canonical_name}\" ({row[0]})")

    # --- Export fuzzy matches for review ---
    fuzzy_all = conn.execute(
        """SELECT erl.source_system, erl.source_entity_name, ce.canonical_name,
                  erl.match_confidence, erl.canonical_id, erl.match_details
           FROM entity_resolution_log erl
           JOIN canonical_entities ce ON erl.canonical_id = ce.canonical_id
           WHERE erl.match_method = 'fuzzy'
           ORDER BY erl.match_confidence ASC"""
    ).fetchall()

    fuzzy_csv_path = "data/output/fuzzy_matches_review.csv"
    os.makedirs(os.path.dirname(fuzzy_csv_path), exist_ok=True)
    with open(fuzzy_csv_path, "w", newline="", encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["source_system", "source_name", "matched_canonical_name",
                         "confidence", "canonical_id", "review_status", "notes"])
        for row in fuzzy_all:
            writer.writerow([row[0], row[1], row[2], row[3], row[4], "", ""])

    print(f"\n   → Exported {len(fuzzy_all)} fuzzy matches to {fuzzy_csv_path}")

    # --- Relationship Summary ---
    print("\n4. RELATIONSHIP SUMMARY")
    rows = conn.execute(
        "SELECT relationship_type, COUNT(*) FROM relationships GROUP BY relationship_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    total_rels = sum(r[1] for r in rows)
    print(f"   Total relationships: {total_rels}")
    for row in rows:
        print(f"     {row[0]}: {row[1]}")

    # --- Multi-source relationships ---
    print("\n5. CROSS-SOURCE CORROBORATION")
    multi_source = conn.execute(
        """SELECT r.relationship_id, ce1.canonical_name, ce2.canonical_name,
                  r.relationship_type, COUNT(DISTINCT rs.source_system) as source_count
           FROM relationships r
           JOIN relationship_sources rs ON r.relationship_id = rs.relationship_id
           JOIN canonical_entities ce1 ON r.source_entity_id = ce1.canonical_id
           JOIN canonical_entities ce2 ON r.target_entity_id = ce2.canonical_id
           GROUP BY r.relationship_id
           HAVING source_count > 1
           ORDER BY source_count DESC
           LIMIT 20"""
    ).fetchall()
    print(f"   Relationships with 2+ sources: {len(multi_source)}")
    for row in multi_source[:10]:
        print(f"     {row[1]} ↔ {row[2]} ({row[3]}): {row[4]} sources")

    # --- Integrity Checks ---
    print("\n6. INTEGRITY CHECKS")

    # Orphaned relationships
    orphaned = conn.execute(
        """SELECT COUNT(*) FROM relationships r
           WHERE NOT EXISTS (SELECT 1 FROM canonical_entities WHERE canonical_id = r.source_entity_id)
              OR NOT EXISTS (SELECT 1 FROM canonical_entities WHERE canonical_id = r.target_entity_id)"""
    ).fetchone()[0]
    print(f"   Orphaned relationships (missing entity): {orphaned} {'✓' if orphaned == 0 else '✗ PROBLEM'}")

    # Duplicate canonical entries (check for exact name + type collisions)
    potential_dupes = conn.execute(
        """SELECT LOWER(canonical_name) as name_lower, entity_type, COUNT(*) as cnt,
                  GROUP_CONCAT(canonical_id, ', ') as ids
           FROM canonical_entities
           GROUP BY LOWER(canonical_name), entity_type
           HAVING cnt > 1
           LIMIT 20"""
    ).fetchall()
    print(f"   Exact-name duplicates: {len(potential_dupes)} {'✓' if len(potential_dupes) == 0 else '✗ NEEDS FIX'}")
    for row in potential_dupes:
        print(f"     \"{row[0]}\" ({row[1]}): {row[2]}x — IDs: {row[3]}")

    # Entities with no resolution log entry
    no_log = conn.execute(
        """SELECT COUNT(*) FROM canonical_entities ce
           WHERE NOT EXISTS (SELECT 1 FROM entity_resolution_log WHERE canonical_id = ce.canonical_id)"""
    ).fetchone()[0]
    print(f"   Entities with no resolution log: {no_log} {'✓' if no_log == 0 else '⚠ CHECK'}")

    # --- Top Connected Entities ---
    print("\n7. TOP CONNECTED ENTITIES")
    top = conn.execute(
        """WITH connection_counts AS (
             SELECT source_entity_id as cid, COUNT(*) as cnt FROM relationships GROUP BY source_entity_id
             UNION ALL
             SELECT target_entity_id as cid, COUNT(*) as cnt FROM relationships GROUP BY target_entity_id
           )
           SELECT ce.canonical_name, ce.entity_type, SUM(cc.cnt) as connections
           FROM connection_counts cc
           JOIN canonical_entities ce ON cc.cid = ce.canonical_id
           GROUP BY cc.cid
           ORDER BY connections DESC
           LIMIT 20"""
    ).fetchall()
    for row in top:
        print(f"   {row[0]} ({row[1]}): {row[2]} connections")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_validation(args.db_path)
