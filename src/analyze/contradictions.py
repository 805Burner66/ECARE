"""
ECARE Analysis: Cross-Source Contradiction Detection

Finds where different sources disagree:
    1. Entity category/role mismatches across sources
    2. Relationship type mismatches for the same entity pair
    3. Entities resolved to the same canonical entry with conflicting metadata

Outputs:
    data/output/cross_source_contradictions.csv

Usage:
    python src/analyze/contradictions.py
"""

import csv
import json
import os
import sys
import argparse
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.utils.common import get_db_connection, now_iso, log_pipeline_run, DEFAULT_DB_PATH

OUTPUT_DIR = "data/output"


def detect_relationship_type_conflicts(conn):
    """Find entity pairs where different sources assign different relationship types."""
    print("\n  Checking for relationship type conflicts...")

    # Get relationships with multiple source entries
    rows = conn.execute("""
        SELECT r.relationship_id,
               ce1.canonical_name as source_name,
               ce2.canonical_name as target_name,
               r.relationship_type as canonical_type,
               rs.source_system,
               rs.source_relationship_type
        FROM relationships r
        JOIN canonical_entities ce1 ON r.source_entity_id = ce1.canonical_id
        JOIN canonical_entities ce2 ON r.target_entity_id = ce2.canonical_id
        JOIN relationship_sources rs ON r.relationship_id = rs.relationship_id
        ORDER BY r.relationship_id
    """).fetchall()

    # Group by relationship_id
    by_rel = defaultdict(list)
    for row in rows:
        by_rel[row[0]].append({
            "source_name": row[1],
            "target_name": row[2],
            "canonical_type": row[3],
            "source_system": row[4],
            "source_rel_type": row[5],
        })

    conflicts = []
    for rel_id, sources in by_rel.items():
        if len(sources) < 2:
            continue

        # Check if source systems assigned different types
        type_by_source = {}
        for s in sources:
            sys_name = s["source_system"]
            src_type = s["source_rel_type"] or s["canonical_type"]
            type_by_source[sys_name] = src_type

        unique_types = set(type_by_source.values())
        if len(unique_types) > 1:
            systems = sorted(type_by_source.keys())
            conflicts.append({
                "record_type": "relationship",
                "record_id": str(rel_id),
                "entity_a": sources[0]["source_name"],
                "entity_b": sources[0]["target_name"],
                "source_a": systems[0],
                "source_b": systems[1] if len(systems) > 1 else "",
                "field": "relationship_type",
                "value_a": type_by_source[systems[0]],
                "value_b": type_by_source[systems[1]] if len(systems) > 1 else "",
                "severity": "low",  # type differences are common and usually just granularity
            })

    print(f"    Relationship type conflicts: {len(conflicts)}")
    return conflicts


def detect_entity_category_conflicts(conn):
    """Find entities where different sources assigned different categories/roles."""
    print("\n  Checking for entity category conflicts...")

    # Get entities that appear in multiple sources
    rows = conn.execute("""
        SELECT ce.canonical_id, ce.canonical_name, ce.metadata,
               erl.source_system, erl.match_details
        FROM canonical_entities ce
        JOIN entity_resolution_log erl ON ce.canonical_id = erl.canonical_id
        WHERE ce.entity_type = 'person'
        ORDER BY ce.canonical_id
    """).fetchall()

    # Group by canonical_id
    by_entity = defaultdict(list)
    for row in rows:
        by_entity[(row[0], row[1])].append({
            "source_system": row[3],
            "match_details": row[4],
            "metadata": row[2],
        })

    conflicts = []
    for (cid, cname), sources in by_entity.items():
        if len(sources) < 2:
            continue

        # Extract categories from different sources
        categories = {}
        for s in sources:
            sys_name = s["source_system"]
            details = json.loads(s["match_details"]) if s["match_details"] else {}
            cat = details.get("category") or details.get("entity_type_key")
            if cat:
                categories[sys_name] = cat

        # Check for roles from epstein-docs
        meta = json.loads(sources[0]["metadata"]) if sources[0]["metadata"] else {}
        observed_roles = meta.get("observed_roles", {})
        if observed_roles:
            top_role = max(observed_roles, key=observed_roles.get)
            categories["epstein-docs_role"] = top_role

        # See if there are genuine conflicts
        unique_cats = set(categories.values())
        if len(unique_cats) > 1:
            # Filter out trivial conflicts (e.g., "associate" vs "business" — both are vague)
            trivial = {"associate", "other"}
            non_trivial = unique_cats - trivial
            if len(non_trivial) > 1 or (non_trivial and unique_cats - non_trivial):
                systems = sorted(categories.keys())
                conflicts.append({
                    "record_type": "entity",
                    "record_id": cid,
                    "entity_a": cname,
                    "entity_b": "",
                    "source_a": systems[0],
                    "source_b": systems[1] if len(systems) > 1 else "",
                    "field": "category/role",
                    "value_a": categories[systems[0]],
                    "value_b": categories[systems[1]] if len(systems) > 1 else "",
                    "severity": "medium" if "victim" in unique_cats or "perpetrator" in unique_cats else "low",
                })

    print(f"    Entity category conflicts: {len(conflicts)}")
    return conflicts


def run_contradiction_detection(db_path: str = DEFAULT_DB_PATH):
    started = now_iso()
    conn = get_db_connection(db_path)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Running cross-source contradiction detection...")

    rel_conflicts = detect_relationship_type_conflicts(conn)
    entity_conflicts = detect_entity_category_conflicts(conn)

    all_conflicts = rel_conflicts + entity_conflicts
    all_conflicts.sort(key=lambda x: {"high": 0, "medium": 1, "low": 2}.get(x["severity"], 3))

    output_path = os.path.join(OUTPUT_DIR, "cross_source_contradictions.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "record_type", "record_id", "entity_a", "entity_b",
            "source_a", "source_b", "field", "value_a", "value_b", "severity"
        ])
        writer.writeheader()
        writer.writerows(all_conflicts)

    # Also write to conflicts table in database
    for c in all_conflicts:
        conn.execute("""
            INSERT INTO conflicts (entity_or_relationship, record_id, source_a, source_b,
                                   field_in_conflict, value_a, value_b, nature_of_conflict,
                                   resolution_status, flagged_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'unresolved', ?)
        """, (c["record_type"], c["record_id"], c["source_a"], c["source_b"],
              c["field"], c["value_a"], c["value_b"],
              f"severity: {c['severity']}", now_iso()))
    conn.commit()

    print(f"\n  Total contradictions: {len(all_conflicts)}")
    from collections import Counter
    sev = Counter(c["severity"] for c in all_conflicts)
    for s, count in sev.most_common():
        print(f"    {s}: {count}")
    print(f"\n  Output: {output_path}")

    log_pipeline_run(conn, "contradiction_detection", "completed",
                     records_processed=len(all_conflicts),
                     notes=f"{len(all_conflicts)} contradictions detected",
                     started_at=started)
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_contradiction_detection(args.db_path)
