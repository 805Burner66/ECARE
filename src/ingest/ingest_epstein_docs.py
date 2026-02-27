"""
ECARE: Ingest epstein-docs/epstein-docs.github.io

Ingests two types of data:
    1. Entity deduplication mapping (dedupe.json) — 8,953 canonical persons,
       4,313 orgs, 2,227 locations with their variant names
    2. Document analyses (analyses.json) — per-document key_people with roles,
       which gives us co-occurrence relationships

Usage:
    python src/ingest/ingest_epstein_docs.py [--db-path data/output/ecare.db]
"""

import json
import os
import sys
import argparse
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.utils.common import (
    get_db_connection, now_iso, log_pipeline_run,
    insert_canonical_entity, insert_resolution_log,
    insert_relationship, insert_relationship_source,
    append_relationship_documents,
    find_existing_relationship, get_next_id, load_canonical_registry,
    DEFAULT_DB_PATH
)
from src.resolve.resolve_persons import EntityResolver, is_redaction_marker, is_noise_entity_name
from src.utils.doc_ids import canonicalize_doc_fields


RAW_DIR = "data/raw/epstein-docs"


def ingest_dedupe_entities(conn, raw_dir: str, entity_type_key: str,
                           canonical_entity_type: str) -> dict:
    """Resolve entities from dedupe.json against the canonical registry.

    Args:
        entity_type_key: key in dedupe.json ('people', 'organizations', 'locations')
        canonical_entity_type: entity_type for canonical_entities table

    Returns:
        dict mapping epstein-docs canonical name -> our canonical_id
    """
    path = os.path.join(raw_dir, "dedupe.json")
    with open(path, encoding='utf-8') as f:
        dedupe = json.load(f)

    variant_map = dedupe.get(entity_type_key, {})
    # Get unique canonical names from the dedupe mapping
    canonical_names = set(variant_map.values())

    print(f"\n  Resolving {len(canonical_names)} {entity_type_key} "
          f"({len(variant_map)} variants)...")

    # Load current registry and build resolver
    registry = load_canonical_registry(conn)
    # Filter to relevant entity type for matching
    type_registry = {k: v for k, v in registry.items()
                     if v["entity_type"] == canonical_entity_type}
    resolver = EntityResolver(type_registry, fuzzy_threshold=90)

    # Also build a mapping of variant -> canonical for alias enrichment
    name_to_variants = defaultdict(set)
    for variant, canonical in variant_map.items():
        if variant != canonical:
            name_to_variants[canonical].add(variant)

    ed_name_to_cid = {}
    stats = Counter()

    for ed_canonical in sorted(canonical_names):
        if is_redaction_marker(ed_canonical):
            stats["skipped_redaction"] += 1
            continue
        if is_noise_entity_name(ed_canonical):
            stats["skipped_noise"] += 1
            continue


        # Try to resolve against our canonical registry
        cid, method, confidence = resolver.resolve(ed_canonical)

        if cid:
            # Matched an existing entity
            ed_name_to_cid[ed_canonical] = cid
            stats[f"matched_{method}"] += 1

            # Merge variants as aliases
            variants = name_to_variants.get(ed_canonical, set())
            if variants:
                new_aliases = [v for v in variants if not is_redaction_marker(v)]
                if new_aliases:
                    merged = resolver.merge_aliases(cid, new_aliases)
                    conn.execute(
                        "UPDATE canonical_entities SET aliases = ?, last_updated = ? WHERE canonical_id = ?",
                        (json.dumps(merged), now_iso(), cid)
                    )

            insert_resolution_log(
                conn, "epstein-docs", f"dedupe:{ed_canonical}", ed_canonical,
                cid, method, confidence,
                match_details={"source": "dedupe.json", "entity_type_key": entity_type_key,
                               "variant_count": len(name_to_variants.get(ed_canonical, set()))}
            )
        else:
            # New entity — create it
            new_cid = get_next_id(conn, canonical_entity_type)
            variants = name_to_variants.get(ed_canonical, set())
            aliases = [v for v in variants if not is_redaction_marker(v) and v != ed_canonical]

            insert_canonical_entity(
                conn, new_cid, canonical_entity_type, ed_canonical,
                aliases=aliases if aliases else None,
                metadata={"source_system": "epstein-docs"}
            )
            insert_resolution_log(
                conn, "epstein-docs", f"dedupe:{ed_canonical}", ed_canonical,
                new_cid, "new_entity", 1.0,
                match_details={"source": "dedupe.json", "entity_type_key": entity_type_key}
            )

            resolver.add_to_registry(new_cid, ed_canonical, aliases, canonical_entity_type)
            ed_name_to_cid[ed_canonical] = new_cid
            stats["new_entity"] += 1

    conn.commit()

    for key, count in sorted(stats.items()):
        print(f"    {key}: {count}")

    return ed_name_to_cid



def ingest_document_analyses(conn, raw_dir: str, person_name_to_cid: dict) -> int:
    """Extract co-occurrence relationships from analyses.json.

    Two people appearing in the same document analysis are connected with a
    'co_documented' relationship (weak evidence). If a relationship already
    exists from a stronger source, we attach epstein-docs provenance to it.

    We *do not* create a provenance row per document (that explodes). Instead we:
      - increment relationship.weight for co_documented edges
      - maintain a per-relationship doc set and write document_count + sample keys

    Returns: number of new relationships created.
    """
    analyses_path = os.path.join(raw_dir, "analyses.json")
    with open(analyses_path, encoding='utf-8') as f:
        raw_data = json.load(f)

    # analyses.json can be a list of objects OR a dict keyed by document_id
    if isinstance(raw_data, list):
        analyses = raw_data
    elif isinstance(raw_data, dict):
        analyses = []
        for doc_id, value in raw_data.items():
            if isinstance(value, dict):
                value.setdefault("document_id", doc_id)
                analyses.append(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        item.setdefault("document_id", doc_id)
                        analyses.append(item)
    else:
        analyses = []

    # Load dedupe mapping so variant person names in key_people resolve consistently
    dedupe_path = os.path.join(raw_dir, "dedupe.json")
    with open(dedupe_path, encoding='utf-8') as f:
        dedupe = json.load(f)
    people_dedupe = dedupe.get("people", {})

    print(f"\n  Processing {len(analyses)} document analyses for co-occurrences...")

    stats = Counter()
    # pair -> (relationship_id, relationship_type) for whatever edge we attached to
    pair_to_rel = {}
    # relationship_id -> set(doc_keys) for epstein-docs
    rel_docs = defaultdict(set)

    # Collect role metadata per person
    person_roles = defaultdict(Counter)

    for analysis in analyses:
        doc_id_raw = str(analysis.get("document_id", "") or "")
        doc_num_raw = str(analysis.get("document_number", "") or "")

        doc_tokens = canonicalize_doc_fields(
            conn,
            raw_fields=(doc_id_raw, doc_num_raw),
            source_system="epstein-docs",
            confidence=0.5,
            notes="analyses.json"
        )
        doc_key = doc_tokens.doc_key

        people = analysis.get("analysis", {}).get("key_people", [])

        if not people or len(people) < 2:
            stats["docs_skipped_insufficient_people"] += 1
            continue

        # Resolve each person to canonical ID
        resolved_people = []
        for person in people:
            raw_name = (person.get("name", "") or "").strip()
            role = (person.get("role", "") or "").strip()

            if is_redaction_marker(raw_name) or is_noise_entity_name(raw_name):
                continue

            # First try epstein-docs dedupe canonical name
            canonical_name = people_dedupe.get(raw_name, raw_name)

            if is_redaction_marker(canonical_name) or is_noise_entity_name(canonical_name):
                continue

            cid = person_name_to_cid.get(canonical_name)

            if cid:
                resolved_people.append((cid, raw_name, role))
                if role:
                    person_roles[cid][role] += 1

        if len(resolved_people) < 2:
            stats["docs_skipped_unresolved_people"] += 1
            continue

        # Create / update co-occurrence edges for all pairs
        for i in range(len(resolved_people)):
            for j in range(i + 1, len(resolved_people)):
                cid_a = resolved_people[i][0]
                cid_b = resolved_people[j][0]
                if cid_a == cid_b:
                    continue

                pair = tuple(sorted([cid_a, cid_b]))

                if pair in pair_to_rel:
                    rel_id, rel_type = pair_to_rel[pair]
                    if rel_type == "co_documented":
                        conn.execute(
                            "UPDATE relationships SET weight = COALESCE(weight, 0) + 1 WHERE relationship_id = ?",
                            (rel_id,)
                        )
                        stats["relationships_incremented"] += 1
                else:
                    existing = conn.execute(
                        """SELECT relationship_id, relationship_type FROM relationships
                           WHERE ((source_entity_id = ? AND target_entity_id = ?)
                                  OR (source_entity_id = ? AND target_entity_id = ?))""",
                        (pair[0], pair[1], pair[1], pair[0])
                    ).fetchone()

                    if existing:
                        rel_id = int(existing[0])
                        rel_type = existing[1]
                        pair_to_rel[pair] = (rel_id, rel_type)

                        # Insert epstein-docs provenance if missing
                        already = conn.execute(
                            "SELECT 1 FROM relationship_sources WHERE relationship_id = ? AND source_system = 'epstein-docs' LIMIT 1",
                            (rel_id,)
                        ).fetchone()
                        if not already:
                            insert_relationship_source(
                                conn, rel_id, "epstein-docs",
                                source_relationship_type="co_documented",
                                source_evidence={
                                    "type": "co_occurrence",
                                    "document_count": 0,
                                    "doc_key_sample": [],
                                    "raw_document_id": doc_id_raw or None,
                                    "raw_document_number": doc_num_raw or None,
                                },
                                source_confidence=0.5,
                                evidence_class="cooccurrence"
                            )
                        stats["existing_relationship_corroborated"] += 1
                    else:
                        rel_id = insert_relationship(
                            conn, pair[0], pair[1], "co_documented",
                            weight=1.0, confidence_score=0.5,
                            source_documents=[doc_key] if doc_key else None,
                            notes="Derived from co-occurrence in document analysis"
                        )
                        insert_relationship_source(
                            conn, rel_id, "epstein-docs",
                            source_relationship_type="co_documented",
                            source_evidence={
                                "type": "co_occurrence",
                                "document_count": 0,
                                "doc_key_sample": [],
                                "raw_document_id": doc_id_raw or None,
                                "raw_document_number": doc_num_raw or None,
                            },
                            source_confidence=0.5,
                            evidence_class="cooccurrence"
                        )
                        pair_to_rel[pair] = (rel_id, "co_documented")
                        stats["new_relationships"] += 1

                # Track doc evidence
                rel_docs[rel_id].add(doc_key)
                append_relationship_documents(conn, rel_id, [doc_key])

        stats["docs_processed"] += 1

    conn.commit()

    # Update relationship_sources evidence for epstein-docs with doc counts + samples
    updated = 0
    for rel_id, docs in rel_docs.items():
        docs = {d for d in docs if d}
        if not docs:
            continue

        row = conn.execute(
            "SELECT source_evidence FROM relationship_sources WHERE relationship_id = ? AND source_system = 'epstein-docs' ORDER BY id DESC LIMIT 1",
            (rel_id,)
        ).fetchone()
        if not row:
            continue

        try:
            ev = json.loads(row[0]) if row[0] else {}
        except Exception:
            ev = {}

        ev["type"] = ev.get("type") or "co_occurrence"
        ev["document_count"] = len(docs)
        ev["doc_key_sample"] = sorted(docs)[:20]

        conn.execute(
            "UPDATE relationship_sources SET source_evidence = ? WHERE relationship_id = ? AND source_system = 'epstein-docs'",
            (json.dumps(ev), rel_id)
        )
        updated += 1

    conn.commit()

    # Update person metadata with observed roles (merge with existing)
    role_updates = 0
    for cid, roles in person_roles.items():
        if not roles:
            continue
        row = conn.execute(
            "SELECT metadata FROM canonical_entities WHERE canonical_id = ?", (cid,)
        ).fetchone()
        if not row:
            continue

        meta = json.loads(row[0]) if row[0] else {}
        existing = meta.get("observed_roles")
        if isinstance(existing, dict):
            # merge counts
            for k, v in roles.items():
                existing[k] = int(existing.get(k, 0)) + int(v)
            meta["observed_roles"] = existing
        else:
            meta["observed_roles"] = dict(roles)

        conn.execute(
            "UPDATE canonical_entities SET metadata = ? WHERE canonical_id = ?",
            (json.dumps(meta), cid)
        )
        role_updates += 1

    conn.commit()

    print(f"  Results:")
    for key, count in sorted(stats.items()):
        print(f"    {key}: {count}")
    print(f"    relationship_sources_updated: {updated}")
    print(f"    person_role_updates: {role_updates}")

    return stats.get("new_relationships", 0)


def main(db_path: str = DEFAULT_DB_PATH, raw_dir: str = RAW_DIR):
    started = now_iso()
    conn = get_db_connection(db_path)

    print("=" * 60)
    print("ECARE: Ingesting epstein-docs/epstein-docs.github.io")
    print("=" * 60)

    # Step 1: Resolve person entities from dedupe.json
    print("\nPhase 1: Entity resolution")
    person_map = ingest_dedupe_entities(conn, raw_dir, "people", "person")

    # Step 2: Resolve organizations
    org_map = ingest_dedupe_entities(conn, raw_dir, "organizations", "organization")

    # Step 3: Resolve locations
    loc_map = ingest_dedupe_entities(conn, raw_dir, "locations", "location")

    # Step 4: Extract co-occurrence relationships from document analyses
    print("\nPhase 2: Co-occurrence relationships")
    new_rels = ingest_document_analyses(conn, raw_dir, person_map)

    # Summary
    entity_count = conn.execute("SELECT COUNT(*) FROM canonical_entities").fetchone()[0]
    by_type = conn.execute(
        "SELECT entity_type, COUNT(*) FROM canonical_entities GROUP BY entity_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    rel_count = conn.execute("SELECT COUNT(*) FROM relationships").fetchone()[0]

    print(f"\n{'=' * 60}")
    print(f"SUMMARY (cumulative after epstein-docs ingestion)")
    print(f"  Canonical entities: {entity_count}")
    for row in by_type:
        print(f"    {row[0]}: {row[1]}")
    print(f"  Relationships: {rel_count}")
    print(f"  Resolution log: {conn.execute('SELECT COUNT(*) FROM entity_resolution_log').fetchone()[0]}")
    print(f"{'=' * 60}")

    log_pipeline_run(conn, "ingest_epstein_docs", "completed",
                     records_processed=len(person_map) + len(org_map) + len(loc_map),
                     notes=f"Resolved {len(person_map)} persons, {len(org_map)} orgs, "
                           f"{len(loc_map)} locations. {new_rels} new co-occurrence relationships.",
                     started_at=started)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--raw-dir", default=RAW_DIR)
    args = parser.parse_args()
    main(args.db_path, args.raw_dir)