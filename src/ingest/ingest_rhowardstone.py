"""
ECARE: Ingest rhowardstone/Epstein-research-data

This is the first ingestion step and establishes the canonical base.

What it does:
    1. Loads persons_registry.json (1,614 persons) → canonical_entities
    2. Loads knowledge_graph_entities.json (606 entities) → merges metadata into
       existing persons, creates new entries for orgs/locations/etc.
    3. Loads knowledge_graph_relationships.json (2,302 relationships) → relationships table

Usage:
    python src/ingest/ingest_rhowardstone.py [--db-path data/output/ecare.db]
"""

import json
import os
import sys
import argparse
from collections import Counter

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.utils.common import (
    get_db_connection, now_iso, log_pipeline_run,
    insert_canonical_entity, insert_resolution_log,
    insert_relationship, insert_relationship_source,
    append_relationship_documents,
    get_next_id, DEFAULT_DB_PATH
)
from src.resolve.resolve_persons import EntityResolver, is_redaction_marker
from src.utils.doc_ids import canonicalize_doc_ref


RAW_DIR = "data/raw/rhowardstone"


def ingest_persons_registry(conn, raw_dir: str) -> dict:
    """Load persons_registry.json as the canonical base.

    Returns: dict mapping slug -> canonical_id (for cross-referencing with KG)
    """
    path = os.path.join(raw_dir, "persons_registry.json")
    with open(path, encoding='utf-8') as f:
        persons = json.load(f)

    print(f"Loading {len(persons)} persons from registry...")

    slug_to_cid = {}
    stats = Counter()
    per_counter = 0

    for person in persons:
        name = person.get("name", "").strip()
        slug = person.get("slug", "")
        aliases = person.get("aliases", [])
        category = person.get("category", "other")
        sources = person.get("sources", [])
        search_terms = person.get("search_terms", [])

        # Skip redaction markers
        if is_redaction_marker(name):
            stats["skipped_redaction"] += 1
            continue

        per_counter += 1
        canonical_id = f"PER-{per_counter:05d}"

        # Build metadata
        metadata = {
            "category": category,
            "registry_sources": sources,
        }
        if search_terms and search_terms != [name]:
            metadata["search_terms"] = search_terms

        # Dedupe aliases (remove duplicates of canonical name)
        clean_aliases = []
        name_lower = name.lower()
        for a in aliases:
            if a and a.strip() and a.strip().lower() != name_lower:
                clean_aliases.append(a.strip())

        insert_canonical_entity(
            conn, canonical_id, "person", name,
            aliases=clean_aliases if clean_aliases else None,
            metadata=metadata
        )

        # Log this as a base registry entry (confidence = 1.0, it IS the source)
        insert_resolution_log(
            conn, "rhowardstone", f"registry:{slug}", name,
            canonical_id, "base_registry", 1.0,
            match_details={"source": "persons_registry.json", "category": category}
        )

        slug_to_cid[slug] = canonical_id
        stats["loaded"] += 1

    conn.commit()
    print(f"  Loaded: {stats['loaded']}")
    print(f"  Skipped (redaction markers): {stats['skipped_redaction']}")
    return slug_to_cid


def ingest_knowledge_graph_entities(conn, raw_dir: str, slug_to_cid: dict) -> dict:
    """Load knowledge_graph_entities.json and merge with existing canonical entries.

    For persons: match by name against existing registry, merge metadata.
    For non-persons: create new canonical entries.

    Returns: dict mapping KG entity id (int) -> canonical_id
    """
    path = os.path.join(raw_dir, "knowledge_graph_entities.json")
    with open(path, encoding='utf-8') as f:
        entities = json.load(f)

    print(f"\nLoading {len(entities)} knowledge graph entities...")

    # Build resolver from current canonical registry
    registry = {}
    rows = conn.execute(
        "SELECT canonical_id, canonical_name, aliases, metadata FROM canonical_entities"
    ).fetchall()
    for row in rows:
        registry[row[0]] = {
            "canonical_name": row[1],
            "aliases": json.loads(row[2]) if row[2] else [],
            "entity_type": "person",
            "metadata": json.loads(row[3]) if row[3] else {},
        }
    resolver = EntityResolver(registry, fuzzy_threshold=90)

    kg_id_to_cid = {}
    stats = Counter()

    for entity in entities:
        kg_id = entity["id"]
        name = entity["name"].strip()
        entity_type = entity.get("entity_type", "person")
        kg_metadata = json.loads(entity["metadata"]) if entity.get("metadata") else {}
        kg_aliases = json.loads(entity["aliases"]) if entity.get("aliases") else []

        if is_redaction_marker(name):
            stats["skipped_redaction"] += 1
            continue

        if entity_type == "person":
            # Try to match against existing registry
            cid, method, confidence = resolver.resolve(name)

            if cid:
                # Match found — merge KG metadata into existing entry
                kg_id_to_cid[kg_id] = cid
                stats[f"matched_{method}"] += 1

                # Update metadata with KG-specific fields
                existing_meta = registry[cid]["metadata"]
                for key in ("occupation", "legal_status", "person_type",
                            "ds10_mention_count", "public_figure"):
                    if key in kg_metadata and key not in existing_meta:
                        existing_meta[key] = kg_metadata[key]

                conn.execute(
                    "UPDATE canonical_entities SET metadata = ?, last_updated = ? WHERE canonical_id = ?",
                    (json.dumps(existing_meta), now_iso(), cid)
                )

                # Merge aliases
                if kg_aliases:
                    merged = resolver.merge_aliases(cid, kg_aliases)
                    conn.execute(
                        "UPDATE canonical_entities SET aliases = ? WHERE canonical_id = ?",
                        (json.dumps(merged), cid)
                    )

                insert_resolution_log(
                    conn, "rhowardstone", f"kg:{kg_id}", name,
                    cid, method, confidence,
                    match_details={"source": "knowledge_graph_entities.json",
                                   "kg_metadata": kg_metadata}
                )
            else:
                # No match — create new person entity
                new_cid = get_next_id(conn, "person")
                metadata = {"category": kg_metadata.get("person_type", "other")}
                for key in ("occupation", "legal_status", "person_type",
                            "ds10_mention_count", "public_figure"):
                    if key in kg_metadata:
                        metadata[key] = kg_metadata[key]

                insert_canonical_entity(
                    conn, new_cid, "person", name,
                    aliases=kg_aliases if kg_aliases else None,
                    metadata=metadata
                )
                insert_resolution_log(
                    conn, "rhowardstone", f"kg:{kg_id}", name,
                    new_cid, "new_entity", 1.0,
                    match_details={"source": "knowledge_graph_entities.json",
                                   "reason": "no match in persons_registry"}
                )

                # Add to resolver for subsequent matches
                resolver.add_to_registry(new_cid, name, kg_aliases)
                kg_id_to_cid[kg_id] = new_cid
                stats["new_person"] += 1

        else:
            # Non-person entity (organization, shell_company, property, aircraft, location)
            new_cid = get_next_id(conn, entity_type)
            metadata = {}
            for key in kg_metadata:
                metadata[key] = kg_metadata[key]

            insert_canonical_entity(
                conn, new_cid, entity_type, name,
                aliases=kg_aliases if kg_aliases else None,
                metadata=metadata
            )
            insert_resolution_log(
                conn, "rhowardstone", f"kg:{kg_id}", name,
                new_cid, "new_entity", 1.0,
                match_details={"source": "knowledge_graph_entities.json",
                               "entity_type": entity_type}
            )
            kg_id_to_cid[kg_id] = new_cid
            stats[f"new_{entity_type}"] += 1

    conn.commit()
    print("  Results:")
    for key, count in sorted(stats.items()):
        print(f"    {key}: {count}")
    return kg_id_to_cid


def ingest_knowledge_graph_relationships(conn, raw_dir: str, kg_id_to_cid: dict) -> int:
    """Load knowledge_graph_relationships.json into the relationships table.

    Returns: number of relationships loaded
    """
    path = os.path.join(raw_dir, "knowledge_graph_relationships.json")
    with open(path, encoding='utf-8') as f:
        rels = json.load(f)

    print(f"\nLoading {len(rels)} knowledge graph relationships...")

    stats = Counter()

    for rel in rels:
        source_kg_id = rel["source_entity_id"]
        target_kg_id = rel["target_entity_id"]
        rel_type = rel["relationship_type"]
        weight = rel.get("weight")
        date_first = rel.get("date_first")
        date_last = rel.get("date_last")
        metadata = json.loads(rel["metadata"]) if rel.get("metadata") else {}

        # Map KG entity IDs to canonical IDs
        source_cid = kg_id_to_cid.get(source_kg_id)
        target_cid = kg_id_to_cid.get(target_kg_id)

        if not source_cid or not target_cid:
            stats["skipped_unmapped"] += 1
            continue

        # Extract / canonicalize document references (prefer EFTA doc_keys)
        doc_keys = []
        if metadata.get("efta"):
            tok = canonicalize_doc_ref(
                conn, str(metadata.get("efta")),
                source_system="rhowardstone",
                confidence=0.9,
                notes="knowledge_graph_relationships.json"
            )
            if tok.doc_key:
                doc_keys.append(tok.doc_key)

        # Extract subtype from metadata
        subtype = metadata.get("original_type")

        # Insert relationship
        rel_id = insert_relationship(
            conn, source_cid, target_cid, rel_type,
            relationship_subtype=subtype,
            date_start=date_first, date_end=date_last,
            weight=weight, confidence_score=1.0,
            source_documents=doc_keys if doc_keys else None,
            notes=metadata.get("notes")
        )

        # Insert provenance
        insert_relationship_source(
            conn, rel_id, "rhowardstone",
            source_relationship_id=str(rel["id"]),
            source_relationship_type=rel_type,
            source_evidence={
                "source": "knowledge_graph_relationships.json",
                "metadata": metadata
            },
            source_confidence=1.0,
            evidence_class="curated"
        )

        stats[f"loaded_{rel_type}"] += 1

    conn.commit()
    total_loaded = sum(v for k, v in stats.items() if k.startswith("loaded_"))
    print(f"  Loaded: {total_loaded}")
    print(f"  Skipped (unmapped entities): {stats['skipped_unmapped']}")
    print("  By type:")
    for key, count in sorted(stats.items()):
        if key.startswith("loaded_"):
            print(f"    {key.replace('loaded_', '')}: {count}")
    return total_loaded


def main(db_path: str = DEFAULT_DB_PATH, raw_dir: str = RAW_DIR):
    started = now_iso()
    conn = get_db_connection(db_path)

    print("=" * 60)
    print("ECARE: Ingesting rhowardstone/Epstein-research-data")
    print("=" * 60)

    # Step 1: Persons registry → canonical base
    slug_to_cid = ingest_persons_registry(conn, raw_dir)

    # Step 2: Knowledge graph entities → merge/extend canonical
    kg_id_to_cid = ingest_knowledge_graph_entities(conn, raw_dir, slug_to_cid)

    # Step 3: Knowledge graph relationships
    rel_count = ingest_knowledge_graph_relationships(conn, raw_dir, kg_id_to_cid)

    # Summary
    entity_count = conn.execute("SELECT COUNT(*) FROM canonical_entities").fetchone()[0]
    person_count = conn.execute("SELECT COUNT(*) FROM canonical_entities WHERE entity_type='person'").fetchone()[0]
    rel_count_total = conn.execute("SELECT COUNT(*) FROM relationships").fetchone()[0]

    print(f"\n{'=' * 60}")
    print(f"SUMMARY")
    print(f"  Canonical entities: {entity_count} ({person_count} persons)")
    print(f"  Relationships: {rel_count_total}")
    print(f"  Resolution log entries: {conn.execute('SELECT COUNT(*) FROM entity_resolution_log').fetchone()[0]}")
    print(f"{'=' * 60}")

    log_pipeline_run(conn, "ingest_rhowardstone", "completed",
                     records_processed=entity_count,
                     notes=f"{entity_count} entities, {rel_count_total} relationships",
                     started_at=started)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--raw-dir", default=RAW_DIR)
    args = parser.parse_args()
    main(args.db_path, args.raw_dir)
