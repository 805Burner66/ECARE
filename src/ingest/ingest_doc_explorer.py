"""
ECARE: Ingest maxandrews/Epstein-doc-explorer

Ingests data from document_analysis.db (279MB SQLite via Git LFS):
    1. Entity aliases → resolve against canonical registry, merge aliases
    2. RDF triples → extract relationships (actor-action-target)

Requires: git lfs pull in data/raw/doc-explorer/ (279MB download)

Usage:
    python src/ingest/ingest_doc_explorer.py [--db-path data/output/ecare.db]
"""

import json
import os
import sys
import sqlite3
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
from src.utils.doc_ids import canonicalize_doc_ref


RAW_DIR = "data/raw/doc-explorer"
SOURCE_DB = "document_analysis.db"


def check_source_db(raw_dir: str) -> str:
    """Verify the source database exists and is not an LFS pointer."""
    db_path = os.path.join(raw_dir, SOURCE_DB)
    if not os.path.exists(db_path):
        print(f"ERROR: {db_path} not found.")
        print("Run: cd data/raw/doc-explorer && git lfs pull")
        sys.exit(1)

    size = os.path.getsize(db_path)
    if size < 1000:
        print(f"ERROR: {db_path} is only {size} bytes — likely a Git LFS pointer.")
        print("Run: cd data/raw/doc-explorer && git lfs pull")
        sys.exit(1)

    print(f"Source database: {db_path} ({size / 1024 / 1024:.1f} MB)")
    return db_path


def discover_schema(source_conn: sqlite3.Connection) -> dict:
    """Discover and print the schema of the source database."""
    tables = source_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()

    schema = {}
    print("\nSource database schema:")
    for (table_name,) in tables:
        cols = source_conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        count = source_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        col_names = [c[1] for c in cols]
        schema[table_name] = {"columns": col_names, "count": count}
        print(f"  {table_name}: {count} rows, columns: {col_names}")

    return schema


def ingest_entity_aliases(ecare_conn, source_conn, schema: dict) -> dict:
    """Resolve entity_aliases from doc-explorer against our canonical registry.

    Returns: dict mapping doc-explorer canonical_name -> our canonical_id
    """
    if "entity_aliases" not in schema:
        print("  WARNING: entity_aliases table not found, skipping.")
        return {}

    print(f"\n  Loading entity aliases ({schema['entity_aliases']['count']} rows)...")

    # Get unique canonical names with their variants and hop distances
    rows = source_conn.execute("""
        SELECT canonical_name, original_name, hop_distance_from_principal
        FROM entity_aliases
        WHERE canonical_name IS NOT NULL
        ORDER BY canonical_name
    """).fetchall()

    # Group by canonical name
    canonical_data = defaultdict(lambda: {"variants": set(), "hop_distance": None})
    for canonical_name, original_name, hop_dist in rows:
        canonical_data[canonical_name]["variants"].add(original_name)
        if hop_dist is not None:
            canonical_data[canonical_name]["hop_distance"] = hop_dist

    print(f"  Unique canonical names: {len(canonical_data)}")

    # Build resolver from current ECARE registry
    registry = load_canonical_registry(ecare_conn)
    # Match against persons (most doc-explorer entities are persons)
    resolver = EntityResolver(registry, fuzzy_threshold=90)

    de_name_to_cid = {}
    stats = Counter()

    for de_canonical, data in canonical_data.items():
        if is_redaction_marker(de_canonical):
            stats["skipped_redaction"] += 1
            continue
        if is_noise_entity_name(de_canonical):
            stats["skipped_noise"] += 1
            continue


        cid, method, confidence = resolver.resolve(de_canonical)

        if cid:
            de_name_to_cid[de_canonical] = cid
            stats[f"matched_{method}"] += 1

            # Merge variants as aliases
            new_aliases = [v for v in data["variants"]
                           if v != de_canonical and not is_redaction_marker(v)]
            if new_aliases:
                merged = resolver.merge_aliases(cid, new_aliases)
                ecare_conn.execute(
                    "UPDATE canonical_entities SET aliases = ?, last_updated = ? WHERE canonical_id = ?",
                    (json.dumps(merged), now_iso(), cid)
                )

            # Add hop distance to metadata if available
            if data["hop_distance"] is not None:
                row = ecare_conn.execute(
                    "SELECT metadata FROM canonical_entities WHERE canonical_id = ?", (cid,)
                ).fetchone()
                if row:
                    meta = json.loads(row[0]) if row[0] else {}
                    if "hop_distance_from_epstein" not in meta:
                        meta["hop_distance_from_epstein"] = data["hop_distance"]
                        ecare_conn.execute(
                            "UPDATE canonical_entities SET metadata = ? WHERE canonical_id = ?",
                            (json.dumps(meta), cid)
                        )

            insert_resolution_log(
                ecare_conn, "doc-explorer", f"alias:{de_canonical}", de_canonical,
                cid, method, confidence,
                match_details={"source": "entity_aliases",
                               "variant_count": len(data["variants"]),
                               "hop_distance": data["hop_distance"]}
            )
        else:
            # New entity
            new_cid = get_next_id(ecare_conn, "person")
            aliases = [v for v in data["variants"]
                       if v != de_canonical and not is_redaction_marker(v)]

            metadata = {"source_system": "doc-explorer"}
            if data["hop_distance"] is not None:
                metadata["hop_distance_from_epstein"] = data["hop_distance"]

            insert_canonical_entity(
                ecare_conn, new_cid, "person", de_canonical,
                aliases=aliases if aliases else None,
                metadata=metadata
            )
            insert_resolution_log(
                ecare_conn, "doc-explorer", f"alias:{de_canonical}", de_canonical,
                new_cid, "new_entity", 1.0,
                match_details={"source": "entity_aliases"}
            )
            resolver.add_to_registry(new_cid, de_canonical, aliases)
            de_name_to_cid[de_canonical] = new_cid
            stats["new_entity"] += 1

    ecare_conn.commit()
    for key, count in sorted(stats.items()):
        print(f"    {key}: {count}")

    return de_name_to_cid



def ingest_rdf_triples(ecare_conn, source_conn, schema: dict, name_to_cid: dict) -> int:
    """Extract relationships from RDF triples.

    Each triple has: actor, action, target, location, timestamp, doc_id.

    We create at most one relationship per (pair, mapped_relationship_type) and:
      - increment relationship.weight for repeat occurrences (capped)
      - aggregate doc evidence into a single relationship_sources row:
          document_count + sample doc keys + top action counts

    Returns: number of NEW relationships created.
    """
    if "rdf_triples" not in schema:
        print("  WARNING: rdf_triples table not found, skipping.")
        return 0

    print(f"\n  Processing RDF triples ({schema['rdf_triples']['count']} rows)...")

    cols = schema["rdf_triples"]["columns"]
    print(f"    Columns: {cols}")

    actor_col = "actor" if "actor" in cols else "subject"
    target_col = "target" if "target" in cols else "object"

    rows = source_conn.execute(f"""
        SELECT {actor_col}, action, {target_col}, doc_id
        FROM rdf_triples
        WHERE {actor_col} IS NOT NULL AND {target_col} IS NOT NULL
    """).fetchall()

    print(f"    Triples with both actor and target: {len(rows)}")

    # Load entity alias mapping
    alias_map = {}
    if "entity_aliases" in schema:
        alias_rows = source_conn.execute(
            "SELECT original_name, canonical_name FROM entity_aliases"
        ).fetchall()
        alias_map = {orig: canon for orig, canon in alias_rows}

    stats = Counter()
    pair_action_count = Counter()

    # Aggregate evidence per relationship
    pairkey_to_relid = {}
    rel_docs = defaultdict(set)      # rel_id -> set(doc_key)
    rel_actions = defaultdict(Counter)  # rel_id -> Counter(action)

    ACTION_MAP = {
        "traveled with": "traveled_with",
        "flew with": "traveled_with",
        "associated with": "associated_with",
        "met with": "associated_with",
        "employed": "employed_by",
        "hired": "employed_by",
        "paid": "financial",
        "funded": "financial",
        "donated to": "financial",
        "communicated with": "communicated_with",
        "called": "communicated_with",
        "emailed": "communicated_with",
        "represented": "represented_by",
    }

    for actor, action, target, doc_id in rows:
        if not actor or not target:
            continue

        actor = str(actor).strip()
        target = str(target).strip()
        action = str(action or "").strip()
        doc_id = str(doc_id or "").strip()

        if is_redaction_marker(actor) or is_noise_entity_name(actor):
            stats["skipped_actor_noise"] += 1
            continue
        if is_redaction_marker(target) or is_noise_entity_name(target):
            stats["skipped_target_noise"] += 1
            continue

        actor_canonical = alias_map.get(actor, actor)
        target_canonical = alias_map.get(target, target)

        if is_noise_entity_name(actor_canonical) or is_noise_entity_name(target_canonical):
            stats["skipped_alias_noise"] += 1
            continue

        actor_cid = name_to_cid.get(actor_canonical)
        target_cid = name_to_cid.get(target_canonical)

        if not actor_cid or not target_cid or actor_cid == target_cid:
            stats["skipped_unresolved"] += 1
            continue

        action_lower = action.lower()
        rel_type = "associated_with"
        for pattern, mapped_type in ACTION_MAP.items():
            if pattern in action_lower:
                rel_type = mapped_type
                break

        pair = tuple(sorted([actor_cid, target_cid]))
        pair_key = (pair, rel_type)
        pair_action_count[pair_key] += 1

        tok = canonicalize_doc_ref(
            ecare_conn,
            doc_id,
            source_system="doc-explorer",
            confidence=0.6,
            notes="rdf_triples"
        )
        doc_key = tok.doc_key

        rel_id = None

        if pair_action_count[pair_key] == 1:
            existing = find_existing_relationship(ecare_conn, pair[0], pair[1], rel_type)

            if existing:
                rel_id = int(existing)

                already = ecare_conn.execute(
                    "SELECT 1 FROM relationship_sources WHERE relationship_id = ? AND source_system = 'doc-explorer' LIMIT 1",
                    (rel_id,)
                ).fetchone()
                if not already:
                    insert_relationship_source(
                        ecare_conn, rel_id, "doc-explorer",
                        source_relationship_type=action,
                        source_evidence={
                            "type": "rdf_triple",
                            "document_count": 0,
                            "doc_key_sample": [],
                            "action_counts": {}
                        },
                        source_confidence=0.7,
                        evidence_class="rdf"
                    )
                stats["existing_corroborated"] += 1
            else:
                rel_id = insert_relationship(
                    ecare_conn, pair[0], pair[1], rel_type,
                    relationship_subtype=action,
                    weight=1.0, confidence_score=0.7,
                    source_documents=[doc_key] if doc_key else None,
                    notes=f"From RDF triple: {actor} → {action} → {target}"
                )
                insert_relationship_source(
                    ecare_conn, rel_id, "doc-explorer",
                    source_relationship_type=action,
                    source_evidence={
                        "type": "rdf_triple",
                        "document_count": 0,
                        "doc_key_sample": [],
                        "action_counts": {}
                    },
                    source_confidence=0.7,
                    evidence_class="rdf"
                )
                stats["new_relationships"] += 1

            pairkey_to_relid[pair_key] = rel_id

        else:
            rel_id = pairkey_to_relid.get(pair_key)
            # Increment weight for a few repeat triples (avoid huge inflation)
            if rel_id and pair_action_count[pair_key] <= 5:
                ecare_conn.execute(
                    "UPDATE relationships SET weight = COALESCE(weight, 0) + 1 WHERE relationship_id = ?",
                    (rel_id,)
                )
                stats["relationships_incremented"] += 1

        if rel_id:
            if doc_key:
                rel_docs[rel_id].add(doc_key)
                append_relationship_documents(ecare_conn, rel_id, [doc_key])
            if action_lower:
                rel_actions[rel_id][action_lower] += 1

        stats["triples_processed"] += 1

    ecare_conn.commit()

    # Update relationship_sources evidence with aggregated counts + samples
    updated = 0
    for rel_id, docs in rel_docs.items():
        docs = {d for d in docs if d}
        row = ecare_conn.execute(
            "SELECT source_evidence FROM relationship_sources WHERE relationship_id = ? AND source_system = 'doc-explorer' ORDER BY id DESC LIMIT 1",
            (rel_id,)
        ).fetchone()
        if not row:
            continue
        try:
            ev = json.loads(row[0]) if row[0] else {}
        except Exception:
            ev = {}

        ev["type"] = "rdf_triple"
        ev["document_count"] = len(docs)
        ev["doc_key_sample"] = sorted(docs)[:20]

        ac = rel_actions.get(rel_id)
        if ac:
            ev["action_counts"] = dict(ac.most_common(20))

        ecare_conn.execute(
            "UPDATE relationship_sources SET source_evidence = ? WHERE relationship_id = ? AND source_system = 'doc-explorer'",
            (json.dumps(ev), rel_id)
        )
        updated += 1

    ecare_conn.commit()

    print(f"  Results:")
    for key, count in sorted(stats.items()):
        print(f"    {key}: {count}")
    print(f"    relationship_sources_updated: {updated}")

    return stats.get("new_relationships", 0)


def main(db_path: str = DEFAULT_DB_PATH, raw_dir: str = RAW_DIR):
    started = now_iso()

    # Verify source database
    source_db_path = check_source_db(raw_dir)
    source_conn = sqlite3.connect(source_db_path)
    source_conn.row_factory = sqlite3.Row

    ecare_conn = get_db_connection(db_path)

    print("=" * 60)
    print("ECARE: Ingesting maxandrews/Epstein-doc-explorer")
    print("=" * 60)

    # Discover schema
    schema = discover_schema(source_conn)

    # Step 1: Resolve entity aliases
    print("\nPhase 1: Entity resolution from entity_aliases")
    name_to_cid = ingest_entity_aliases(ecare_conn, source_conn, schema)

    # Step 2: Extract relationships from RDF triples
    print("\nPhase 2: Relationship extraction from RDF triples")
    new_rels = ingest_rdf_triples(ecare_conn, source_conn, schema, name_to_cid)

    # Summary
    entity_count = ecare_conn.execute("SELECT COUNT(*) FROM canonical_entities").fetchone()[0]
    by_type = ecare_conn.execute(
        "SELECT entity_type, COUNT(*) FROM canonical_entities GROUP BY entity_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    rel_count = ecare_conn.execute("SELECT COUNT(*) FROM relationships").fetchone()[0]

    print(f"\n{'=' * 60}")
    print(f"SUMMARY (cumulative after doc-explorer ingestion)")
    print(f"  Canonical entities: {entity_count}")
    for row in by_type:
        print(f"    {row[0]}: {row[1]}")
    print(f"  Relationships: {rel_count}")
    print(f"  Resolution log: {ecare_conn.execute('SELECT COUNT(*) FROM entity_resolution_log').fetchone()[0]}")
    print(f"{'=' * 60}")

    log_pipeline_run(ecare_conn, "ingest_doc_explorer", "completed",
                     records_processed=len(name_to_cid),
                     notes=f"Resolved {len(name_to_cid)} entities. {new_rels} new relationships.",
                     started_at=started)

    source_conn.close()
    ecare_conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--raw-dir", default=RAW_DIR)
    args = parser.parse_args()
    main(args.db_path, args.raw_dir)
