"""
ECARE: Full-Text Corpus Integration

Uses rhowardstone's full_text_corpus.db (6GB, 1.39M docs, 2.77M pages, FTS5)
to enrich the ECARE database with document-level evidence:

    1. Entity mention counts — how many documents mention each person
    2. Co-occurrence corroboration — for existing relationships, checks whether
       both entities appear in the same documents (independent evidence)
    3. Stores results in entity metadata and relationship_sources

This should run AFTER all ingestion steps and BEFORE the analysis modules.

Requires: full_text_corpus.db downloaded from rhowardstone v3.0 release.

Usage:
    python src/analyze/corpus_integration.py
    python src/analyze/corpus_integration.py --corpus-path /path/to/full_text_corpus.db
    python src/analyze/corpus_integration.py --top-n 500  # limit entity count
"""

import json
import os
import sys
import sqlite3
import argparse
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.utils.common import (
    get_db_connection, now_iso, log_pipeline_run,
    insert_relationship, insert_relationship_source,
    find_existing_relationship, DEFAULT_DB_PATH
)

CORPUS_DB_PATH = "data/raw/rhowardstone/full_text_corpus.db"


def connect_corpus(corpus_path):
    """Connect to corpus and verify it's a real database (not LFS pointer).
    Returns (connection, has_fts) or (None, False) if unavailable."""
    if not os.path.exists(corpus_path):
        print(f"Full-text corpus not found at {corpus_path}")
        print("Skipping corpus integration. Download from:")
        print("  https://github.com/rhowardstone/Epstein-research-data/releases/tag/v3.0")
        return None, False

    size = os.path.getsize(corpus_path)
    if size < 1_000_000:
        print(f"{corpus_path} is only {size} bytes — likely a Git LFS pointer.")
        print("Skipping corpus integration.")
        return None, False

    conn = sqlite3.connect(corpus_path)

    # Detect FTS5 — rhowardstone uses a separate pages_fts table
    has_fts = False
    try:
        conn.execute("SELECT * FROM pages_fts WHERE pages_fts MATCH 'test' LIMIT 0")
        has_fts = True
    except Exception:
        pass

    # Get stats
    doc_count = conn.execute("SELECT COUNT(DISTINCT efta_number) FROM pages").fetchone()[0]
    page_count = conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]

    print(f"Corpus: {corpus_path} ({size / (1024**3):.1f} GB)")
    print(f"  Documents: {doc_count:,}")
    print(f"  Pages: {page_count:,}")
    print(f"  FTS5: {'yes' if has_fts else 'no (will use LIKE — much slower)'}")

    return conn, has_fts


def corpus_search(corpus_conn, name, has_fts):
    """Search corpus for a name. Returns set of EFTA numbers."""
    try:
        if has_fts:
            rows = corpus_conn.execute(
                'SELECT DISTINCT efta_number FROM pages_fts WHERE pages_fts MATCH ?',
                (f'"{name}"',)
            ).fetchall()
        else:
            rows = corpus_conn.execute(
                "SELECT DISTINCT efta_number FROM pages WHERE text_content LIKE ?",
                (f"%{name}%",)
            ).fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def corpus_cooccurrence(corpus_conn, name_a, name_b, has_fts):
    """Find documents where both names appear. Returns set of EFTA numbers."""
    try:
        if has_fts:
            rows = corpus_conn.execute(
                'SELECT DISTINCT efta_number FROM pages_fts WHERE pages_fts MATCH ?',
                (f'"{name_a}" AND "{name_b}"',)
            ).fetchall()
        else:
            rows = corpus_conn.execute(
                """SELECT DISTINCT efta_number FROM pages
                   WHERE text_content LIKE ? AND text_content LIKE ?""",
                (f"%{name_a}%", f"%{name_b}%")
            ).fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def step1_entity_mention_counts(ecare_conn, corpus_conn, has_fts, top_n):
    """For each person, count how many corpus documents mention them."""
    print(f"\n--- Step 1: Entity mention counts (top {top_n} by connections) ---")

    # Get persons ordered by connection count
    persons = ecare_conn.execute("""
        SELECT ce.canonical_id, ce.canonical_name, ce.aliases, ce.metadata,
               COUNT(DISTINCT r.relationship_id) as rel_count
        FROM canonical_entities ce
        LEFT JOIN relationships r ON r.source_entity_id = ce.canonical_id
                                  OR r.target_entity_id = ce.canonical_id
        WHERE ce.entity_type = 'person'
        GROUP BY ce.canonical_id
        ORDER BY rel_count DESC
        LIMIT ?
    """, (top_n,)).fetchall()

    print(f"  Searching corpus for {len(persons)} persons...")

    updated = 0
    start = time.time()
    last_report = start

    for i, (cid, cname, aliases_json, meta_json, rel_count) in enumerate(persons):
        # Build search names
        search_names = [cname]
        if aliases_json:
            try:
                for alias in json.loads(aliases_json):
                    if alias and len(alias) > 3:
                        search_names.append(alias)
            except (json.JSONDecodeError, TypeError):
                pass

        # Search each name variant, take the best count
        best_count = 0
        best_name = cname
        best_docs = set()
        for sname in search_names:
            if len(sname) < 4:
                continue
            docs = corpus_search(corpus_conn, sname, has_fts)
            if len(docs) > best_count:
                best_count = len(docs)
                best_name = sname
                best_docs = docs

        if best_count > 0:
            # Update entity metadata with corpus mention count
            meta = json.loads(meta_json) if meta_json else {}
            meta["corpus_document_count"] = best_count
            meta["corpus_search_term"] = best_name

            ecare_conn.execute(
                "UPDATE canonical_entities SET metadata = ?, last_updated = ? WHERE canonical_id = ?",
                (json.dumps(meta), now_iso(), cid)
            )
            updated += 1

        # Progress
        if time.time() - last_report > 15:
            elapsed = time.time() - start
            rate = (i + 1) / elapsed
            remaining = (len(persons) - i - 1) / rate if rate > 0 else 0
            print(f"    {i + 1}/{len(persons)} searched, {updated} with mentions "
                  f"({elapsed:.0f}s elapsed, ~{remaining:.0f}s remaining)")
            last_report = time.time()

    ecare_conn.commit()
    elapsed = time.time() - start
    print(f"  Done: {updated} entities with corpus mentions ({elapsed:.0f}s)")
    return updated


def step2_relationship_corroboration(ecare_conn, corpus_conn, has_fts, top_n):
    """For high-value relationships, check if both entities co-occur in corpus documents."""
    print(f"\n--- Step 2: Corpus co-occurrence corroboration ---")

    # Get relationships involving well-connected entities, that currently have
    # only 1 source (the biggest opportunity for improvement)
    relationships = ecare_conn.execute("""
        SELECT r.relationship_id, r.source_entity_id, r.target_entity_id,
               r.relationship_type,
               src.canonical_name AS source_name,
               tgt.canonical_name AS target_name,
               COUNT(DISTINCT rs.source_system) AS source_count
        FROM relationships r
        JOIN canonical_entities src ON src.canonical_id = r.source_entity_id
        JOIN canonical_entities tgt ON tgt.canonical_id = r.target_entity_id
        JOIN relationship_sources rs ON rs.relationship_id = r.relationship_id
        WHERE src.entity_type = 'person' AND tgt.entity_type = 'person'
        GROUP BY r.relationship_id
        HAVING source_count <= 2
        ORDER BY source_count ASC,
                 (SELECT COUNT(*) FROM relationships r2
                  WHERE r2.source_entity_id = r.source_entity_id
                     OR r2.target_entity_id = r.source_entity_id) DESC
        LIMIT ?
    """, (top_n,)).fetchall()

    print(f"  Checking {len(relationships)} relationships for corpus co-occurrence...")

    corroborated = 0
    new_relationships = 0
    start = time.time()
    last_report = start

    for i, (rel_id, src_cid, tgt_cid, rel_type, src_name, tgt_name, src_count) in enumerate(relationships):
        # Search for documents mentioning both entities
        co_docs = corpus_cooccurrence(corpus_conn, src_name, tgt_name, has_fts)

        if co_docs:
            # Check if corpus is already a source for this relationship
            existing_corpus = ecare_conn.execute(
                """SELECT id FROM relationship_sources
                   WHERE relationship_id = ? AND source_system = 'corpus'""",
                (rel_id,)
            ).fetchone()

            if not existing_corpus:
                # Add corpus as a provenance source
                # Limit stored EFTA list to 20 to avoid bloating the db
                efta_sample = sorted(co_docs)[:20]
                insert_relationship_source(
                    ecare_conn, rel_id, "corpus",
                    source_relationship_type="co_occurrence",
                    source_evidence={
                        "type": "full_text_corpus_co_occurrence",
                        "document_count": len(co_docs),
                        "efta_sample": efta_sample,
                    },
                    source_confidence=0.6,
                    evidence_class="corpus_cooccurrence"
                )
                corroborated += 1

        # Progress
        if time.time() - last_report > 15:
            elapsed = time.time() - start
            rate = (i + 1) / elapsed
            remaining = (len(relationships) - i - 1) / rate if rate > 0 else 0
            print(f"    {i + 1}/{len(relationships)} checked, {corroborated} corroborated "
                  f"({elapsed:.0f}s elapsed, ~{remaining:.0f}s remaining)")
            last_report = time.time()

    ecare_conn.commit()
    elapsed = time.time() - start
    print(f"  Done: {corroborated} relationships gained corpus corroboration ({elapsed:.0f}s)")
    return corroborated


def step3_discover_new_cooccurrences(ecare_conn, corpus_conn, has_fts, top_n_entities=100):
    """For the most prominent entities WITHOUT many relationships, find corpus co-occurrences."""
    print(f"\n--- Step 3: Discover new co-occurrences from corpus ---")

    # Get top entities by corpus mention count that have few relationships
    entities = ecare_conn.execute("""
        SELECT ce.canonical_id, ce.canonical_name,
               json_extract(ce.metadata, '$.corpus_document_count') AS doc_count,
               COUNT(DISTINCT r.relationship_id) AS rel_count
        FROM canonical_entities ce
        LEFT JOIN relationships r ON r.source_entity_id = ce.canonical_id
                                  OR r.target_entity_id = ce.canonical_id
        WHERE ce.entity_type = 'person'
          AND json_extract(ce.metadata, '$.corpus_document_count') > 10
        GROUP BY ce.canonical_id
        HAVING rel_count < 5
        ORDER BY doc_count DESC
        LIMIT ?
    """, (top_n_entities,)).fetchall()

    if not entities:
        print("  No under-connected high-mention entities found.")
        return 0

    print(f"  Checking {len(entities)} under-connected but frequently mentioned entities...")

    # Get the top 50 most connected people as potential connection targets
    hubs = ecare_conn.execute("""
        SELECT ce.canonical_id, ce.canonical_name
        FROM canonical_entities ce
        JOIN relationships r ON r.source_entity_id = ce.canonical_id
                             OR r.target_entity_id = ce.canonical_id
        WHERE ce.entity_type = 'person'
        GROUP BY ce.canonical_id
        ORDER BY COUNT(DISTINCT r.relationship_id) DESC
        LIMIT 50
    """).fetchall()

    new_rels = 0
    start = time.time()

    for cid, cname, doc_count, rel_count in entities:
        for hub_cid, hub_name in hubs:
            if cid == hub_cid:
                continue

            # Check if relationship already exists
            existing = find_existing_relationship(ecare_conn, cid, hub_cid, "co_documented")
            if existing:
                continue
            # Also check other relationship types
            any_existing = ecare_conn.execute(
                """SELECT 1 FROM relationships
                   WHERE (source_entity_id = ? AND target_entity_id = ?)
                      OR (source_entity_id = ? AND target_entity_id = ?)
                   LIMIT 1""",
                (cid, hub_cid, hub_cid, cid)
            ).fetchone()
            if any_existing:
                continue

            # Search for co-occurrence
            co_docs = corpus_cooccurrence(corpus_conn, cname, hub_name, has_fts)

            if len(co_docs) >= 3:  # Require 3+ co-occurring documents to reduce noise
                efta_sample = sorted(co_docs)[:20]
                rel_id = insert_relationship(
                    ecare_conn, cid, hub_cid, "co_documented",
                    weight=len(co_docs),
                    confidence_score=0.5,
                    source_documents=efta_sample,
                    notes=f"Discovered via corpus co-occurrence ({len(co_docs)} shared documents)"
                )
                insert_relationship_source(
                    ecare_conn, rel_id, "corpus",
                    source_relationship_type="co_occurrence",
                    source_evidence={
                        "type": "corpus_discovery",
                        "document_count": len(co_docs),
                        "efta_sample": efta_sample,
                    },
                    source_confidence=0.5,
                    evidence_class="corpus_cooccurrence"
                )
                new_rels += 1

    ecare_conn.commit()
    elapsed = time.time() - start
    print(f"  Done: {new_rels} new co-occurrence relationships discovered ({elapsed:.0f}s)")
    return new_rels


def main(db_path=DEFAULT_DB_PATH, corpus_path=None, top_n=1000):
    if corpus_path is None:
        corpus_path = CORPUS_DB_PATH

    started = now_iso()

    print("=" * 60)
    print("ECARE: Full-Text Corpus Integration")
    print("=" * 60)

    corpus_conn, has_fts = connect_corpus(corpus_path)
    if corpus_conn is None:
        print("\nCorpus not available — skipping integration.")
        return

    ecare_conn = get_db_connection(db_path)

    # Step 1: Entity mention counts
    mention_count = step1_entity_mention_counts(ecare_conn, corpus_conn, has_fts, top_n)

    # Step 2: Corroborate existing relationships
    corroborated = step2_relationship_corroboration(ecare_conn, corpus_conn, has_fts, top_n * 3)

    # Step 3: Discover new co-occurrences for under-connected entities
    new_rels = step3_discover_new_cooccurrences(ecare_conn, corpus_conn, has_fts)

    # Summary
    total_sources = ecare_conn.execute(
        "SELECT COUNT(*) FROM relationship_sources WHERE source_system = 'corpus'"
    ).fetchone()[0]
    entities_with_corpus = ecare_conn.execute(
        "SELECT COUNT(*) FROM canonical_entities WHERE json_extract(metadata, '$.corpus_document_count') > 0"
    ).fetchone()[0]

    print(f"\n{'=' * 60}")
    print(f"CORPUS INTEGRATION COMPLETE")
    print(f"  Entities with corpus mention counts: {entities_with_corpus}")
    print(f"  Relationships corroborated by corpus: {corroborated}")
    print(f"  New co-occurrence relationships: {new_rels}")
    print(f"  Total corpus provenance records: {total_sources}")
    print(f"{'=' * 60}")

    log_pipeline_run(ecare_conn, "corpus_integration", "completed",
                     records_processed=mention_count + corroborated + new_rels,
                     notes=f"{entities_with_corpus} entities enriched, "
                           f"{corroborated} relationships corroborated, "
                           f"{new_rels} new relationships discovered",
                     started_at=started)

    corpus_conn.close()
    ecare_conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ECARE Full-Text Corpus Integration")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--corpus-path", default=None,
                        help="Path to full_text_corpus.db")
    parser.add_argument("--top-n", type=int, default=1000,
                        help="Number of top entities to search (default 1000)")
    args = parser.parse_args()
    main(args.db_path, args.corpus_path, args.top_n)
