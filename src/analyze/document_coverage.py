"""
ECARE Analysis: Document Coverage

For each canonical person, counts how many DOJ documents mention them
versus how many are referenced in the knowledge graph.

Two modes:
    1. Full-text corpus mode (preferred): Uses rhowardstone's full_text_corpus.db
       FTS5 index to search 1.39M documents / 2.77M pages. Accurate counts.
    2. Extracted entities fallback: Uses extracted_entities_filtered.json
       (3,881 pre-extracted names). Limited to names that were NER-detected.

The full-text corpus is ~6GB and must be downloaded separately from
rhowardstone's v3.0 release.

Outputs:
    data/output/document_coverage.csv

Usage:
    python src/analyze/document_coverage.py
    python src/analyze/document_coverage.py --corpus-path data/raw/rhowardstone/full_text_corpus.db
"""

import csv
import json
import os
import sys
import sqlite3
import argparse
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.utils.common import get_db_connection, now_iso, log_pipeline_run, DEFAULT_DB_PATH
from src.utils.doc_ids import lookup_efta_for_doc_key


OUTPUT_DIR = "data/output"
EXTRACTED_ENTITIES_PATH = "data/raw/rhowardstone/extracted_entities_filtered.json"
CORPUS_DB_PATH = "data/raw/rhowardstone/full_text_corpus.db"


def discover_corpus_schema(corpus_conn):
    """Discover the full-text corpus schema and FTS5 table."""
    tables = corpus_conn.execute(
        "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name"
    ).fetchall()

    schema = {}
    fts_table = None
    for name, ttype in tables:
        try:
            count = corpus_conn.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
        except Exception:
            count = -1
        schema[name] = {"type": ttype, "count": count}

        # Check for FTS5 virtual tables
        if "fts" in name.lower() or "search" in name.lower():
            fts_table = name

    # Check if 'pages' is an FTS5 table
    try:
        # FTS5 tables support MATCH
        corpus_conn.execute("SELECT * FROM pages WHERE pages MATCH 'test' LIMIT 0")
        fts_table = "pages"
    except Exception:
        pass

    # Look for a separate FTS table that shadows 'pages'
    if not fts_table:
        for name in schema:
            if name.startswith("pages_") and "fts" in name.lower():
                fts_table = name
                break

    return schema, fts_table


def search_corpus_fts(corpus_conn, fts_table, search_term):
    """Search the FTS5 index for a name. Returns count of distinct EFTA documents."""
    try:
        # FTS5 phrase search with double quotes
        row = corpus_conn.execute(
            f'SELECT COUNT(DISTINCT efta_number) FROM [{fts_table}] WHERE [{fts_table}] MATCH ?',
            (f'"{search_term}"',)
        ).fetchone()
        return row[0] if row else 0
    except Exception:
        return 0


def search_corpus_like(corpus_conn, search_term):
    """Fallback: search using LIKE (slower but always works)."""
    try:
        row = corpus_conn.execute(
            "SELECT COUNT(DISTINCT efta_number) FROM pages WHERE text_content LIKE ?",
            (f"%{search_term}%",)
        ).fetchone()
        return row[0] if row else 0
    except Exception:
        return 0


def run_with_corpus(db_path, corpus_path):
    """Run document coverage using the full-text corpus FTS5 index."""
    started = now_iso()
    conn = get_db_connection(db_path)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    corpus_conn = sqlite3.connect(corpus_path)
    corpus_size = os.path.getsize(corpus_path) / (1024 * 1024 * 1024)
    print(f"Full-text corpus: {corpus_path} ({corpus_size:.1f} GB)")

    # Discover schema
    schema, fts_table = discover_corpus_schema(corpus_conn)
    tables_str = ", ".join([f"{k} ({v['count']:,})" for k, v in schema.items() if v.get("count", 0) > 0])
    print(f"Corpus tables: {tables_str}")

    if fts_table:
        print(f"FTS5 table detected: {fts_table}")
        search_fn = lambda term: search_corpus_fts(corpus_conn, fts_table, term)
    else:
        print("No FTS5 table found — falling back to LIKE search (slower)")
        search_fn = lambda term: search_corpus_like(corpus_conn, term)

    # Get persons sorted by connection count (most connected first)
    persons = conn.execute("""
        SELECT ce.canonical_id, ce.canonical_name, ce.aliases,
               COUNT(DISTINCT r.relationship_id) as rel_count
        FROM canonical_entities ce
        LEFT JOIN relationships r ON r.source_entity_id = ce.canonical_id
                                  OR r.target_entity_id = ce.canonical_id
        WHERE ce.entity_type = 'person'
        GROUP BY ce.canonical_id
        ORDER BY rel_count DESC
    """).fetchall()

    # Limit to top N persons by connection count to keep runtime reasonable
    # FTS5 is fast, but 30K LIKE queries on 6GB would take hours
    MAX_PERSONS = 2000 if fts_table else 500
    search_persons = persons[:MAX_PERSONS]

    print(f"\nSearching corpus for {len(search_persons)} persons "
          f"(of {len(persons)} total, limited to top {MAX_PERSONS} by connection count)...")

    coverage = []
    search_start = time.time()
    last_report = search_start

    for i, (cid, cname, aliases_json, rel_count) in enumerate(search_persons):
        # Build search terms: canonical name + aliases
        search_names = [cname]
        if aliases_json:
            try:
                for alias in json.loads(aliases_json):
                    if alias and len(alias) > 3:  # Skip very short aliases
                        search_names.append(alias)
            except (json.JSONDecodeError, TypeError):
                pass

        # Search for best match (highest document count across name variants)
        best_count = 0
        best_name = cname
        for sname in search_names:
            # Skip names that are too generic or too short
            if len(sname) < 4 or sname.lower() in ("unknown", "john doe", "jane doe"):
                continue
            count = search_fn(sname)
            if count > best_count:
                best_count = count
                best_name = sname

        if best_count == 0:
            continue

        # Count documents referenced in this entity's relationships
        docs_in_graph = conn.execute("""
            SELECT source_documents FROM relationships
            WHERE source_entity_id = ? OR target_entity_id = ?
        """, (cid, cid)).fetchall()

        referenced_docs = set()
        for row in docs_in_graph:
            if row[0]:
                try:
                    doc_list = json.loads(row[0])
                    if isinstance(doc_list, list):
                        for d in doc_list:
                            efta = lookup_efta_for_doc_key(conn, str(d))
                            if efta:
                                referenced_docs.add(efta)
                except (json.JSONDecodeError, TypeError):
                    pass

        total = best_count
        in_graph = len(referenced_docs)
        ratio = in_graph / total if total > 0 else 0
        unanalyzed = total - in_graph

        coverage.append({
            "canonical_id": cid,
            "canonical_name": cname,
            "search_term_used": best_name,
            "total_documents_mentioning": total,
            "documents_in_knowledge_graph": in_graph,
            "coverage_ratio": round(ratio, 4),
            "unanalyzed_count": max(0, unanalyzed),
            "connections": rel_count,
        })

        # Progress reporting
        if time.time() - last_report > 10:
            elapsed = time.time() - search_start
            rate = (i + 1) / elapsed
            remaining = (len(search_persons) - i - 1) / rate if rate > 0 else 0
            print(f"    {i + 1}/{len(search_persons)} searched "
                  f"({len(coverage)} with mentions, "
                  f"{elapsed:.0f}s elapsed, ~{remaining:.0f}s remaining)")
            last_report = time.time()

    elapsed = time.time() - search_start
    print(f"  Search complete: {len(coverage)} persons with corpus mentions ({elapsed:.0f}s)")

    # Sort by unanalyzed count descending
    coverage.sort(key=lambda x: -x["unanalyzed_count"])

    output_path = os.path.join(OUTPUT_DIR, "document_coverage.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "canonical_id", "canonical_name", "search_term_used",
            "total_documents_mentioning", "documents_in_knowledge_graph",
            "coverage_ratio", "unanalyzed_count", "connections"
        ])
        writer.writeheader()
        writer.writerows(coverage)

    print(f"  Output: {output_path} ({len(coverage)} rows)")

    if coverage:
        print(f"\n  Top 20 entities by unanalyzed document count:")
        for c in coverage[:20]:
            print(f"    {c['canonical_name']}: {c['unanalyzed_count']:,} unanalyzed "
                  f"({c['documents_in_knowledge_graph']}/{c['total_documents_mentioning']:,} covered, "
                  f"{c['coverage_ratio']:.1%}) [{c['connections']} connections]")

    log_pipeline_run(conn, "document_coverage", "completed",
                     records_processed=len(coverage),
                     notes=f"Full-text corpus mode. {len(coverage)} entities with coverage data. "
                           f"Searched {len(search_persons)} of {len(persons)} persons. "
                           f"Top gap: {coverage[0]['canonical_name'] if coverage else 'N/A'}",
                     started_at=started)
    corpus_conn.close()
    conn.close()


def run_with_extracted_entities(db_path):
    """Fallback: run document coverage using extracted_entities_filtered.json."""
    started = now_iso()
    conn = get_db_connection(db_path)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Running document coverage analysis (extracted entities mode)...")
    print("  NOTE: For better results, download the full-text corpus from")
    print("  rhowardstone v3.0 release and pass --corpus-path")

    # Load extracted entities (name -> EFTA document list)
    name_to_docs = {}
    if os.path.exists(EXTRACTED_ENTITIES_PATH):
        with open(EXTRACTED_ENTITIES_PATH, encoding="utf-8") as f:
            data = json.load(f)

        for entry in data.get("names", []):
            name = entry.get("entity_value", "")
            doc_count = entry.get("document_count", 0)
            efta_list = entry.get("efta_numbers", [])
            if name and doc_count > 0:
                name_to_docs[name.lower()] = {
                    "name": name,
                    "total_documents": doc_count,
                    "efta_numbers": set(efta_list),
                }
        print(f"  Loaded {len(name_to_docs)} names from extracted_entities_filtered.json")
    else:
        print(f"  WARNING: {EXTRACTED_ENTITIES_PATH} not found")
        print("  No document coverage data available")
        log_pipeline_run(conn, "document_coverage", "completed",
                         records_processed=0,
                         notes="No data source available (no corpus, no extracted entities)",
                         started_at=started)
        conn.close()
        return

    # For each canonical person, count documents
    persons = conn.execute("""
        SELECT canonical_id, canonical_name, aliases FROM canonical_entities
        WHERE entity_type = 'person'
    """).fetchall()

    print(f"  Matching {len(persons)} persons against document mentions...")

    coverage = []

    for cid, cname, aliases_json in persons:
        search_names = [cname.lower()]
        if aliases_json:
            try:
                for alias in json.loads(aliases_json):
                    if alias:
                        search_names.append(alias.lower())
            except (json.JSONDecodeError, TypeError):
                pass

        best_match = None
        for sname in search_names:
            if sname in name_to_docs:
                if best_match is None or name_to_docs[sname]["total_documents"] > best_match["total_documents"]:
                    best_match = name_to_docs[sname]

        if not best_match:
            continue

        docs_in_graph = conn.execute("""
            SELECT source_documents FROM relationships
            WHERE source_entity_id = ? OR target_entity_id = ?
        """, (cid, cid)).fetchall()

        referenced_docs = set()
        for row in docs_in_graph:
            if row[0]:
                try:
                    doc_list = json.loads(row[0])
                    if isinstance(doc_list, list):
                        for d in doc_list:
                            efta = lookup_efta_for_doc_key(conn, str(d))
                            if efta:
                                referenced_docs.add(efta)
                except (json.JSONDecodeError, TypeError):
                    pass

        total = best_match["total_documents"]
        in_graph = len(referenced_docs)
        ratio = in_graph / total if total > 0 else 0
        unanalyzed = total - in_graph

        coverage.append({
            "canonical_id": cid,
            "canonical_name": cname,
            "search_term_used": best_match["name"],
            "total_documents_mentioning": total,
            "documents_in_knowledge_graph": in_graph,
            "coverage_ratio": round(ratio, 4),
            "unanalyzed_count": max(0, unanalyzed),
            "connections": 0,
        })

    coverage.sort(key=lambda x: -x["unanalyzed_count"])

    output_path = os.path.join(OUTPUT_DIR, "document_coverage.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "canonical_id", "canonical_name", "search_term_used",
            "total_documents_mentioning", "documents_in_knowledge_graph",
            "coverage_ratio", "unanalyzed_count", "connections"
        ])
        writer.writeheader()
        writer.writerows(coverage)

    print(f"\n  Matched {len(coverage)} persons to document mention data")
    print(f"  Output: {output_path}")

    if coverage:
        print(f"\n  Top 15 entities by unanalyzed document count:")
        for c in coverage[:15]:
            print(f"    {c['canonical_name']}: {c['unanalyzed_count']} unanalyzed "
                  f"({c['documents_in_knowledge_graph']}/{c['total_documents_mentioning']} covered, "
                  f"{c['coverage_ratio']:.0%})")

    log_pipeline_run(conn, "document_coverage", "completed",
                     records_processed=len(coverage),
                     notes=f"Extracted entities mode. {len(coverage)} entities. "
                           f"Top gap: {coverage[0]['canonical_name'] if coverage else 'N/A'}",
                     started_at=started)
    conn.close()


def run_document_coverage(db_path: str = DEFAULT_DB_PATH, corpus_path: str = None):
    """Main entry point. Uses corpus if available, falls back to extracted entities."""
    # Auto-detect corpus
    if corpus_path is None:
        corpus_path = CORPUS_DB_PATH

    if os.path.exists(corpus_path):
        size = os.path.getsize(corpus_path)
        if size > 1_000_000:  # More than 1MB = real database, not LFS pointer
            print("=" * 60)
            print("ECARE: Document Coverage Analysis (full-text corpus mode)")
            print("=" * 60)
            run_with_corpus(db_path, corpus_path)
            return

    print("=" * 60)
    print("ECARE: Document Coverage Analysis (extracted entities mode)")
    print("=" * 60)
    run_with_extracted_entities(db_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--corpus-path", default=None,
                        help="Path to full_text_corpus.db (auto-detected if not specified)")
    args = parser.parse_args()
    run_document_coverage(args.db_path, args.corpus_path)
