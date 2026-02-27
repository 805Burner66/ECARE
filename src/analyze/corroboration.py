"""
ECARE Analysis: Corroboration Scoring (v2)

For every relationship in the unified graph, computes a corroboration score that
reflects:
  - how many independent sources assert the relationship
  - the "class" of evidence (curated KG edge vs. RDF triple vs. co-occurrence)
  - document diversity per source (when available)

Outputs:
    data/output/corroboration_rankings.csv — all relationships with scores + evidence stats
    data/output/weakly_corroborated.csv — single-source relationships involving prominent entities

Usage:
    python src/analyze/corroboration.py
"""

from __future__ import annotations

import csv
import json
import math
import os
import sys
import argparse
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.utils.common import get_db_connection, now_iso, log_pipeline_run, DEFAULT_DB_PATH
from src.utils.doc_ids import canonicalize_doc_ref, extract_efta, extract_doj_ogr

OUTPUT_DIR = "data/output"

# Evidence class weights. These are deliberately *not* subtle.
# Co-occurrence is weak. Curated KG edges are strong. RDF is in the middle.
EVIDENCE_CLASS_WEIGHT = {
    "curated": 1.5,              # human-curated KG (rhowardstone)
    "rdf": 1.0,                  # extracted actor-action-target triples
    "corpus_cooccurrence": 0.9,  # full-text co-occurrence (better than per-doc AI summaries)
    "cooccurrence": 0.5,         # epstein-docs per-document co-occurrence
    "other": 0.6,
}


def infer_evidence_class(source_system: str, evidence_class: Optional[str]) -> str:
    if evidence_class:
        return evidence_class
    ss = (source_system or "").lower().strip()
    if ss == "rhowardstone":
        return "curated"
    if ss == "doc-explorer":
        return "rdf"
    if ss == "epstein-docs":
        return "cooccurrence"
    if ss == "corpus":
        return "corpus_cooccurrence"
    return "other"


def iter_strings(obj: Any) -> Iterable[str]:
    """Yield all strings in a nested JSON-ish structure."""
    if obj is None:
        return
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from iter_strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from iter_strings(v)


def extract_doc_keys_from_evidence(conn, source_system: str, evidence: Dict[str, Any]) -> Set[str]:
    """Best-effort extraction of doc keys from relationship_sources.source_evidence."""
    keys: Set[str] = set()
    if not evidence:
        return keys

    # Explicit lists first
    for list_field in ("efta_sample", "doc_key_sample", "doc_keys", "documents"):
        v = evidence.get(list_field)
        if isinstance(v, list):
            for item in v:
                if not item:
                    continue
                tok = canonicalize_doc_ref(conn, str(item), source_system=source_system, confidence=0.6)
                if tok.doc_key:
                    keys.add(tok.doc_key)

    # Common scalar fields across sources
    for field in ("document_id", "document_number", "doc_id", "raw_document_id", "raw_id"):
        v = evidence.get(field)
        if v:
            tok = canonicalize_doc_ref(conn, str(v), source_system=source_system, confidence=0.6)
            if tok.doc_key:
                keys.add(tok.doc_key)

    # rhowardstone stores doc refs in nested metadata sometimes
    meta = evidence.get("metadata")
    if isinstance(meta, dict):
        # Direct key
        if meta.get("efta"):
            tok = canonicalize_doc_ref(conn, str(meta.get("efta")), source_system=source_system, confidence=0.7)
            keys.add(tok.doc_key)

    # As a last resort, scan all strings for EFTA/DOJ-OGR tokens
    for s in iter_strings(evidence):
        if not s:
            continue
        if extract_efta(s) or extract_doj_ogr(s):
            tok = canonicalize_doc_ref(conn, s, source_system=source_system, confidence=0.4)
            if tok.doc_key:
                keys.add(tok.doc_key)

    return keys


def compute_score(num_sources: int, evidence_points: float, num_docs: int) -> float:
    """Compute corroboration_score in [0, 1]."""
    evidence_strength = 1.0 - math.exp(-0.9 * evidence_points)      # saturates nicely
    doc_strength = 1.0 - math.exp(-0.25 * max(num_docs, 0))         # 1 doc is small, 10 docs is big
    source_strength = 0.0 if num_sources <= 1 else 1.0 - math.exp(-0.8 * (num_sources - 1))

    score = 0.05 + 0.65 * evidence_strength + 0.20 * doc_strength + 0.10 * source_strength
    if score < 0.0:
        score = 0.0
    if score > 1.0:
        score = 1.0
    return score


def compute_corroboration(db_path: str = DEFAULT_DB_PATH):
    started = now_iso()
    conn = get_db_connection(db_path)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Computing corroboration scores (v2)...")

    # Relationships with endpoints
    rel_rows = conn.execute("""
        SELECT
            r.relationship_id,
            r.source_entity_id,
            r.target_entity_id,
            ce1.canonical_name as source_name,
            ce2.canonical_name as target_name,
            r.relationship_type,
            r.weight,
            r.source_documents
        FROM relationships r
        JOIN canonical_entities ce1 ON r.source_entity_id = ce1.canonical_id
        JOIN canonical_entities ce2 ON r.target_entity_id = ce2.canonical_id
    """).fetchall()

    print(f"  Loaded {len(rel_rows)} relationships")

    # Provenance rows
    src_rows = conn.execute("""
        SELECT relationship_id, source_system, evidence_class, source_evidence
        FROM relationship_sources
    """).fetchall()

    rel_to_sources: Dict[int, List[Tuple[str, Optional[str], Optional[str]]]] = defaultdict(list)
    for r in src_rows:
        rel_to_sources[int(r[0])].append((r[1], r[2], r[3]))

    # Entity prominence (connection count)
    prominence: Dict[str, int] = {}
    prom_rows = conn.execute("""
        WITH counts AS (
            SELECT source_entity_id as cid, COUNT(*) as c FROM relationships GROUP BY source_entity_id
            UNION ALL
            SELECT target_entity_id as cid, COUNT(*) as c FROM relationships GROUP BY target_entity_id
        )
        SELECT cid, SUM(c) FROM counts GROUP BY cid
    """).fetchall()
    for cid, count in prom_rows:
        prominence[str(cid)] = int(count or 0)

    all_rankings: List[Dict[str, Any]] = []
    weak: List[Dict[str, Any]] = []

    for row in rel_rows:
        rel_id = int(row[0])
        src_cid = str(row[1])
        tgt_cid = str(row[2])
        source_name = row[3]
        target_name = row[4]
        rel_type = row[5]
        weight = float(row[6] or 0.0)

        # Gather doc keys from relationships.source_documents
        docs_total: Set[str] = set()
        docs_json = row[7]
        if docs_json:
            try:
                loaded = json.loads(docs_json)
                if isinstance(loaded, list):
                    for d in loaded:
                        if d:
                            docs_total.add(str(d))
            except Exception:
                pass

        # Gather per-source evidence
        sources = rel_to_sources.get(rel_id, [])
        source_systems: Set[str] = set()
        evidence_classes: Set[str] = set()
        docs_by_source: Dict[str, Set[str]] = defaultdict(set)
        weight_by_source: Dict[str, float] = {}

        for source_system, evidence_class, evidence_json in sources:
            ss = (source_system or "unknown").strip()
            source_systems.add(ss)
            cls = infer_evidence_class(ss, evidence_class)
            evidence_classes.add(cls)

            # Source weight: max per source across rows
            w = EVIDENCE_CLASS_WEIGHT.get(cls, EVIDENCE_CLASS_WEIGHT["other"])
            if ss not in weight_by_source or w > weight_by_source[ss]:
                weight_by_source[ss] = w

            if evidence_json:
                try:
                    evidence = json.loads(evidence_json)
                except Exception:
                    evidence = {}
                for dk in extract_doc_keys_from_evidence(conn, ss, evidence):
                    docs_by_source[ss].add(dk)
                    docs_total.add(dk)

        num_sources = max(1, len(source_systems)) if source_systems else 1
        num_docs = len(docs_total)

        evidence_points = sum(weight_by_source.values()) if weight_by_source else 0.0
        score = compute_score(num_sources=num_sources, evidence_points=evidence_points, num_docs=num_docs)

        # Per-source doc counts (handy in CSV)
        docs_rhowardstone = len(docs_by_source.get("rhowardstone", set()))
        docs_epstein_docs = len(docs_by_source.get("epstein-docs", set()))
        docs_doc_explorer = len(docs_by_source.get("doc-explorer", set()))
        docs_corpus = len(docs_by_source.get("corpus", set()))

        record = {
            "relationship_id": rel_id,
            "source_name": source_name,
            "target_name": target_name,
            "relationship_type": rel_type,
            "weight": round(weight, 3),
            "num_sources": num_sources,
            "evidence_points": round(evidence_points, 3),
            "num_documents_total": num_docs,
            "docs_rhowardstone": docs_rhowardstone,
            "docs_epstein_docs": docs_epstein_docs,
            "docs_doc_explorer": docs_doc_explorer,
            "docs_corpus": docs_corpus,
            "source_systems": ",".join(sorted(source_systems)) if source_systems else "unknown",
            "evidence_classes": ",".join(sorted(evidence_classes)) if evidence_classes else "unknown",
            "corroboration_score": round(score, 3),
        }
        all_rankings.append(record)

        # Weak relationship flagging:
        # - only 1 source AND not purely epstein-docs co-occurrence
        # - at least one endpoint is moderately prominent
        max_prom = max(prominence.get(src_cid, 0), prominence.get(tgt_cid, 0))
        if num_sources == 1 and max_prom >= 20:
            # Identify the single evidence class
            single_cls = next(iter(evidence_classes)) if evidence_classes else "unknown"
            if single_cls != "cooccurrence":
                weak_record = dict(record)
                weak_record["max_entity_prominence"] = max_prom
                weak.append(weak_record)

    # Sort rankings
    all_rankings.sort(key=lambda x: (-x["corroboration_score"], -x["evidence_points"], -x["weight"]))
    weak.sort(key=lambda x: (-x.get("max_entity_prominence", 0), x["corroboration_score"]))

    rankings_path = os.path.join(OUTPUT_DIR, "corroboration_rankings.csv")
    with open(rankings_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(all_rankings[0].keys()) if all_rankings else [])
        writer.writeheader()
        writer.writerows(all_rankings)

    weak_path = os.path.join(OUTPUT_DIR, "weakly_corroborated.csv")
    with open(weak_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(weak[0].keys()) if weak else [])
        writer.writeheader()
        writer.writerows(weak)

    score_dist = Counter(r["corroboration_score"] for r in all_rankings)
    print(f"\n  Corroboration score distribution:")
    for score in sorted(score_dist.keys(), reverse=True):
        print(f"    {score}: {score_dist[score]} relationships")

    print(f"\n  Weakly corroborated (single-source, prominent endpoints): {len(weak)}")
    print(f"\n  Output: {rankings_path}")
    print(f"  Output: {weak_path}")

    log_pipeline_run(conn, "corroboration_scoring_v2", "completed",
                     records_processed=len(all_rankings),
                     notes=f"Scored {len(all_rankings)} relationships. {len(weak)} weakly corroborated flagged.",
                     started_at=started)
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    compute_corroboration(args.db_path)
