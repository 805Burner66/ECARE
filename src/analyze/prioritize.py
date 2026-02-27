"""
ECARE Analysis: Research Priority Synthesis

Combines all analytical outputs into a composite priority score for each entity,
producing a ranked "where to look next" list.

Scoring factors:
    - Document coverage gaps (many unanalyzed documents)
    - Structural gaps (expected-but-missing connections)
    - Weakly corroborated relationships (single-source claims about prominent people)
    - Cross-source contradictions
    - Community bridge status (connects different network clusters)
    - Entity prominence (more connections = higher baseline importance)

Outputs:
    data/output/research_priorities.csv
    data/output/research_priorities_summary.md

Usage:
    python src/analyze/prioritize.py
"""

import csv
import json
import os
import sys
import argparse
import re
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.utils.common import get_db_connection, now_iso, log_pipeline_run, load_excluded_ids, DEFAULT_DB_PATH

OUTPUT_DIR = "data/output"

# Noise entities to exclude from priority rankings
NOISE_PATTERNS = [
    "unknown person",
    "unknown company",
    "author",
    "narrator",
    "unidentified",
    "unnamed",
    "various ",
    "multiple ",
    "plaintiff and ",
    "participants",
    "security clearance",
    "technology industry",
    "whistleblower",
    "embryonic stem",
    "unknown organization",
    "unknown individual",
    "unknown entity",
    "redacted",
    "sealed",
    "attendee",
    "attendees",
    "reporter",
    "plaintiff",
    "defendant",
    "witness",
    "victim",
    "employee-",
    "employee ",
    "victim ",
    "witness ",
    "john doe",
    "jane doe",
    "the government",
    "the court",
    "prosecution",
    "defense counsel",
]

NOISE_REGEXES = [
    re.compile(r"^(employee|victim|witness)\s*[#]?\d+\b", re.IGNORECASE),
    re.compile(r"^(john|jane)\s+doe\s*[#]?\d+\b", re.IGNORECASE),
    re.compile(r"^\(b\)\(\d+\)", re.IGNORECASE),
]
def is_noise(name: str) -> bool:
    lower = (name or "").strip().lower()
    if not lower:
        return True
    if any(p in lower for p in NOISE_PATTERNS):
        return True
    return any(rx.search(lower) for rx in NOISE_REGEXES)

def load_csv_data(filename):
    """Load a CSV from the output directory, returning list of dicts."""
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def run_prioritization(db_path: str = DEFAULT_DB_PATH):
    started = now_iso()
    conn = get_db_connection(db_path)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Computing research priorities...")

    # Load all analysis outputs
    coverage_data = load_csv_data("document_coverage.csv")
    weak_data = load_csv_data("weakly_corroborated.csv")
    gaps_data = load_csv_data("gap_analysis_common_neighbors.csv")
    bridges_data = load_csv_data("community_bridges.csv")
    contradictions_data = load_csv_data("cross_source_contradictions.csv")

    print(f"  Loaded: {len(coverage_data)} coverage, {len(weak_data)} weak relationships, "
          f"{len(gaps_data)} gaps, {len(bridges_data)} bridges, {len(contradictions_data)} contradictions")

    # Build per-entity scoring
    scores = defaultdict(lambda: {
        "canonical_id": "",
        "canonical_name": "",
        "priority_score": 0.0,
        "factors": [],
        "prominence": 0,
    })

    # Factor 1: Entity prominence (baseline)
    print("  Computing entity prominence...")
    prom_rows = conn.execute("""
        WITH counts AS (
            SELECT source_entity_id as cid, COUNT(*) as c FROM relationships GROUP BY source_entity_id
            UNION ALL
            SELECT target_entity_id as cid, COUNT(*) as c FROM relationships GROUP BY target_entity_id
        )
        SELECT cid, SUM(c) as total FROM counts GROUP BY cid
    """).fetchall()

    # Load exclusion flags (noise entities too entangled to delete)
    excluded_ids = load_excluded_ids(conn)
    if excluded_ids:
        print(f"  Entities flagged exclude_from_analysis: {len(excluded_ids)} (will be skipped)")

    id_to_name = {}
    for row in conn.execute("SELECT canonical_id, canonical_name, entity_type FROM canonical_entities WHERE entity_type = 'person'"):
        if row[0] not in excluded_ids:
            id_to_name[row[0]] = row[1]

    for cid, total in prom_rows:
        name = id_to_name.get(cid)
        if not name or is_noise(name):
            continue
        scores[name]["canonical_id"] = cid
        scores[name]["canonical_name"] = name
        scores[name]["prominence"] = total
        # Prominence gives a small baseline score (log-scaled)
        import math
        scores[name]["priority_score"] += min(math.log2(total + 1) * 2, 20)

    # Factor 2: Document coverage gaps
    print("  Applying document coverage gaps...")
    for row in coverage_data:
        name = row["canonical_name"]
        if is_noise(name):
            continue
        unanalyzed = int(row.get("unanalyzed_count", 0))
        if unanalyzed >= 10:
            bonus = min(unanalyzed / 10, 30)  # cap at 30 points
            scores[name]["priority_score"] += bonus
            scores[name]["factors"].append(f"document_gap:{unanalyzed}_unanalyzed")
            if not scores[name]["canonical_id"]:
                scores[name]["canonical_id"] = row.get("canonical_id", "")
                scores[name]["canonical_name"] = name

    # Factor 2b: Corpus mention volume (entities mentioned in many docs
    # but with few graph relationships = under-investigated)
    print("  Applying corpus mention volume...")
    corpus_rows = conn.execute("""
        SELECT ce.canonical_id, ce.canonical_name,
               CAST(json_extract(ce.metadata, '$.corpus_document_count') AS INTEGER) AS doc_count,
               COUNT(DISTINCT r.relationship_id) AS rel_count
        FROM canonical_entities ce
        LEFT JOIN relationships r ON r.source_entity_id = ce.canonical_id
                                  OR r.target_entity_id = ce.canonical_id
        WHERE ce.entity_type = 'person'
          AND json_extract(ce.metadata, '$.corpus_document_count') IS NOT NULL
        GROUP BY ce.canonical_id
    """).fetchall()

    corpus_enriched = 0
    for cid, name, doc_count, rel_count in corpus_rows:
        if not doc_count or is_noise(name):
            continue
        # Ratio of document mentions to graph relationships
        # High ratio = mentioned in many docs but few analyzed relationships
        if doc_count > 20 and rel_count < doc_count / 10:
            bonus = min(doc_count / 50, 25)  # cap at 25
            scores[name]["priority_score"] += bonus
            scores[name]["factors"].append(f"corpus_mentions:{doc_count}_docs/{rel_count}_rels")
            if not scores[name]["canonical_id"]:
                scores[name]["canonical_id"] = cid
                scores[name]["canonical_name"] = name
            corpus_enriched += 1

    print(f"    {corpus_enriched} entities scored from corpus mention data")

    # Factor 3: Weakly corroborated relationships
    print("  Applying weak corroboration signals...")
    weak_by_entity = defaultdict(int)
    for row in weak_data:
        for name_field in ["source_name", "target_name"]:
            name = row[name_field]
            if not is_noise(name):
                weak_by_entity[name] += 1

    for name, count in weak_by_entity.items():
        if count >= 2:
            bonus = min(count * 3, 20)
            scores[name]["priority_score"] += bonus
            scores[name]["factors"].append(f"weak_relationships:{count}")

    # Factor 4: Structural gaps (appears in expected-but-missing pairs)
    print("  Applying structural gap signals...")
    gap_by_entity = defaultdict(int)
    for row in gaps_data:
        for name_field in ["entity_a", "entity_b"]:
            name = row[name_field]
            if not is_noise(name):
                gap_by_entity[name] += 1

    for name, count in gap_by_entity.items():
        if count >= 1:
            bonus = min(count * 5, 25)
            scores[name]["priority_score"] += bonus
            scores[name]["factors"].append(f"structural_gaps:{count}")

    # Factor 5: Community bridges
    print("  Applying community bridge signals...")
    for row in bridges_data:
        name = row.get("entity", "")
        if is_noise(name):
            continue
        communities = int(row.get("communities_connected", 0))
        if communities >= 3:
            bonus = min(communities * 3, 15)
            scores[name]["priority_score"] += bonus
            scores[name]["factors"].append(f"bridges:{communities}_communities")

    # Factor 6: Cross-source contradictions
    print("  Applying contradiction signals...")
    for row in contradictions_data:
        name = row.get("entity_a", "")
        if is_noise(name):
            continue
        severity = row.get("severity", "low")
        bonus = {"high": 10, "medium": 5, "low": 2}.get(severity, 1)
        scores[name]["priority_score"] += bonus
        scores[name]["factors"].append(f"contradiction:{row.get('field', 'unknown')}:{severity}")

    # Filter and sort
    ranked = [v for v in scores.values() if v["priority_score"] > 0 and v["canonical_name"]]
    ranked.sort(key=lambda x: -x["priority_score"])

    # Write CSV
    csv_path = os.path.join(OUTPUT_DIR, "research_priorities.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "canonical_id", "canonical_name", "priority_score",
            "prominence", "contributing_factors"
        ])
        writer.writeheader()
        for r in ranked:
            writer.writerow({
                "canonical_id": r["canonical_id"],
                "canonical_name": r["canonical_name"],
                "priority_score": round(r["priority_score"], 1),
                "prominence": r["prominence"],
                "contributing_factors": "; ".join(r["factors"]) if r["factors"] else "prominence_only",
            })

    # Write markdown summary
    md_path = os.path.join(OUTPUT_DIR, "research_priorities_summary.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# ECARE Research Priorities\n\n")
        f.write(f"Generated: {now_iso()}\n\n")
        f.write("**Disclaimer:** Inclusion in this list does not imply guilt or wrongdoing. ")
        f.write("Priority scores reflect gaps in available research data, not suspicion. ")
        f.write("Entities are ranked by the combination of their network prominence, ")
        f.write("unanalyzed document volume, weakly-sourced relationships, and structural ")
        f.write("gaps in the knowledge graph.\n\n")
        f.write("---\n\n")
        f.write("## Top 50 Research Priorities\n\n")
        f.write("| Rank | Entity | Score | Prominence | Key Factors |\n")
        f.write("|------|--------|-------|------------|-------------|\n")
        for i, r in enumerate(ranked[:50], 1):
            factors_short = "; ".join(r["factors"][:3]) if r["factors"] else "prominence"
            f.write(f"| {i} | {r['canonical_name']} | {r['priority_score']:.1f} | "
                    f"{r['prominence']} | {factors_short} |\n")

        f.write("\n---\n\n## Factor Explanations\n\n")
        f.write("- **document_gap:N_unanalyzed** — This entity is mentioned in N documents ")
        f.write("that have not been incorporated into the knowledge graph.\n")
        f.write("- **corpus_mentions:N_docs/M_rels** — This entity appears in N full-text corpus ")
        f.write("documents but has only M relationships in the graph, suggesting under-investigation.\n")
        f.write("- **weak_relationships:N** — This entity has N relationships supported by ")
        f.write("only a single source with minimal documentation.\n")
        f.write("- **structural_gaps:N** — This entity appears in N pairs of people who share ")
        f.write("multiple mutual connections but have no documented direct relationship.\n")
        f.write("- **bridges:N_communities** — This entity connects N distinct network communities, ")
        f.write("suggesting they may be an under-investigated intermediary.\n")
        f.write("- **contradiction:field:severity** — Different sources disagree about this ")
        f.write("entity's classification or relationships.\n")
        f.write("- **prominence** — Baseline score from network connectivity.\n")

    print(f"\n  Ranked {len(ranked)} entities")
    print(f"  Output: {csv_path}")
    print(f"  Output: {md_path}")

    print(f"\n  Top 20 research priorities:")
    for i, r in enumerate(ranked[:20], 1):
        factors = ", ".join(r["factors"][:2]) if r["factors"] else "prominence"
        print(f"    {i:2d}. {r['canonical_name']} (score: {r['priority_score']:.1f}) — {factors}")

    log_pipeline_run(conn, "research_priorities", "completed",
                     records_processed=len(ranked),
                     notes=f"Ranked {len(ranked)} entities. Top: {ranked[0]['canonical_name'] if ranked else 'N/A'}",
                     started_at=started)
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_prioritization(args.db_path)
