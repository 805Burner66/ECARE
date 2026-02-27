"""
ECARE Analysis: Structural Gap Analysis

Identifies expected-but-missing connections in the knowledge graph using:
    1. Common neighbor analysis — unconnected pairs sharing many mutual connections
    2. Community bridge analysis — entities that bridge network communities

Performance note: Full pairwise analysis on 36K+ nodes is infeasible.
We restrict analysis to "interesting" entities (persons with 5+ connections)
which keeps runtime under a minute.

Outputs:
    data/output/gap_analysis_common_neighbors.csv
    data/output/community_bridges.csv

Usage:
    python src/analyze/gap_analysis.py
"""

import csv
import json
import os
import sys
import argparse
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.utils.common import get_db_connection, now_iso, log_pipeline_run, load_excluded_ids, DEFAULT_DB_PATH

OUTPUT_DIR = "data/output"

# Filter out known noise entities from doc-explorer
NOISE_PATTERNS = [
    "unknown person", "unknown company", "author", "narrator",
    "unidentified", "unnamed", "various ", "multiple ",
]


def is_noise_entity(name: str) -> bool:
    lower = name.lower()
    return any(p in lower for p in NOISE_PATTERNS)


def build_graph(conn, min_connections: int = 5):
    """Build a NetworkX graph from the database, restricted to well-connected person entities."""
    import networkx as nx

    print("  Building graph...")

    # Find entities with enough connections to be interesting
    connected = conn.execute("""
        WITH counts AS (
            SELECT source_entity_id as cid, COUNT(*) as c FROM relationships GROUP BY source_entity_id
            UNION ALL
            SELECT target_entity_id as cid, COUNT(*) as c FROM relationships GROUP BY target_entity_id
        )
        SELECT cid, SUM(c) as total FROM counts GROUP BY cid HAVING total >= ?
    """, (min_connections,)).fetchall()

    interesting_ids = {row[0] for row in connected}
    print(f"  Entities with {min_connections}+ connections: {len(interesting_ids)}")

    # Load exclusion flags (noise entities too entangled to delete)
    excluded_ids = load_excluded_ids(conn)
    if excluded_ids:
        print(f"  Entities flagged exclude_from_analysis: {len(excluded_ids)}")

    # Get names for interesting entities
    id_to_name = {}
    for row in conn.execute("SELECT canonical_id, canonical_name, entity_type FROM canonical_entities").fetchall():
        if row[0] in interesting_ids and not is_noise_entity(row[1]) and row[0] not in excluded_ids:
            id_to_name[row[0]] = row[1]

    interesting_ids = set(id_to_name.keys())
    print(f"  After noise + exclusion filtering: {len(interesting_ids)}")

    # Build graph with only interesting entities
    G = nx.Graph()
    for cid in interesting_ids:
        G.add_node(cid, name=id_to_name[cid])

    rels = conn.execute("""
        SELECT source_entity_id, target_entity_id, relationship_type, weight
        FROM relationships
        WHERE source_entity_id IN ({ids}) AND target_entity_id IN ({ids})
    """.format(ids=",".join(f"'{i}'" for i in interesting_ids))).fetchall()

    for src, tgt, rel_type, weight in rels:
        if src in interesting_ids and tgt in interesting_ids:
            if G.has_edge(src, tgt):
                G[src][tgt]["weight"] = G[src][tgt].get("weight", 1) + (weight or 1)
            else:
                G.add_edge(src, tgt, weight=weight or 1, rel_type=rel_type)

    print(f"  Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G, id_to_name


def common_neighbor_gaps(G, id_to_name, top_n: int = 500):
    """Find unconnected pairs that share many common neighbors."""
    import networkx as nx

    print("\n  Running common neighbor analysis...")

    # Only check pairs among the most-connected nodes to keep runtime sane
    degree_sorted = sorted(G.degree(), key=lambda x: x[1], reverse=True)
    top_nodes = [n for n, d in degree_sorted[:200]]  # top 200 by degree

    gaps = []
    checked = 0
    for i, node_a in enumerate(top_nodes):
        for node_b in top_nodes[i+1:]:
            if not G.has_edge(node_a, node_b):
                common = list(nx.common_neighbors(G, node_a, node_b))
                if len(common) >= 3:
                    common_names = [id_to_name.get(c, c) for c in common[:10]]
                    gaps.append({
                        "entity_a": id_to_name.get(node_a, node_a),
                        "entity_b": id_to_name.get(node_b, node_b),
                        "shared_neighbor_count": len(common),
                        "shared_neighbors": "; ".join(common_names),
                        "priority_score": len(common) * (G.degree(node_a) + G.degree(node_b)),
                    })
            checked += 1

    gaps.sort(key=lambda x: -x["priority_score"])
    print(f"    Pairs checked: {checked}")
    print(f"    Gaps found (3+ shared neighbors): {len(gaps)}")
    return gaps[:top_n]


def community_bridge_analysis(G, id_to_name):
    """Find entities that bridge different network communities."""
    try:
        import community as community_louvain
    except ImportError:
        print("    WARNING: python-louvain not installed, skipping community analysis")
        return []

    print("\n  Running community detection (Louvain)...")
    partition = community_louvain.best_partition(G)

    num_communities = len(set(partition.values()))
    print(f"    Communities detected: {num_communities}")

    # Find bridge nodes: connected to multiple communities
    bridges = []
    for node in G.nodes():
        neighbor_communities = set()
        for neighbor in G.neighbors(node):
            neighbor_communities.add(partition.get(neighbor))

        if len(neighbor_communities) >= 2:
            bridges.append({
                "entity": id_to_name.get(node, node),
                "entity_id": node,
                "home_community": partition.get(node),
                "communities_connected": len(neighbor_communities),
                "community_ids": "; ".join(str(c) for c in sorted(neighbor_communities)),
                "documented_connections": G.degree(node),
                "bridge_score": len(neighbor_communities) * G.degree(node),
            })

    bridges.sort(key=lambda x: -x["bridge_score"])
    print(f"    Bridge entities (connecting 2+ communities): {len(bridges)}")
    return bridges


def run_gap_analysis(db_path: str = DEFAULT_DB_PATH):
    started = now_iso()
    conn = get_db_connection(db_path)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Running structural gap analysis...")

    G, id_to_name = build_graph(conn, min_connections=5)

    # Analysis 1: Common neighbor gaps
    gaps = common_neighbor_gaps(G, id_to_name)
    gaps_path = os.path.join(OUTPUT_DIR, "gap_analysis_common_neighbors.csv")
    if gaps:
        with open(gaps_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "entity_a", "entity_b", "shared_neighbor_count",
                "shared_neighbors", "priority_score"
            ])
            writer.writeheader()
            writer.writerows(gaps)
        print(f"\n  Output: {gaps_path}")

        print(f"\n  Top 10 expected-but-missing connections:")
        for g in gaps[:10]:
            print(f"    {g['entity_a']} ↔ {g['entity_b']} "
                  f"({g['shared_neighbor_count']} shared neighbors)")

    # Analysis 2: Community bridges
    bridges = community_bridge_analysis(G, id_to_name)
    bridges_path = os.path.join(OUTPUT_DIR, "community_bridges.csv")
    if bridges:
        with open(bridges_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "entity", "entity_id", "home_community", "communities_connected",
                "community_ids", "documented_connections", "bridge_score"
            ])
            writer.writeheader()
            writer.writerows(bridges)
        print(f"  Output: {bridges_path}")

        print(f"\n  Top 10 community bridge entities:")
        for b in bridges[:10]:
            print(f"    {b['entity']} — bridges {b['communities_connected']} communities, "
                  f"{b['documented_connections']} connections")

    log_pipeline_run(conn, "gap_analysis", "completed",
                     records_processed=len(gaps) + len(bridges),
                     notes=f"{len(gaps)} gaps found, {len(bridges)} bridge entities identified.",
                     started_at=started)
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_gap_analysis(args.db_path)
