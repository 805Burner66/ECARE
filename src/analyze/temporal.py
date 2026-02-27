"""
ECARE Analysis: Temporal Analysis

Analyzes timing patterns in relationships and documents:
    1. Relationships with date metadata — formation rate over time
    2. Document date ranges from doc-explorer — activity clustering
    3. Known event timeline cross-referencing

Outputs:
    data/output/temporal_analysis.csv
    data/output/relationship_timeline.csv

Usage:
    python src/analyze/temporal.py
"""

import csv
import json
import os
import sys
import sqlite3
import argparse
from collections import Counter, defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.utils.common import get_db_connection, now_iso, log_pipeline_run, DEFAULT_DB_PATH

OUTPUT_DIR = "data/output"

# Key dates in the Epstein case for cross-referencing
KEY_EVENTS = {
    "2005-03": "Palm Beach PD investigation begins",
    "2006-05": "FBI investigation opens",
    "2007-06": "First non-prosecution agreement draft",
    "2008-06": "Epstein pleads guilty (FL state charges)",
    "2008-07": "Epstein begins 13-month sentence",
    "2009-07": "Epstein released from custody",
    "2014-12": "Giuffre lawsuit filed (SDFL)",
    "2015-01": "Prince Andrew allegations become public",
    "2019-07": "Epstein arrested (SDNY)",
    "2019-08": "Epstein death in custody",
    "2020-07": "Maxwell arrested",
    "2021-12": "Maxwell trial verdict",
    "2024-01": "First document unsealing (Giuffre v. Maxwell)",
    "2025-01": "EFTA signed into law",
    "2025-12": "DOJ first batch release",
    "2026-01": "DOJ second batch release",
}


def analyze_relationship_dates(conn):
    """Analyze relationships that have date metadata."""
    print("\n  Analyzing relationship dates...")

    # Get relationships with date information
    dated = conn.execute("""
        SELECT r.relationship_id, ce1.canonical_name, ce2.canonical_name,
               r.relationship_type, r.date_start, r.date_end, r.weight
        FROM relationships r
        JOIN canonical_entities ce1 ON r.source_entity_id = ce1.canonical_id
        JOIN canonical_entities ce2 ON r.target_entity_id = ce2.canonical_id
        WHERE r.date_start IS NOT NULL OR r.date_end IS NOT NULL
        ORDER BY r.date_start
    """).fetchall()

    print(f"    Relationships with date metadata: {len(dated)}")

    timeline = []
    year_month_counts = Counter()

    for row in dated:
        date_start = row[4]
        date_end = row[5]

        # Extract year-month for clustering
        date_str = date_start or date_end
        if date_str:
            try:
                # Handle various date formats
                for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
                    try:
                        dt = datetime.strptime(date_str[:len(fmt.replace("%", "0"))], fmt)
                        ym = dt.strftime("%Y-%m")
                        year_month_counts[ym] += 1
                        break
                    except ValueError:
                        continue
            except Exception:
                pass

        # Check proximity to key events
        nearby_events = []
        if date_str and len(date_str) >= 7:
            ym = date_str[:7]
            for event_date, event_desc in KEY_EVENTS.items():
                if ym == event_date:
                    nearby_events.append(event_desc)

        timeline.append({
            "source_name": row[1],
            "target_name": row[2],
            "relationship_type": row[3],
            "date_start": row[4] or "",
            "date_end": row[5] or "",
            "weight": row[6] or 0,
            "nearby_events": "; ".join(nearby_events) if nearby_events else "",
        })

    return timeline, year_month_counts


def analyze_doc_explorer_dates(conn):
    """Pull document date ranges from doc-explorer if available."""
    print("\n  Checking for doc-explorer document dates...")

    doc_explorer_db = "data/raw/doc-explorer/document_analysis.db"
    if not os.path.exists(doc_explorer_db):
        print("    doc-explorer DB not found, skipping")
        return []

    de_conn = sqlite3.connect(doc_explorer_db)

    # Check if documents table has date columns
    try:
        dates = de_conn.execute("""
            SELECT date_range_earliest, date_range_latest, category,
                   one_sentence_summary
            FROM documents
            WHERE date_range_earliest IS NOT NULL
            ORDER BY date_range_earliest
        """).fetchall()
        print(f"    Documents with dates: {len(dates)}")

        date_clusters = Counter()
        for row in dates:
            earliest = row[0]
            if earliest and len(earliest) >= 7:
                ym = earliest[:7]
                date_clusters[ym] += 1

        # Find anomalous months (> 2 standard deviations above mean)
        if date_clusters:
            values = list(date_clusters.values())
            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / len(values)
            std = variance ** 0.5
            threshold = mean + 2 * std

            anomalies = []
            for ym, count in sorted(date_clusters.items()):
                if count > threshold:
                    nearby = ""
                    for event_date, event_desc in KEY_EVENTS.items():
                        if ym == event_date:
                            nearby = event_desc
                    anomalies.append({
                        "date": ym,
                        "document_count": count,
                        "anomaly_type": "high_volume",
                        "description": f"{count} documents dated this month (avg: {mean:.1f}, threshold: {threshold:.1f})",
                        "nearby_event": nearby,
                    })

            print(f"    Anomalous months (>2σ above mean): {len(anomalies)}")
            de_conn.close()
            return anomalies

    except Exception as e:
        print(f"    Error reading doc-explorer dates: {e}")

    de_conn.close()
    return []


def run_temporal_analysis(db_path: str = DEFAULT_DB_PATH):
    started = now_iso()
    conn = get_db_connection(db_path)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Running temporal analysis...")

    # Relationship timeline
    timeline, ym_counts = analyze_relationship_dates(conn)

    timeline_path = os.path.join(OUTPUT_DIR, "relationship_timeline.csv")
    if timeline:
        with open(timeline_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "source_name", "target_name", "relationship_type",
                "date_start", "date_end", "weight", "nearby_events"
            ])
            writer.writeheader()
            writer.writerows(timeline)
        print(f"\n  Output: {timeline_path} ({len(timeline)} dated relationships)")

    # Document date anomalies
    doc_anomalies = analyze_doc_explorer_dates(conn)

    # Combine into temporal anomalies report
    all_anomalies = doc_anomalies  # relationship date anomalies could be added here
    anomalies_path = os.path.join(OUTPUT_DIR, "temporal_anomalies.csv")
    if all_anomalies:
        with open(anomalies_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "date", "document_count", "anomaly_type",
                "description", "nearby_event"
            ])
            writer.writeheader()
            writer.writerows(all_anomalies)
        print(f"  Output: {anomalies_path} ({len(all_anomalies)} anomalies)")
    else:
        print("\n  No temporal anomalies detected (limited date metadata available)")

    # Summary of date coverage
    total_rels = conn.execute("SELECT COUNT(*) FROM relationships").fetchone()[0]
    dated_rels = conn.execute(
        "SELECT COUNT(*) FROM relationships WHERE date_start IS NOT NULL OR date_end IS NOT NULL"
    ).fetchone()[0]
    print(f"\n  Date coverage: {dated_rels}/{total_rels} relationships have dates "
          f"({100*dated_rels/total_rels:.1f}%)")

    log_pipeline_run(conn, "temporal_analysis", "completed",
                     records_processed=len(timeline),
                     notes=f"{len(timeline)} dated relationships, {len(all_anomalies)} anomalies",
                     started_at=started)
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    run_temporal_analysis(args.db_path)
