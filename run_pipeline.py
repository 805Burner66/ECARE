#!/usr/bin/env python3
"""
ECARE Pipeline Runner
Executes all pipeline steps in order.

Usage:
    python run_pipeline.py                    # Full pipeline
    python run_pipeline.py --skip-doc-explorer  # Skip if LFS not pulled
    python run_pipeline.py --analysis-only    # Skip ingestion, re-run analysis
    python run_pipeline.py --cleanup-only     # Re-run entity cleanup + analysis

Output:
    data/output/ecare.db           — Unified SQLite database
    data/output/*.csv              — Analysis outputs
    data/output/*.md               — Summary reports
"""

import subprocess
import sys
import argparse
import os
import time
import sqlite3


INGEST_STEPS = [
    ("Creating database",             "python src/utils/create_db.py --force"),
    ("Ingesting rhowardstone data",   "python src/ingest/ingest_rhowardstone.py"),
    ("Ingesting epstein-docs data",   "python src/ingest/ingest_epstein_docs.py"),
    ("Ingesting doc-explorer data",   "python src/ingest/ingest_doc_explorer.py"),
]

VALIDATE_STEP = [
    ("Validating database integrity", "python src/utils/validate.py"),
]

CLEANUP_STEPS = [
    ("Entity cleanup & merge",        "python src/resolve/merge_entities.py"),
]

ANALYSIS_STEPS = [
    ("Corpus integration",            "python src/analyze/corpus_integration.py"),
    ("Corroboration scoring",         "python src/analyze/corroboration.py"),
    ("Gap analysis",                  "python src/analyze/gap_analysis.py"),
    ("Document coverage analysis",    "python src/analyze/document_coverage.py"),
    ("Temporal analysis",             "python src/analyze/temporal.py"),
    ("Contradiction detection",       "python src/analyze/contradictions.py"),
    ("Research priorities",           "python src/analyze/prioritize.py"),
]


def print_summary():
    """Print final database summary."""
    db_path = "data/output/ecare.db"
    if not os.path.exists(db_path):
        return

    conn = sqlite3.connect(db_path)
    entities = conn.execute("SELECT COUNT(*) FROM canonical_entities").fetchone()[0]
    persons = conn.execute("SELECT COUNT(*) FROM canonical_entities WHERE entity_type='person'").fetchone()[0]
    rels = conn.execute("SELECT COUNT(*) FROM relationships").fetchone()[0]
    resolutions = conn.execute("SELECT COUNT(*) FROM entity_resolution_log").fetchone()[0]
    conflicts = conn.execute("SELECT COUNT(*) FROM conflicts").fetchone()[0]

    # Check for merges table
    merges = 0
    try:
        merges = conn.execute("SELECT COUNT(*) FROM entity_merges").fetchone()[0]
    except Exception:
        pass

    db_size = os.path.getsize(db_path) / (1024 * 1024)
    conn.close()

    print(f"\n  Database: {db_path} ({db_size:.1f} MB)")
    print(f"  Canonical entities: {entities:,} ({persons:,} persons)")
    print(f"  Relationships: {rels:,}")
    print(f"  Resolution log entries: {resolutions:,}")
    print(f"  Entity merges performed: {merges:,}")
    print(f"  Cross-source conflicts: {conflicts:,}")

    # List output files
    output_dir = "data/output"
    csvs = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
    mds = [f for f in os.listdir(output_dir) if f.endswith('.md')]
    if csvs or mds:
        print(f"\n  Output files ({len(csvs)} CSVs, {len(mds)} reports):")
        for f in sorted(csvs + mds):
            size = os.path.getsize(os.path.join(output_dir, f))
            print(f"    {f} ({size / 1024:.0f} KB)")


def main():
    parser = argparse.ArgumentParser(description="ECARE Pipeline Runner")
    parser.add_argument("--skip-doc-explorer", action="store_true",
                        help="Skip doc-explorer ingestion (if LFS not pulled)")
    parser.add_argument("--analysis-only", action="store_true",
                        help="Skip ingestion + cleanup, re-run analysis only")
    parser.add_argument("--cleanup-only", action="store_true",
                        help="Skip ingestion, re-run cleanup + analysis")
    args = parser.parse_args()

    pipeline_start = time.time()

    print("=" * 60)
    print("ECARE Pipeline — Cross-Platform Analytical Reconciliation")
    print("=" * 60)

    if args.analysis_only:
        steps = ANALYSIS_STEPS
        print("\nMode: analysis-only (using existing database)")
    elif args.cleanup_only:
        steps = CLEANUP_STEPS + ANALYSIS_STEPS
        print("\nMode: cleanup + analysis (using existing database)")
    else:
        steps = INGEST_STEPS + VALIDATE_STEP + CLEANUP_STEPS + ANALYSIS_STEPS

    step_num = 0
    for description, command in steps:
        step_num += 1

        if args.skip_doc_explorer and "doc-explorer" in command:
            print(f"\n[{step_num}/{len(steps)}] {description} — SKIPPED")
            continue

        print(f"\n[{step_num}/{len(steps)}] {description}")
        print("-" * 40)

        step_start = time.time()
        result = subprocess.run(command, shell=True)
        step_time = time.time() - step_start

        if result.returncode != 0:
            print(f"\nERROR: Step failed with return code {result.returncode}")
            print("Pipeline aborted. Fix the error and re-run.")
            sys.exit(1)

        print(f"  Completed in {step_time:.1f}s")

    total_time = time.time() - pipeline_start

    print(f"\n{'=' * 60}")
    print(f"PIPELINE COMPLETE ({total_time:.0f}s total)")
    print_summary()
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
