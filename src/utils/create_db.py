"""
ECARE Database Initialization
Creates the canonical SQLite database with the unified schema.

Usage:
    python src/utils/create_db.py [--db-path data/output/ecare.db] [--force]
"""

import sqlite3
import argparse
import os
from datetime import datetime, timezone


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS canonical_entities (
    canonical_id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    aliases TEXT,
    metadata TEXT,
    first_seen_date TEXT,
    last_updated TEXT,
    notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_entities_type ON canonical_entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_name ON canonical_entities(canonical_name);

CREATE TABLE IF NOT EXISTS entity_resolution_log (
    resolution_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_system TEXT NOT NULL,
    source_entity_id TEXT NOT NULL,
    source_entity_name TEXT NOT NULL,
    canonical_id TEXT NOT NULL,
    match_method TEXT NOT NULL,
    match_confidence REAL NOT NULL,
    match_details TEXT,
    resolved_by TEXT DEFAULT 'pipeline',
    resolved_date TEXT NOT NULL,
    FOREIGN KEY (canonical_id) REFERENCES canonical_entities(canonical_id)
);
CREATE INDEX IF NOT EXISTS idx_resolution_source ON entity_resolution_log(source_system, source_entity_id);
CREATE INDEX IF NOT EXISTS idx_resolution_canonical ON entity_resolution_log(canonical_id);
CREATE INDEX IF NOT EXISTS idx_resolution_confidence ON entity_resolution_log(match_confidence);

CREATE TABLE IF NOT EXISTS relationships (
    relationship_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_entity_id TEXT NOT NULL,
    target_entity_id TEXT NOT NULL,
    relationship_type TEXT NOT NULL,
    relationship_subtype TEXT,
    date_start TEXT,
    date_end TEXT,
    weight REAL,
    confidence_score REAL,
    source_documents TEXT,
    notes TEXT,
    FOREIGN KEY (source_entity_id) REFERENCES canonical_entities(canonical_id),
    FOREIGN KEY (target_entity_id) REFERENCES canonical_entities(canonical_id)
);
CREATE INDEX IF NOT EXISTS idx_rel_source ON relationships(source_entity_id);
CREATE INDEX IF NOT EXISTS idx_rel_target ON relationships(target_entity_id);
CREATE INDEX IF NOT EXISTS idx_rel_type ON relationships(relationship_type);
CREATE INDEX IF NOT EXISTS idx_rel_pair ON relationships(source_entity_id, target_entity_id);

CREATE TABLE IF NOT EXISTS relationship_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    relationship_id INTEGER NOT NULL,
    source_system TEXT NOT NULL,
    source_relationship_id TEXT,
    source_relationship_type TEXT,
    source_evidence TEXT,
    source_confidence REAL,
    evidence_class TEXT,
    date_added TEXT NOT NULL,
    FOREIGN KEY (relationship_id) REFERENCES relationships(relationship_id)
);
CREATE INDEX IF NOT EXISTS idx_relsrc_rel ON relationship_sources(relationship_id);
CREATE INDEX IF NOT EXISTS idx_relsrc_system ON relationship_sources(source_system);
CREATE INDEX IF NOT EXISTS idx_relsrc_class ON relationship_sources(evidence_class);

-- Document ID mapping (EFTA <-> DOJ-OGR <-> raw identifiers)
CREATE TABLE IF NOT EXISTS document_ids (
    doc_key TEXT PRIMARY KEY,
    efta_number TEXT,
    doj_ogr_id TEXT,
    source_system TEXT,
    raw_id TEXT,
    confidence REAL,
    notes TEXT,
    last_updated TEXT
);
CREATE INDEX IF NOT EXISTS idx_docids_efta ON document_ids(efta_number);
CREATE INDEX IF NOT EXISTS idx_docids_ogr ON document_ids(doj_ogr_id);

CREATE TABLE IF NOT EXISTS conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_or_relationship TEXT NOT NULL,
    record_id TEXT NOT NULL,
    source_a TEXT NOT NULL,
    source_b TEXT NOT NULL,
    field_in_conflict TEXT NOT NULL,
    value_a TEXT,
    value_b TEXT,
    nature_of_conflict TEXT,
    resolution_status TEXT DEFAULT 'unresolved',
    resolution_notes TEXT,
    flagged_date TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_conflicts_status ON conflicts(resolution_status);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    step_name TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT DEFAULT 'running',
    records_processed INTEGER,
    notes TEXT
);
"""


def create_database(db_path: str, force: bool = False) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    if os.path.exists(db_path):
        if force:
            os.remove(db_path)
            print(f"Removed existing database at {db_path}")
        else:
            print(f"Database already exists at {db_path}. Use --force to recreate.")
            return

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    conn.commit()

    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"Database created at: {db_path}")
    print(f"Tables: {', '.join(tables)}")

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO pipeline_runs (step_name, started_at, completed_at, status, notes) VALUES (?, ?, ?, ?, ?)",
        ("create_database", now, now, "completed", "Schema v1.1 (doc IDs + evidence classes)")
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="data/output/ecare.db")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    create_database(args.db_path, force=args.force)
