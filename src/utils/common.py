"""
Shared utilities for the ECARE pipeline.
"""

from __future__ import annotations

import sqlite3
import json
import os
from datetime import datetime, timezone
from typing import Optional, Iterable, Set


# Default paths — all relative to project root
DEFAULT_DB_PATH = "data/output/ecare.db"
RAW_DATA_DIR = "data/raw"


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Best-effort lightweight migrations so older DBs don't explode.

    The pipeline normally recreates the DB with --force, but people are creative
    and will run analysis-only on an older file. This keeps that from turning
    into a choose-your-own-traceback adventure.
    """
    # document_ids table (doc_key -> (EFTA, DOJ-OGR) mapping)
    conn.execute(
        """
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
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_docids_efta ON document_ids(efta_number)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_docids_ogr ON document_ids(doj_ogr_id)")

    # relationship_sources.evidence_class column
    cols = [r[1] for r in conn.execute("PRAGMA table_info(relationship_sources)").fetchall()]
    if "evidence_class" not in cols:
        conn.execute("ALTER TABLE relationship_sources ADD COLUMN evidence_class TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_relsrc_class ON relationship_sources(evidence_class)")

    conn.commit()


def get_db_connection(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Get a connection to the ECARE database with standard settings."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    try:
        ensure_schema(conn)
    except Exception:
        # If the DB is brand new and tables aren't there yet, ensure_schema may fail.
        # create_db.py will create the base tables anyway. Don't die here.
        pass
    return conn


def now_iso() -> str:
    """Return current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()


def log_pipeline_run(conn: sqlite3.Connection, step_name: str, status: str,
                     records_processed: Optional[int] = None, notes: Optional[str] = None,
                     started_at: Optional[str] = None) -> int:
    """Log a pipeline step execution. Returns run_id."""
    now = now_iso()
    cursor = conn.execute(
        """INSERT INTO pipeline_runs (step_name, started_at, completed_at, status, records_processed, notes)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (step_name, started_at or now, now, status, records_processed, notes)
    )
    conn.commit()
    return cursor.lastrowid


def insert_canonical_entity(conn: sqlite3.Connection, canonical_id: str, entity_type: str,
                            canonical_name: str, aliases: list = None, metadata: dict = None,
                            first_seen_date: str = None, notes: str = None) -> None:
    """Insert a canonical entity record."""
    conn.execute(
        """INSERT INTO canonical_entities
           (canonical_id, entity_type, canonical_name, aliases, metadata, first_seen_date, last_updated, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (canonical_id, entity_type, canonical_name,
         json.dumps(aliases) if aliases else None,
         json.dumps(metadata) if metadata else None,
         first_seen_date, now_iso(), notes)
    )


def insert_resolution_log(conn: sqlite3.Connection, source_system: str, source_entity_id: str,
                          source_entity_name: str, canonical_id: str, match_method: str,
                          match_confidence: float, match_details: dict = None,
                          resolved_by: str = "pipeline") -> None:
    """Log an entity resolution decision."""
    conn.execute(
        """INSERT INTO entity_resolution_log
           (source_system, source_entity_id, source_entity_name, canonical_id,
            match_method, match_confidence, match_details, resolved_by, resolved_date)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (source_system, source_entity_id, source_entity_name, canonical_id,
         match_method, match_confidence,
         json.dumps(match_details) if match_details else None,
         resolved_by, now_iso())
    )


def insert_relationship(conn: sqlite3.Connection, source_entity_id: str, target_entity_id: str,
                        relationship_type: str, relationship_subtype: str = None,
                        date_start: str = None, date_end: str = None,
                        weight: float = None, confidence_score: float = None,
                        source_documents: list = None, notes: str = None) -> int:
    """Insert a relationship. Returns relationship_id."""
    cursor = conn.execute(
        """INSERT INTO relationships
           (source_entity_id, target_entity_id, relationship_type, relationship_subtype,
            date_start, date_end, weight, confidence_score, source_documents, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (source_entity_id, target_entity_id, relationship_type, relationship_subtype,
         date_start, date_end, weight, confidence_score,
         json.dumps(source_documents) if source_documents else None, notes)
    )
    return cursor.lastrowid


def insert_relationship_source(conn: sqlite3.Connection, relationship_id: int,
                               source_system: str, source_relationship_id: str = None,
                               source_relationship_type: str = None,
                               source_evidence: dict = None,
                               source_confidence: float = None,
                               evidence_class: str = None) -> None:
    """Log provenance for a relationship."""
    # evidence_class is optional for backwards compatibility (ensure_schema adds the column).
    conn.execute(
        """INSERT INTO relationship_sources
           (relationship_id, source_system, source_relationship_id, source_relationship_type,
            source_evidence, source_confidence, evidence_class, date_added)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (relationship_id, source_system, source_relationship_id, source_relationship_type,
         json.dumps(source_evidence) if source_evidence else None,
         source_confidence, evidence_class, now_iso())
    )


def append_relationship_documents(conn: sqlite3.Connection, relationship_id: int,
                                  doc_keys: Iterable[str], *, cap: int = 200) -> None:
    """Append doc_keys to relationships.source_documents JSON array (deduped)."""
    keys = [k for k in (doc_keys or []) if k]
    if not keys:
        return

    row = conn.execute(
        "SELECT source_documents FROM relationships WHERE relationship_id = ?",
        (relationship_id,)
    ).fetchone()
    existing: Set[str] = set()
    if row and row[0]:
        try:
            loaded = json.loads(row[0])
            if isinstance(loaded, list):
                existing.update(str(x) for x in loaded if x)
        except Exception:
            pass

    for k in keys:
        existing.add(str(k))

    # Stable-ish ordering: EFTA first, then DOJ-OGR, then RAW
    def sort_key(x: str):
        ux = x.upper()
        if ux.startswith("EFTA"):
            return (0, ux)
        if ux.startswith("DOJ-OGR"):
            return (1, ux)
        return (2, ux)

    merged = sorted(existing, key=sort_key)[:cap]
    conn.execute(
        "UPDATE relationships SET source_documents = ? WHERE relationship_id = ?",
        (json.dumps(merged), relationship_id)
    )


def find_existing_relationship(conn: sqlite3.Connection, source_id: str, target_id: str,
                               rel_type: str) -> Optional[int]:
    """Check if a relationship already exists (in either direction). Returns relationship_id or None."""
    row = conn.execute(
        """SELECT relationship_id FROM relationships
           WHERE ((source_entity_id = ? AND target_entity_id = ?)
                  OR (source_entity_id = ? AND target_entity_id = ?))
             AND relationship_type = ?""",
        (source_id, target_id, target_id, source_id, rel_type)
    ).fetchone()
    return row[0] if row else None


def get_next_id(conn: sqlite3.Connection, entity_type: str) -> str:
    """Get the next canonical_id for a given entity type."""
    prefix_map = {
        "person": "PER", "organization": "ORG", "location": "LOC",
        "document": "DOC", "aircraft": "AIR", "property": "PROP",
        "shell_company": "ORG",  # shell companies are a subtype of org
    }
    prefix = prefix_map.get(entity_type, "ENT")

    row = conn.execute(
        "SELECT canonical_id FROM canonical_entities WHERE canonical_id LIKE ? ORDER BY canonical_id DESC LIMIT 1",
        (f"{prefix}-%",)
    ).fetchone()

    if row:
        last_num = int(row[0].split("-")[1])
        return f"{prefix}-{last_num + 1:05d}"
    else:
        return f"{prefix}-00001"


def load_canonical_registry(conn: sqlite3.Connection) -> dict:
    """Load the full canonical entity registry into memory for matching.

    Returns dict: canonical_id -> {canonical_name, aliases, entity_type, metadata}
    """
    registry = {}
    rows = conn.execute(
        "SELECT canonical_id, entity_type, canonical_name, aliases, metadata FROM canonical_entities"
    ).fetchall()

    for row in rows:
        aliases = json.loads(row["aliases"]) if row["aliases"] else []
        metadata = json.loads(row["metadata"]) if row["metadata"] else {}
        registry[row["canonical_id"]] = {
            "canonical_name": row["canonical_name"],
            "aliases": aliases,
            "entity_type": row["entity_type"],
            "metadata": metadata,
        }
    return registry


def is_excluded_from_analysis(conn: sqlite3.Connection, canonical_id: str) -> bool:
    """Check if an entity is flagged as excluded from analysis.

    Used by analysis modules (prioritize, gap_analysis, community_bridges) to
    skip noise entities that are too entangled to delete but shouldn't appear
    in analytical outputs.
    """
    row = conn.execute(
        "SELECT metadata FROM canonical_entities WHERE canonical_id = ?",
        (canonical_id,)
    ).fetchone()
    if not row or not row["metadata"]:
        return False
    try:
        meta = json.loads(row["metadata"])
        return bool(meta.get("exclude_from_analysis"))
    except Exception:
        return False


def load_excluded_ids(conn: sqlite3.Connection) -> set:
    """Load all canonical_ids flagged with exclude_from_analysis.

    Returns a set for O(1) membership checks in analysis loops.
    """
    excluded = set()
    rows = conn.execute(
        "SELECT canonical_id, metadata FROM canonical_entities WHERE metadata LIKE '%exclude_from_analysis%'"
    ).fetchall()
    for row in rows:
        try:
            meta = json.loads(row["metadata"]) if row["metadata"] else {}
            if meta.get("exclude_from_analysis"):
                excluded.add(row["canonical_id"])
        except Exception:
            pass
    return excluded
