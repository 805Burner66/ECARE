"""
Document ID normalization + mapping utilities for ECARE.

Problem:
- Different sources refer to the same DOJ document using different IDs:
  - EFTA numbers (EFTA00012345)
  - DOJ-OGR numbers (DOJ-OGR-00001234)
  - Assorted "raw" IDs (case numbers, filenames, compound strings)

Goal:
- Provide a *canonical* document key (doc_key) used throughout the ECARE DB.
  Preference order:
    1) EFTA number (best)
    2) DOJ-OGR number (good)
    3) Stable hash of the raw identifier (fallback)

This module:
- Extracts EFTA / DOJ-OGR tokens from arbitrary strings
- Stores mappings in the document_ids table
- Canonicalizes any raw doc id into a doc_key (prefer EFTA when resolvable)
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
from dataclasses import dataclass
from typing import Optional, Tuple


EFTA_RE = re.compile(r"\b(EFTA\d{6,})\b", re.IGNORECASE)
# Allow DOJ OGR in a bunch of sloppy formats: "doj-ogr-123", "DOJ_OGR_00001234", etc.
DOJ_OGR_RE = re.compile(r"\bDOJ[\s\-_]?OGR[\s\-_]?(\d{1,12})\b", re.IGNORECASE)


@dataclass(frozen=True)
class DocTokens:
    doc_key: str
    raw_id: str
    efta_number: Optional[str] = None
    doj_ogr_id: Optional[str] = None


def _stable_hash(raw: str) -> str:
    h = hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()
    return h[:12]


def normalize_raw_id(raw: str) -> str:
    return (raw or "").strip()


def extract_efta(raw: str) -> Optional[str]:
    if not raw:
        return None
    m = EFTA_RE.search(raw)
    if not m:
        return None
    return m.group(1).upper()


def extract_doj_ogr(raw: str) -> Optional[str]:
    if not raw:
        return None
    m = DOJ_OGR_RE.search(raw)
    if not m:
        return None
    digits = m.group(1)
    # Most commonly 8 digits in the wild, but keep it safe.
    if len(digits) < 8:
        digits = digits.zfill(8)
    return f"DOJ-OGR-{digits}"


def doc_key_for(efta_number: Optional[str], doj_ogr_id: Optional[str], raw_id: str) -> str:
    if efta_number:
        return efta_number
    if doj_ogr_id:
        return doj_ogr_id
    raw_id = normalize_raw_id(raw_id)
    if not raw_id:
        return "RAW-000000000000"
    return f"RAW-{_stable_hash(raw_id)}"


def upsert_document_id(
    conn: sqlite3.Connection,
    *,
    doc_key: str,
    efta_number: Optional[str] = None,
    doj_ogr_id: Optional[str] = None,
    source_system: Optional[str] = None,
    raw_id: Optional[str] = None,
    confidence: Optional[float] = None,
    notes: Optional[str] = None,
) -> None:
    """Upsert into document_ids.

    The table is intentionally simple: it stores the best-known mapping between
    doc_key and (EFTA, DOJ-OGR), plus where it came from.
    """
    conn.execute(
        """
        INSERT INTO document_ids
            (doc_key, efta_number, doj_ogr_id, source_system, raw_id, confidence, notes, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(doc_key) DO UPDATE SET
            efta_number = COALESCE(excluded.efta_number, document_ids.efta_number),
            doj_ogr_id = COALESCE(excluded.doj_ogr_id, document_ids.doj_ogr_id),
            source_system = COALESCE(excluded.source_system, document_ids.source_system),
            raw_id = COALESCE(excluded.raw_id, document_ids.raw_id),
            confidence = COALESCE(excluded.confidence, document_ids.confidence),
            notes = COALESCE(excluded.notes, document_ids.notes),
            last_updated = CURRENT_TIMESTAMP
        """,
        (doc_key, efta_number, doj_ogr_id, source_system, raw_id, confidence, notes),
    )


def lookup_efta_for_doj_ogr(conn: sqlite3.Connection, doj_ogr_id: str) -> Optional[str]:
    if not doj_ogr_id:
        return None
    row = conn.execute(
        "SELECT efta_number FROM document_ids WHERE doj_ogr_id = ? AND efta_number IS NOT NULL LIMIT 1",
        (doj_ogr_id,),
    ).fetchone()
    return row[0] if row else None


def lookup_efta_for_doc_key(conn: sqlite3.Connection, doc_key: str) -> Optional[str]:
    if not doc_key:
        return None
    if doc_key.upper().startswith("EFTA"):
        return doc_key.upper()
    row = conn.execute(
        "SELECT efta_number FROM document_ids WHERE doc_key = ? AND efta_number IS NOT NULL LIMIT 1",
        (doc_key,),
    ).fetchone()
    return row[0] if row else None


def canonicalize_doc_ref(
    conn: sqlite3.Connection,
    raw_id: str,
    *,
    source_system: Optional[str] = None,
    confidence: float = 0.5,
    notes: Optional[str] = None,
) -> DocTokens:
    """Convert an arbitrary raw doc identifier to a canonical doc_key.

    Rules:
    - If an EFTA token is present: doc_key = EFTA...
    - Else if a DOJ-OGR token is present:
        - If we have a mapping to EFTA: doc_key = EFTA...
        - Else: doc_key = DOJ-OGR-...
    - Else: doc_key = RAW-<sha1-12>
    """
    raw_id = normalize_raw_id(raw_id)
    efta = extract_efta(raw_id)
    ogr = extract_doj_ogr(raw_id)

    if efta:
        doc_key = efta
    elif ogr:
        mapped = lookup_efta_for_doj_ogr(conn, ogr)
        doc_key = mapped or ogr
    else:
        doc_key = doc_key_for(None, None, raw_id)

    upsert_document_id(
        conn,
        doc_key=doc_key,
        efta_number=efta if efta else (doc_key if doc_key.startswith("EFTA") else None),
        doj_ogr_id=ogr,
        source_system=source_system,
        raw_id=raw_id if raw_id else None,
        confidence=confidence,
        notes=notes,
    )

    return DocTokens(doc_key=doc_key, raw_id=raw_id, efta_number=efta, doj_ogr_id=ogr)


def canonicalize_doc_fields(
    conn: sqlite3.Connection,
    *,
    raw_fields: Tuple[str, ...],
    source_system: Optional[str] = None,
    confidence: float = 0.5,
    notes: Optional[str] = None,
) -> DocTokens:
    """Given multiple fields that might contain IDs (document_id, document_number, filename),
    extract best tokens and generate a doc_key.

    If any field contains an EFTA token, that wins.
    Otherwise if any contains DOJ-OGR token, use it (mapped to EFTA if known).
    Otherwise hash the concatenated field string.
    """
    joined = " | ".join([normalize_raw_id(f) for f in raw_fields if f and normalize_raw_id(f)])
    if not joined:
        return canonicalize_doc_ref(conn, "", source_system=source_system, confidence=confidence, notes=notes)

    # Prefer EFTA if present anywhere
    efta = None
    ogr = None
    for f in raw_fields:
        if not f:
            continue
        if not efta:
            efta = extract_efta(f)
        if not ogr:
            ogr = extract_doj_ogr(f)
    raw_id = joined

    if efta:
        doc_key = efta
    elif ogr:
        mapped = lookup_efta_for_doj_ogr(conn, ogr)
        doc_key = mapped or ogr
    else:
        doc_key = f"RAW-{_stable_hash(raw_id)}"

    upsert_document_id(
        conn,
        doc_key=doc_key,
        efta_number=efta if efta else (doc_key if doc_key.startswith("EFTA") else None),
        doj_ogr_id=ogr,
        source_system=source_system,
        raw_id=raw_id,
        confidence=confidence,
        notes=notes,
    )

    return DocTokens(doc_key=doc_key, raw_id=raw_id, efta_number=efta, doj_ogr_id=ogr)
