"""
ECARE: Post-Ingestion Entity Cleanup & Merge

Runs after all ingestion steps but before analysis. Fixes three classes of
entity resolution failures that the fuzzy matcher can't catch:

    1. UNMERGED DUPLICATES — entities that refer to the same person but whose
       surface forms are too different for token_sort_ratio ≥ 90 to catch.
       Root causes: title prefixes ("President Clinton" vs "Bill Clinton"),
       ALL-CAPS transcript variants ("MR. LARRY VISOSKI" vs "Larry Visoski"),
       hyphen-vs-space ("Jean-Luc" vs "Jean Luc").

    2. NOISE ENTITIES — strings that survived is_noise_entity_name() but are
       clearly not real entities: single first names ("Mary", "David"),
       standalone titles ("President"), generic role words ("journalist"),
       question-mark placeholders ("Roger ?").

    3. CASE / TITLE NORMALIZATION — canonical_name cleanup for entities that
       are kept but have ugly names (ALL-CAPS → Title Case, strip Mr./Ms.).

For every merge or deletion, this module:
    - Repoints all FK references (relationships, entity_resolution_log)
    - Merges aliases from absorbed entity into survivor
    - Consolidates duplicate relationships created by the merge
    - Logs the action in an entity_merges audit table

Usage:
    python src/resolve/merge_entities.py [--db-path data/output/ecare.db] [--dry-run]
"""

from __future__ import annotations

import json
import os
import re
import sys
import argparse
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.utils.common import (
    get_db_connection, now_iso, log_pipeline_run, load_canonical_registry,
    DEFAULT_DB_PATH
)
from src.resolve.resolve_persons import normalize_name, is_noise_entity_name

# ---------------------------------------------------------------------------
# Title / honorific prefixes to strip for matching
# ---------------------------------------------------------------------------
TITLE_PREFIXES = [
    "president ", "professor ", "prof. ", "senator ", "rep. ",
    "judge ", "justice ", "attorney ", "agent ",
    "dr. ", "mr. ", "mrs. ", "ms. ", "miss ",
    "sir ", "lord ", "lady ", "prince ", "princess ",
    "king ", "queen ", "sheikh ", "imam ",
    "general ", "colonel ", "captain ", "detective ",
    "governor ", "mayor ", "ambassador ",
]

# ---------------------------------------------------------------------------
# Expanded noise filter — catches things is_noise_entity_name() misses
# ---------------------------------------------------------------------------
EXPANDED_NOISE_EXACT = {
    # Standalone titles / roles
    "president", "attorney", "judge", "senator", "detective",
    "agent", "officer", "prosecutor", "prosecutors", "counsel",
    # Generic words that aren't entities
    "public", "communication", "journalist", "journalists",
    "applicant", "appellant", "respondent",
    "government", "state", "country",
    # Multi-word generic phrases the base filter missed
    "federal prosecutors", "federal agents", "federal government",
    "law enforcement", "defense counsel", "legal counsel",
    "epstein case", "jeffrey epstein appellate case",
}

EXPANDED_NOISE_PATTERNS = [
    re.compile(r"^\w+\s+\?$"),                 # Question-mark placeholders: "Roger ?", "Bruce ?"
    re.compile(r"^(Jeffrey\s+)?Epstein\s+(case|appellate|investigation|matter)", re.IGNORECASE),
    re.compile(r"^\w{1,2}$"),                  # 1-2 char strings
    re.compile(r"^[A-Z]\.[A-Z]\.$"),           # Initials like "L.M.", "J.D."
]

# Known single first names that are common enough to be noise in this corpus
# (they exist because co-occurrence extraction pulled them from partial names)
NOISE_FIRST_NAMES = {
    "mary", "david", "sarah", "john", "james", "michael", "robert",
    "tony", "leon", "eva", "rebecca", "bruce", "roger", "ralph",
    "warren", "mark", "steve", "chris", "peter", "paul", "george",
    "jane", "tom", "joe",
}

# Single-word entities that are real orgs/countries/entities and should NOT
# be treated as noise even though they're one word
PROTECTED_SINGLE_WORDS = {
    "hamas", "isis", "hezbollah", "mossad", "interpol",
    "libya", "iraq", "iran", "syria", "yemen", "qatar", "dubai",
    "harvard", "yale", "princeton", "stanford", "columbia", "mit",
    "citibank", "barclays", "jpmorgan", "wexner", "victoria",
}


def is_expanded_noise(name: str) -> bool:
    """Catches noise entities that the base filter misses."""
    if not name or not name.strip():
        return True
    s = name.strip()

    # Already caught by base filter
    if is_noise_entity_name(s):
        return True

    lowered = s.lower().strip()

    # Protect known real single-word entities
    if lowered in PROTECTED_SINGLE_WORDS:
        return False

    # Exact noise words / phrases
    if lowered in EXPANDED_NOISE_EXACT:
        return True

    # Single-word entries that are common first names (not real full entity names)
    if " " not in s.strip() and lowered in NOISE_FIRST_NAMES:
        return True

    # Question-mark names
    if s.endswith(" ?") or (s.endswith("?") and len(s) <= 15):
        return True

    # Expanded patterns
    for pat in EXPANDED_NOISE_PATTERNS:
        if pat.match(s):
            return True

    return False


def strip_titles(name: str) -> str:
    """Remove title/honorific prefixes from a name."""
    s = name.strip()
    lowered = s.lower()
    changed = True
    while changed:
        changed = False
        for prefix in TITLE_PREFIXES:
            if lowered.startswith(prefix):
                s = s[len(prefix):].strip()
                lowered = s.lower()
                changed = True
    return s


def to_title_case(name: str) -> str:
    """Convert ALL-CAPS or mixed names to Title Case, preserving particles.

    "MR. LARRY VISOSKI" → "Larry Visoski"
    "HEATHER MANN" → "Heather Mann"
    "Jean-Luc Brunel" → "Jean-Luc Brunel" (no change)
    """
    # Only act if the name is mostly uppercase
    alpha_chars = [c for c in name if c.isalpha()]
    if not alpha_chars:
        return name
    upper_ratio = sum(1 for c in alpha_chars if c.isupper()) / len(alpha_chars)
    if upper_ratio < 0.7:
        return name  # Already mixed case, leave it alone

    # Title-case each word, but handle hyphenated names
    parts = name.split()
    result = []
    for part in parts:
        if "-" in part:
            result.append("-".join(w.capitalize() for w in part.split("-")))
        elif part.upper() == part and len(part) <= 3 and "." in part:
            result.append(part)  # Keep abbreviations like "J.D."
        else:
            result.append(part.capitalize())
    return " ".join(result)


def clean_name_for_matching(name: str) -> str:
    """Normalize a name aggressively for duplicate detection.

    Strips titles, normalizes case, removes punctuation, normalizes whitespace.
    Used ONLY for matching — not for display.
    """
    s = name.strip()
    s = strip_titles(s)
    s = to_title_case(s)
    s = normalize_name(s)
    s = s.lower().strip()
    # Remove remaining dots and commas
    s = re.sub(r'[.,]', '', s)
    # Normalize hyphens to spaces
    s = s.replace("-", " ")
    # Remove stutter: "nadia nadia marcinkova" → "nadia marcinkova"
    words = s.split()
    if len(words) >= 3 and words[0] == words[1]:
        words = words[1:]
    # Remove single-letter middle initials: "jack a goldberger" → "jack goldberger"
    if len(words) >= 3:
        words = [w for i, w in enumerate(words)
                 if not (0 < i < len(words) - 1 and len(w) == 1)]
    s = " ".join(words)
    # Collapse whitespace
    s = " ".join(s.split())
    return s


def pick_survivor_name(name_a: str, name_b: str) -> str:
    """Given two names for the same entity, pick the better canonical name.

    Prefer: proper case > ALL-CAPS, no title > title, longer > shorter.
    """
    def score(n: str) -> Tuple[int, int, int]:
        alpha = [c for c in n if c.isalpha()]
        upper_ratio = (sum(1 for c in alpha if c.isupper()) / len(alpha)) if alpha else 0
        has_title = any(n.lower().startswith(p) for p in TITLE_PREFIXES)
        return (
            0 if has_title else 1,         # no title preferred
            0 if upper_ratio > 0.7 else 1,  # mixed case preferred
            len(n),                          # longer preferred (more complete name)
        )

    if score(name_a) >= score(name_b):
        return name_a
    return name_b


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

def ensure_merges_table(conn) -> None:
    """Create the entity_merges audit table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_merges (
            merge_id INTEGER PRIMARY KEY AUTOINCREMENT,
            survivor_id TEXT NOT NULL,
            absorbed_id TEXT NOT NULL,
            survivor_name TEXT,
            absorbed_name TEXT,
            merge_reason TEXT,
            match_key TEXT,
            relationships_repointed INTEGER DEFAULT 0,
            resolution_logs_repointed INTEGER DEFAULT 0,
            duplicate_rels_consolidated INTEGER DEFAULT 0,
            merged_at TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_merges_survivor ON entity_merges(survivor_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_merges_absorbed ON entity_merges(absorbed_id)")
    conn.commit()


def _merge_metadata(survivor_meta: dict, absorbed_meta: dict) -> dict:
    """Merge metadata dicts when absorbing an entity.

    Rules:
        - Numeric fields: keep the max (corpus counts, hop distance, etc.)
        - List fields: union
        - String fields: keep survivor's value (it's the authority), but store
          absorbed's value under a "_from_absorbed" suffix if different
        - Bool fields: OR (if either says True, keep True)
        - Special: preserve exclude_from_analysis if either has it
    """
    merged = dict(survivor_meta)

    for key, abs_val in absorbed_meta.items():
        surv_val = merged.get(key)

        if surv_val is None:
            # Survivor doesn't have this key at all — take absorbed's value
            merged[key] = abs_val
        elif isinstance(surv_val, (int, float)) and isinstance(abs_val, (int, float)):
            merged[key] = max(surv_val, abs_val)
        elif isinstance(surv_val, list) and isinstance(abs_val, list):
            # Union, preserving order, deduped
            seen = set()
            result = []
            for item in surv_val + abs_val:
                item_key = str(item).lower() if isinstance(item, str) else item
                if item_key not in seen:
                    seen.add(item_key)
                    result.append(item)
            merged[key] = result
        elif isinstance(surv_val, bool) and isinstance(abs_val, bool):
            merged[key] = surv_val or abs_val
        # else: keep survivor's value (strings, dicts, etc.)

    return merged


def merge_entity_pair(conn, survivor_id: str, absorbed_id: str,
                      reason: str, match_key: str, dry_run: bool = False) -> dict:
    """Merge absorbed entity into survivor.

    Steps:
        1. Merge aliases (absorbed name + its aliases → survivor aliases)
        2. Repoint relationships (source_entity_id, target_entity_id)
        3. Repoint entity_resolution_log
        4. Consolidate duplicate relationships (same pair + type after merge)
        5. Delete absorbed entity
        6. Log in entity_merges

    Returns: dict with stats
    """
    stats = {"relationships_repointed": 0, "resolution_logs_repointed": 0,
             "duplicate_rels_consolidated": 0}

    # Get entity details
    survivor = conn.execute(
        "SELECT canonical_name, aliases, metadata FROM canonical_entities WHERE canonical_id = ?",
        (survivor_id,)
    ).fetchone()
    absorbed = conn.execute(
        "SELECT canonical_name, aliases, metadata FROM canonical_entities WHERE canonical_id = ?",
        (absorbed_id,)
    ).fetchone()

    if not survivor or not absorbed:
        return stats

    survivor_name = survivor["canonical_name"]
    absorbed_name = absorbed["canonical_name"]

    if dry_run:
        print(f"  [DRY RUN] Would merge: \"{absorbed_name}\" ({absorbed_id}) → \"{survivor_name}\" ({survivor_id}) — {reason}")
        return stats

    # 1. Merge aliases
    surv_aliases = json.loads(survivor["aliases"]) if survivor["aliases"] else []
    abs_aliases = json.loads(absorbed["aliases"]) if absorbed["aliases"] else []
    existing_lower = {a.lower() for a in surv_aliases}
    existing_lower.add(survivor_name.lower())

    # Add absorbed name as alias
    if absorbed_name.lower() not in existing_lower:
        surv_aliases.append(absorbed_name)
        existing_lower.add(absorbed_name.lower())

    # Add absorbed aliases
    for alias in abs_aliases:
        if alias and alias.lower() not in existing_lower:
            surv_aliases.append(alias)
            existing_lower.add(alias.lower())

    conn.execute(
        "UPDATE canonical_entities SET aliases = ?, last_updated = ? WHERE canonical_id = ?",
        (json.dumps(surv_aliases), now_iso(), survivor_id)
    )

    # 1b. Merge metadata
    surv_meta = json.loads(survivor["metadata"]) if survivor["metadata"] else {}
    abs_meta = json.loads(absorbed["metadata"]) if absorbed["metadata"] else {}
    merged_meta = _merge_metadata(surv_meta, abs_meta)
    conn.execute(
        "UPDATE canonical_entities SET metadata = ? WHERE canonical_id = ?",
        (json.dumps(merged_meta), survivor_id)
    )

    # 2. Repoint relationships
    for col in ("source_entity_id", "target_entity_id"):
        cursor = conn.execute(
            f"UPDATE relationships SET {col} = ? WHERE {col} = ?",
            (survivor_id, absorbed_id)
        )
        stats["relationships_repointed"] += cursor.rowcount

    # 3. Repoint entity_resolution_log
    cursor = conn.execute(
        "UPDATE entity_resolution_log SET canonical_id = ? WHERE canonical_id = ?",
        (survivor_id, absorbed_id)
    )
    stats["resolution_logs_repointed"] = cursor.rowcount

    # 4. Consolidate duplicate relationships
    # After repointing, we may have two relationships with the same
    # (source, target, type) — one from each original entity.
    stats["duplicate_rels_consolidated"] = _consolidate_duplicate_relationships(conn, survivor_id)

    # 5. Remove self-referential relationships (entity linked to itself after merge)
    self_refs = conn.execute(
        "SELECT relationship_id FROM relationships WHERE source_entity_id = ? AND target_entity_id = ?",
        (survivor_id, survivor_id)
    ).fetchall()
    for row in self_refs:
        conn.execute("DELETE FROM relationship_sources WHERE relationship_id = ?", (row[0],))
        conn.execute("DELETE FROM relationships WHERE relationship_id = ?", (row[0],))

    # 6. Delete absorbed entity
    conn.execute("DELETE FROM canonical_entities WHERE canonical_id = ?", (absorbed_id,))

    # 7. Log merge
    conn.execute(
        """INSERT INTO entity_merges
           (survivor_id, absorbed_id, survivor_name, absorbed_name,
            merge_reason, match_key, relationships_repointed,
            resolution_logs_repointed, duplicate_rels_consolidated, merged_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (survivor_id, absorbed_id, survivor_name, absorbed_name,
         reason, match_key,
         stats["relationships_repointed"], stats["resolution_logs_repointed"],
         stats["duplicate_rels_consolidated"], now_iso())
    )

    return stats


def _consolidate_duplicate_relationships(conn, entity_id: str) -> int:
    """After a merge, find and consolidate duplicate relationships for an entity.

    Duplicate = same (source, target, type) pair. We keep the one with higher
    weight, merge doc lists, and move relationship_sources from the loser.
    """
    # Find all relationships involving this entity
    rels = conn.execute(
        """SELECT relationship_id, source_entity_id, target_entity_id,
                  relationship_type, weight, source_documents
           FROM relationships
           WHERE source_entity_id = ? OR target_entity_id = ?""",
        (entity_id, entity_id)
    ).fetchall()

    # Group by normalized pair + type
    groups: Dict[Tuple, List] = defaultdict(list)
    for r in rels:
        pair = tuple(sorted([r["source_entity_id"], r["target_entity_id"]]))
        key = (pair[0], pair[1], r["relationship_type"])
        groups[key].append(r)

    consolidated = 0
    for key, group in groups.items():
        if len(group) < 2:
            continue

        # Keep the one with highest weight
        group.sort(key=lambda r: -(r["weight"] or 0))
        survivor_rel = group[0]
        survivor_rid = survivor_rel["relationship_id"]

        for loser in group[1:]:
            loser_rid = loser["relationship_id"]

            # Merge doc lists
            try:
                surv_docs = json.loads(survivor_rel["source_documents"]) if survivor_rel["source_documents"] else []
            except Exception:
                surv_docs = []
            try:
                loser_docs = json.loads(loser["source_documents"]) if loser["source_documents"] else []
            except Exception:
                loser_docs = []

            merged_docs = list(set(surv_docs + loser_docs))[:200]

            # Add weights
            new_weight = (survivor_rel["weight"] or 0) + (loser["weight"] or 0)

            conn.execute(
                "UPDATE relationships SET weight = ?, source_documents = ? WHERE relationship_id = ?",
                (new_weight, json.dumps(merged_docs), survivor_rid)
            )

            # Move relationship_sources
            conn.execute(
                "UPDATE relationship_sources SET relationship_id = ? WHERE relationship_id = ?",
                (survivor_rid, loser_rid)
            )

            # Delete the loser relationship
            conn.execute("DELETE FROM relationships WHERE relationship_id = ?", (loser_rid,))
            consolidated += 1

    return consolidated


def delete_noise_entity(conn, canonical_id: str, name: str, reason: str,
                        dry_run: bool = False) -> dict:
    """Remove a noise entity and all its relationships.

    Returns: dict with stats
    """
    stats = {"relationships_deleted": 0, "sources_deleted": 0}

    if dry_run:
        print(f"  [DRY RUN] Would delete noise: \"{name}\" ({canonical_id}) — {reason}")
        return stats

    # Get relationship IDs to clean up
    rel_ids = conn.execute(
        "SELECT relationship_id FROM relationships WHERE source_entity_id = ? OR target_entity_id = ?",
        (canonical_id, canonical_id)
    ).fetchall()

    for row in rel_ids:
        rid = row[0]
        conn.execute("DELETE FROM relationship_sources WHERE relationship_id = ?", (rid,))
        stats["sources_deleted"] += 1
        conn.execute("DELETE FROM relationships WHERE relationship_id = ?", (rid,))
        stats["relationships_deleted"] += 1

    # Delete resolution log entries
    conn.execute("DELETE FROM entity_resolution_log WHERE canonical_id = ?", (canonical_id,))

    # Delete entity
    conn.execute("DELETE FROM canonical_entities WHERE canonical_id = ?", (canonical_id,))

    # Log as merge with absorbed_id = deleted entity, survivor_id = "DELETED"
    conn.execute(
        """INSERT INTO entity_merges
           (survivor_id, absorbed_id, survivor_name, absorbed_name,
            merge_reason, match_key, merged_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ("NOISE_DELETED", canonical_id, "N/A", name,
         f"noise_removal: {reason}", name.lower(), now_iso())
    )

    return stats


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------

def find_merge_candidates(conn) -> List[Tuple[str, str, str, str]]:
    """Find entity pairs that should be merged.

    Two-pass strategy:
        Pass 1: Clean both names aggressively and match on identical cleaned form.
                 Catches: title variants, hyphen/space, ALL-CAPS, stutters.
        Pass 2: Find title+last-name-only entities (e.g. "Mr. Cassell") and match
                 to full-name entities by last name. Only merges if unambiguous
                 (exactly one candidate) or disambiguated by graph overlap.

    Returns: list of (survivor_id, absorbed_id, reason, match_key)
    """
    registry = load_canonical_registry(conn)
    candidates = []
    seen_absorbed: Set[str] = set()

    persons = {cid: e for cid, e in registry.items() if e["entity_type"] == "person"}

    # ---------------------------------------------------------------
    # Pass 1: Cleaned name matching
    # ---------------------------------------------------------------
    clean_to_entities: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for cid, entity in persons.items():
        cname = entity["canonical_name"]
        cleaned = clean_name_for_matching(cname)
        if cleaned:
            clean_to_entities[cleaned].append((cid, cname))

        # Also check aliases
        for alias in entity.get("aliases", []):
            if alias:
                alias_cleaned = clean_name_for_matching(alias)
                if alias_cleaned and alias_cleaned != cleaned:
                    clean_to_entities[alias_cleaned].append((cid, cname))

    for cleaned, entities in clean_to_entities.items():
        # Deduplicate by cid
        unique = {}
        for cid, name in entities:
            if cid not in unique:
                unique[cid] = name

        if len(unique) < 2:
            continue

        items = list(unique.items())
        items.sort(key=lambda x: (
            -_get_prominence(conn, x[0]),
            any(x[1].lower().startswith(p) for p in TITLE_PREFIXES),
            x[1] == x[1].upper(),
        ))

        survivor_id, survivor_name = items[0]

        for absorbed_id, absorbed_name in items[1:]:
            if absorbed_id == survivor_id or absorbed_id in seen_absorbed:
                continue

            reasons = []
            if any(absorbed_name.lower().startswith(p) for p in TITLE_PREFIXES):
                reasons.append("title_prefix")
            if absorbed_name == absorbed_name.upper() and len(absorbed_name) > 3:
                reasons.append("all_caps_variant")
            if "-" in absorbed_name and " " in survivor_name:
                reasons.append("hyphen_normalization")
            if not reasons:
                reasons.append("cleaned_name_match")

            candidates.append((survivor_id, absorbed_id, "; ".join(reasons), cleaned))
            seen_absorbed.add(absorbed_id)

    # ---------------------------------------------------------------
    # Pass 2: Last-name-only entities (title + single word after strip)
    # ---------------------------------------------------------------
    # Build last-name → full-name-entity lookup
    lastname_to_full: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for cid, entity in persons.items():
        if cid in seen_absorbed:
            continue
        cname = entity["canonical_name"]
        cleaned = clean_name_for_matching(cname)
        words = cleaned.split()
        if len(words) >= 2:
            last = words[-1]
            lastname_to_full[last].append((cid, cname))

    for cid, entity in persons.items():
        if cid in seen_absorbed:
            continue
        cname = entity["canonical_name"]

        # Check if this entity reduces to a single word after title strip
        stripped = strip_titles(cname).strip()
        cleaned = clean_name_for_matching(cname)
        clean_words = cleaned.split()

        if len(clean_words) != 1:
            continue

        last_name = clean_words[0]

        # Find full-name entities with this last name
        full_name_matches = [
            (fid, fname) for fid, fname in lastname_to_full.get(last_name, [])
            if fid != cid and fid not in seen_absorbed
        ]

        if len(full_name_matches) == 0:
            continue

        if len(full_name_matches) == 1:
            # Unambiguous — single full-name entity with this last name
            survivor_id, survivor_name = full_name_matches[0]
            candidates.append((
                survivor_id, cid,
                "lastname_only_unambiguous",
                f"{last_name} (from \"{cname}\")"
            ))
            seen_absorbed.add(cid)

        else:
            # Ambiguous — multiple candidates. Use graph overlap to disambiguate.
            best = _disambiguate_by_graph(conn, cid, full_name_matches)
            if best:
                survivor_id, survivor_name = best
                candidates.append((
                    survivor_id, cid,
                    "lastname_only_graph_disambiguated",
                    f"{last_name} (from \"{cname}\")"
                ))
                seen_absorbed.add(cid)

    return candidates


def _get_neighbors(conn, cid: str) -> Set[str]:
    """Get all entity IDs connected to this entity."""
    rows = conn.execute(
        """SELECT source_entity_id, target_entity_id FROM relationships
           WHERE source_entity_id = ? OR target_entity_id = ?""",
        (cid, cid)
    ).fetchall()
    neighbors = set()
    for r in rows:
        if r[0] != cid:
            neighbors.add(r[0])
        if r[1] != cid:
            neighbors.add(r[1])
    return neighbors


def _disambiguate_by_graph(conn, absorbed_cid: str,
                           candidates: List[Tuple[str, str]]) -> Optional[Tuple[str, str]]:
    """Given an ambiguous last-name-only entity, pick the best merge target
    by comparing graph neighborhoods.

    Returns the candidate with highest Jaccard overlap, or None if no clear winner.
    """
    absorbed_neighbors = _get_neighbors(conn, absorbed_cid)
    if not absorbed_neighbors:
        return None

    best_overlap = 0.0
    best_candidate = None
    second_best = 0.0

    for cid, name in candidates:
        cand_neighbors = _get_neighbors(conn, cid)
        if not cand_neighbors:
            continue
        intersection = len(absorbed_neighbors & cand_neighbors)
        union = len(absorbed_neighbors | cand_neighbors)
        if union == 0:
            continue
        jaccard = intersection / union
        if jaccard > best_overlap:
            second_best = best_overlap
            best_overlap = jaccard
            best_candidate = (cid, name)
        elif jaccard > second_best:
            second_best = jaccard

    # Require clear winner: best must be meaningfully better than second-best
    # and have at least some overlap
    if best_candidate and best_overlap >= 0.05 and best_overlap > second_best * 1.5:
        return best_candidate

    return None


def _get_prominence(conn, cid: str) -> int:
    """Quick prominence lookup (connection count)."""
    row = conn.execute(
        """SELECT COUNT(*) FROM relationships
           WHERE source_entity_id = ? OR target_entity_id = ?""",
        (cid, cid)
    ).fetchone()
    return row[0] if row else 0


def find_noise_entities(conn) -> Tuple[List[Tuple[str, str, str]], List[Tuple[str, str, str]]]:
    """Find entities that are noise.

    Returns:
        (deletable, flaggable)
        - deletable: low-prominence noise → safe to remove
        - flaggable: high-prominence noise → keep the node, flag exclude_from_analysis
    """
    deletable = []
    flaggable = []
    registry = load_canonical_registry(conn)

    for cid, entity in registry.items():
        name = entity["canonical_name"]

        if not is_expanded_noise(name):
            continue

        # Already flagged?
        meta = entity.get("metadata", {})
        if meta.get("exclude_from_analysis"):
            continue

        prom = _get_prominence(conn, cid)
        reason = _classify_noise(name)

        if prom > 50:
            flaggable.append((cid, name, reason))
        else:
            deletable.append((cid, name, reason))

    return deletable, flaggable


def flag_noise_entity(conn, canonical_id: str, name: str, reason: str,
                      dry_run: bool = False) -> None:
    """Flag a noise entity with exclude_from_analysis=true instead of deleting it.

    Preserves the entity and all its relationships but marks it so analysis
    modules (prioritize, gap_analysis, community_bridges) skip it.
    """
    if dry_run:
        prom = _get_prominence(conn, canonical_id)
        print(f"  [DRY RUN] Would flag: \"{name}\" ({canonical_id}, prom={prom}) — {reason}")
        return

    row = conn.execute(
        "SELECT metadata FROM canonical_entities WHERE canonical_id = ?",
        (canonical_id,)
    ).fetchone()
    meta = json.loads(row["metadata"]) if row and row["metadata"] else {}
    meta["exclude_from_analysis"] = True
    meta["exclude_reason"] = reason
    conn.execute(
        "UPDATE canonical_entities SET metadata = ?, last_updated = ? WHERE canonical_id = ?",
        (json.dumps(meta), now_iso(), canonical_id)
    )


def _classify_noise(name: str) -> str:
    """Classify why a name is considered noise (for logging)."""
    lowered = name.strip().lower()
    if lowered in EXPANDED_NOISE_EXACT:
        return f"generic_word:{lowered}"
    if " " not in name.strip() and lowered in NOISE_FIRST_NAMES:
        return f"standalone_first_name:{lowered}"
    if name.endswith(" ?") or name.endswith("?"):
        return "question_mark_placeholder"
    if is_noise_entity_name(name):
        return "base_noise_filter"
    for pat in EXPANDED_NOISE_PATTERNS:
        if pat.match(name):
            return f"pattern_match:{pat.pattern}"
    return "expanded_noise"


def find_name_cleanups(conn) -> List[Tuple[str, str, str]]:
    """Find entities whose canonical_name should be cleaned up.

    Returns: list of (canonical_id, old_name, new_name) where old != new.
    """
    cleanups = []
    registry = load_canonical_registry(conn)

    for cid, entity in registry.items():
        name = entity["canonical_name"]
        new_name = name

        # Strip titles
        stripped = strip_titles(new_name)
        if stripped and stripped != new_name:
            new_name = stripped

        # Fix ALL-CAPS
        new_name = to_title_case(new_name)

        # Fix "Nadia Nadia Marcinkova" type stutters
        words = new_name.split()
        if len(words) >= 3 and words[0].lower() == words[1].lower():
            new_name = " ".join(words[1:])

        # Normalize whitespace
        new_name = " ".join(new_name.split())

        if new_name != name:
            cleanups.append((cid, name, new_name))

    return cleanups


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(db_path: str = DEFAULT_DB_PATH, dry_run: bool = False):
    started = now_iso()
    conn = get_db_connection(db_path)
    ensure_merges_table(conn)

    print("=" * 60)
    print("ECARE: Post-Ingestion Entity Cleanup")
    print("=" * 60)
    if dry_run:
        print("*** DRY RUN — no changes will be made ***\n")

    total_stats = Counter()

    # --- Phase 1: Handle noise entities ---
    print("\nPhase 1: Noise Entity Handling")
    print("-" * 40)

    deletable, flaggable = find_noise_entities(conn)
    print(f"  Found {len(deletable)} noise entities to delete (low prominence)")
    print(f"  Found {len(flaggable)} noise entities to flag (high prominence, exclude_from_analysis)")

    for cid, name, reason in deletable:
        stats = delete_noise_entity(conn, cid, name, reason, dry_run=dry_run)
        total_stats["noise_deleted"] += 1
        total_stats["noise_rels_deleted"] += stats["relationships_deleted"]

    for cid, name, reason in flaggable:
        flag_noise_entity(conn, cid, name, reason, dry_run=dry_run)
        total_stats["noise_flagged"] += 1

    if not dry_run:
        conn.commit()

    # Show sample
    if deletable:
        print(f"\n  Sample deletions:")
        for cid, name, reason in deletable[:15]:
            print(f"    \"{name}\" — {reason}")
        if len(deletable) > 15:
            print(f"    ... and {len(deletable) - 15} more")
    if flaggable:
        print(f"\n  Flagged (exclude_from_analysis):")
        for cid, name, reason in flaggable[:15]:
            prom = _get_prominence(conn, cid)
            print(f"    \"{name}\" (prom={prom}) — {reason}")
        if len(flaggable) > 15:
            print(f"    ... and {len(flaggable) - 15} more")

    # --- Phase 2: Merge duplicates ---
    print(f"\nPhase 2: Duplicate Entity Merging")
    print("-" * 40)

    candidates = find_merge_candidates(conn)
    print(f"  Found {len(candidates)} merge candidates")

    for survivor_id, absorbed_id, reason, match_key in candidates:
        # Verify both still exist (earlier merges in this batch may have
        # already absorbed one of them)
        s_exists = conn.execute(
            "SELECT 1 FROM canonical_entities WHERE canonical_id = ?", (survivor_id,)
        ).fetchone()
        a_exists = conn.execute(
            "SELECT 1 FROM canonical_entities WHERE canonical_id = ?", (absorbed_id,)
        ).fetchone()
        if not s_exists or not a_exists:
            continue

        stats = merge_entity_pair(conn, survivor_id, absorbed_id, reason, match_key,
                                  dry_run=dry_run)
        total_stats["entities_merged"] += 1
        total_stats["merge_rels_repointed"] += stats["relationships_repointed"]
        total_stats["merge_rels_consolidated"] += stats["duplicate_rels_consolidated"]
        total_stats["merge_logs_repointed"] += stats["resolution_logs_repointed"]

    if not dry_run:
        conn.commit()

    # Show sample
    if candidates:
        print(f"\n  Sample merges:")
        for sid, aid, reason, mk in candidates[:20]:
            # Look up names (may have been deleted if dry_run=False, so handle gracefully)
            srow = conn.execute("SELECT canonical_name FROM canonical_entities WHERE canonical_id = ?", (sid,)).fetchone()
            print(f"    [{reason}] key=\"{mk}\" → survivor={sid} ({srow['canonical_name'] if srow else '?'})")
        if len(candidates) > 20:
            print(f"    ... and {len(candidates) - 20} more")

    # --- Phase 3: Name cleanup (non-destructive) ---
    print(f"\nPhase 3: Canonical Name Cleanup")
    print("-" * 40)

    cleanups = find_name_cleanups(conn)
    print(f"  Found {len(cleanups)} names to clean up")

    for cid, old_name, new_name in cleanups:
        if dry_run:
            print(f"  [DRY RUN] \"{old_name}\" → \"{new_name}\"")
        else:
            # Add old name as alias before renaming
            row = conn.execute(
                "SELECT aliases FROM canonical_entities WHERE canonical_id = ?", (cid,)
            ).fetchone()
            aliases = json.loads(row["aliases"]) if row and row["aliases"] else []
            existing_lower = {a.lower() for a in aliases}
            existing_lower.add(new_name.lower())
            if old_name.lower() not in existing_lower:
                aliases.append(old_name)

            conn.execute(
                "UPDATE canonical_entities SET canonical_name = ?, aliases = ?, last_updated = ? WHERE canonical_id = ?",
                (new_name, json.dumps(aliases), now_iso(), cid)
            )
        total_stats["names_cleaned"] += 1

    if not dry_run:
        conn.commit()

    # Show sample
    if cleanups:
        print(f"\n  Sample cleanups:")
        for cid, old_name, new_name in cleanups[:20]:
            print(f"    \"{old_name}\" → \"{new_name}\"")
        if len(cleanups) > 20:
            print(f"    ... and {len(cleanups) - 20} more")

    # --- Summary ---
    entity_count = conn.execute("SELECT COUNT(*) FROM canonical_entities").fetchone()[0]
    rel_count = conn.execute("SELECT COUNT(*) FROM relationships").fetchone()[0]

    print(f"\n{'=' * 60}")
    print(f"CLEANUP SUMMARY")
    print(f"  Noise entities removed: {total_stats['noise_deleted']}")
    print(f"    Relationships deleted: {total_stats['noise_rels_deleted']}")
    print(f"  Noise entities flagged (exclude_from_analysis): {total_stats['noise_flagged']}")
    print(f"  Entities merged: {total_stats['entities_merged']}")
    print(f"    Relationships repointed: {total_stats['merge_rels_repointed']}")
    print(f"    Duplicate relationships consolidated: {total_stats['merge_rels_consolidated']}")
    print(f"    Resolution log entries repointed: {total_stats['merge_logs_repointed']}")
    print(f"  Names cleaned up: {total_stats['names_cleaned']}")
    print(f"  Remaining entities: {entity_count}")
    print(f"  Remaining relationships: {rel_count}")
    print(f"{'=' * 60}")

    if not dry_run:
        notes = (
            f"Removed {total_stats['noise_deleted']} noise entities. "
            f"Flagged {total_stats['noise_flagged']} noise entities. "
            f"Merged {total_stats['entities_merged']} duplicates. "
            f"Cleaned {total_stats['names_cleaned']} names."
        )
        log_pipeline_run(conn, "merge_entities", "completed",
                         records_processed=sum(total_stats.values()),
                         notes=notes, started_at=started)
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Post-ingestion entity cleanup")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without making changes")
    args = parser.parse_args()
    main(args.db_path, dry_run=args.dry_run)
