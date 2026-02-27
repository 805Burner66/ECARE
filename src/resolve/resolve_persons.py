"""
ECARE Entity Resolution Module

Resolves source entity names against the canonical registry using a multi-tier
matching strategy:

    1. Exact match on canonical_name (case-insensitive)
    2. Exact match on any alias (case-insensitive)
    3. Fuzzy match via rapidfuzz token_sort_ratio (threshold configurable, default 90)
    4. No match → returns None for the caller to handle

Design decisions:
    - token_sort_ratio is used instead of plain ratio because names appear in
      different orderings across sources ("Black, Leon" vs "Leon Black")
    - The threshold of 90 is intentionally high to minimize false positives.
      We'd rather create a duplicate entity than incorrectly merge two people.
    - All matches are logged with method and confidence so manual review can
      catch errors downstream.
"""

import re
from rapidfuzz import fuzz, process
from typing import Optional, Tuple, List


# Patterns that should never be fuzzy-matched to each other
# (numbered/indexed references to distinct individuals)
NUMBERED_PATTERN = re.compile(r'(?:Jane|John)\s+Doe\s*[#]?\d|Employee[- ]?\d|Detective\s*\d|Victim\s*[#]?\d', re.IGNORECASE)


# Very common non-entity strings that show up in automated extraction pipelines.
# We block these from fuzzy matching AND from creating new canonical entities.
NOISE_SUBSTRINGS = [
    "unknown person", "unknown company", "unknown organization",
    "unidentified", "unnamed", "redacted", "sealed",
    "various ", "multiple ", "participants", "attendees",
    "author", "narrator", "reporter",
    "plaintiff", "defendant", "witness", "victim", "employee",
    "the government", "the court", "prosecution", "defense counsel",
    "security clearance", "technology industry",
]

NOISE_REGEX = [
    re.compile(r"^unknown(\s+(person|individual|man|woman|company|organization))?$", re.IGNORECASE),
    re.compile(r"^(unidentified|unnamed)\b", re.IGNORECASE),
    re.compile(r"^(various|multiple)\b", re.IGNORECASE),
    re.compile(r"^(employee|victim|witness)\s*[#]?\d+\b", re.IGNORECASE),
    re.compile(r"^(john|jane)\s+doe\s*[#]?\d+\b", re.IGNORECASE),
    re.compile(r"^\(b\)\(\d+\)", re.IGNORECASE),
]


def looks_like_non_entity(name: str) -> bool:
    """Heuristics for strings that are obviously not names."""
    if not name:
        return True
    s = name.strip()
    if not s:
        return True
    if "\n" in s or "\t" in s:
        return True
    if "http://" in s.lower() or "https://" in s.lower():
        return True
    if "@" in s and "." in s:
        return True
    # Too long or too many words tends to be a sentence fragment, not a name.
    if len(s) > 90 or len(s.split()) > 7:
        return True
    # No alphabetic characters? Not a person/org/location.
    alpha = sum(1 for c in s if c.isalpha())
    if alpha == 0:
        return True
    # Very low alphabetic ratio suggests it's mostly an ID blob.
    if len(s) >= 25 and (alpha / len(s)) < 0.35:
        return True
    return False


def is_noise_entity_name(name: str) -> bool:
    """Returns True if the string is too generic / non-entity to be treated as a real entity."""
    if looks_like_non_entity(name):
        return True
    lowered = normalize_name(name).lower().strip()
    if not lowered:
        return True
    if any(sub in lowered for sub in NOISE_SUBSTRINGS):
        return True
    return any(rx.search(lowered) for rx in NOISE_REGEX)


def normalize_name(name: str) -> str:
    """Normalize a name for better matching.

    Handles:
        - "LAST, FIRST MIDDLE" → "FIRST LAST"
        - Strip punctuation, extra whitespace
        - Case normalization
        - Remove common suffixes (Esq., Jr., Sr., III, etc.)
    """
    if not name:
        return ""

    s = name.strip()

    # Remove common suffixes
    for suffix in [", Esq.", " Esq.", ", Jr.", " Jr.", ", Sr.", " Sr.",
                   ", III", " III", ", II", " II", ", IV", " IV",
                   ", M.D.", " M.D.", ", Ph.D.", " Ph.D.", ", J.D.", " J.D."]:
        if s.endswith(suffix):
            s = s[:-len(suffix)].strip()

    # Handle "LAST, FIRST" or "LAST, FIRST MIDDLE" format
    if "," in s:
        parts = s.split(",", 1)
        if len(parts) == 2:
            last = parts[0].strip()
            first_middle = parts[1].strip()
            if first_middle and last:
                s = f"{first_middle} {last}"

    # Remove parenthetical notes like "(Pilot)" or "(Attorney)"
    s = re.sub(r'\s*\([^)]*\)\s*', ' ', s)

    # Normalize whitespace
    s = ' '.join(s.split())

    return s


def get_short_name(name: str) -> str:
    """Extract just first and last name (drop middle names).

    "Jeffrey Edward Epstein" → "Jeffrey Epstein"
    "Ghislaine Noelle Marion Maxwell" → "Ghislaine Maxwell"
    """
    normalized = normalize_name(name)
    parts = normalized.split()
    if len(parts) <= 2:
        return normalized
    # Assume first + last
    return f"{parts[0]} {parts[-1]}"


def is_numbered_reference(name: str) -> bool:
    """Check if name is a numbered reference (Jane Doe #1, Employee-2, etc.)"""
    return bool(NUMBERED_PATTERN.search(name))


class EntityResolver:
    """Resolves entity names against a canonical registry.

    Args:
        registry: dict of canonical_id -> {canonical_name, aliases, entity_type, metadata}
        fuzzy_threshold: minimum score (0-100) for fuzzy matches to be accepted
    """

    def __init__(self, registry: dict, fuzzy_threshold: int = 90):
        self.registry = registry
        self.fuzzy_threshold = fuzzy_threshold

        # Build lookup structures for fast matching
        # name_lower -> canonical_id (for exact matches — includes raw AND normalized forms)
        self.exact_lookup = {}
        # list of (name, canonical_id) for fuzzy matching
        self.all_names = []

        for cid, entity in registry.items():
            cname = entity["canonical_name"]
            self._index_name(cname, cid)

            for alias in entity.get("aliases", []):
                if alias:
                    self._index_name(alias, cid)

        # Precompute the name list for rapidfuzz
        self._name_strings = [n[0] for n in self.all_names]

    def _index_name(self, name: str, canonical_id: str) -> None:
        """Index a name in all lookup structures, including normalized forms."""
        # Raw form
        self.exact_lookup[name.lower()] = canonical_id
        self.all_names.append((name, canonical_id))

        # Normalized form
        norm = normalize_name(name).lower()
        if norm and norm != name.lower():
            self.exact_lookup[norm] = canonical_id
            self.all_names.append((normalize_name(name), canonical_id))

        # Short form (first + last only)
        short = get_short_name(name).lower()
        if short and short != name.lower() and short != norm:
            self.exact_lookup[short] = canonical_id
            self.all_names.append((get_short_name(name), canonical_id))

    def resolve(self, source_name: str) -> Tuple[Optional[str], str, float]:
        """Resolve a source entity name against the canonical registry.

        Args:
            source_name: the name as it appears in the source system

        Returns:
            (canonical_id, match_method, confidence)
            If no match: (None, 'no_match', 0.0)
        """
        if not source_name or not source_name.strip():
            return None, "no_match", 0.0

        cleaned = source_name.strip()

        # Block obvious junk from matching/creation.
        if is_noise_entity_name(cleaned):
            return None, "noise", 0.0

        # --- Tier 1: Exact match (raw, normalized, and short forms) ---
        for form in [cleaned.lower(), normalize_name(cleaned).lower(),
                     get_short_name(cleaned).lower()]:
            if form in self.exact_lookup:
                cid = self.exact_lookup[form]
                canonical_name = self.registry[cid]["canonical_name"]
                if form == canonical_name.lower():
                    return cid, "exact", 1.0
                else:
                    return cid, "alias", 0.95

        # --- Tier 2: Fuzzy match ---
        # Block fuzzy matching for numbered references (Jane Doe #1 ≠ Jane Doe #2)
        if is_numbered_reference(cleaned):
            return None, "no_match", 0.0

        if not self._name_strings:
            return None, "no_match", 0.0

        # Use normalized form for fuzzy matching
        normalized = normalize_name(cleaned)

        result = process.extractOne(
            normalized,
            self._name_strings,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=self.fuzzy_threshold
        )

        if result:
            matched_name, score, idx = result
            matched_cid = self.all_names[idx][1]

            # Extra validation for short names (≤ 10 chars) — require higher threshold
            # This prevents "John Perry" → "John Kerry" type false positives
            if len(normalized) <= 10 and score < 95:
                return None, "no_match", 0.0

            return matched_cid, "fuzzy", score / 100.0

        # --- No match ---
        return None, "no_match", 0.0

    def resolve_batch(self, names: list) -> list:
        """Resolve a list of names. Returns list of (name, canonical_id, method, confidence)."""
        results = []
        for name in names:
            cid, method, conf = self.resolve(name)
            results.append((name, cid, method, conf))
        return results

    def add_to_registry(self, canonical_id: str, canonical_name: str,
                        aliases: list = None, entity_type: str = "person",
                        metadata: dict = None) -> None:
        """Add a new entity to the in-memory registry (for resolving subsequent names).

        Note: This does NOT write to the database — the caller must do that separately.
        """
        self.registry[canonical_id] = {
            "canonical_name": canonical_name,
            "aliases": aliases or [],
            "entity_type": entity_type,
            "metadata": metadata or {},
        }

        # Update lookup structures using normalized indexing
        self._index_name(canonical_name, canonical_id)
        self._name_strings = [n[0] for n in self.all_names]

        for alias in (aliases or []):
            if alias:
                self._index_name(alias, canonical_id)
        self._name_strings = [n[0] for n in self.all_names]

    def merge_aliases(self, canonical_id: str, new_aliases: list) -> list:
        """Merge new aliases into an existing entity's alias list. Returns the merged list.

        Only adds aliases not already present (case-insensitive).
        """
        if canonical_id not in self.registry:
            return new_aliases

        existing = self.registry[canonical_id].get("aliases", [])
        existing_lower = {a.lower() for a in existing}
        cname_lower = self.registry[canonical_id]["canonical_name"].lower()
        existing_lower.add(cname_lower)

        added = []
        for alias in new_aliases:
            if alias and alias.lower() not in existing_lower:
                existing.append(alias)
                existing_lower.add(alias.lower())
                added.append(alias)

                # Also update the lookup structures
                self.exact_lookup[alias.lower()] = canonical_id
                self.all_names.append((alias, canonical_id))
                self._name_strings.append(alias)

        self.registry[canonical_id]["aliases"] = existing
        return existing


def is_redaction_marker(name: str) -> bool:
    """Check if a name is a FOIA redaction marker rather than a real entity.

    Common patterns: (b)(6), (b)(7)(C), [REDACTED], etc.
    """
    if not name:
        return True
    cleaned = name.strip().lower()
    # Redaction codes
    if cleaned.startswith("(b)") or cleaned.startswith("(b)("):
        return True
    if "[redacted]" in cleaned or "[sealed]" in cleaned:
        return True
    if cleaned in ("unknown", "unidentified", "n/a", "redacted", "sealed"):
        return True
    # Very short names that are likely garbage
    if len(cleaned) <= 1:
        return True
    return False
