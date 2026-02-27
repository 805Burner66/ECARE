# ECARE Methodology

> **Important:** Inclusion in this database does not imply guilt or wrongdoing. This is a research tool based on publicly available government documents released under the Epstein Files Transparency Act (Public Law 118-299).

## Overview

ECARE reconciles entity and relationship data from three independent research platforms into a unified knowledge graph. This document explains every methodological decision — what was done, why, and what the limitations are.

**Pipeline build date:** February 26, 2026
**Data sources accessed:** February 25, 2026
**Pipeline version:** 1.2 (evidence classes, corpus integration, entity cleanup)

---

## Phase 0: Source Assessment

### Source Selection

Three Tier 1 sources were selected based on: public availability, structured data formats, active maintenance, and complementary coverage of the DOJ Epstein file releases (194.5 GB, 1.38M+ documents across 12 datasets + supplementary releases).

| Source | Strengths | Limitations |
|--------|-----------|-------------|
| rhowardstone | Only curated typed relationship graph; EFTA standard; person registry merged from 6 upstream sources | Smaller entity count (1,614 persons); knowledge graph covers only 524 of those |
| epstein-docs | Broadest person coverage (8,953); good dedup mapping (11,299 variants); per-document role metadata | No explicit relationships; document IDs incompatible with EFTA |
| doc-explorer | Largest raw data (107K RDF triples, 26K entity aliases); hop distance from Epstein | LFS-gated; RDF triples include unrelated documents from DOJ dump; noisier entities |

Four Tier 2 sources (EpsteinExposed, Epstein Transparency Project, EpsteinWeb, Epstein Wiki) were deferred. The pipeline is designed to be additive — new sources can be integrated without rebuilding.

### Document ID Incompatibility

A critical finding: the three sources use incompatible document identifiers.

- rhowardstone: EFTA numbers (e.g., `EFTA00016836`) — the standard established by the DOJ release
- doc-explorer: DOJ-OGR numbers (e.g., `DOJ-OGR-0099`) — a different numbering from the original government review
- epstein-docs: ad-hoc IDs derived from filenames/case numbers (e.g., `1:23-cv-01234`)

**Decision:** Entity resolution must rely on name matching rather than document-level cross-referencing. This is a known limitation.

---

## Phase 1: Entity Resolution

### Canonical Base

rhowardstone's `persons_registry.json` (1,614 persons, merged from 6 upstream sources) was used as the canonical base because:
- It is the most curated source (human-reviewed categories, search terms, source attribution)
- It establishes the EFTA numbering convention understood by the broader community
- It includes alias lists already merged from multiple upstream sources
- 3 entries were redaction markers (`(b)(6)`, `(b)(7)(C)` patterns) and were excluded

### Name Normalization

Before matching, all names are normalized:
- Strip leading/trailing whitespace
- Convert "LAST, FIRST MIDDLE" to "FIRST LAST" (handles government document formatting)
- Remove parenthetical suffixes ("John Smith (deceased)" → "John Smith")
- Remove common suffixes (Jr., Sr., III, Esq., M.D., Ph.D.)
- Normalize Unicode (accented characters preserved but standardized)
- Generate a "short form" for matching (first initial + last name)

**Rationale:** Government documents frequently format names as "LAST, FIRST" while research platforms use "FIRST LAST". Without normalization, these would not match on exact comparison.

### Matching Hierarchy

For each source entity name, resolution proceeds through these tiers:

1. **Exact match on canonical_name** (case-insensitive) → confidence 1.0
2. **Exact match on any alias** (case-insensitive, including normalized forms) → confidence 0.95
3. **Fuzzy match** via rapidfuzz `token_sort_ratio` → confidence = score/100

   `token_sort_ratio` was chosen over plain `ratio` because names appear in different orderings across sources. It tokenizes the name, sorts alphabetically, then computes similarity, so "Black, Leon" and "Leon Black" score 100.

4. **No match** → create new canonical entity

### Fuzzy Match Thresholds

| Condition | Threshold | Rationale |
|-----------|-----------|-----------|
| Standard names (> 10 chars) | ≥ 90 | High enough to avoid false positives; we'd rather create a duplicate than incorrectly merge two people |
| Short names (≤ 10 chars) | ≥ 95 | Short names have much higher collision risk ("John Smith" vs "Joan Smith") |
| Numbered references | Blocked | "Jane Doe #1" must never fuzzy-match "Jane Doe #2" — these are distinct legal identities |

**All fuzzy matches are logged** with their scores and exported to `fuzzy_matches_review.csv` for manual verification. ~280 fuzzy matches remain after post-ingestion cleanup merges, of which an unknown proportion are false positives pending human review.

### Resolution Results by Source

| Source | Exact | Alias | Fuzzy | New Entities | Total |
|--------|-------|-------|-------|-------------|-------|
| rhowardstone (registry) | — | — | — | 1,611 (base) | 1,611 |
| rhowardstone (KG) | 411 | 31 | 7 | 157 | 606 |
| epstein-docs (people) | 743 | 128 | 162 | 7,916 | 8,949 |
| epstein-docs (orgs) | 43 | 333 | 91 | 3,846 | 4,313 |
| epstein-docs (locations) | 26 | 42 | 29 | 2,129 | 2,226 |
| doc-explorer | 1,167 | 3,799 | 797 | 21,091 | 26,854 |
| **Total** | **2,390** | **4,333** | **1,086** | **36,750** | **44,559** |

### Known Entity Resolution Issues

1. **doc-explorer noise:** ~21K new entities from doc-explorer include many non-person references like "Unknown person A", "Author", "narrator", "the victim". These inflate the entity count but don't affect relationship analysis (they have no connections).

2. **Entity type misclassification:** Some organizations were classified as persons by source systems (e.g., "Bank of America Merrill Lynch"). These appear in the person table but are functionally organizations.

3. **Key entity unification successes:**
   - "Jeffrey Epstein" + "EPSTEIN, JEFFREY EDWARD" + 12 other variants → single canonical entry (1,066 connections)
   - "Ghislaine Maxwell" + "MAXWELL, GHISLAINE NOELLE" + variants → single entry (592 connections)
   - Name normalization ("LAST, FIRST" → "FIRST LAST") was critical for these merges

---

## Post-Ingestion Entity Cleanup

After all three sources are ingested but before analysis runs, `merge_entities.py` performs three cleanup passes that catch entity resolution failures the fuzzy matcher can't handle.

### Pass 1: Noise Entity Handling

**Low-prominence noise** (≤50 relationships) is deleted outright: standalone first names ("Mary", "David"), generic role words ("journalist", "applicant"), question-mark placeholders ("Roger ?"), initials ("L.M."), and case-specific junk ("Jeffrey Epstein appellate case").

**High-prominence noise** (>50 relationships) is flagged with `metadata.exclude_from_analysis = true` rather than deleted. Entities like "Federal prosecutors" (58 relationships) are too entangled to safely remove — deleting them would orphan dozens of relationship records. The flag keeps them out of gap analysis, community bridge detection, and priority rankings while preserving graph integrity.

**Rationale for the threshold:** 50 was chosen empirically. Below 50, the entity is peripheral enough that deletion is clean. Above 50, the cascade of orphaned relationships and broken provenance chains outweighs the noise reduction benefit.

### Pass 2: Duplicate Merging

Two-pass detection catches duplicates the fuzzy matcher (token_sort_ratio ≥ 90) misses:

**Cleaned-name matching:** Both names are aggressively normalized — strip titles/honorifics, normalize ALL-CAPS to title case, remove hyphens, strip middle initials, fix stutters ("Nadia Nadia Marcinkova") — and compared. This catches: "Professor Alan Dershowitz" ↔ "Alan Dershowitz", "Jean-Luc Brunel" ↔ "Jean Luc Brunel", "MR. LARRY VISOSKI" ↔ "Larry Visoski", "Jack A. Goldberger" ↔ "Mr. Jack Goldberger".

**Last-name-only matching:** Entities that reduce to a single word after title stripping ("Mr. Cassell" → "cassell", "President Clinton" → "clinton") are matched to full-name entities by last name. If unambiguous (exactly one candidate), the merge proceeds. If ambiguous (multiple candidates), Jaccard graph overlap between the absorbed entity's and each candidate's neighborhoods is used for disambiguation, requiring the best candidate to have ≥5% overlap and ≥1.5× the runner-up.

For every merge:
- The higher-prominence entity survives; the absorbed entity becomes an alias
- All FK references (relationships, resolution log) are repointed
- Metadata dicts are merged: max for numeric fields (corpus counts), union for lists (search terms), inherit for missing fields
- Duplicate relationships (same pair + type after repointing) are consolidated
- Self-referential relationships (entity linked to itself post-merge) are removed
- Everything is logged in `entity_merges` audit table

### Pass 3: Name Cleanup

Non-destructive cosmetic fixes on surviving entities: strip titles from canonical_name, normalize ALL-CAPS to title case, fix stuttered names. The old name is preserved as an alias.

### Impact

| Metric | Before Cleanup | After Cleanup |
|--------|---------------|---------------|
| Entities | 19,460 | 18,826 |
| Relationships | 29,713 | 28,083 |
| Multi-source relationships | 1,691 | 3,093 |
| Noise in top-200 priorities | 56 | 18 |

The multi-source jump from 1,691 to 3,093 is the biggest analytical win — merging duplicate entities consolidated their evidence, so relationships that previously appeared single-source now correctly show cross-platform corroboration.

---

## Evidence Classification

Not all evidence is equal. A curated, human-reviewed relationship from rhowardstone carries more weight than two names co-occurring in the same document. The pipeline assigns an `evidence_class` to every entry in `relationship_sources`:

| Class | Weight | Source | Meaning |
|-------|--------|--------|---------|
| `curated` | 1.5× | rhowardstone | Human-reviewed typed relationships from knowledge graph |
| `rdf` | 1.0× | doc-explorer | LLM-extracted subject-action-object triples |
| `corpus_cooccurrence` | 0.9× | full-text corpus | Names co-occurring within FTS5 snippet windows |
| `cooccurrence` | 0.5× | epstein-docs | Names extracted from the same document |

Weights are applied during corroboration scoring (v2). A `curated` + `rdf` confirmation scores higher than three `cooccurrence` hits.

**Rationale:** Co-occurrence in a document is weak evidence — two names appearing in the same legal filing doesn't mean they have a relationship. RDF triples capture explicit actions ("traveled with", "paid") and are stronger. Curated relationships from rhowardstone's knowledge graph have been human-reviewed and represent the highest confidence.

---

## Corpus Integration

The full-text corpus from rhowardstone (1.39M documents, 6 GB FTS5 index) is used to enrich entity metadata after ingestion:

1. For each canonical person entity, search the FTS5 index using the canonical name and aliases
2. Record total document count, matching snippet samples, and search terms that produced hits
3. Inject corpus-derived co-occurrence relationships for entity pairs that appear within the same snippet window but have no existing relationship

This adds a `corpus_document_count` field to entity metadata and creates `corpus_cooccurrence`-class relationships. These corpus mentions are a key input to the research priority scoring — entities mentioned in thousands of documents but with few graph relationships are flagged as under-investigated.

---

## Phase 2: Cross-Source Analysis

### Corroboration Scoring (v2)

**Purpose:** Measure how many independent sources confirm each relationship, weighted by evidence quality.

**Scoring formula (v2):**

The base score uses the same tiered structure as v1, but applies evidence class weights:

| Condition | Base Score |
|-----------|-----------|
| 3+ source systems AND 5+ source documents | 1.0 |
| 2+ source systems AND 2+ documents | 0.8 |
| 2+ source systems (any doc count) | 0.6 |
| 1 source system AND 2+ documents | 0.4 |
| 1 source system AND 1 document | 0.2 |

The v2 score multiplies the base by evidence class weights: `curated` (1.5×), `rdf` (1.0×), `corpus_cooccurrence` (0.9×), `cooccurrence` (0.5×). Per-source document counts are extracted from the `source_evidence` JSON field, providing more accurate document counting than v1's aggregate approach.

**Results:** 3,093 relationships have multi-source confirmation (up from 1,691 pre-cleanup due to duplicate entity merges consolidating evidence). 11,333 relationships are weakly corroborated (single-source with minimal documentation).

**Limitation:** Evidence class weighting is a judgment call. The current weights (1.5/1.0/0.9/0.5) reflect a reasonable ordering but haven't been empirically calibrated. Users can adjust `EVIDENCE_CLASS_WEIGHT` in `corroboration.py`.

### Structural Gap Analysis

**Purpose:** Identify expected-but-missing connections using network topology.

**Method 1 — Common Neighbors:**
For every pair of unconnected nodes in the graph, count shared neighbors. Pairs sharing 3+ neighbors are flagged as potential missing connections.

- Restricted to well-connected persons (5+ connections) for performance (O(n²) check), excluding entities flagged with `exclude_from_analysis`
- 500 gaps identified
- Top gap: Trump ↔ Ehud Barak (shared neighbors, no direct relationship documented)

**Method 2 — Community Detection:**
Louvain community detection was run on the relationship graph. Communities were identified. Entities bridging 2+ communities are flagged as structurally important intermediaries. Entities with `exclude_from_analysis` metadata are excluded from both the common-neighbor and bridge analyses.

**Limitation:** A "structural gap" does not mean a connection exists. Many pairs that share neighbors have no actual relationship. These are leads for investigation, not findings.

### Document Coverage Analysis

**Purpose:** For each person, what fraction of documents mentioning them have been analyzed?

**Method:** Matched canonical persons against rhowardstone's `extracted_entities_filtered.json` (3,881 names appearing in 2+ documents), cross-referenced with knowledge graph coverage.

- 257 persons successfully matched
- Coverage ratio = (documents referenced in knowledge graph) / (total documents mentioning person)

**Key finding:** Lesley Groff has 951 document mentions but only 3 are referenced in the knowledge graph (0.3% coverage). Richard Kahn has 425 mentions with 0% coverage.

**Limitation:** This analysis used the extracted entities file, not the full-text corpus FTS5 index. The full-text corpus (6 GB, 1.39M documents) would provide substantially better coverage but requires significant disk space and processing time. Coverage numbers reported here are lower bounds.

### Temporal Analysis

**Purpose:** Identify suspicious timing patterns in relationships and document volumes.

**Method:**
- Extracted dates from relationship metadata (1,531 of 35,268 relationships = 23.8% have date data)
- Extracted document dates from doc-explorer's RDF triples
- Computed monthly document volume and flagged months exceeding 2 standard deviations from mean

**Findings:**
- 34 volume anomalies detected
- January 2015 spike (222 documents) — aligns with Prince Andrew allegations going public
- June 2016 spike (216 documents) — no known triggering event, flagged for investigation

**Limitation:** Date coverage is sparse (23.8%). Many relationships and documents have no date metadata. This analysis captures only patterns visible in the dated subset.

### Contradiction Detection

**Purpose:** Find where sources disagree about the same entity or relationship.

**Method:** For each entity and relationship appearing in 2+ sources, compared: relationship types, entity categories, role classifications.

**Results:** ~2,487 contradictions detected, predominantly low severity (different categorization schemes between sources). 3 medium severity: role mismatches for Maxwell, Brunel, and Epstein across source systems.

**Interpretation:** Most contradictions reflect different categorization schemes rather than factual disagreements. The relationship type conflicts are more meaningful — they suggest the sources captured different aspects of the same connection.

### Research Priority Scoring

**Purpose:** Synthesize all analyses into a single ranked priority list.

**Composite score formula (additive, capped per factor):**

| Component | Max Points | Source |
|-----------|-----------|--------|
| Prominence baseline (log-scaled connection count) | 20 | relationships table |
| Document coverage gap (unanalyzed doc count / 10) | 30 | document_coverage.py |
| Corpus mention disparity (mentioned in many docs, few relationships) | 25 | corpus_integration.py |
| Weakly corroborated relationships (3 × count) | 20 | corroboration.py |
| Structural gap involvement (5 × count) | 25 | gap_analysis.py |
| Community bridge status (3 × communities bridged) | 15 | gap_analysis.py |
| Contradiction involvement (severity-weighted) | 10 | contradictions.py |

Entities flagged with `exclude_from_analysis` are excluded from scoring.

**Top 5 results (v1.2):**
1. Jeffrey Epstein (3,446) — driven by massive corpus mentions and relationship volume
2. Trump (935) — high weak-relationship count and structural gaps
3. Ghislaine Maxwell (764) — corpus mentions + weak relationships + structural gaps
4. Alan Dershowitz (575) — high post-merge prominence + corpus mentions
5. Hillary Clinton (326) — corpus mentions + structural gaps

**Known noise:** Some entities from unrelated documents in the DOJ dump score high (e.g., Edward Snowden, Steve Bannon appear in doc-explorer's RDF triples from documents that happened to be in the same DOJ release). These are real signals from the data — they appear in documents released under the EFTA — but their connections to the Epstein case are often peripheral.

---

## Manual Review Status

### Completed
- Verified top 10 entities' canonical records, resolution logs, and relationships
- Confirmed key entity unifications (Epstein, Maxwell, Black, Wexner, Prince Andrew)
- Validated corroboration scores against manual source inspection
- Quality audit of top 200 research priorities: identified and fixed 12 duplicate pairs, 15+ noise entities
- Two rounds of merge_entities.py cleanup, reducing noise in top 200 from 56 to 18

### Pending
- ~280 fuzzy matches exported to `fuzzy_matches_review.csv` for human verification
- Remaining single-last-name entities ("Farmer", "Aldrich", "Pagliucca") need manual disambiguation
- "Trump" canonical name should be corrected to "Donald Trump" (artifact of title stripping on highest-prominence entry)
- Entity type misclassifications (organizations classified as persons) need correction pass

---

## Reproducibility

- All source data pinned to specific Git commits/releases (rhowardstone v3.0, Feb 2026)
- All code available in this repository
- `run_pipeline.py` executes the complete 13-step pipeline from raw data to final outputs
- Partial re-runs supported: `--analysis-only` (skip ingestion), `--cleanup-only` (skip ingestion, re-run cleanup + analysis)
- `data/raw/` contains unmodified source data; `data/output/` contains all generated files
- Pipeline execution is logged in `pipeline_runs` table with timestamps
- Entity merges logged in `entity_merges` audit table with full details

## Tools and Libraries

| Library | Version | Purpose |
|---------|---------|---------|
| pandas | ≥ 2.1.0 | Dataframe operations, CSV I/O |
| networkx | ≥ 3.2 | Graph algorithms (centrality, community detection, neighbor analysis) |
| rapidfuzz | ≥ 3.5.0 | Fuzzy string matching for entity resolution |
| python-louvain | ≥ 0.16 | Louvain community detection |
| matplotlib | ≥ 3.8.0 | Analysis visualizations |
| sqlite3 | stdlib | Database operations |
