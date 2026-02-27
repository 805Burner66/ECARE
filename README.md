# ECARE — EFTA Cross-Platform Analytical Reconciliation Engine

> **Disclaimer:** Inclusion in this database does not imply guilt or wrongdoing. This is a research tool based on publicly available government documents released under the Epstein Files Transparency Act (Public Law 118-299).

ECARE is a data engineering pipeline that reconciles entities and relationships from multiple independent Epstein research platforms into a unified knowledge graph. It produces a portable SQLite database, reproducible Python pipeline, and analytical outputs designed for handoff to existing research projects.

## What Problem This Solves

The Epstein research ecosystem has 78+ tools and platforms, but they operate in silos. The same person might appear as "Leon Black" in one dataset, "BLACK, LEON" in government documents, and "Mr. Black" in court transcripts — with no cross-reference. Relationships confirmed by one source can't be validated against another.

ECARE bridges this gap. It ingests structured data from three open-source research projects, resolves entities across them, merges their relationship graphs with full provenance, and runs cross-source analytics that no single platform can produce alone.

## What It Produces

**`ecare.db`** — A single SQLite file containing:
- 18,800+ canonical entities with resolved aliases
- 28,000+ relationships with typed edges and confidence scores
- 3,000+ multi-source corroborated relationships
- Full provenance tracking (which sources assert what, with what evidence)
- Entity resolution audit trail (how every source name was mapped)

**Analytical outputs** (CSV + markdown):
- Corroboration rankings — relationships confirmed by multiple independent sources
- Structural gap analysis — expected-but-missing connections in the network
- Document coverage — entities with large volumes of unanalyzed documents
- Temporal anomalies — suspicious timing patterns
- Cross-source contradictions — where sources disagree
- Research priority rankings — composite "where to look next" scoring

## Data Sources

| Source | What It Provides | Entities | Relationships |
|--------|-----------------|----------|---------------|
| [rhowardstone/Epstein-research-data](https://github.com/rhowardstone/Epstein-research-data) | Curated knowledge graph, person registry, financial transactions, EFTA standard | 1,614 persons | 2,302 typed relationships |
| [epstein-docs](https://github.com/epstein-docs/epstein-docs.github.io) | OCR text extraction, entity dedup, co-occurrence data | 8,949 persons + 6,539 orgs/locs | 4,018 co-occurrence relationships |
| [doc-explorer](https://github.com/maxandrews/Epstein-doc-explorer) | Claude-extracted RDF triples, entity aliases, hop distance | 26,854 entities | 23,492 RDF-derived relationships |

All source data is derived from the DOJ Epstein file releases (3.5M+ pages across 12 datasets, Dec 2025–Jan 2026).

## Quick Start

### Prerequisites

- Python 3.10+
- ~2 GB disk space (source data + database)
- Git LFS (for doc-explorer data)

### Setup

```bash
git clone https://github.com/805Burner66/ecare.git
cd ecare
pip install -r requirements.txt
```

### Download Source Data

Place source data in `data/raw/`:

```bash
mkdir -p data/raw

# rhowardstone (primary source)
git clone https://github.com/rhowardstone/Epstein-research-data.git data/raw/rhowardstone

# epstein-docs
git clone https://github.com/epstein-docs/epstein-docs.github.io.git data/raw/epstein-docs

# doc-explorer (requires Git LFS)
git lfs install
git clone https://github.com/maxandrews/Epstein-doc-explorer.git data/raw/doc-explorer
```

### Run the Pipeline

```bash
python run_pipeline.py
```

This executes all 13 steps: database creation → ingestion (3 sources) → validation → entity cleanup → corpus integration → analysis (6 modules). Takes 5–10 minutes depending on hardware.

**Partial runs:**
```bash
python run_pipeline.py --skip-doc-explorer   # Skip if LFS not pulled
python run_pipeline.py --analysis-only       # Re-run analysis on existing DB
python run_pipeline.py --cleanup-only        # Re-run cleanup + analysis
```

### Outputs

All outputs land in `data/output/`:

| File | Description |
|------|-------------|
| `ecare.db` | Unified SQLite database (~80 MB) |
| `corroboration_rankings.csv` | All relationships ranked by cross-source confirmation |
| `weakly_corroborated.csv` | Single-source relationships on prominent entities |
| `gap_analysis_common_neighbors.csv` | Unconnected pairs sharing many mutual connections |
| `community_bridges.csv` | Entities bridging distinct network communities |
| `document_coverage.csv` | Per-entity document analysis coverage |
| `temporal_anomalies.csv` | Unusual timing patterns in document volumes |
| `cross_source_contradictions.csv` | Where sources disagree |
| `relationship_timeline.csv` | Dated relationships for temporal analysis |
| `research_priorities.csv` | Composite ranked priority list |
| `research_priorities_summary.md` | Human-readable top-50 report |
| `fuzzy_matches_review.csv` | Fuzzy entity matches requiring manual review |

## Pipeline Architecture

```
run_pipeline.py
│
├── create_db.py              # Schema initialization
├── ingest_rhowardstone.py    # Curated knowledge graph + person registry
├── ingest_epstein_docs.py    # OCR entity extraction + co-occurrence
├── ingest_doc_explorer.py    # RDF triples + entity aliases
├── validate.py               # Integrity checks + fuzzy match export
├── merge_entities.py         # Post-ingestion cleanup (noise, dupes, names)
├── corpus_integration.py     # Full-text corpus enrichment
├── corroboration.py          # Cross-source relationship scoring
├── gap_analysis.py           # Structural gap + community bridge detection
├── document_coverage.py      # Per-entity document coverage
├── temporal.py               # Date-based anomaly detection
├── contradictions.py         # Cross-source conflict detection
└── prioritize.py             # Composite research priority ranking
```

## Entity Resolution

The pipeline resolves entities through a multi-stage process:

1. **Base registry** — rhowardstone's curated person registry (1,614 entries) serves as the canonical starting point
2. **Name normalization** — strips titles, flips "LAST, FIRST" format, removes suffixes, normalizes unicode
3. **Matching hierarchy** — exact match → alias match → fuzzy match (rapidfuzz token_sort_ratio ≥ 90) → create new entity
4. **Post-ingestion cleanup** (`merge_entities.py`) — catches duplicates the fuzzy matcher misses:
   - Title/honorific variants ("President Clinton" → "Bill Clinton")
   - ALL-CAPS transcript forms ("MR. LARRY VISOSKI" → "Larry Visoski")
   - Hyphen normalization ("Jean-Luc" vs "Jean Luc")
   - Last-name-only disambiguation via graph overlap

Every resolution decision is logged in `entity_resolution_log` with method, confidence score, and source details.

## Evidence Classification

Not all evidence is equal. The pipeline classifies relationship sources by evidence quality:

| Class | Weight | Source | What It Means |
|-------|--------|--------|---------------|
| `curated` | 1.5× | rhowardstone | Human-reviewed typed relationships |
| `rdf` | 1.0× | doc-explorer | LLM-extracted subject-action-object triples |
| `cooccurrence` | 0.5× | epstein-docs | Names appearing in the same document |
| `corpus_cooccurrence` | 0.9× | full-text corpus | Names co-occurring in full-text search |

Corroboration scoring uses these weights, so a curated+RDF confirmation scores higher than two co-occurrence hits.

## Key Design Decisions

**SQLite, not Postgres.** The deliverable needs to be a single file anyone can open and query. No server setup, no credentials, no Docker.

**No web framework.** This is a data product, not a platform. Scripts, databases, CSVs, and markdown. UI is someone else's job.

**Conservative entity resolution.** Fuzzy threshold of 90 means we'd rather create a duplicate than incorrectly merge two different people. The `fuzzy_matches_review.csv` export lets humans verify edge cases.

**Evidence provenance on every claim.** Every relationship traces back to which source systems asserted it, what documents they cited, and how confident the assertion is. Nothing is anonymous.

**Noise flagging over deletion.** High-prominence noise entities ("Federal prosecutors", "investigation") are flagged with `metadata.exclude_from_analysis = true` rather than deleted, preserving graph integrity while keeping them out of analytical outputs.

## Adding New Sources

The pipeline is designed to be additive. To add a new source:

1. Create `src/ingest/ingest_newsource.py` following the pattern of existing ingest scripts
2. Use `resolve_or_create_entity()` from `resolve_persons.py` for entity resolution against the canonical base
3. Use `insert_relationship()` + `insert_relationship_source()` from `common.py` for relationships
4. Set an appropriate `evidence_class` on relationship sources
5. Add the step to `run_pipeline.py`'s `INGEST_STEPS` list

Tier 2 sources (EpsteinExposed, Epstein Transparency Project, EpsteinWeb, Epstein Wiki) are natural next additions.

## Querying the Database

Open `ecare.db` with any SQLite client (DB Browser, DBeaver, `sqlite3` CLI, Python):

```sql
-- Find all connections for a person
SELECT r.relationship_type, ce.canonical_name, r.weight, r.confidence_score
FROM relationships r
JOIN canonical_entities ce ON ce.canonical_id = r.target_entity_id
WHERE r.source_entity_id = (
    SELECT canonical_id FROM canonical_entities
    WHERE canonical_name = 'Ghislaine Maxwell'
)
ORDER BY r.weight DESC;

-- Find relationships confirmed by multiple independent sources
SELECT src.canonical_name, tgt.canonical_name, r.relationship_type,
       COUNT(DISTINCT rs.source_system) AS sources
FROM relationships r
JOIN canonical_entities src ON src.canonical_id = r.source_entity_id
JOIN canonical_entities tgt ON tgt.canonical_id = r.target_entity_id
JOIN relationship_sources rs ON rs.relationship_id = r.relationship_id
GROUP BY r.relationship_id
HAVING sources >= 2
ORDER BY sources DESC;

-- Trace how an entity was resolved across sources
SELECT source_system, source_entity_name, match_method, match_confidence
FROM entity_resolution_log
WHERE canonical_id = (
    SELECT canonical_id FROM canonical_entities
    WHERE canonical_name = 'Leon Black'
);
```

See [docs/schema.md](docs/schema.md) for the full database schema and more example queries.

## Documentation

| Document | Contents |
|----------|----------|
| [docs/methodology.md](docs/methodology.md) | Every decision explained: matching thresholds, scoring weights, algorithm choices, known limitations |
| [docs/schema.md](docs/schema.md) | Database schema with column types, indexes, JSON field structures, example queries |
| [docs/source_catalog.md](docs/source_catalog.md) | Detailed inventory of each data source: formats, schemas, entity counts, access methods |

## Limitations

- **Entity resolution is imperfect.** ~280 fuzzy matches need manual review. Some duplicates and noise entities remain.
- **Document ID incompatibility.** The three sources use different document numbering schemes (EFTA, DOJ-OGR, ad-hoc). Cross-referencing at the document level is limited.
- **Temporal data is sparse.** Only ~24% of relationships have date metadata. Temporal analysis captures patterns in the dated subset only.
- **Doc-explorer noise.** ~21K entities from doc-explorer include non-person references from unrelated documents in the DOJ dump. These are filtered from analysis but inflate raw entity counts.
- **No Tier 2 sources yet.** EpsteinExposed (1,700+ profiles), Epstein Transparency Project, and others would significantly enrich the graph.

## Contributing

This project welcomes contributions. Priorities:
- Manual review of `fuzzy_matches_review.csv` (entity resolution verification)
- Tier 2 source integration (especially EpsteinExposed)
- Additional noise entity identification and flagging
- Validation spot-checks on lesser-known entities

## License

MIT. All source data is derived from publicly released government documents.

## Acknowledgments

This project builds on the work of:
- [rhowardstone](https://github.com/rhowardstone/Epstein-research-data) — foundational knowledge graph and EFTA standard
- [epstein-docs](https://github.com/epstein-docs/epstein-docs.github.io) — OCR extraction and entity deduplication
- [maxandrews/doc-explorer](https://github.com/maxandrews/Epstein-doc-explorer) — RDF triple extraction
