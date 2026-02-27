# ECARE Source Catalog

Last updated: 2026-02-25

## Tier 1 Sources

### 1. rhowardstone/Epstein-research-data

**Repository:** https://github.com/rhowardstone/Epstein-research-data
**Cloned:** 2026-02-25
**Companion site:** https://epstein-data.com

#### Key Files

| File | Records | Description |
|------|---------|-------------|
| `persons_registry.json` | 1,614 persons | Unified person registry merged from 6 sources. Fields: name, slug, aliases (sparse — only 222/1614 have aliases), category, search_terms, sources. |
| `knowledge_graph_entities.json` | 606 entities | Curated entities: 571 persons, 12 shell companies, 9 organizations, 7 properties, 4 aircraft, 3 locations. Fields: id, name, entity_type, source_id, source_table, aliases, metadata (JSON with occupation, legal_status, person_type, ds10_mention_count), created_at. |
| `knowledge_graph_relationships.json` | 2,302 relationships | Typed, weighted edges. Fields: id, source_entity_id, target_entity_id, relationship_type, weight, date_first, date_last, metadata (JSON with evidence, EFTA refs, notes). |
| `extracted_entities_filtered.json` | 8,081 entities | NER output filtered from 107K raw: 3,881 names, 2,238 phones, 1,489 amounts, 357 emails, 116 orgs. Each entry includes EFTA document numbers. |
| `extracted_names_multi_doc.csv` | 3,881 names | Same as names in extracted_entities but CSV format. |
| `efta_dataset_mapping.json` | 12 datasets | Maps EFTA number ranges to DOJ download URLs. |
| `efta_dataset_mapping.csv` | Same | CSV version. |
| `la-rana-chicana-list_2-11-26_10am.csv` | 266 persons | Name, description, involvement summary. Third-party compiled list. |
| `document_summary.csv.gz` | 519,438 docs | Per-document redaction summary for every EFTA. |
| `image_catalog.csv.gz` | 38,955 images | Images with people, text, objects, settings identified. |
| `image_catalog_notable.json.gz` | 38,864 images | Notable images subset. |
| `reconstructed_pages_high_interest.json.gz` | 39,588 pages | Pages with recovered text from under redactions. |

#### Category Distribution (persons_registry)

| Category | Count |
|----------|-------|
| associate | 747 |
| other | 338 |
| business | 174 |
| celebrity | 85 |
| academic | 61 |
| politician | 54 |
| legal | 44 |
| socialite | 34 |
| staff | 28 |
| political | 25 |
| royalty | 9 |
| financial | 4 |
| perpetrator | 3 |
| enabler | 3 |
| intelligence | 2 |
| media | 2 |
| victim | 1 |

#### Source Systems (persons_registry)

| Source | Persons |
|--------|---------|
| epstein-pipeline | 1,195 |
| knowledge-graph | 285 |
| la-rana-chicana | 237 |
| wikipedia-epstein-list | 45 |
| bondi-pep-letter-2026 | 19 |
| jmail-world | 9 |
| corpus-investigation | 2 |
| khanna-massie-2026 | 2 |
| doj-release-2026 | 1 |

#### Relationship Types (knowledge_graph)

| Type | Count |
|------|-------|
| traveled_with | 1,449 |
| associated_with | 589 |
| communicated_with | 215 |
| owned_by | 23 |
| victim_of | 13 |
| employed_by | 7 |
| represented_by | 3 |
| paid_by | 1 |
| recruited_by | 1 |
| related_to | 1 |

#### Identifiers
- **Entities:** Sequential integer IDs (1-606) in knowledge graph
- **Persons:** No formal IDs in registry; slug serves as key
- **Documents:** EFTA numbers (e.g., "EFTA02731023")

#### Notes
- Knowledge graph uses integer entity IDs that map to names. Relationships reference source_entity_id/target_entity_id.
- Financial transactions are embedded in relationship metadata, not a separate file. ~16 relationships have financial detail in metadata.
- The 186 transactions / $755M figure from project plan likely comes from the full pipeline database (v3.0 release SQLite), not the JSON exports.
- Full-text corpus (v3.0 release, ~4GB) not downloaded — requires separate LFS/release download. Would be needed for document coverage analysis (Phase 2).

---

### 2. maxandrews/Epstein-doc-explorer

**Repository:** https://github.com/maxandrews/Epstein-doc-explorer
**Cloned:** 2026-02-25
**Status:** ⚠️ Main SQLite database (279MB) is behind Git LFS and could not be downloaded (proxy blocks github-cloud.githubusercontent.com). Schema documented from TypeScript source.

#### Database Schema (document_analysis.db — NOT YET DOWNLOADED)

**Table: rdf_triples**
- Fields: actor, action, target, location, timestamp, doc_id
- Contains Claude-extracted subject-action-object triples from documents

**Table: entity_aliases**
- Fields: original_name, canonical_name, hop_distance_from_principal
- Entity deduplication mapping

**Table: canonical_entities**
- Fields: canonical_name (PK), hop_distance_from_principal, created_at
- Derived from entity_aliases, unique canonical entries with network distance from Epstein

#### Available Data (without LFS)

| File | Records | Description |
|------|---------|-------------|
| `tag_clusters.json` | 30 clusters | Topic/theme clusters (e.g., "Coercion", "Financial") with associated tag lists |
| `data/new_docs_nov2024/tranche_*.csv` | ~2M lines across 24 files | Raw document text in filename,text format |
| `analysis_pipeline/extracted/documents.csv` | 1,576 docs | Document ID ranges mapped to filenames |
| `analysis_pipeline/extracted/images.csv` | 33,296 images | Image extraction metadata |
| `community_schema.sql` | — | Community edit/vote schema (edit_proposals, edit_votes, edit_comments) |

#### Notes
- **BLOCKER:** The core value (RDF triples, entity deduplication, relationship clusters) is in the LFS database. Must be downloaded locally: `cd data/raw/doc-explorer && git lfs pull`
- The tranche CSVs contain raw OCR text, not structured entity/relationship data.
- Hop distance from principal is a useful metric — entities at hop 1 are directly connected to Epstein, hop 2 are one degree removed, etc.

---

### 3. epstein-docs/epstein-docs.github.io

**Repository:** https://github.com/epstein-docs/epstein-docs.github.io
**Cloned:** 2026-02-25

#### Key Files

| File | Records | Description |
|------|---------|-------------|
| `analyses.json` | 8,186 documents | Per-document analyses with key_people (name + role), key_topics, document_type, significance, summary. 7,701 docs have key_people. 19,353 total people mentions across 4,136 unique raw names. |
| `dedupe.json` | 19,845 mappings | Entity deduplication: 11,299 person variants → 8,953 canonical; 5,590 org variants → 4,313; 2,956 location variants → 2,227 |
| `dedupe_types.json` | 1,116 mappings | Document type normalization (e.g., "court document" → "Court Filing") |

#### Analysis Object Schema

Each document in `analyses.json` has:
```json
{
  "document_id": "DOJ-OGR-XXXXXXXX",
  "document_number": "...",
  "page_count": N,
  "analysis": {
    "document_type": "Court Filing",
    "key_people": [
      {"name": "Jeffrey Epstein", "role": "Defendant"}
    ],
    "key_topics": ["..."],
    "significance": "...",
    "summary": "..."
  }
}
```

#### Notes
- **No explicit relationships.** Relationships must be derived from co-occurrence: people appearing in the same document analysis are implicitly connected.
- Document IDs use DOJ-OGR format, not EFTA numbers. Will need EFTA ↔ DOJ-OGR mapping (may be derivable from rhowardstone's document data).
- The dedupe mapping is the most comprehensive person name resolution of the three sources (8,953 unique persons vs. rhowardstone's 1,614 in registry / 606 in knowledge graph).
- Roles in key_people (Defendant, Attorney, Witness, etc.) provide entity metadata not available in other sources.

---

## Cross-Source Comparison Matrix

| Feature | rhowardstone | doc-explorer | epstein-docs |
|---------|-------------|--------------|--------------|
| **Person entities** | 1,614 (registry) / 606 (KG) | Unknown (in LFS DB) | 8,953 (deduplicated) |
| **Organizations** | 12 shell cos + 9 orgs (KG) / 116 (NER) | Unknown | 4,313 (deduplicated) |
| **Locations** | 3 (KG) | Unknown | 2,227 (deduplicated) |
| **Explicit relationships** | 2,302 (typed, weighted) | Yes (RDF triples — LFS) | None (co-occurrence only) |
| **Relationship types** | traveled_with, associated_with, communicated_with, owned_by, victim_of, employed_by, represented_by, paid_by, recruited_by, related_to | actor-action-target | N/A |
| **Entity dedup** | 222 persons have aliases | entity_aliases table | 11,299 → 8,953 mapping |
| **Document coverage** | 519,438 EFTA docs summarized | ~2M lines raw text | 8,186 docs analyzed |
| **Document ID format** | EFTA number | DOJ-OGR (+ filename) | DOJ-OGR |
| **Financial data** | Embedded in relationship metadata | Unknown | No |
| **Network metrics** | Mention counts in KG metadata | hop_distance_from_principal | No |
| **Categories/roles** | 17 category types | 30 tag clusters | Per-document roles |
| **Data format** | JSON, CSV | SQLite (LFS), CSV, JSON | JSON |

## Key Observations

1. **rhowardstone is the backbone.** It has the only structured, typed, weighted relationship graph. It also has the EFTA numbering system that serves as universal key. The persons_registry is the natural base for entity resolution.

2. **epstein-docs has the broadest person coverage** (8,953 deduplicated persons vs 1,614) but no relationship data. Its main value is extending the entity list and providing per-document role metadata.

3. **doc-explorer has potentially the richest relationship data** (RDF triples with actor-action-target structure) but is blocked behind LFS. This is priority to resolve.

4. **Document ID mismatch:** rhowardstone uses EFTA numbers; epstein-docs and doc-explorer use DOJ-OGR numbers. Bridging this gap is essential for cross-referencing.

5. **Entity resolution complexity:** rhowardstone's persons_registry has sparse aliases (only 222/1614). epstein-docs' dedupe mapping is far richer for name variants. Merging these two dedup systems will be a key Phase 1 task.

## Blocking Issues

- [ ] **doc-explorer LFS database:** Must be downloaded outside this environment. Run `cd data/raw/doc-explorer && git lfs pull` locally.
- [ ] **EFTA ↔ DOJ-OGR mapping:** Need to establish how EFTA numbers correspond to DOJ-OGR document IDs. Check if rhowardstone's efta_dataset_mapping or document_summary provides this.
- [ ] **rhowardstone full-text corpus:** The v3.0 release SQLite (~4GB) is needed for document coverage analysis but not for Phase 1 entity resolution.
