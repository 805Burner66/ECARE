# ECARE Database Schema (v1.2)

The unified database (`ecare.db`) is a single SQLite file containing 8 tables. All JSON fields use standard JSON encoding. All dates use ISO 8601 format. The database uses WAL journal mode and enforces foreign keys.

## Table: `canonical_entities`

The single source of truth for all entities in the unified graph.

| Column | Type | Description |
|--------|------|-------------|
| `canonical_id` | TEXT PK | Prefixed ID: `PER-00001`, `ORG-00001`, `LOC-00001`, `AIR-00001`, `PROP-00001` |
| `entity_type` | TEXT NOT NULL | One of: `person`, `organization`, `location`, `aircraft`, `property`, `shell_company` |
| `canonical_name` | TEXT NOT NULL | Primary display name (e.g., "Jeffrey Epstein", "Southern Trust Company") |
| `aliases` | TEXT (JSON) | JSON array of alternate names. Example: `["EPSTEIN, JEFFREY EDWARD", "Jeff Epstein"]` |
| `metadata` | TEXT (JSON) | Structured attributes. See Metadata Schema below. |
| `first_seen_date` | TEXT | Earliest known date (ISO 8601) if available |
| `last_updated` | TEXT | ISO timestamp of last pipeline update |
| `notes` | TEXT | Free-text notes |

**Indexes:** `entity_type`, `canonical_name`

### Metadata Schema (JSON)

Person metadata may include:
```json
{
  "category": "political|business|academic|staff|financial|legal|media|other",
  "registry_sources": ["epstein-pipeline", "knowledge-graph"],
  "search_terms": ["alternate search strings"],
  "occupation": "...",
  "legal_status": "convicted|charged|named|witness|victim|...",
  "person_type": "...",
  "ds10_mention_count": 42,
  "public_figure": true,
  "observed_roles": {"Defendant": 5, "Associate": 12},
  "hop_distance_from_epstein": 2,
  "source_system": "epstein-docs",
  "corpus_document_count": 1500,
  "corpus_search_terms": ["leon black", "l. black"],
  "exclude_from_analysis": false,
  "exclude_reason": "generic_word:federal prosecutors"
}
```

**`exclude_from_analysis`**: When `true`, analysis modules (prioritize, gap_analysis, community_bridges) skip this entity. Set by `merge_entities.py` on noise entities too entangled to delete (>50 relationships).

**`corpus_document_count`**: Number of full-text corpus documents mentioning this entity. Set by `corpus_integration.py`.

## Table: `entity_resolution_log`

Audit trail for every entity matching decision. Every source entity gets exactly one row here, documenting how it was mapped to its canonical entry.

| Column | Type | Description |
|--------|------|-------------|
| `resolution_id` | INTEGER PK | Auto-increment |
| `source_system` | TEXT NOT NULL | `rhowardstone`, `epstein-docs`, or `doc-explorer` |
| `source_entity_id` | TEXT NOT NULL | ID in the source system (e.g., `registry:leon-black`, `kg:42`, `dedupe:Leon Black`) |
| `source_entity_name` | TEXT NOT NULL | Name as it appears in the source |
| `canonical_id` | TEXT NOT NULL | FK → `canonical_entities.canonical_id` |
| `match_method` | TEXT NOT NULL | How the match was made (see below) |
| `match_confidence` | REAL NOT NULL | 0.0 to 1.0 |
| `match_details` | TEXT (JSON) | Why this match was made — source file, context |
| `resolved_by` | TEXT | `pipeline` or `manual_review` |
| `resolved_date` | TEXT NOT NULL | ISO timestamp |

**Match methods:**
- `base_registry` (1.0) — Entity came from rhowardstone persons_registry.json (the canonical base)
- `exact` (1.0) — Case-insensitive exact match on canonical_name
- `alias` (0.95) — Case-insensitive exact match on an alias
- `fuzzy` (0.90–0.99) — rapidfuzz token_sort_ratio above threshold
- `new_entity` (1.0) — No match found; created as new canonical entry
- `manual` (varies) — Human-reviewed match

**Indexes:** `(source_system, source_entity_id)`, `canonical_id`, `match_confidence`

## Table: `relationships`

Unified relationship graph. Each row represents a connection between two canonical entities.

| Column | Type | Description |
|--------|------|-------------|
| `relationship_id` | INTEGER PK | Auto-increment |
| `source_entity_id` | TEXT NOT NULL | FK → `canonical_entities.canonical_id` (the "from") |
| `target_entity_id` | TEXT NOT NULL | FK → `canonical_entities.canonical_id` (the "to") |
| `relationship_type` | TEXT NOT NULL | Typed edge (see below) |
| `relationship_subtype` | TEXT | More specific classification from source |
| `date_start` | TEXT | ISO date or NULL |
| `date_end` | TEXT | ISO date or NULL |
| `weight` | REAL | Connection strength (source-dependent scale) |
| `confidence_score` | REAL | Aggregate confidence (0.0–1.0) |
| `source_documents` | TEXT (JSON) | JSON array of EFTA numbers |
| `notes` | TEXT | Free-text |

**Relationship types:**
- `associated_with` — General association
- `traveled_with` — Co-travel (flights, trips)
- `employed_by` — Employment relationship
- `financial` — Financial transaction or flow
- `represented_by` — Legal representation
- `victim_of` — Victim relationship
- `owned_by` — Property/entity ownership
- `communicated_with` — Communication (calls, emails)
- `co_documented` — Co-occurrence in same document analysis (weaker evidence)

**Indexes:** `source_entity_id`, `target_entity_id`, `relationship_type`, `(source_entity_id, target_entity_id)`

## Table: `relationship_sources`

Provenance tracking. Multiple rows per relationship when confirmed by multiple sources.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `relationship_id` | INTEGER NOT NULL | FK → `relationships.relationship_id` |
| `source_system` | TEXT NOT NULL | `rhowardstone`, `epstein-docs`, `doc-explorer`, or `corpus` |
| `source_relationship_id` | TEXT | ID in source system if available |
| `source_relationship_type` | TEXT | How the source classified it |
| `source_evidence` | TEXT (JSON) | Documents, confidence, notes from source. May include `document_count` and `doc_key_sample`. |
| `source_confidence` | REAL | Source-specific confidence |
| `evidence_class` | TEXT | Evidence quality tier: `curated`, `rdf`, `cooccurrence`, `corpus_cooccurrence` |
| `date_added` | TEXT NOT NULL | ISO timestamp |

**Evidence classes:**
- `curated` — Human-reviewed typed relationships (rhowardstone knowledge graph)
- `rdf` — LLM-extracted subject-action-object triples (doc-explorer)
- `cooccurrence` — Names co-occurring in the same document (epstein-docs)
- `corpus_cooccurrence` — Names co-occurring within FTS5 snippet windows (corpus integration)

**Indexes:** `relationship_id`, `source_system`, `evidence_class`

## Table: `document_ids`

Cross-reference mapping between document identifier schemes.

| Column | Type | Description |
|--------|------|-------------|
| `doc_key` | TEXT PK | Canonical document key (EFTA number preferred, DOJ-OGR fallback, RAW-hash last resort) |
| `efta_number` | TEXT | EFTA number if extractable (e.g., `EFTA00016836`) |
| `doj_ogr_id` | TEXT | DOJ-OGR identifier if present (e.g., `DOJ-OGR-0099`) |
| `source_system` | TEXT | Which source provided this document reference |
| `raw_id` | TEXT | Original identifier as it appeared in source data |
| `confidence` | REAL | Confidence in the ID extraction |
| `notes` | TEXT | Free-text |
| `last_updated` | TEXT | ISO timestamp |

**Indexes:** `efta_number`, `doj_ogr_id`

## Table: `entity_merges`

Audit trail for post-ingestion entity merges and noise deletions.

| Column | Type | Description |
|--------|------|-------------|
| `merge_id` | INTEGER PK | Auto-increment |
| `survivor_id` | TEXT NOT NULL | Canonical ID of the entity that survived (or `NOISE_DELETED` for deletions) |
| `absorbed_id` | TEXT NOT NULL | Canonical ID of the entity that was absorbed/deleted |
| `survivor_name` | TEXT | Display name of survivor at merge time |
| `absorbed_name` | TEXT | Display name of absorbed entity |
| `merge_reason` | TEXT | Why the merge was performed (e.g., `title_prefix`, `lastname_only_unambiguous`, `noise_removal: standalone_first_name:mary`) |
| `match_key` | TEXT | The normalized key that triggered the match |
| `relationships_repointed` | INTEGER | Number of relationships repointed to survivor |
| `resolution_logs_repointed` | INTEGER | Number of resolution log entries repointed |
| `duplicate_rels_consolidated` | INTEGER | Number of duplicate relationships merged after repointing |
| `merged_at` | TEXT NOT NULL | ISO timestamp |

**Indexes:** `survivor_id`, `absorbed_id`

## Table: `conflicts`

Cross-source disagreements flagged for review.

| Column | Type | Description |
|--------|------|-------------|
| `conflict_id` | INTEGER PK | Auto-increment |
| `entity_or_relationship` | TEXT NOT NULL | `entity` or `relationship` |
| `record_id` | TEXT NOT NULL | The canonical_id or relationship_id in question |
| `source_a` | TEXT NOT NULL | First source system |
| `source_b` | TEXT NOT NULL | Second source system |
| `field_in_conflict` | TEXT NOT NULL | Which attribute disagrees |
| `value_a` | TEXT | Value from source A |
| `value_b` | TEXT | Value from source B |
| `nature_of_conflict` | TEXT | Description |
| `resolution_status` | TEXT | `unresolved`, `resolved_a`, `resolved_b`, `resolved_merged` |
| `resolution_notes` | TEXT | How/why it was resolved |
| `flagged_date` | TEXT NOT NULL | ISO timestamp |

**Index:** `resolution_status`

## Table: `pipeline_runs`

Execution metadata for each pipeline step.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | INTEGER PK | Auto-increment |
| `step_name` | TEXT NOT NULL | Script/step identifier |
| `started_at` | TEXT NOT NULL | ISO timestamp |
| `completed_at` | TEXT | ISO timestamp (NULL if still running) |
| `status` | TEXT | `running`, `completed`, `failed` |
| `records_processed` | INTEGER | Count of records handled |
| `notes` | TEXT | Summary of results |

## Common Queries

### Find all connections for a person
```sql
SELECT r.*, ce.canonical_name AS target_name
FROM relationships r
JOIN canonical_entities ce ON ce.canonical_id = r.target_entity_id
WHERE r.source_entity_id = 'PER-00001'
UNION ALL
SELECT r.*, ce.canonical_name AS target_name
FROM relationships r
JOIN canonical_entities ce ON ce.canonical_id = r.source_entity_id
WHERE r.target_entity_id = 'PER-00001';
```

### Find multi-source corroborated relationships
```sql
SELECT r.relationship_id, src.canonical_name, tgt.canonical_name,
       r.relationship_type, COUNT(DISTINCT rs.source_system) AS source_count
FROM relationships r
JOIN canonical_entities src ON src.canonical_id = r.source_entity_id
JOIN canonical_entities tgt ON tgt.canonical_id = r.target_entity_id
JOIN relationship_sources rs ON rs.relationship_id = r.relationship_id
GROUP BY r.relationship_id
HAVING source_count >= 2
ORDER BY source_count DESC;
```

### Trace how an entity was resolved
```sql
SELECT source_system, source_entity_name, match_method, match_confidence
FROM entity_resolution_log
WHERE canonical_id = 'PER-00001'
ORDER BY source_system;
```

### Find unresolved conflicts
```sql
SELECT * FROM conflicts WHERE resolution_status = 'unresolved' ORDER BY flagged_date;
```

### View entity merge history
```sql
SELECT survivor_name, absorbed_name, merge_reason, relationships_repointed
FROM entity_merges
WHERE survivor_id != 'NOISE_DELETED'
ORDER BY relationships_repointed DESC;
```

### Find entities excluded from analysis
```sql
SELECT canonical_id, canonical_name, json_extract(metadata, '$.exclude_reason') AS reason
FROM canonical_entities
WHERE json_extract(metadata, '$.exclude_from_analysis') = 1;
```

### Find relationships with evidence class breakdown
```sql
SELECT src.canonical_name, tgt.canonical_name, r.relationship_type,
       GROUP_CONCAT(DISTINCT rs.evidence_class) AS evidence_types,
       COUNT(DISTINCT rs.source_system) AS source_count
FROM relationships r
JOIN canonical_entities src ON src.canonical_id = r.source_entity_id
JOIN canonical_entities tgt ON tgt.canonical_id = r.target_entity_id
JOIN relationship_sources rs ON rs.relationship_id = r.relationship_id
GROUP BY r.relationship_id
HAVING source_count >= 2
ORDER BY source_count DESC;
```
