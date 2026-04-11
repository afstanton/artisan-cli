use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
};

use artisan_core::{
    CanonicalId, CoreCatalog, InMemoryReconciliationStore, Reconciler, ReconciliationPolicy,
    ResolutionOutcome, SourceRecord,
    reconcile::{MatchCandidate, MatchQuery, ReconciliationStore, SubjectKind},
};
use artisan_pcgen::{ParsedEntityCandidate, PcgenLoader};
use artisan_toml::parse_catalog;
use serde::{Deserialize, Serialize};

#[derive(clap::Args, Debug)]
pub struct ReconcileReviewArgs {
    #[arg(long, value_name = "PCGEN_LST_FILE")]
    pub pcgen_lst: PathBuf,

    #[arg(long, value_name = "PCGEN_PCC_FILE")]
    pub pcgen_pcc: Option<PathBuf>,

    #[arg(long, value_name = "IN_CORE_TOML_FILE")]
    pub from_core_toml: Option<PathBuf>,

    #[arg(long, default_value_t = 5)]
    pub max_suggestions: usize,

    #[arg(long, default_value_t = true)]
    pub use_core_reconciler: bool,

    #[arg(long, value_name = "REVIEW_STATE_JSON")]
    pub state_file: PathBuf,

    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewDecision {
    pub mapped_entity_type_key: Option<String>,
    pub note: Option<String>,
    pub accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewItem {
    pub candidate_key: String,
    pub name: String,
    pub inferred_entity_type_key: String,
    pub suggested_entity_type_key: String,
    #[serde(default)]
    pub game_system_hint: Option<String>,
    #[serde(default)]
    pub source_hint: Option<String>,
    #[serde(default)]
    pub match_candidates: Vec<ReviewMatchCandidate>,
    pub source_format: String,
    pub line_number: Option<u64>,
    pub decision: Option<ReviewDecision>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewMatchCandidate {
    pub canonical_id: String,
    pub name: String,
    pub confidence: f32,
    pub reason: String,
    pub entity_type_key: Option<String>,
    pub source_matched: bool,
    pub game_system_matched: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewState {
    pub schema_version: u32,
    pub source_file: String,
    pub total_candidates: usize,
    pub items: Vec<ReviewItem>,
}

pub fn run(args: ReconcileReviewArgs) -> Result<(), String> {
    let input = fs::read_to_string(&args.pcgen_lst)
        .map_err(|e| format!("failed to read input {}: {e}", args.pcgen_lst.display()))?;

    let (game_system_hint, source_title_hint) = if let Some(path) = &args.pcgen_pcc {
        let pcc_input = fs::read_to_string(path)
            .map_err(|e| format!("failed to read pcc input {}: {e}", path.display()))?;
        let campaign = PcgenLoader::parse_pcc(&pcc_input).map_err(|e| e.to_string())?;
        (
            campaign
                .metadata
                .get("GAMEMODE")
                .and_then(|v| v.first())
                .cloned(),
            campaign
                .metadata
                .get("CAMPAIGN")
                .and_then(|v| v.first())
                .cloned(),
        )
    } else {
        (None, None)
    };

    let candidates = PcgenLoader::parse_entity_candidates_with_context(
        &input,
        game_system_hint.as_deref(),
        source_title_hint.as_deref(),
    )
    .map_err(|e| e.to_string())?;

    let catalog = if let Some(path) = &args.from_core_toml {
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("failed to read core catalog {}: {e}", path.display()))?;
        Some(parse_catalog(&raw).map_err(|e| format!("failed to parse core catalog: {e}"))?)
    } else {
        None
    };

    let existing = if args.state_file.exists() {
        Some(load_state(&args.state_file)?)
    } else {
        None
    };

    let next_state = merge_state(
        &args.pcgen_lst,
        &candidates,
        catalog.as_ref(),
        args.max_suggestions,
        args.use_core_reconciler,
        existing.as_ref(),
    );

    let pending = next_state
        .items
        .iter()
        .filter(|i| i.decision.is_none())
        .count();
    let accepted = next_state
        .items
        .iter()
        .filter(|i| i.decision.as_ref().is_some_and(|d| d.accepted))
        .count();

    if args.dry_run {
        println!("reconcile review (dry-run)");
        println!("  state file: {}", args.state_file.display());
        println!("  total: {}", next_state.items.len());
        println!("  pending: {}", pending);
        println!("  accepted: {}", accepted);
        return Ok(());
    }

    let encoded = serde_json::to_string_pretty(&next_state)
        .map_err(|e| format!("failed to encode review state: {e}"))?;
    fs::write(&args.state_file, encoded).map_err(|e| {
        format!(
            "failed to write review state {}: {e}",
            args.state_file.display()
        )
    })?;

    println!("reconcile review state updated");
    println!("  file: {}", args.state_file.display());
    println!("  total: {}", next_state.items.len());
    println!("  pending: {}", pending);
    println!("  accepted: {}", accepted);

    Ok(())
}

fn load_state(path: &PathBuf) -> Result<ReviewState, String> {
    let raw = fs::read_to_string(path)
        .map_err(|e| format!("failed to read review state {}: {e}", path.display()))?;
    serde_json::from_str(&raw)
        .map_err(|e| format!("failed to parse review state {}: {e}", path.display()))
}

fn merge_state(
    source_file: &PathBuf,
    candidates: &[ParsedEntityCandidate],
    catalog: Option<&CoreCatalog>,
    max_suggestions: usize,
    use_core_reconciler: bool,
    existing: Option<&ReviewState>,
) -> ReviewState {
    let index = catalog.map(CatalogMatchIndex::new);

    let mut existing_map: BTreeMap<String, ReviewItem> = BTreeMap::new();
    if let Some(state) = existing {
        for item in &state.items {
            existing_map.insert(item.candidate_key.clone(), item.clone());
        }
    }

    let mut items = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let key = candidate_key(candidate);
        let source_hint = candidate_source_hint(candidate);
        let game_system_hint = candidate_game_system_hint(candidate);
        let match_candidates = match (catalog, &index, use_core_reconciler) {
            (Some(c), Some(i), true) => {
                suggest_matches_via_core_reconciler(candidate, c, i, max_suggestions)
            }
            (Some(c), Some(i), false) => suggest_matches(candidate, c, i, max_suggestions),
            _ => Vec::new(),
        };
        let suggested_entity_type_key = match_candidates
            .first()
            .and_then(|m| m.entity_type_key.clone())
            .unwrap_or_else(|| candidate.entity_type_key.clone());

        if let Some(prev) = existing_map.get(&key) {
            let mut merged = prev.clone();
            merged.name = candidate.entity.name.clone();
            merged.inferred_entity_type_key = candidate.entity_type_key.clone();
            merged.suggested_entity_type_key = suggested_entity_type_key;
            merged.source_hint = source_hint;
            merged.game_system_hint = game_system_hint;
            merged.match_candidates = match_candidates;
            merged.line_number = line_number(candidate);
            items.push(merged);
            continue;
        }

        items.push(ReviewItem {
            candidate_key: key,
            name: candidate.entity.name.clone(),
            inferred_entity_type_key: candidate.entity_type_key.clone(),
            suggested_entity_type_key,
            game_system_hint,
            source_hint,
            match_candidates,
            source_format: "pcgen".to_string(),
            line_number: line_number(candidate),
            decision: None,
        });
    }

    ReviewState {
        schema_version: 2,
        source_file: source_file.display().to_string(),
        total_candidates: items.len(),
        items,
    }
}

struct CatalogMatchIndex<'a> {
    entity_type_keys: HashMap<CanonicalId, String>,
    citations: HashMap<CanonicalId, &'a artisan_core::CitationRecord>,
    sources: HashMap<CanonicalId, &'a SourceRecord>,
}

impl<'a> CatalogMatchIndex<'a> {
    fn new(catalog: &'a CoreCatalog) -> Self {
        let entity_type_keys = catalog
            .entity_types
            .iter()
            .map(|ty| (ty.id, ty.key.clone()))
            .collect();
        let citations = catalog.citations.iter().map(|c| (c.id, c)).collect();
        let sources = catalog.sources.iter().map(|s| (s.id, s)).collect();

        Self {
            entity_type_keys,
            citations,
            sources,
        }
    }
}

fn suggest_matches(
    candidate: &ParsedEntityCandidate,
    catalog: &CoreCatalog,
    index: &CatalogMatchIndex<'_>,
    max_suggestions: usize,
) -> Vec<ReviewMatchCandidate> {
    let requested_source = candidate_source_hint(candidate);
    let requested_game_system = candidate_game_system_hint(candidate);

    let mut scored = Vec::new();
    for entity in &catalog.entities {
        let mut confidence = 0.0f32;
        let mut reasons = Vec::new();

        let name_score = score_name_similarity(&candidate.entity.name, &entity.name);
        if name_score > 0.0 {
            confidence += name_score;
            reasons.push("name similarity".to_string());
        }

        let entity_type_key = index.entity_type_keys.get(&entity.entity_type).cloned();
        if entity_type_key
            .as_deref()
            .is_some_and(|k| k.eq_ignore_ascii_case(&candidate.entity_type_key))
        {
            confidence += 0.15;
            reasons.push("entity type key match".to_string());
        }

        let mut source_matched = false;
        if let Some(wanted_source) = &requested_source {
            let existing_sources = entity_source_titles(entity, index);
            if existing_sources
                .iter()
                .any(|source| fuzzy_match(wanted_source, source))
            {
                confidence += 0.10;
                source_matched = true;
                reasons.push(format!("source hint match ({wanted_source})"));
            } else if !existing_sources.is_empty() {
                confidence -= 0.05;
                reasons.push(format!("source mismatch ({wanted_source})"));
            }
        }

        let mut game_system_matched = false;
        if let Some(wanted_game) = &requested_game_system {
            let existing_game_systems = entity_game_systems(entity, index);
            if existing_game_systems
                .iter()
                .any(|system| system.eq_ignore_ascii_case(wanted_game))
            {
                confidence += 0.10;
                game_system_matched = true;
                reasons.push(format!("game system match ({wanted_game})"));
            } else if !existing_game_systems.is_empty() {
                confidence -= 0.10;
                reasons.push(format!("game system mismatch ({wanted_game})"));
            }
        }

        let confidence = confidence.clamp(0.0, 1.0);
        if confidence < 0.40 {
            continue;
        }

        scored.push(ReviewMatchCandidate {
            canonical_id: entity.id.0.to_string(),
            name: entity.name.clone(),
            confidence,
            reason: reasons.join(", "),
            entity_type_key,
            source_matched,
            game_system_matched,
        });
    }

    scored.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    scored.truncate(max_suggestions.max(1));
    scored
}

fn suggest_matches_via_core_reconciler(
    candidate: &ParsedEntityCandidate,
    catalog: &CoreCatalog,
    index: &CatalogMatchIndex<'_>,
    max_suggestions: usize,
) -> Vec<ReviewMatchCandidate> {
    let mut reconciler = Reconciler {
        store: InMemoryReconciliationStore::new(catalog.clone()),
        policy: ReconciliationPolicy::Guided,
    };
    let import_candidate = candidate.clone().into_import_candidate();
    let outcome = reconciler
        .reconcile_entities(vec![import_candidate])
        .into_iter()
        .next();

    let mut candidates = match outcome {
        Some(ResolutionOutcome::Ambiguous { candidates }) => candidates,
        Some(ResolutionOutcome::Matched { id, confidence }) => {
            vec![MatchCandidate {
                id,
                confidence,
                reason: "matched by core reconciler".to_string(),
            }]
        }
        Some(ResolutionOutcome::Created { .. })
        | Some(ResolutionOutcome::Conflict { .. })
        | None => {
            let query = entity_match_query(candidate);
            let store = InMemoryReconciliationStore::new(catalog.clone());
            store.search_candidates(SubjectKind::Entity, query)
        }
    };

    candidates.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    candidates.truncate(max_suggestions.max(1));

    let requested_source = candidate_source_hint(candidate);
    let requested_game_system = candidate_game_system_hint(candidate);

    candidates
        .into_iter()
        .map(|entry| {
            let entity = catalog.entities.iter().find(|e| e.id == entry.id);
            let name = entity
                .map(|e| e.name.clone())
                .unwrap_or_else(|| "<unknown>".to_string());
            let entity_type_key =
                entity.and_then(|e| index.entity_type_keys.get(&e.entity_type).cloned());

            let source_matched = entity.is_some_and(|e| {
                requested_source.as_ref().is_some_and(|wanted_source| {
                    entity_source_titles(e, index)
                        .iter()
                        .any(|source| fuzzy_match(wanted_source, source))
                })
            });

            let game_system_matched = entity.is_some_and(|e| {
                requested_game_system.as_ref().is_some_and(|wanted_game| {
                    entity_game_systems(e, index)
                        .iter()
                        .any(|system| system.eq_ignore_ascii_case(wanted_game))
                })
            });

            ReviewMatchCandidate {
                canonical_id: entry.id.0.to_string(),
                name,
                confidence: entry.confidence,
                reason: entry.reason,
                entity_type_key,
                source_matched,
                game_system_matched,
            }
        })
        .collect()
}

fn entity_match_query(candidate: &ParsedEntityCandidate) -> MatchQuery {
    let kind_hint = candidate
        .entity
        .attributes
        .get("pcgen_entity_type_key")
        .and_then(|v| v.as_str())
        .map(ToString::to_string);
    let source_hint = candidate
        .source_hints
        .iter()
        .find_map(|hint| hint.title.clone());
    let game_system_hint = candidate
        .source_hints
        .iter()
        .find_map(|hint| hint.game_system.clone());

    MatchQuery {
        display_name: Some(candidate.entity.name.clone()),
        kind_hint,
        source_hint,
        game_system_hint,
    }
}

fn entity_source_titles(
    entity: &artisan_core::Entity,
    index: &CatalogMatchIndex<'_>,
) -> Vec<String> {
    let mut titles = Vec::new();

    if let Some(page) = entity
        .attributes
        .get("pcgen_source_page")
        .and_then(|v| v.as_str())
    {
        titles.push(page.to_string());
    }

    for citation_id in &entity.citations {
        if let Some(citation) = index.citations.get(citation_id)
            && let Some(source) = index.sources.get(&citation.source)
        {
            titles.push(source.title.clone());
        }
    }

    titles
}

fn entity_game_systems(
    entity: &artisan_core::Entity,
    index: &CatalogMatchIndex<'_>,
) -> Vec<String> {
    let mut systems = Vec::new();

    if let Some(mode) = entity
        .attributes
        .get("pcgen_game_mode")
        .and_then(|v| v.as_str())
    {
        systems.push(mode.to_string());
    }

    for citation_id in &entity.citations {
        if let Some(citation) = index.citations.get(citation_id)
            && let Some(source) = index.sources.get(&citation.source)
        {
            for game_system in &source.game_systems {
                systems.push(game_system.clone());
            }
        }
    }

    systems
}

fn score_name_similarity(left: &str, right: &str) -> f32 {
    if left.eq_ignore_ascii_case(right) {
        return 0.75;
    }

    let left_norm = normalize_for_match(left);
    let right_norm = normalize_for_match(right);
    if left_norm.is_empty() || right_norm.is_empty() {
        return 0.0;
    }

    if left_norm == right_norm {
        return 0.65;
    }

    if left_norm.contains(&right_norm) || right_norm.contains(&left_norm) {
        return 0.40;
    }

    0.0
}

fn fuzzy_match(left: &str, right: &str) -> bool {
    let left_norm = normalize_for_match(left);
    let right_norm = normalize_for_match(right);
    if left_norm.is_empty() || right_norm.is_empty() {
        return false;
    }

    left_norm == right_norm || left_norm.contains(&right_norm) || right_norm.contains(&left_norm)
}

fn normalize_for_match(input: &str) -> String {
    input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn candidate_key(candidate: &ParsedEntityCandidate) -> String {
    if let Some(ext) = candidate.entity.external_ids.first() {
        return format!("{}:{}", "pcgen", ext.value);
    }

    format!(
        "pcgen:{}:{}",
        candidate.entity.name,
        line_number(candidate).unwrap_or(0)
    )
}

fn line_number(candidate: &ParsedEntityCandidate) -> Option<u64> {
    candidate
        .entity
        .attributes
        .get("pcgen_line_number")
        .and_then(|v| v.as_u64())
}

fn candidate_source_hint(candidate: &ParsedEntityCandidate) -> Option<String> {
    candidate
        .source_hints
        .iter()
        .find_map(|hint| hint.title.clone())
        .or_else(|| {
            candidate
                .entity
                .attributes
                .get("pcgen_source_page")
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
        })
}

fn candidate_game_system_hint(candidate: &ParsedEntityCandidate) -> Option<String> {
    candidate
        .source_hints
        .iter()
        .find_map(|hint| hint.game_system.clone())
}
