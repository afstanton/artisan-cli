use std::{collections::BTreeSet, fs, path::PathBuf};

use artisan_core::{CanonicalId, CoreCatalog, ExternalId, FormatId, IdentityLink, MappingRecord};
use artisan_toml::{dump_catalog, parse_catalog};

use super::local_workspace::{
    ensure_parent_dir, resolve_existing_core_catalog_path, resolve_output_core_catalog_path,
};
use super::reconcile_review::{ReviewItem, ReviewState};

#[derive(clap::Args, Debug)]
pub struct ReconcileApplyArgs {
    #[arg(long, value_name = "REVIEW_STATE_JSON")]
    pub review_state: PathBuf,

    #[arg(long, value_name = "OUT_CORE_TOML_FILE")]
    pub to_core_toml: Option<PathBuf>,

    #[arg(long, value_name = "IN_CORE_TOML_FILE")]
    pub from_core_toml: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

pub fn run(args: ReconcileApplyArgs) -> Result<(), String> {
    let state = load_review_state(&args.review_state)?;

    let input_catalog_path = resolve_existing_core_catalog_path(args.from_core_toml.as_ref());
    let mut catalog = if let Some(path) = &input_catalog_path {
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("failed to read base core toml {}: {e}", path.display()))?;
        parse_catalog(&raw).map_err(|e| format!("failed to parse base core toml: {e}"))?
    } else {
        CoreCatalog::default()
    };

    let mut existing_identity: BTreeSet<String> = catalog
        .identity_links
        .iter()
        .map(|l| identity_key(&l.kind, l.canonical_id, &l.external_id))
        .collect();
    let mut existing_mapping: BTreeSet<String> = catalog
        .mapping_records
        .iter()
        .map(|m| m.id.clone())
        .collect();

    let mut applied = 0usize;
    let mut skipped = 0usize;

    for item in &state.items {
        let Some(decision) = &item.decision else {
            continue;
        };
        if !decision.accepted {
            continue;
        }

        let entity_external_id = candidate_entity_external_id(item);
        let entity_type_external_id = candidate_entity_type_external_id(item);

        let canonical_entity_id =
            resolve_canonical_entity_id(&catalog, item, entity_external_id.as_ref());
        let canonical_entity_type_id = resolve_canonical_entity_type_id(
            &catalog,
            item,
            entity_type_external_id.as_ref(),
            canonical_entity_id,
        );

        if canonical_entity_id.is_none() && canonical_entity_type_id.is_none() {
            skipped += 1;
            continue;
        }

        if let (Some(entity_id), Some(external_id)) =
            (canonical_entity_id, entity_external_id.as_ref())
        {
            ensure_identity_link(
                &mut catalog,
                &mut existing_identity,
                "entity",
                entity_id,
                external_id.clone(),
            );
            ensure_entity_external_id(&mut catalog, entity_id, external_id.clone());
        }

        if let (Some(entity_type_id), Some(external_id)) =
            (canonical_entity_type_id, entity_type_external_id.as_ref())
        {
            ensure_identity_link(
                &mut catalog,
                &mut existing_identity,
                "entity_type",
                entity_type_id,
                external_id.clone(),
            );
            ensure_entity_type_external_id(&mut catalog, entity_type_id, external_id.clone());
        }

        let map_id = sanitize_mapping_id(&format!("manual:{}", item.candidate_key));
        if !existing_mapping.contains(&map_id) {
            catalog.mapping_records.push(MappingRecord {
                id: map_id.clone(),
                description: decision
                    .note
                    .clone()
                    .or_else(|| Some(format!("manual reconciliation for {}", item.candidate_key))),
                source_entity_type: None,
                target_entity_type: canonical_entity_type_id,
                payload: serde_json::json!({
                    "source_format": item.source_format,
                    "candidate_key": item.candidate_key,
                    "name": item.name,
                    "inferred_entity_type_key": item.inferred_entity_type_key,
                    "mapped_entity_type_key": decision.mapped_entity_type_key,
                    "matched_canonical_id": decision.matched_canonical_id,
                    "matched_entity_type_canonical_id": decision.matched_entity_type_canonical_id,
                    "resolved_canonical_id": canonical_entity_id.map(|id| id.0.to_string()),
                    "resolved_entity_type_canonical_id": canonical_entity_type_id.map(|id| id.0.to_string()),
                    "source_hint": item.source_hint,
                    "game_system_hint": item.game_system_hint,
                    "match_candidates": item.match_candidates,
                    "accepted": decision.accepted,
                }),
            });
            existing_mapping.insert(map_id);
        }

        applied += 1;
    }

    let output_catalog_path = resolve_output_core_catalog_path(args.to_core_toml.as_ref());

    if args.dry_run {
        println!("reconcile apply (dry-run)");
        println!("  review file: {}", args.review_state.display());
        println!("  would apply: {}", applied);
        println!("  skipped: {}", skipped);
        println!("  output: {}", output_catalog_path.display());
        return Ok(());
    }

    let encoded =
        dump_catalog(&catalog).map_err(|e| format!("failed to encode core catalog toml: {e}"))?;
    ensure_parent_dir(&output_catalog_path)?;
    fs::write(&output_catalog_path, encoded).map_err(|e| {
        format!(
            "failed to write output {}: {e}",
            output_catalog_path.display()
        )
    })?;

    println!("reconcile apply complete");
    println!("  review file: {}", args.review_state.display());
    println!("  applied: {}", applied);
    println!("  skipped: {}", skipped);
    println!("  output: {}", output_catalog_path.display());

    Ok(())
}

fn load_review_state(path: &PathBuf) -> Result<ReviewState, String> {
    let raw = fs::read_to_string(path)
        .map_err(|e| format!("failed to read review state {}: {e}", path.display()))?;
    serde_json::from_str(&raw)
        .map_err(|e| format!("failed to parse review state {}: {e}", path.display()))
}

fn candidate_external_value(candidate_key: &str) -> Option<&str> {
    candidate_key.strip_prefix("pcgen:")
}

fn find_entity_id_by_external_value(
    catalog: &CoreCatalog,
    external_value: &str,
) -> Option<artisan_core::CanonicalId> {
    catalog
        .entities
        .iter()
        .find(|entity| {
            entity
                .external_ids
                .iter()
                .any(|eid| eid.format == FormatId::Pcgen && eid.value == external_value)
        })
        .map(|entity| entity.id)
}

fn find_entity_type_id_by_key(catalog: &CoreCatalog, key: &str) -> Option<CanonicalId> {
    catalog
        .entity_types
        .iter()
        .find(|entity_type| entity_type.key == key)
        .map(|entity_type| entity_type.id)
}

fn resolve_canonical_entity_id(
    catalog: &CoreCatalog,
    item: &ReviewItem,
    entity_external_id: Option<&ExternalId>,
) -> Option<CanonicalId> {
    item.decision
        .as_ref()
        .and_then(|decision| decision.matched_canonical_id.as_deref())
        .and_then(parse_canonical_id)
        .or_else(|| {
            item.match_candidates
                .first()
                .and_then(|m| parse_canonical_id(&m.canonical_id))
        })
        .or_else(|| {
            entity_external_id
                .map(|external_id| external_id.value.as_str())
                .and_then(|external_value| {
                    find_entity_id_by_external_value(catalog, external_value)
                })
        })
}

fn resolve_canonical_entity_type_id(
    catalog: &CoreCatalog,
    item: &ReviewItem,
    entity_type_external_id: Option<&ExternalId>,
    canonical_entity_id: Option<CanonicalId>,
) -> Option<CanonicalId> {
    item.decision
        .as_ref()
        .and_then(|decision| decision.matched_entity_type_canonical_id.as_deref())
        .and_then(parse_canonical_id)
        .or_else(|| {
            item.decision
                .as_ref()
                .and_then(|decision| decision.mapped_entity_type_key.as_deref())
                .and_then(|key| find_entity_type_id_by_key(catalog, key))
        })
        .or_else(|| {
            item.match_candidates
                .first()
                .and_then(|m| m.entity_type_key.as_deref())
                .and_then(|key| find_entity_type_id_by_key(catalog, key))
        })
        .or_else(|| {
            canonical_entity_id.and_then(|entity_id| {
                catalog
                    .entities
                    .iter()
                    .find(|entity| entity.id == entity_id)
                    .map(|entity| entity.entity_type)
            })
        })
        .or_else(|| {
            entity_type_external_id
                .and_then(|external_id| {
                    catalog.entity_types.iter().find(|entity_type| {
                        entity_type.external_ids.iter().any(|existing| {
                            existing.format == external_id.format
                                && existing.namespace == external_id.namespace
                                && existing.value == external_id.value
                        })
                    })
                })
                .map(|entity_type| entity_type.id)
        })
}

fn candidate_entity_external_id(item: &ReviewItem) -> Option<ExternalId> {
    let external_value = candidate_external_value(&item.candidate_key)?;
    Some(ExternalId {
        format: match item.source_format.as_str() {
            "pcgen" => FormatId::Pcgen,
            "herolab" => FormatId::Herolab,
            other => FormatId::Other(other.to_string()),
        },
        namespace: Some("candidate".to_string()),
        value: external_value.to_string(),
    })
}

fn candidate_entity_type_external_id(item: &ReviewItem) -> Option<ExternalId> {
    if item.inferred_entity_type_key.trim().is_empty() {
        return None;
    }
    Some(ExternalId {
        format: match item.source_format.as_str() {
            "pcgen" => FormatId::Pcgen,
            "herolab" => FormatId::Herolab,
            other => FormatId::Other(other.to_string()),
        },
        namespace: Some("entity_type_key".to_string()),
        value: item.inferred_entity_type_key.clone(),
    })
}

fn ensure_identity_link(
    catalog: &mut CoreCatalog,
    existing_identity: &mut BTreeSet<String>,
    kind: &str,
    canonical_id: CanonicalId,
    external_id: ExternalId,
) {
    let key = identity_key(kind, canonical_id, &external_id);
    if existing_identity.insert(key) {
        catalog.identity_links.push(IdentityLink {
            kind: kind.to_string(),
            canonical_id,
            external_id,
        });
    }
}

fn ensure_entity_external_id(
    catalog: &mut CoreCatalog,
    canonical_id: CanonicalId,
    external_id: ExternalId,
) {
    if let Some(entity) = catalog
        .entities
        .iter_mut()
        .find(|entity| entity.id == canonical_id)
    {
        if !entity
            .external_ids
            .iter()
            .any(|existing| existing == &external_id)
        {
            entity.external_ids.push(external_id);
        }
    }
}

fn ensure_entity_type_external_id(
    catalog: &mut CoreCatalog,
    canonical_id: CanonicalId,
    external_id: ExternalId,
) {
    if let Some(entity_type) = catalog
        .entity_types
        .iter_mut()
        .find(|entity_type| entity_type.id == canonical_id)
        && !entity_type
            .external_ids
            .iter()
            .any(|existing| existing == &external_id)
    {
        entity_type.external_ids.push(external_id);
    }
}

fn identity_key(kind: &str, canonical_id: CanonicalId, external_id: &ExternalId) -> String {
    format!(
        "{}:{}:{:?}:{}",
        kind, canonical_id.0, external_id.namespace, external_id.value
    )
}

fn parse_canonical_id(raw: &str) -> Option<CanonicalId> {
    uuid::Uuid::parse_str(raw).ok().map(CanonicalId)
}

fn sanitize_mapping_id(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use artisan_core::{
        Entity, EntityType,
        domain::{
            CitationRecord, FieldCardinality, FieldDef, FieldType, SourceRecord, SubjectRef,
            VerificationState, citation::CitationLocator, entity::CompletenessState,
        },
    };
    use uuid::Uuid;

    use super::*;
    use crate::commands::reconcile_review::{
        ReviewDecision, ReviewItem, ReviewMatchCandidate, ReviewState,
    };

    #[test]
    fn reconcile_apply_persists_entity_and_entity_type_links() {
        let entity_type_id = CanonicalId(Uuid::new_v4());
        let entity_id = CanonicalId(Uuid::new_v4());
        let source_id = CanonicalId(Uuid::new_v4());
        let citation_id = CanonicalId(Uuid::new_v4());

        let catalog = CoreCatalog {
            sources: vec![SourceRecord {
                id: source_id,
                title: "Core Book".to_string(),
                publisher: None,
                publisher_ids: Vec::new(),
                edition: None,
                license: None,
                game_systems: vec!["pathfinder".to_string()],
                external_ids: Vec::new(),
            }],
            citations: vec![CitationRecord {
                id: citation_id,
                subject: SubjectRef::Entity(entity_id),
                source: source_id,
                locators: vec![CitationLocator {
                    kind: "thing".to_string(),
                    value: "Power Attack".to_string(),
                    canonical: true,
                }],
                verification: VerificationState::Unverified,
                external_ids: Vec::new(),
            }],
            entity_types: vec![EntityType {
                id: entity_type_id,
                key: "herolab.pathfinder.feat".to_string(),
                name: "HeroLab Feat (Pathfinder)".to_string(),
                game_system: Some("Pathfinder".to_string()),
                parent: None,
                fields: vec![FieldDef {
                    key: "thing_id".to_string(),
                    name: "Thing ID".to_string(),
                    field_type: FieldType::Text,
                    cardinality: FieldCardinality::One,
                    required: false,
                    description: None,
                }],
                relationships: Vec::new(),
                external_ids: Vec::new(),
                provenance: None,
            }],
            entities: vec![Entity {
                id: entity_id,
                entity_type: entity_type_id,
                name: "Power Attack".to_string(),
                attributes: Default::default(),
                effects: Vec::new(),
                prerequisites: Vec::new(),
                rule_hooks: Vec::new(),
                citations: vec![citation_id],
                external_ids: Vec::new(),
                completeness: CompletenessState::Descriptive,
                provenance: None,
            }],
            ..CoreCatalog::default()
        };

        let state = ReviewState {
            schema_version: 2,
            source_file: "review".to_string(),
            total_candidates: 1,
            items: vec![ReviewItem {
                candidate_key: "pcgen:FEAT:Power Attack".to_string(),
                name: "Power Attack".to_string(),
                inferred_entity_type_key: "pcgen.lst".to_string(),
                suggested_entity_type_key: "herolab.pathfinder.feat".to_string(),
                game_system_hint: Some("pathfinder".to_string()),
                source_hint: Some("Core Book".to_string()),
                match_candidates: vec![ReviewMatchCandidate {
                    canonical_id: entity_id.0.to_string(),
                    name: "Power Attack".to_string(),
                    confidence: 0.95,
                    reason: "name similarity".to_string(),
                    entity_type_key: Some("herolab.pathfinder.feat".to_string()),
                    source_matched: true,
                    game_system_matched: true,
                }],
                source_format: "pcgen".to_string(),
                line_number: Some(1),
                decision: Some(ReviewDecision {
                    mapped_entity_type_key: Some("herolab.pathfinder.feat".to_string()),
                    matched_canonical_id: Some(entity_id.0.to_string()),
                    matched_entity_type_canonical_id: Some(entity_type_id.0.to_string()),
                    note: Some("manual acceptance".to_string()),
                    accepted: true,
                }),
            }],
        };

        let temp_root = std::env::temp_dir().join(format!(
            "artisan-cli-reconcile-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&temp_root).unwrap();
        let review_path = temp_root.join("review.json");
        let input_catalog_path = temp_root.join("in.toml");
        let output_catalog_path = temp_root.join("out.toml");

        fs::write(&review_path, serde_json::to_string_pretty(&state).unwrap()).unwrap();
        fs::write(&input_catalog_path, dump_catalog(&catalog).unwrap()).unwrap();

        run(ReconcileApplyArgs {
            review_state: review_path,
            to_core_toml: Some(output_catalog_path.clone()),
            from_core_toml: Some(input_catalog_path),
            dry_run: false,
        })
        .expect("reconcile apply should succeed");

        let output = fs::read_to_string(output_catalog_path).unwrap();
        let updated = parse_catalog(&output).unwrap();

        assert!(updated.identity_links.iter().any(|link| {
            link.kind == "entity"
                && link.canonical_id == entity_id
                && link.external_id.format == FormatId::Pcgen
                && link.external_id.namespace.as_deref() == Some("candidate")
                && link.external_id.value == "FEAT:Power Attack"
        }));
        assert!(updated.identity_links.iter().any(|link| {
            link.kind == "entity_type"
                && link.canonical_id == entity_type_id
                && link.external_id.format == FormatId::Pcgen
                && link.external_id.namespace.as_deref() == Some("entity_type_key")
                && link.external_id.value == "pcgen.lst"
        }));
        assert!(updated.entities.iter().any(|entity| {
            entity.id == entity_id
                && entity.external_ids.iter().any(|id| {
                    id.format == FormatId::Pcgen
                        && id.namespace.as_deref() == Some("candidate")
                        && id.value == "FEAT:Power Attack"
                })
        }));
        assert!(updated.entity_types.iter().any(|entity_type| {
            entity_type.id == entity_type_id
                && entity_type.external_ids.iter().any(|id| {
                    id.format == FormatId::Pcgen
                        && id.namespace.as_deref() == Some("entity_type_key")
                        && id.value == "pcgen.lst"
                })
        }));
        assert_eq!(updated.mapping_records.len(), 1);

        let _ = fs::remove_dir_all(temp_root);
    }
}
