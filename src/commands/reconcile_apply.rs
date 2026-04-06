use std::{collections::BTreeSet, fs, path::PathBuf};

use artisan_core::{CoreCatalog, ExternalId, FormatId, IdentityLink, MappingRecord};
use artisan_toml::{dump_catalog, parse_catalog};

use super::reconcile_review::ReviewState;

#[derive(clap::Args, Debug)]
pub struct ReconcileApplyArgs {
    #[arg(long, value_name = "REVIEW_STATE_JSON")]
    pub review_state: PathBuf,

    #[arg(long, value_name = "OUT_CORE_TOML_FILE")]
    pub to_core_toml: PathBuf,

    #[arg(long, value_name = "IN_CORE_TOML_FILE")]
    pub from_core_toml: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

pub fn run(args: ReconcileApplyArgs) -> Result<(), String> {
    let state = load_review_state(&args.review_state)?;

    let mut catalog = if let Some(path) = &args.from_core_toml {
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("failed to read base core toml {}: {e}", path.display()))?;
        parse_catalog(&raw).map_err(|e| format!("failed to parse base core toml: {e}"))?
    } else {
        CoreCatalog::default()
    };

    let mut existing_identity: BTreeSet<String> = catalog
        .identity_links
        .iter()
        .map(|l| format!("{}:{}", l.canonical_id.0, l.external_id.value))
        .collect();
    let mut existing_mapping: BTreeSet<String> =
        catalog.mapping_records.iter().map(|m| m.id.clone()).collect();

    let mut applied = 0usize;
    let mut skipped = 0usize;

    for item in &state.items {
        let Some(decision) = &item.decision else {
            continue;
        };
        if !decision.accepted {
            continue;
        }

        let Some(external_value) = candidate_external_value(&item.candidate_key) else {
            skipped += 1;
            continue;
        };

        let Some(entity_id) = find_entity_id_by_external_value(&catalog, external_value) else {
            skipped += 1;
            continue;
        };

        let identity_key = format!("{}:{}", entity_id.0, external_value);
        if !existing_identity.contains(&identity_key) {
            catalog.identity_links.push(IdentityLink {
                kind: "entity".to_string(),
                canonical_id: entity_id,
                external_id: ExternalId {
                    format: FormatId::Pcgen,
                    namespace: Some("lst".to_string()),
                    value: external_value.to_string(),
                },
            });
            existing_identity.insert(identity_key);
        }

        let map_id = sanitize_mapping_id(&format!("manual:{}", item.candidate_key));
        if !existing_mapping.contains(&map_id) {
            catalog.mapping_records.push(MappingRecord {
                id: map_id.clone(),
                description: decision.note.clone().or_else(|| {
                    Some(format!(
                        "manual reconciliation for {}",
                        item.candidate_key
                    ))
                }),
                source_entity_type: None,
                target_entity_type: None,
                payload: serde_json::json!({
                    "source_format": item.source_format,
                    "candidate_key": item.candidate_key,
                    "name": item.name,
                    "inferred_entity_type_key": item.inferred_entity_type_key,
                    "mapped_entity_type_key": decision.mapped_entity_type_key,
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

    if args.dry_run {
        println!("reconcile apply (dry-run)");
        println!("  review file: {}", args.review_state.display());
        println!("  would apply: {}", applied);
        println!("  skipped: {}", skipped);
        println!("  output: {}", args.to_core_toml.display());
        return Ok(());
    }

    let encoded = dump_catalog(&catalog)
        .map_err(|e| format!("failed to encode core catalog toml: {e}"))?;
    fs::write(&args.to_core_toml, encoded)
        .map_err(|e| format!("failed to write output {}: {e}", args.to_core_toml.display()))?;

    println!("reconcile apply complete");
    println!("  review file: {}", args.review_state.display());
    println!("  applied: {}", applied);
    println!("  skipped: {}", skipped);
    println!("  output: {}", args.to_core_toml.display());

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

fn find_entity_id_by_external_value(catalog: &CoreCatalog, external_value: &str) -> Option<artisan_core::CanonicalId> {
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
