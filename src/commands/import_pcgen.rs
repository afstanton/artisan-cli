use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use artisan_core::{
    CoreCatalog,
    reconcile::{
        ImportCandidate, InMemoryReconciliationStore, Reconciler, ReconciliationPolicy,
        ResolutionOutcome,
    },
};
use artisan_pcgen::{ParsedEntityCandidate, parse_text_to_catalog};
use artisan_toml::{dump_catalog, parse_catalog};
use serde::Serialize;

use super::corpus::{CorpusSide, load_corpus_paths};

#[derive(clap::Args, Debug)]
pub struct ImportPcgenArgs {
    #[arg(long, value_name = "PCGEN_FILE_OR_DIR")]
    pub input: Option<PathBuf>,

    #[arg(long, value_name = "CORPUS_MANIFEST_TOML")]
    pub corpus_manifest: Option<PathBuf>,

    #[arg(long = "corpus-group", value_name = "GROUP_NAME")]
    pub corpus_groups: Vec<String>,

    #[arg(long, value_name = "IN_CORE_TOML_FILE")]
    pub from_core_toml: Option<PathBuf>,

    #[arg(long, value_name = "REPORT_JSON")]
    pub report_json: Option<PathBuf>,

    #[arg(long, value_name = "OUT_CORE_TOML_FILE")]
    pub out_core_toml: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

#[derive(Debug, Serialize)]
struct ImportPcgenReport {
    input: String,
    files_scanned: usize,
    files_parsed: usize,
    files_failed: usize,
    entity_types_discovered: usize,
    entities_parsed: usize,
    publishers_discovered: usize,
    sources_discovered: usize,
    citations_discovered: usize,
    by_extension: BTreeMap<String, usize>,
    unresolved_type_entities: usize,
    reconcile: ReconcileSummary,
    failures: Vec<String>,
    hook_points: HookPoints,
}

#[derive(Debug, Serialize)]
struct ReconcileSummary {
    baseline_publishers: usize,
    baseline_sources: usize,
    baseline_citations: usize,
    baseline_entities: usize,
    baseline_entity_types: usize,
    imported_publishers: usize,
    imported_sources: usize,
    imported_citations: usize,
    imported_entities: usize,
    imported_entity_types: usize,
    publisher_outcomes: OutcomeCounts,
    source_outcomes: OutcomeCounts,
    citation_outcomes: OutcomeCounts,
    entity_outcomes: OutcomeCounts,
    entity_type_outcomes: OutcomeCounts,
}

#[derive(Debug, Serialize, Default)]
struct OutcomeCounts {
    matched: usize,
    created: usize,
    ambiguous: usize,
    conflict: usize,
}

#[derive(Debug, Serialize)]
struct HookPoints {
    reconcile: HookStatus,
    inference: HookStatus,
}

#[derive(Debug, Serialize)]
struct HookStatus {
    status: String,
    next_step: String,
}

pub fn run(args: ImportPcgenArgs) -> Result<(), String> {
    let files = if let Some(manifest_path) = &args.corpus_manifest {
        if args.input.is_some() {
            return Err(
                "use either --input <file-or-dir> or --corpus-manifest <file>, not both"
                    .to_string(),
            );
        }
        load_corpus_paths(manifest_path, CorpusSide::Pcgen, &args.corpus_groups)?
    } else {
        let Some(input) = &args.input else {
            return Err(
                "missing input: use --input <file-or-dir> or --corpus-manifest <file>".to_string(),
            );
        };
        collect_pcgen_files(input)
            .map_err(|e| format!("failed to collect input files {}: {e}", input.display()))?
    };

    let mut by_extension: BTreeMap<String, usize> = BTreeMap::new();
    let mut files_parsed = 0usize;
    let files_failed = 0usize;
    let mut entity_type_keys = std::collections::BTreeSet::new();
    let mut entities_parsed = 0usize;
    let mut publishers_discovered = 0usize;
    let mut sources_discovered = 0usize;
    let mut citations_discovered = 0usize;
    let mut imported_entity_types = Vec::new();
    let mut imported_publishers = Vec::new();
    let mut imported_sources = Vec::new();
    let mut imported_citations = Vec::new();
    let mut imported_candidates: Vec<ParsedEntityCandidate> = Vec::new();
    let failures = Vec::new();
    let mut unresolved_type_entities = 0usize;

    for file in &files {
        let ext = extension_of(file);
        *by_extension.entry(ext.clone()).or_insert(0) += 1;

        let raw = fs::read_to_string(file)
            .map_err(|e| format!("failed to read {}: {e}", file.display()))?;
        let parsed = parse_text_to_catalog(&raw, &file.display().to_string(), &ext);
        files_parsed += 1;
        entities_parsed += parsed.entities.len();
        publishers_discovered += parsed.publishers.len();
        sources_discovered += parsed.sources.len();
        citations_discovered += parsed.citations.len();
        for entity_type in parsed.entity_types.clone() {
            entity_type_keys.insert(entity_type.key.clone());
            imported_entity_types.push(entity_type);
        }
        imported_publishers.extend(parsed.publishers.clone());
        imported_sources.extend(parsed.sources.clone());
        imported_citations.extend(parsed.citations.clone());
        for entity in parsed.entities {
            let entity_type_key = entity
                .attributes
                .get("pcgen_entity_type_key")
                .and_then(|v| v.as_str())
                .unwrap_or("pcgen:type:unresolved")
                .to_string();
            if entity_type_key == "pcgen:type:unresolved" {
                unresolved_type_entities += 1;
            }
            imported_candidates.push(ParsedEntityCandidate {
                entity,
                entity_type_key,
                source_hints: Vec::new(),
            });
        }
    }

    let base_catalog = if let Some(path) = &args.from_core_toml {
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("failed to read base core toml {}: {e}", path.display()))?;
        parse_catalog(&raw).map_err(|e| format!("failed to parse base core toml: {e}"))?
    } else {
        CoreCatalog::default()
    };
    let baseline_publishers = base_catalog.publishers.len();
    let baseline_sources = base_catalog.sources.len();
    let baseline_citations = base_catalog.citations.len();
    let baseline_entities = base_catalog.entities.len();
    let baseline_entity_types = base_catalog.entity_types.len();

    let mut reconciler = Reconciler {
        store: InMemoryReconciliationStore::new(base_catalog),
        policy: ReconciliationPolicy::Guided,
    };

    let entity_type_imports: Vec<ImportCandidate<_>> = imported_entity_types
        .into_iter()
        .map(|entity_type| ImportCandidate {
            external_ids: entity_type.external_ids.clone(),
            display_name: Some(entity_type.name.clone()),
            source_hints: Vec::new(),
            provenance: entity_type.provenance.clone(),
            payload: entity_type,
        })
        .collect();
    let publisher_imports: Vec<ImportCandidate<_>> = imported_publishers
        .into_iter()
        .map(|publisher| ImportCandidate {
            external_ids: publisher.external_ids.clone(),
            display_name: Some(publisher.name.clone()),
            source_hints: Vec::new(),
            provenance: None,
            payload: publisher,
        })
        .collect();
    let source_imports: Vec<ImportCandidate<_>> = imported_sources
        .into_iter()
        .map(|source| ImportCandidate {
            external_ids: source.external_ids.clone(),
            display_name: Some(source.title.clone()),
            source_hints: Vec::new(),
            provenance: None,
            payload: source,
        })
        .collect();
    let citation_imports: Vec<ImportCandidate<_>> = imported_citations
        .into_iter()
        .map(|citation| ImportCandidate {
            external_ids: citation.external_ids.clone(),
            display_name: None,
            source_hints: Vec::new(),
            provenance: None,
            payload: citation,
        })
        .collect();
    let entity_imports: Vec<ImportCandidate<_>> = imported_candidates
        .iter()
        .cloned()
        .map(|c| c.into_import_candidate())
        .collect();

    let publisher_outcomes = reconciler.reconcile_publishers(publisher_imports);
    let source_outcomes = reconciler.reconcile_sources(source_imports);
    let entity_type_outcomes = reconciler.reconcile_entity_types(entity_type_imports);
    let entity_outcomes = reconciler.reconcile_entities(entity_imports);
    let citation_outcomes = reconciler.reconcile_citations(citation_imports);

    // Persist reconciled catalog if requested
    let persist_status = if let Some(out_path) = &args.out_core_toml {
        let reconciled = reconciler.store.into_catalog();
        let toml_text = dump_catalog(&reconciled)
            .map_err(|e| format!("failed to encode reconciled catalog: {e}"))?;
        fs::write(out_path, toml_text).map_err(|e| {
            format!(
                "failed to write reconciled catalog {}: {e}",
                out_path.display()
            )
        })?;
        format!("written to {}", out_path.display())
    } else {
        "skipped (no --out-core-toml)".to_string()
    };

    let report = ImportPcgenReport {
        input: args
            .input
            .as_ref()
            .map(|path| path.display().to_string())
            .or_else(|| {
                args.corpus_manifest
                    .as_ref()
                    .map(|path| path.display().to_string())
            })
            .unwrap_or_else(|| "<unknown>".to_string()),
        files_scanned: files.len(),
        files_parsed,
        files_failed,
        entity_types_discovered: entity_type_keys.len(),
        entities_parsed,
        publishers_discovered,
        sources_discovered,
        citations_discovered,
        by_extension,
        unresolved_type_entities,
        reconcile: ReconcileSummary {
            baseline_publishers,
            baseline_sources,
            baseline_citations,
            baseline_entities,
            baseline_entity_types,
            imported_publishers: publishers_discovered,
            imported_sources: sources_discovered,
            imported_citations: citations_discovered,
            imported_entities: imported_candidates.len(),
            imported_entity_types: entity_type_keys.len(),
            publisher_outcomes: count_outcomes(&publisher_outcomes),
            source_outcomes: count_outcomes(&source_outcomes),
            citation_outcomes: count_outcomes(&citation_outcomes),
            entity_outcomes: count_outcomes(&entity_outcomes),
            entity_type_outcomes: count_outcomes(&entity_type_outcomes),
        },
        failures,
        hook_points: HookPoints {
            reconcile: HookStatus {
                status: "implemented".to_string(),
                next_step: persist_status,
            },
            inference: HookStatus {
                status: "not_implemented".to_string(),
                next_step: "run inference passes for unresolved type entities and unresolved links"
                    .to_string(),
            },
        },
    };

    if args.dry_run {
        println!("pcgen import dry-run");
        println!("  input: {}", report.input);
        println!("  files scanned: {}", report.files_scanned);
        println!("  files parsed: {}", report.files_parsed);
        println!("  files failed: {}", report.files_failed);
        println!("  entities parsed: {}", report.entities_parsed);
        println!("  publishers discovered: {}", report.publishers_discovered);
        println!("  sources discovered: {}", report.sources_discovered);
        println!("  citations discovered: {}", report.citations_discovered);
        println!(
            "  entity types discovered: {}",
            report.entity_types_discovered
        );
        println!(
            "  unresolved type entities: {}",
            report.unresolved_type_entities
        );
        println!(
            "  reconcile publishers (matched/created/ambiguous/conflict): {}/{}/{}/{}",
            report.reconcile.publisher_outcomes.matched,
            report.reconcile.publisher_outcomes.created,
            report.reconcile.publisher_outcomes.ambiguous,
            report.reconcile.publisher_outcomes.conflict,
        );
        println!(
            "  reconcile sources (matched/created/ambiguous/conflict): {}/{}/{}/{}",
            report.reconcile.source_outcomes.matched,
            report.reconcile.source_outcomes.created,
            report.reconcile.source_outcomes.ambiguous,
            report.reconcile.source_outcomes.conflict,
        );
        println!(
            "  reconcile entities (matched/created/ambiguous/conflict): {}/{}/{}/{}",
            report.reconcile.entity_outcomes.matched,
            report.reconcile.entity_outcomes.created,
            report.reconcile.entity_outcomes.ambiguous,
            report.reconcile.entity_outcomes.conflict,
        );
        println!(
            "  reconcile entity types (matched/created/ambiguous/conflict): {}/{}/{}/{}",
            report.reconcile.entity_type_outcomes.matched,
            report.reconcile.entity_type_outcomes.created,
            report.reconcile.entity_type_outcomes.ambiguous,
            report.reconcile.entity_type_outcomes.conflict,
        );
        println!(
            "  reconcile citations (matched/created/ambiguous/conflict): {}/{}/{}/{}",
            report.reconcile.citation_outcomes.matched,
            report.reconcile.citation_outcomes.created,
            report.reconcile.citation_outcomes.ambiguous,
            report.reconcile.citation_outcomes.conflict,
        );
        return Ok(());
    }

    if let Some(path) = &args.report_json {
        let encoded = serde_json::to_string_pretty(&report)
            .map_err(|e| format!("failed to encode report json: {e}"))?;
        fs::write(path, encoded)
            .map_err(|e| format!("failed to write report {}: {e}", path.display()))?;
        println!("wrote import report to {}", path.display());
    }

    println!("pcgen import summary");
    println!("  input: {}", report.input);
    println!("  files scanned: {}", report.files_scanned);
    println!("  files parsed: {}", report.files_parsed);
    println!("  files failed: {}", report.files_failed);
    println!("  entities parsed: {}", report.entities_parsed);
    println!("  publishers discovered: {}", report.publishers_discovered);
    println!("  sources discovered: {}", report.sources_discovered);
    println!("  citations discovered: {}", report.citations_discovered);
    println!(
        "  entity types discovered: {}",
        report.entity_types_discovered
    );
    println!(
        "  unresolved type entities: {}",
        report.unresolved_type_entities
    );
    println!(
        "  reconcile publishers (matched/created/ambiguous/conflict): {}/{}/{}/{}",
        report.reconcile.publisher_outcomes.matched,
        report.reconcile.publisher_outcomes.created,
        report.reconcile.publisher_outcomes.ambiguous,
        report.reconcile.publisher_outcomes.conflict,
    );
    println!(
        "  reconcile sources (matched/created/ambiguous/conflict): {}/{}/{}/{}",
        report.reconcile.source_outcomes.matched,
        report.reconcile.source_outcomes.created,
        report.reconcile.source_outcomes.ambiguous,
        report.reconcile.source_outcomes.conflict,
    );
    println!(
        "  reconcile entities (matched/created/ambiguous/conflict): {}/{}/{}/{}",
        report.reconcile.entity_outcomes.matched,
        report.reconcile.entity_outcomes.created,
        report.reconcile.entity_outcomes.ambiguous,
        report.reconcile.entity_outcomes.conflict,
    );
    println!(
        "  reconcile entity types (matched/created/ambiguous/conflict): {}/{}/{}/{}",
        report.reconcile.entity_type_outcomes.matched,
        report.reconcile.entity_type_outcomes.created,
        report.reconcile.entity_type_outcomes.ambiguous,
        report.reconcile.entity_type_outcomes.conflict,
    );
    println!(
        "  reconcile citations (matched/created/ambiguous/conflict): {}/{}/{}/{}",
        report.reconcile.citation_outcomes.matched,
        report.reconcile.citation_outcomes.created,
        report.reconcile.citation_outcomes.ambiguous,
        report.reconcile.citation_outcomes.conflict,
    );
    println!("  reconcile hook: {}", report.hook_points.reconcile.status);
    println!("  inference hook: {}", report.hook_points.inference.status);

    Ok(())
}

fn count_outcomes(outcomes: &[ResolutionOutcome]) -> OutcomeCounts {
    let mut counts = OutcomeCounts::default();
    for outcome in outcomes {
        match outcome {
            ResolutionOutcome::Matched { .. } => counts.matched += 1,
            ResolutionOutcome::Created { .. } => counts.created += 1,
            ResolutionOutcome::Ambiguous { .. } => counts.ambiguous += 1,
            ResolutionOutcome::Conflict { .. } => counts.conflict += 1,
        }
    }
    counts
}

fn collect_pcgen_files(input: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    collect_pcgen_files_recursive(input, &mut out)?;
    out.sort();
    Ok(out)
}

fn collect_pcgen_files_recursive(path: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if path.is_file() {
        if is_pcgen_data_file(path) {
            out.push(path.to_path_buf());
        }
        return Ok(());
    }

    if !path.is_dir() {
        return Ok(());
    }

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let child = entry.path();
        if child
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with('.'))
        {
            continue;
        }
        collect_pcgen_files_recursive(&child, out)?;
    }

    Ok(())
}

fn is_pcgen_data_file(path: &Path) -> bool {
    matches!(extension_of(path).as_str(), "lst" | "pcc" | "pcg")
}

fn extension_of(path: &Path) -> String {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_else(|| "unknown".to_string())
}
