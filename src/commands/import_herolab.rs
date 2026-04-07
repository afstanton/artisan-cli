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
use artisan_herolab::HerolabLoader;
use artisan_toml::{dump_catalog, parse_catalog};
use serde::Serialize;

#[derive(clap::Args, Debug)]
pub struct ImportHerolabArgs {
    #[arg(long, value_name = "HEROLAB_FILE_OR_DIR")]
    pub input: PathBuf,

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
struct ImportHerolabReport {
    input: String,
    files_scanned: usize,
    files_parsed: usize,
    files_failed: usize,
    entities_parsed: usize,
    entity_types_discovered: usize,
    publishers_discovered: usize,
    sources_discovered: usize,
    citations_discovered: usize,
    by_extension: BTreeMap<String, usize>,
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

pub fn run(args: ImportHerolabArgs) -> Result<(), String> {
    let files = collect_herolab_files(&args.input)
        .map_err(|e| format!("failed to collect input files {}: {e}", args.input.display()))?;

    let mut by_extension: BTreeMap<String, usize> = BTreeMap::new();
    let mut files_parsed = 0usize;
    let mut files_failed = 0usize;
    let mut entities_parsed = 0usize;
    let mut imported_entity_types = Vec::new();
    let mut imported_entities = Vec::new();
    let mut imported_publishers = Vec::new();
    let mut imported_sources = Vec::new();
    let mut imported_citations = Vec::new();
    let mut failures = Vec::new();

    for file in &files {
        let ext = extension_of(file);
        *by_extension.entry(ext.clone()).or_insert(0) += 1;

        match parse_herolab_file(file, &ext) {
            Ok(parsed) => {
                files_parsed += 1;
                entities_parsed += parsed.entities.len();
                imported_entity_types.extend(parsed.entity_types);
                imported_entities.extend(parsed.entities);
                imported_publishers.extend(parsed.publishers);
                imported_sources.extend(parsed.sources);
                imported_citations.extend(parsed.citations);
            }
            Err(err) => {
                files_failed += 1;
                failures.push(format!("{}: {}", file.display(), err));
            }
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

    let publisher_count = imported_publishers.len();
    let source_count = imported_sources.len();
    let citation_count = imported_citations.len();
    let entity_type_count = imported_entity_types.len();
    let entity_count = imported_entities.len();

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
    let entity_imports: Vec<ImportCandidate<_>> = imported_entities
        .into_iter()
        .map(|entity| ImportCandidate {
            external_ids: entity.external_ids.clone(),
            display_name: Some(entity.name.clone()),
            source_hints: Vec::new(),
            provenance: entity.provenance.clone(),
            payload: entity,
        })
        .collect();

    let publisher_outcomes = reconciler.reconcile_publishers(publisher_imports);
    let source_outcomes = reconciler.reconcile_sources(source_imports);
    let entity_type_outcomes = reconciler.reconcile_entity_types(entity_type_imports);
    let entity_outcomes = reconciler.reconcile_entities(entity_imports);
    let citation_outcomes = reconciler.reconcile_citations(citation_imports);

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

    let report = ImportHerolabReport {
        input: args.input.display().to_string(),
        files_scanned: files.len(),
        files_parsed,
        files_failed,
        entities_parsed,
        entity_types_discovered: entity_type_count,
        publishers_discovered: publisher_count,
        sources_discovered: source_count,
        citations_discovered: citation_count,
        by_extension,
        reconcile: ReconcileSummary {
            baseline_publishers,
            baseline_sources,
            baseline_citations,
            baseline_entities,
            baseline_entity_types,
            imported_publishers: publisher_count,
            imported_sources: source_count,
            imported_citations: citation_count,
            imported_entities: entity_count,
            imported_entity_types: entity_type_count,
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
                next_step: "expand HeroLab semantic extraction and lead graph entity inference"
                    .to_string(),
            },
        },
    };

    if args.dry_run {
        print_report("herolab import dry-run", &report);
        return Ok(());
    }

    if let Some(path) = &args.report_json {
        let encoded = serde_json::to_string_pretty(&report)
            .map_err(|e| format!("failed to encode report json: {e}"))?;
        fs::write(path, encoded)
            .map_err(|e| format!("failed to write report {}: {e}", path.display()))?;
        println!("wrote import report to {}", path.display());
    }

    print_report("herolab import summary", &report);
    Ok(())
}

fn print_report(title: &str, report: &ImportHerolabReport) {
    println!("{title}");
    println!("  input: {}", report.input);
    println!("  files scanned: {}", report.files_scanned);
    println!("  files parsed: {}", report.files_parsed);
    println!("  files failed: {}", report.files_failed);
    println!("  entities parsed: {}", report.entities_parsed);
    println!("  publishers discovered: {}", report.publishers_discovered);
    println!("  sources discovered: {}", report.sources_discovered);
    println!("  citations discovered: {}", report.citations_discovered);
    println!("  entity types discovered: {}", report.entity_types_discovered);
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
}

fn parse_herolab_file(path: &Path, ext: &str) -> Result<artisan_herolab::ParsedCatalog, String> {
    let source_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("herolab");
    match ext {
        "user" | "xml" => {
            let text = fs::read_to_string(path)
                .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
            HerolabLoader::parse_user_catalog(&text, source_name)
                .map_err(|e| format!("failed to parse user XML: {e}"))
        }
        "por" | "stock" | "zip" => {
            let bytes = fs::read(path)
                .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
            let manifest = HerolabLoader::inspect_portfolio_archive(&bytes)
                .map_err(|e| format!("failed to inspect portfolio archive: {e}"))?;
            Ok(manifest.catalog)
        }
        _ => Err(format!("unsupported HeroLab extension: .{ext}")),
    }
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

fn collect_herolab_files(input: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    collect_herolab_files_recursive(input, &mut out)?;
    out.sort();
    Ok(out)
}

fn collect_herolab_files_recursive(path: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if path.is_file() {
        if is_herolab_file(path) {
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
        collect_herolab_files_recursive(&child, out)?;
    }

    Ok(())
}

fn is_herolab_file(path: &Path) -> bool {
    matches!(extension_of(path).as_str(), "user" | "por" | "stock" | "xml" | "zip")
}

fn extension_of(path: &Path) -> String {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_else(|| "unknown".to_string())
}
