use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use artisan_core::CoreCatalog;
use artisan_herolab::HerolabLoader;
use artisan_pcgen::parse_text_to_catalog;
use artisan_toml::dump_catalog;

#[derive(clap::Args, Debug)]
pub struct ConvertArgs {
    #[arg(long, value_name = "PCGEN_FILE_OR_DIR")]
    pub from_pcgen: Option<PathBuf>,

    #[arg(long, value_name = "HEROLAB_FILE_OR_DIR")]
    pub from_herolab: Option<PathBuf>,

    #[arg(long, value_name = "OUT_TOML_FILE")]
    pub to_core_toml: PathBuf,
}

pub fn run(args: ConvertArgs) -> Result<(), String> {
    let catalog = match (&args.from_pcgen, &args.from_herolab) {
        (Some(_), Some(_)) => return Err(
            "choose exactly one input: --from-pcgen <file-or-dir> or --from-herolab <file-or-dir>"
                .to_string(),
        ),
        (Some(input_path), None) => load_pcgen_catalog(input_path)?,
        (None, Some(input_path)) => load_herolab_catalog(input_path)?,
        (None, None) => {
            return Err(
                "missing input: use --from-pcgen <file-or-dir> or --from-herolab <file-or-dir>"
                    .to_string(),
            );
        }
    };

    let encoded = dump_catalog(&catalog).map_err(|e| e.to_string())?;
    fs::write(&args.to_core_toml, encoded).map_err(|e| {
        format!(
            "failed to write output {}: {e}",
            args.to_core_toml.display()
        )
    })?;

    println!(
        "wrote core catalog TOML with {} entity types, {} entities, {} sources, and {} citations to {}",
        catalog.entity_types.len(),
        catalog.entities.len(),
        catalog.sources.len(),
        catalog.citations.len(),
        args.to_core_toml.display()
    );

    Ok(())
}

fn load_pcgen_catalog(input: &Path) -> Result<CoreCatalog, String> {
    let files = collect_files_recursive(input, is_pcgen_file).map_err(|e| {
        format!(
            "failed to collect PCGen input files {}: {e}",
            input.display()
        )
    })?;
    let mut merged = CoreCatalog::default();
    for file in files {
        let raw = fs::read_to_string(&file)
            .map_err(|e| format!("failed to read {}: {e}", file.display()))?;
        let source_name = file.file_name().and_then(|n| n.to_str()).unwrap_or("pcgen");
        let ext = extension_of(&file);
        let parsed = parse_text_to_catalog(&raw, source_name, &ext);
        merge_catalog(&mut merged, parsed);
    }
    dedupe_catalog(&mut merged);
    Ok(merged)
}

fn load_herolab_catalog(input: &Path) -> Result<CoreCatalog, String> {
    let files = collect_files_recursive(input, is_herolab_file).map_err(|e| {
        format!(
            "failed to collect HeroLab input files {}: {e}",
            input.display()
        )
    })?;
    let mut merged = CoreCatalog::default();
    for file in files {
        let ext = extension_of(&file);
        let source_name = file
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("herolab");
        let parsed = match ext.as_str() {
            "user" | "xml" | "1st" | "dat" => {
                let bytes = fs::read(&file)
                    .map_err(|e| format!("failed to read {}: {e}", file.display()))?;
                let text = String::from_utf8_lossy(&bytes).to_string();
                HerolabLoader::parse_user_catalog(&text, source_name)
                    .map_err(|e| format!("failed to parse {}: {e}", file.display()))?
            }
            "por" | "stock" | "zip" => {
                let bytes = fs::read(&file)
                    .map_err(|e| format!("failed to read {}: {e}", file.display()))?;
                HerolabLoader::inspect_portfolio_archive(&bytes)
                    .map_err(|e| format!("failed to inspect {}: {e}", file.display()))?
                    .catalog
            }
            _ => continue,
        };
        merge_catalog(&mut merged, parsed);
    }
    dedupe_catalog(&mut merged);
    Ok(merged)
}

fn merge_catalog(into: &mut CoreCatalog, other: CoreCatalog) {
    into.publishers.extend(other.publishers);
    into.sources.extend(other.sources);
    into.citations.extend(other.citations);
    into.entity_types.extend(other.entity_types);
    into.entities.extend(other.entities);
    into.character_graphs.extend(other.character_graphs);
    into.identity_links.extend(other.identity_links);
    into.mapping_records.extend(other.mapping_records);
    into.projection_maps.extend(other.projection_maps);
    into.loss_notes.extend(other.loss_notes);
}

fn dedupe_catalog(catalog: &mut CoreCatalog) {
    dedupe_by_id(&mut catalog.publishers, |item| item.id.0.to_string());
    dedupe_by_id(&mut catalog.sources, |item| item.id.0.to_string());
    dedupe_by_id(&mut catalog.citations, |item| item.id.0.to_string());
    dedupe_by_id(&mut catalog.entity_types, |item| item.id.0.to_string());
    dedupe_by_id(&mut catalog.entities, |item| item.id.0.to_string());
    dedupe_by_id(&mut catalog.identity_links, |item| {
        format!(
            "{}:{}:{}",
            item.kind, item.canonical_id.0, item.external_id.value
        )
    });
    dedupe_by_id(&mut catalog.mapping_records, |item| item.id.clone());
}

fn dedupe_by_id<T, F>(items: &mut Vec<T>, mut key_fn: F)
where
    F: FnMut(&T) -> String,
{
    let mut seen = BTreeSet::new();
    items.retain(|item| seen.insert(key_fn(item)));
}

fn collect_files_recursive(
    input: &Path,
    predicate: fn(&Path) -> bool,
) -> std::io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    collect_files_recursive_inner(input, predicate, &mut out)?;
    out.sort();
    Ok(out)
}

fn collect_files_recursive_inner(
    path: &Path,
    predicate: fn(&Path) -> bool,
    out: &mut Vec<PathBuf>,
) -> std::io::Result<()> {
    if path.is_file() {
        if predicate(path) {
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
        collect_files_recursive_inner(&child, predicate, out)?;
    }

    Ok(())
}

fn is_pcgen_file(path: &Path) -> bool {
    matches!(
        extension_of(path).as_str(),
        "lst" | "pcc" | "pcg" | "kit" | "equipmod"
    )
}

fn is_herolab_file(path: &Path) -> bool {
    matches!(
        extension_of(path).as_str(),
        "user" | "xml" | "1st" | "dat" | "por" | "stock" | "zip"
    )
}

fn extension_of(path: &Path) -> String {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_else(|| "unknown".to_string())
}
