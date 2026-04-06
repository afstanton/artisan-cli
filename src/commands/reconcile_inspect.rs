use std::{collections::BTreeMap, fs, path::PathBuf};

use artisan_pcgen::PcgenLoader;

#[derive(clap::Args, Debug)]
pub struct ReconcileInspectArgs {
    #[arg(long, value_name = "PCGEN_LST_FILE")]
    pub pcgen_lst: PathBuf,
}

pub fn run(args: ReconcileInspectArgs) -> Result<(), String> {
    let input = fs::read_to_string(&args.pcgen_lst)
        .map_err(|e| format!("failed to read input {}: {e}", args.pcgen_lst.display()))?;

    let candidates = PcgenLoader::parse_entity_candidates(&input).map_err(|e| e.to_string())?;

    let mut by_type: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for candidate in &candidates {
        by_type
            .entry(candidate.entity_type_key.clone())
            .or_default()
            .push(candidate.entity.name.clone());
    }

    println!("reconciliation inspection");
    println!("  total entities: {}", candidates.len());
    println!("  inferred type buckets: {}", by_type.len());

    for (ty, names) in &by_type {
        println!("\n- type key: {}", ty);
        println!("  count: {}", names.len());
        for name in names.iter().take(10) {
            println!("  - {}", name);
        }
        if names.len() > 10 {
            println!("  - ... and {} more", names.len() - 10);
        }
    }

    let unresolved = by_type
        .get("pcgen:type:unresolved")
        .map_or(0usize, |items| items.len());
    if unresolved > 0 {
        println!(
            "\nwarning: {} entities have unresolved type keys and may need manual reconciliation.",
            unresolved
        );
    }

    Ok(())
}
