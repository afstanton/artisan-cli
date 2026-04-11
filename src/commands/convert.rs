use std::{fs, path::PathBuf};

use artisan_core::CoreCatalog;
use artisan_pcgen::PcgenLoader;
use artisan_toml::dump_catalog;

#[derive(clap::Args, Debug)]
pub struct ConvertArgs {
    #[arg(long, value_name = "PCGEN_LST_FILE")]
    pub from_pcgen_lst: Option<PathBuf>,

    #[arg(long, value_name = "OUT_TOML_FILE")]
    pub to_core_toml: PathBuf,
}

pub fn run(args: ConvertArgs) -> Result<(), String> {
    let Some(input_path) = args.from_pcgen_lst else {
        return Err(
            "currently supported: --from-pcgen-lst <file> --to-core-toml <file>".to_string(),
        );
    };

    let input = fs::read_to_string(&input_path)
        .map_err(|e| format!("failed to read input {}: {e}", input_path.display()))?;

    let entities = PcgenLoader::parse_entities(&input).map_err(|e| e.to_string())?;

    let catalog = CoreCatalog {
        entities,
        ..CoreCatalog::default()
    };

    let encoded = dump_catalog(&catalog).map_err(|e| e.to_string())?;
    fs::write(&args.to_core_toml, encoded).map_err(|e| {
        format!(
            "failed to write output {}: {e}",
            args.to_core_toml.display()
        )
    })?;

    println!(
        "wrote core catalog TOML with {} entities to {}",
        catalog.entities.len(),
        args.to_core_toml.display()
    );

    Ok(())
}
