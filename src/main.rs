mod commands;

use clap::{Parser, Subcommand};
use commands::{convert::ConvertArgs, reconcile_inspect::ReconcileInspectArgs};

#[derive(Parser, Debug)]
#[command(name = "artisan-cli")]
#[command(about = "CLI tooling for Artisan format conversion and reconciliation inspection")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Convert between supported input formats and canonical core TOML.
    Convert(ConvertArgs),
    /// Inspect parsed candidates and inferred type buckets for reconciliation planning.
    ReconcileInspect(ReconcileInspectArgs),
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Command::Convert(args) => commands::convert::run(args),
        Command::ReconcileInspect(args) => commands::reconcile_inspect::run(args),
    };

    if let Err(err) = result {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}
