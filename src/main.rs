mod commands;

use clap::{Parser, Subcommand};
use commands::{
    convert::ConvertArgs, import_herolab::ImportHerolabArgs, import_pcgen::ImportPcgenArgs,
    reconcile_apply::ReconcileApplyArgs, reconcile_inspect::ReconcileInspectArgs,
    reconcile_review::ReconcileReviewArgs,
};

#[derive(Parser, Debug)]
#[command(name = "artisan-cli")]
#[command(about = "CLI tooling for Artisan format conversion and reconciliation inspection")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Import HeroLab data and produce parse output plus reconciliation/inference hook report.
    ImportHerolab(ImportHerolabArgs),
    /// Import PCGen data and produce parse output plus reconciliation/inference hook report.
    ImportPcgen(ImportPcgenArgs),
    /// Convert between supported input formats and canonical core TOML.
    Convert(ConvertArgs),
    /// Inspect parsed candidates and inferred type buckets for reconciliation planning.
    ReconcileInspect(ReconcileInspectArgs),
    /// Build or update a manual reconciliation review state file.
    ReconcileReview(ReconcileReviewArgs),
    /// Apply accepted review decisions into core mapping/link records.
    ReconcileApply(ReconcileApplyArgs),
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Command::ImportHerolab(args) => commands::import_herolab::run(args),
        Command::ImportPcgen(args) => commands::import_pcgen::run(args),
        Command::Convert(args) => commands::convert::run(args),
        Command::ReconcileInspect(args) => commands::reconcile_inspect::run(args),
        Command::ReconcileReview(args) => commands::reconcile_review::run(args),
        Command::ReconcileApply(args) => commands::reconcile_apply::run(args),
    };

    if let Err(err) = result {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}
