//! CLI entry point: parse args, load and validate config, initialise logging,
//! enforce the live-trading gate, then hand over to [`Bot`].

use std::path::PathBuf;

use clap::Parser;

use bookend::bot::{self, Bot};
use bookend::config::{Config, Mode};
use bookend::logging;

#[derive(Parser, Debug)]
#[command(name = "bookend", version, about = "Exchange-agnostic market-making engine")]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(long, env = "MM_CONFIG", value_name = "PATH")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config = Config::load(&cli.config)?;
    logging::init(&config.logging)?;

    bot::check_live_gate(config.bot.mode, std::env::var("ENABLE_LIVE_TRADING").ok().as_deref())?;
    if config.bot.mode == Mode::Live {
        bot::print_live_banner();
    }

    tracing::info!(path = %cli.config.display(), exchanges = ?config.exchanges.enabled(), "config loaded");

    Bot::new(config).run().await
}
