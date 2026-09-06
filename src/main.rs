//! Bookend — exchange-agnostic market-making engine.
//!
//! Entry point: parse the CLI, load and validate config, initialise logging,
//! enforce the live-trading gate, then hand over to [`bot::Bot`].

// Most of the domain model is defined ahead of the code that uses it.
// TODO: remove once M3 (paper market maker) wires everything together.
#![allow(dead_code)]

mod bot;
mod config;
mod events;
mod exchange;
mod logging;
mod market_data;
mod orders;
mod persistence;
mod portfolio;
mod quote;
mod risk;
mod strategy;
mod types;

use std::path::PathBuf;

use clap::Parser;

use crate::bot::Bot;
use crate::config::{Config, Mode};

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
