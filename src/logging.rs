//! `tracing` initialisation. `RUST_LOG` overrides `[logging].level`.

use tracing_subscriber::EnvFilter;

use crate::config::{LogFormat, LoggingConfig};

pub fn init(cfg: &LoggingConfig) -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new(&cfg.level))?;

    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_target(false);

    match cfg.format {
        LogFormat::Json => builder.json().flatten_event(true).init(),
        LogFormat::Pretty => builder.init(),
    }
    Ok(())
}
