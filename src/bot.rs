//! Bot controller: startup, task supervision, shutdown, live-trading gate.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{Config, Mode};
use crate::types::Symbol;

#[derive(Debug, thiserror::Error)]
#[error("live mode requires ENABLE_LIVE_TRADING=true in the environment (got {got:?})")]
pub struct LiveGateError {
    got: Option<String>,
}

/// Refuse to start in `live` mode unless explicitly enabled.
pub fn check_live_gate(mode: Mode, enable_var: Option<&str>) -> Result<(), LiveGateError> {
    match (mode, enable_var.map(str::trim)) {
        (Mode::Live, Some("true")) => Ok(()),
        (Mode::Live, got) => Err(LiveGateError { got: got.map(str::to_owned) }),
        _ => Ok(()),
    }
}

pub fn print_live_banner() {
    eprintln!(
        "\n==========================================\n \
         WARNING: LIVE TRADING ENABLED\n \
         REAL ORDERS WILL BE SENT\n\
         ==========================================\n"
    );
    warn!(mode = "live", "live trading enabled — real orders will be sent");
}

pub struct Bot {
    config: Arc<Config>,
    symbol: Symbol,
    shutdown: CancellationToken,
}

impl Bot {
    pub fn new(config: Config) -> Self {
        let symbol = Symbol::new(&config.bot.base, &config.bot.quote);
        Self { config: Arc::new(config), symbol, shutdown: CancellationToken::new() }
    }

    /// Token that every task observes; cancelled on signal or fatal error.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub async fn run(self) -> anyhow::Result<()> {
        info!(
            version = env!("CARGO_PKG_VERSION"),
            mode = %self.config.bot.mode,
            symbol = %self.symbol,
            name = %self.config.bot.name,
            "starting"
        );

        let token = self.shutdown.clone();
        tokio::spawn(async move {
            let signal = wait_for_signal().await;
            info!(signal, "shutdown");
            token.cancel();
        });

        // M2+: initialise exchanges, market data, strategy, risk, orders here.

        info!(status = "RUNNING", "ready");
        self.shutdown.cancelled().await;

        // M3+: stop strategy, cancel open orders, wait, persist, close sockets.

        info!(code = 0, "exit");
        Ok(())
    }
}

#[cfg(unix)]
async fn wait_for_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};

    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = int.recv() => "SIGINT",
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() -> &'static str {
    tokio::signal::ctrl_c().await.expect("install Ctrl-C handler");
    "CTRL_C"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_gate_only_opens_on_exact_true() {
        assert!(check_live_gate(Mode::Live, Some("true")).is_ok());
        assert!(check_live_gate(Mode::Live, Some(" true ")).is_ok());
        assert!(check_live_gate(Mode::Live, None).is_err());
        assert!(check_live_gate(Mode::Live, Some("")).is_err());
        assert!(check_live_gate(Mode::Live, Some("1")).is_err());
        assert!(check_live_gate(Mode::Live, Some("TRUE")).is_err());
        assert!(check_live_gate(Mode::Live, Some("yes")).is_err());
    }

    #[test]
    fn live_gate_ignored_outside_live_mode() {
        assert!(check_live_gate(Mode::Paper, None).is_ok());
        assert!(check_live_gate(Mode::Testnet, Some("false")).is_ok());
    }

    #[tokio::test]
    async fn bot_exits_when_token_is_cancelled() {
        let config = Config::parse(include_str!("../configs/paper.toml")).unwrap();
        let bot = Bot::new(config);
        let token = bot.shutdown_token();
        let handle = tokio::spawn(bot.run());
        token.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
