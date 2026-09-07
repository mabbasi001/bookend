//! Bot controller: startup, task supervision, shutdown, live-trading gate.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{Config, Mode};
use crate::exchange::Exchange;
use crate::exchange::binance::BinanceExchange;
use crate::market_data::manager::{BookWatch, MarketDataManager};
use crate::types::{ExchangeId, MarketInfo, Symbol};

/// Startup steps that fail for a while (exchange down, DNS) are retried this often.
const STARTUP_RETRY: Duration = Duration::from_secs(5);
/// Book log line rate limit.
const BOOK_LOG_INTERVAL: Duration = Duration::from_secs(1);

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

        // --- exchanges -------------------------------------------------------
        let exchanges = self.build_exchanges()?;

        // --- market metadata (retried: nothing works without it) -------------
        let mut markets: HashMap<ExchangeId, MarketInfo> = HashMap::new();
        for ex in &exchanges {
            let Some(market) = self.load_market(ex.as_ref()).await else {
                return self.finish();
            };
            info!(
                exchange = %ex.id(),
                native = %market.native_symbol,
                tick = %market.price_tick,
                step = %market.quantity_step,
                min_qty = %market.min_quantity,
                min_notional = %market.min_notional,
                "market loaded"
            );
            markets.insert(ex.id(), market);
        }

        // --- market data -----------------------------------------------------
        let max_age = Duration::from_millis(self.config.risk.max_market_data_age_ms);
        let mut market_data = MarketDataManager::new(max_age);
        let mut watches: Vec<(ExchangeId, BookWatch)> = Vec::new();
        for ex in &exchanges {
            let rx = ex.subscribe_market_data(&self.symbol).await?;
            let watch = market_data.attach(ex.id(), rx, self.shutdown.clone());
            watches.push((ex.id(), watch));
        }

        // M3+: strategy, risk, quote manager, order manager, portfolio.

        info!(status = "RUNNING", "ready");
        self.log_books(watches).await;
        self.finish()
    }

    fn build_exchanges(&self) -> anyhow::Result<Vec<Arc<dyn Exchange>>> {
        let mut out: Vec<Arc<dyn Exchange>> = Vec::new();
        for (id, cfg) in self.config.exchanges.iter() {
            if !cfg.enabled {
                continue;
            }
            let ex: Arc<dyn Exchange> = match id {
                ExchangeId::Binance => {
                    Arc::new(BinanceExchange::new(self.config.bot.mode, cfg.credentials.clone())?)
                }
                other => anyhow::bail!("exchange {other} is not implemented yet"),
            };
            out.push(ex);
        }
        Ok(out)
    }

    /// Retry until it works or shutdown is requested (`None`).
    async fn load_market(&self, ex: &dyn Exchange) -> Option<MarketInfo> {
        loop {
            let attempt = tokio::select! {
                _ = self.shutdown.cancelled() => return None,
                r = ex.get_market(&self.symbol) => r,
            };
            match attempt {
                Ok(m) => return Some(m),
                Err(e) if e.is_retryable_read() => {
                    warn!(exchange = %ex.id(), error = %e, retry_in = ?STARTUP_RETRY, "market metadata unavailable");
                    tokio::select! {
                        _ = self.shutdown.cancelled() => return None,
                        _ = tokio::time::sleep(STARTUP_RETRY) => {}
                    }
                }
                Err(e) => {
                    tracing::error!(exchange = %ex.id(), error = %e, "cannot load market metadata; shutting down");
                    self.shutdown.cancel();
                    return None;
                }
            }
        }
    }

    /// Log every book change, rate-limited per exchange, until shutdown.
    /// Stands in for the strategy until M3.
    async fn log_books(&self, watches: Vec<(ExchangeId, BookWatch)>) {
        for (exchange, mut watch) in watches {
            let shutdown = self.shutdown.clone();
            tokio::spawn(async move {
                let mut last_log = tokio::time::Instant::now() - BOOK_LOG_INTERVAL;
                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        changed = watch.changed() => {
                            if changed.is_err() {
                                return;
                            }
                        }
                    }
                    let Some(book) = watch.borrow_and_update().clone() else { continue };
                    if last_log.elapsed() < BOOK_LOG_INTERVAL {
                        continue;
                    }
                    last_log = tokio::time::Instant::now();
                    if let (Some(bid), Some(ask), Some(mid)) =
                        (book.best_bid(), book.best_ask(), book.mid())
                    {
                        info!(
                            %exchange,
                            bid = %bid.price,
                            ask = %ask.price,
                            mid = %mid.normalize(),
                            bid_qty = %bid.quantity,
                            ask_qty = %ask.quantity,
                            seq = book.sequence,
                            age_ms = book.received.elapsed().as_millis() as u64,
                            "book"
                        );
                    }
                }
            });
        }
        self.shutdown.cancelled().await;
    }

    fn finish(&self) -> anyhow::Result<()> {
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

    #[test]
    fn unimplemented_exchange_is_a_startup_error() {
        let mut config = Config::parse(include_str!("../configs/paper.toml")).unwrap();
        config.exchanges.bybit.enabled = true;
        let err = match Bot::new(config).build_exchanges() {
            Ok(_) => panic!("bybit must not build yet"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("bybit"));
    }
}
