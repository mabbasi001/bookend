//! Bot controller: startup, the engine loop, shutdown, live-trading gate.
//!
//! Engine loop (one task, no shared mutable state):
//!
//! ```text
//! book changed ──► strategy.on_market_data ──► quote manager ──► order manager plan ──► exchanges
//! user event   ──► order manager / portfolio ──► strategy.on_fill ──► (same path) ──► persist
//! tick         ──► staleness guard (cancel on stale venues), status log
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{Config, Mode};
use crate::events::UserEvent;
use crate::exchange::binance::BinanceExchange;
use crate::exchange::paper::PaperExchange;
use crate::exchange::{Exchange, ExchangeError};
use crate::market_data::manager::{BookWatch, MarketDataManager};
use crate::orders::{ClientIdGen, OrderManager, Plan};
use crate::persistence::{State, Store};
use crate::portfolio::{Portfolio, PortfolioSnapshot};
use crate::quote::{QuoteManager, Trigger};
use crate::strategy::fair_price::fair_price;
use crate::strategy::{self, Strategy, StrategyContext};
use crate::types::{ExchangeId, Fees, MarketInfo, OrderBook, OrderId, OrderStatus, Quote, Symbol};

/// Startup steps that fail for a while (exchange down, DNS) are retried this often.
const STARTUP_RETRY: Duration = Duration::from_secs(5);
const TICK: Duration = Duration::from_secs(1);
const STATUS_INTERVAL: Duration = Duration::from_secs(10);
/// How long shutdown waits for cancel confirmations before persisting and exiting.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Live gate
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Bot
// ---------------------------------------------------------------------------

/// One venue as the engine sees it: `key` is the market-data exchange (what
/// books and quotes are tagged with); `exec` is where orders go — the real
/// adapter, or a [`PaperExchange`] wrapped around it in paper mode.
struct Venue {
    key: ExchangeId,
    exec: Arc<dyn Exchange>,
    market: MarketInfo,
    fees: Fees,
}

enum EngineEvent {
    Book(ExchangeId),
    User(ExchangeId, UserEvent),
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
        let run_id = ClientIdGen::run_id_now();
        info!(
            version = env!("CARGO_PKG_VERSION"),
            mode = %self.config.bot.mode,
            symbol = %self.symbol,
            name = %self.config.bot.name,
            %run_id,
            "starting"
        );

        let token = self.shutdown.clone();
        tokio::spawn(async move {
            let signal = wait_for_signal().await;
            info!(signal, "shutdown");
            token.cancel();
        });

        // --- persisted state ---------------------------------------------------
        let store = Store::new(&self.config.persistence.path);
        let saved = store.load()?;

        // --- venues ------------------------------------------------------------
        let Some(venues) = self.build_venues().await? else {
            return self.finish();
        };

        // --- portfolio ---------------------------------------------------------
        let mut portfolio = Portfolio::new(self.symbol.clone(), Utc::now());
        if let Some(saved) = &saved
            && self.config.bot.mode != Mode::Paper
        {
            portfolio.restore(saved.position, saved.daily, Utc::now());
            info!(position = ?saved.position, daily = ?saved.daily, "position restored");
        }
        for v in &venues {
            let balances = v
                .exec
                .get_balances()
                .await
                .map_err(|e| anyhow::anyhow!("cannot load balances from {}: {e}", v.key))?;
            for b in &balances {
                portfolio.on_balance(v.key, b);
            }
            let h = portfolio.snapshot().holdings_on(v.key);
            info!(exchange = %v.key, base = %h.base, quote = %h.quote, "balances loaded");
        }

        // --- orders ------------------------------------------------------------
        let mut orders =
            OrderManager::new(self.symbol.clone(), run_id.clone(), self.config.strategy.post_only);
        for v in &venues {
            let open = v
                .exec
                .get_open_orders(&self.symbol)
                .await
                .map_err(|e| anyhow::anyhow!("cannot load open orders from {}: {e}", v.key))?;
            let n = orders.adopt(open);
            if n > 0 {
                info!(exchange = %v.key, adopted = n, "open orders adopted from previous run");
            }
        }

        // --- streams -----------------------------------------------------------
        let (events_tx, events_rx) = mpsc::channel::<EngineEvent>(1024);
        let max_age = Duration::from_millis(self.config.risk.max_market_data_age_ms);
        let mut market_data = MarketDataManager::new(max_age);
        for v in &venues {
            let user_rx = v.exec.subscribe_user_events().await?;
            self.forward_user_events(v.key, user_rx, events_tx.clone());

            let md_rx = v.exec.subscribe_market_data(&self.symbol).await?;
            let watch = market_data.attach(v.key, md_rx, self.shutdown.clone());
            self.forward_book_changes(v.key, watch, events_tx.clone());
        }
        drop(events_tx);

        // --- engine ------------------------------------------------------------
        let strategy = strategy::build(&self.config.strategy)?;
        info!(strategy = strategy.name(), status = "RUNNING", "ready");

        let mut engine = Engine {
            config: self.config.clone(),
            symbol: self.symbol.clone(),
            run_id,
            venues,
            market_data,
            books: HashMap::new(),
            strategy,
            quotes: QuoteManager::new(&self.config.quote),
            orders,
            portfolio,
            store,
            last_status: Instant::now(),
        };
        engine.run(events_rx, self.shutdown.clone()).await;
        self.finish()
    }

    /// Real adapters, wrapped in a paper exchange when `mode = paper`.
    /// `None` when shutdown was requested while retrying.
    async fn build_venues(&self) -> anyhow::Result<Option<Vec<Venue>>> {
        let mut venues = Vec::new();
        for (id, cfg) in self.config.exchanges.iter() {
            if !cfg.enabled {
                continue;
            }
            let real: Arc<dyn Exchange> = match id {
                ExchangeId::Binance => {
                    Arc::new(BinanceExchange::new(self.config.bot.mode, cfg.credentials.clone())?)
                }
                other => anyhow::bail!("exchange {other} is not implemented yet"),
            };

            let Some(market) = self.load_market(real.as_ref()).await else {
                return Ok(None);
            };
            info!(
                exchange = %id,
                native = %market.native_symbol,
                tick = %market.price_tick,
                step = %market.quantity_step,
                min_qty = %market.min_quantity,
                min_notional = %market.min_notional,
                "market loaded"
            );

            let config_fees =
                Fees::from_bps(self.config.fees.maker_bps, self.config.fees.taker_bps);
            let fees = match real.get_fees(&self.symbol).await {
                Ok(f) => f,
                Err(e) => {
                    warn!(exchange = %id, error = %e, maker_bps = %config_fees.maker_bps, "using configured fees");
                    config_fees
                }
            };

            let exec: Arc<dyn Exchange> = if self.config.bot.mode == Mode::Paper {
                Arc::new(PaperExchange::new(
                    real,
                    market.clone(),
                    fees,
                    self.config.paper.initial_base,
                    self.config.paper.initial_quote,
                ))
            } else {
                real
            };
            venues.push(Venue { key: id, exec, market, fees });
        }
        Ok(Some(venues))
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
                    error!(exchange = %ex.id(), error = %e, "cannot load market metadata; shutting down");
                    self.shutdown.cancel();
                    return None;
                }
            }
        }
    }

    /// Not tied to the shutdown token on purpose: the shutdown sequence still
    /// needs cancel confirmations. The task ends when the exchange stream or
    /// the engine goes away.
    fn forward_user_events(
        &self,
        key: ExchangeId,
        mut rx: mpsc::Receiver<UserEvent>,
        tx: mpsc::Sender<EngineEvent>,
    ) {
        tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                if tx.send(EngineEvent::User(key, ev)).await.is_err() {
                    break;
                }
            }
        });
    }

    /// Book changes are coalesced: a dropped notification is covered by the next.
    fn forward_book_changes(
        &self,
        key: ExchangeId,
        mut watch: BookWatch,
        tx: mpsc::Sender<EngineEvent>,
    ) {
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    changed = watch.changed() => if changed.is_err() { break },
                }
                watch.borrow_and_update();
                if let Err(mpsc::error::TrySendError::Closed(_)) =
                    tx.try_send(EngineEvent::Book(key))
                {
                    break;
                }
            }
        });
    }

    fn finish(&self) -> anyhow::Result<()> {
        info!(code = 0, "exit");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Engine loop
// ---------------------------------------------------------------------------

struct Engine {
    config: Arc<Config>,
    symbol: Symbol,
    run_id: String,
    venues: Vec<Venue>,
    market_data: MarketDataManager,
    /// Fresh, valid books only — what the strategy is allowed to see.
    books: HashMap<ExchangeId, Arc<OrderBook>>,
    strategy: Box<dyn Strategy>,
    quotes: QuoteManager,
    orders: OrderManager,
    portfolio: Portfolio,
    store: Store,
    last_status: Instant,
}

impl Engine {
    async fn run(&mut self, mut events: mpsc::Receiver<EngineEvent>, shutdown: CancellationToken) {
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tick.tick() => self.on_tick().await,
                ev = events.recv() => match ev {
                    Some(EngineEvent::Book(key)) => self.on_book(key).await,
                    Some(EngineEvent::User(key, ev)) => self.on_user(key, ev).await,
                    None => {
                        error!("all event sources ended");
                        break;
                    }
                },
            }
        }

        self.shutdown_sequence(&mut events).await;
    }

    fn venue(&self, key: ExchangeId) -> Option<&Venue> {
        self.venues.iter().find(|v| v.key == key)
    }

    /// Strategy inputs: cloned per call; small maps, and it keeps the borrow
    /// checker out of the event handlers.
    fn context_parts(
        &self,
    ) -> (HashMap<ExchangeId, MarketInfo>, HashMap<ExchangeId, Fees>, PortfolioSnapshot) {
        (
            self.venues.iter().map(|v| (v.key, v.market.clone())).collect(),
            self.venues.iter().map(|v| (v.key, v.fees)).collect(),
            self.portfolio.snapshot(),
        )
    }

    // --- events ---------------------------------------------------------------

    async fn on_book(&mut self, key: ExchangeId) {
        match self.market_data.fresh(key) {
            Some(book) => {
                self.books.insert(key, book);
            }
            None => {
                self.books.remove(&key);
                return;
            }
        }
        let (markets, fees, portfolio) = self.context_parts();
        let ctx = StrategyContext {
            books: &self.books,
            markets: &markets,
            fees: &fees,
            portfolio: &portfolio,
            config: &self.config.strategy,
        };
        if let Some(q) = self.strategy.on_market_data(&ctx, key) {
            self.requote(q, Trigger::MarketData).await;
        }
    }

    async fn on_user(&mut self, key: ExchangeId, ev: UserEvent) {
        match ev {
            UserEvent::Balance(b) => self.portfolio.on_balance(key, &b),
            UserEvent::Order(update) => {
                let Some(order) = self.orders.on_order_update(&update) else { return };
                let (markets, fees, portfolio) = self.context_parts();
                let ctx = StrategyContext {
                    books: &self.books,
                    markets: &markets,
                    fees: &fees,
                    portfolio: &portfolio,
                    config: &self.config.strategy,
                };
                let quotes = self.strategy.on_order_update(&ctx, &order);
                let lost_liquidity = matches!(
                    order.status,
                    OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Expired
                );
                if let Some(q) = quotes {
                    self.requote(q, Trigger::OrderEvent).await;
                } else if lost_liquidity {
                    // A side went missing without new input from the strategy:
                    // re-plan the current quotes so the order manager replaces it.
                    let current = self.quotes.current().to_vec();
                    let plan = self.orders.plan(&current);
                    self.execute(plan).await;
                }
            }
            UserEvent::Fill(fill) => {
                self.portfolio.on_fill(&fill);
                info!(
                    exchange = %key,
                    side = %fill.side,
                    price = %fill.price,
                    quantity = %fill.quantity,
                    fee = %fill.fee,
                    client_order_id = %fill.client_order_id,
                    "fill"
                );
                let (markets, fees, portfolio) = self.context_parts();
                self.log_pnl(&portfolio);
                let ctx = StrategyContext {
                    books: &self.books,
                    markets: &markets,
                    fees: &fees,
                    portfolio: &portfolio,
                    config: &self.config.strategy,
                };
                if let Some(q) = self.strategy.on_fill(&ctx, &fill) {
                    self.requote(q, Trigger::Fill).await;
                }
                self.persist();
            }
        }
    }

    async fn on_tick(&mut self) {
        // Staleness guard: never leave orders resting on a venue we cannot see.
        let stale: Vec<ExchangeId> = self
            .venues
            .iter()
            .map(|v| v.key)
            .filter(|k| self.market_data.fresh(*k).is_none() && self.orders.open_count(*k) > 0)
            .collect();
        for key in stale {
            warn!(exchange = %key, freshness = ?self.market_data.freshness(key), "market data not fresh; cancelling orders");
            self.books.remove(&key);
            let cancel = Plan {
                cancel: self
                    .orders
                    .live_on(key)
                    .filter(|o| o.status != OrderStatus::CancelRequested)
                    .cloned()
                    .collect(),
                place: Vec::new(),
            };
            self.execute(cancel).await;
            self.quotes.reset();
        }

        if self.last_status.elapsed() >= STATUS_INTERVAL {
            self.last_status = Instant::now();
            let snap = self.portfolio.snapshot();
            let fair = self.fair();
            let open: usize = self.venues.iter().map(|v| self.orders.open_count(v.key)).sum();
            info!(
                fair = %opt(fair.map(|f| f.normalize())),
                inventory_ratio = %opt(fair.and_then(|f| snap.inventory_ratio(f)).map(|r| r.round_dp(4))),
                base = %snap.total().base,
                quote = %snap.total().quote.round_dp(2),
                open_orders = open,
                fresh_books = self.books.len(),
                "status"
            );
            self.log_pnl(&snap);
        }
    }

    // --- quoting --------------------------------------------------------------

    async fn requote(&mut self, desired: Vec<Quote>, trigger: Trigger) {
        // M5: risk engine check goes here, between strategy and quote manager.
        let Some(quotes) = self.quotes.filter(desired, trigger, Instant::now()) else { return };
        for q in &quotes {
            info!(
                exchange = %q.exchange,
                bid = %q.bid_price,
                ask = %q.ask_price,
                bid_qty = %q.bid_quantity,
                ask_qty = %q.ask_quantity,
                trigger = ?trigger,
                "quote"
            );
        }
        let plan = self.orders.plan(&quotes);
        self.execute(plan).await;
    }

    async fn execute(&mut self, plan: Plan) {
        for order in plan.cancel {
            let Some(exec) = self.venue(order.exchange).map(|v| v.exec.clone()) else { continue };
            self.orders.on_cancel_requested(&order.client_order_id);
            let id = OrderId::Client(order.client_order_id.clone());
            match exec.cancel_order(&self.symbol, &id).await {
                Ok(()) => info!(
                    exchange = %order.exchange,
                    client_order_id = %order.client_order_id,
                    side = %order.side,
                    price = %opt(order.price),
                    "cancel requested"
                ),
                Err(e) => {
                    log_exchange_error(&e, order.exchange, "cancel failed");
                    self.orders.on_cancel_failed(&order.client_order_id, &e);
                }
            }
        }
        for p in plan.place {
            let Some(exec) = self.venue(p.exchange).map(|v| v.exec.clone()) else {
                warn!(exchange = %p.exchange, "no venue for placement; dropped");
                continue;
            };
            self.orders.on_submitting(p.exchange, &p.request);
            match exec.place_order(p.request.clone()).await {
                Ok(ack) => {
                    info!(
                        exchange = %p.exchange,
                        client_order_id = %ack.client_order_id,
                        side = %ack.side,
                        price = %opt(ack.price),
                        quantity = %ack.quantity,
                        status = ?ack.status,
                        "order placed"
                    );
                    self.orders.on_placed(&ack);
                }
                Err(e) => {
                    log_exchange_error(&e, p.exchange, "placement failed");
                    self.orders.on_place_failed(&p.request.client_order_id, &e);
                }
            }
        }
    }

    // --- helpers --------------------------------------------------------------

    fn fair(&self) -> Option<Decimal> {
        fair_price(&self.config.strategy.fair_price, &self.books)
    }

    fn log_pnl(&self, snap: &PortfolioSnapshot) {
        let fair = self.fair();
        let pos = snap.position;
        info!(
            position = %pos.quantity,
            avg_cost = %pos.avg_cost.normalize(),
            realized = %pos.realized_pnl.round_dp(4),
            unrealized = %opt(fair.map(|f| pos.unrealized_pnl(f).round_dp(4))),
            fees = %pos.fees.round_dp(4),
            net = %opt(fair.map(|f| pos.net_pnl(f).round_dp(4))),
            daily_net = %snap.daily.net().round_dp(4),
            "pnl"
        );
    }

    fn persist(&self) {
        let state = State {
            run_id: self.run_id.clone(),
            position: self.portfolio.position(),
            daily: self.portfolio.daily(),
            open_orders: self.orders.orders().cloned().collect(),
            updated_at: Utc::now(),
        };
        if let Err(e) = self.store.save(&state) {
            error!(path = %self.store.path().display(), error = %e, "cannot persist state");
        }
    }

    async fn shutdown_sequence(&mut self, events: &mut mpsc::Receiver<EngineEvent>) {
        info!("stopping strategy");
        let plan = self.orders.cancel_all_plan();
        let n = plan.cancel.len();
        if n > 0 {
            info!(orders = n, "cancelling open orders");
            self.execute(plan).await;
            let deadline = tokio::time::Instant::now() + SHUTDOWN_DRAIN;
            while self.orders.orders().any(|o| o.status.is_live()) {
                match tokio::time::timeout_at(deadline, events.recv()).await {
                    Ok(Some(EngineEvent::User(_, UserEvent::Order(u)))) => {
                        self.orders.on_order_update(&u);
                    }
                    Ok(Some(EngineEvent::User(key, UserEvent::Balance(b)))) => {
                        self.portfolio.on_balance(key, &b)
                    }
                    Ok(Some(EngineEvent::User(_, UserEvent::Fill(f)))) => {
                        self.portfolio.on_fill(&f)
                    }
                    Ok(Some(EngineEvent::Book(_))) => {}
                    Ok(None) | Err(_) => break,
                }
            }
            let remaining = self.orders.orders().filter(|o| o.status.is_live()).count();
            if remaining > 0 {
                warn!(
                    remaining,
                    "orders not confirmed cancelled before exit; reconcile on next start"
                );
            } else {
                info!("all orders cancelled");
            }
        }
        self.persist();
        let snap = self.portfolio.snapshot();
        self.log_pnl(&snap);
    }
}

fn opt<T: std::fmt::Display>(v: Option<T>) -> String {
    v.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
}

fn log_exchange_error(e: &ExchangeError, exchange: ExchangeId, what: &str) {
    match e {
        ExchangeError::InvalidOrder(_)
        | ExchangeError::InsufficientBalance
        | ExchangeError::OrderNotFound => warn!(%exchange, error = %e, "{what}"),
        _ => error!(%exchange, error = %e, "{what}"),
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
    async fn unimplemented_exchange_is_a_startup_error() {
        let mut config = Config::parse(include_str!("../configs/paper.toml")).unwrap();
        config.exchanges.binance.enabled = false;
        config.exchanges.bybit.enabled = true;
        let err = match Bot::new(config).build_venues().await {
            Ok(_) => panic!("bybit must not build yet"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("bybit"));
    }
}
