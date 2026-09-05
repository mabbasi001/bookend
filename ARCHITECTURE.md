# Architecture

Design reference for the MM engine. Read [README.md](readme.md) first for scope, stack and configuration.

Contents

1. [Data flow](#1-data-flow)
2. [Repository layout](#2-repository-layout)
3. [Domain types](#3-domain-types)
4. [Exchange trait and adapters](#4-exchange-trait-and-adapters)
5. [Market data](#5-market-data)
6. [Strategy](#6-strategy)
7. [Inventory](#7-inventory)
8. [Quote manager](#8-quote-manager)
9. [Order manager](#9-order-manager)
10. [Risk engine](#10-risk-engine)
11. [Portfolio](#11-portfolio)
12. [Errors and retries](#12-errors-and-retries)
13. [Events and channels](#13-events-and-channels)
14. [Lifecycle](#14-lifecycle)
15. [Persistence](#15-persistence)
16. [Logging and metrics](#16-logging-and-metrics)
17. [Testing](#17-testing)
18. [Future](#18-future)

---

## 1. Data flow

```text
Exchange WS ──► Exchange Adapter ──► Market Data Manager ──► Strategy ──► Risk ──► Quote Manager ──► Order Manager ──► Exchange Adapter ──► Exchange REST
                                                              ▲                                          │
                                                              └──────── fills / order updates ◄──────────┘
                                                                        (private WS, via adapter)
```

Each stage has one job and does not reach around the next:

| Stage | Owns | Must not |
|---|---|---|
| Adapter | REST/WS, auth, signing, reconnection, symbol/precision mapping, rate limits | contain strategy logic |
| Market Data Manager | local order books, sequence validation, staleness | place orders |
| Strategy | fair price, spread, skew → desired quotes | call an exchange |
| Risk | allow/reject desired quotes, kill switch | be bypassed |
| Quote Manager | requote throttling, diffing desired vs current quotes | talk to REST directly |
| Order Manager | order state machine, submission, cancellation, reconciliation | decide prices |
| Portfolio | balances, position, inventory ratio, PnL from confirmed fills | assume fills |

The strategy works only against internal types and the `Exchange` trait. `binance_client.place_order(...)` never appears outside `src/exchange/binance/`.

---

## 2. Repository layout

```text
bookend/
├── Cargo.toml
├── readme.md · ARCHITECTURE.md · ROADMAP.md
├── Dockerfile · docker-compose.yml · .dockerignore · .env.example
├── configs/            paper.toml · testnet.toml · live.example.toml
├── data/               runtime state (git-ignored)
├── src/
│   ├── main.rs         CLI (--config), wiring
│   ├── config.rs
│   ├── events.rs       MarketEvent, UserEvent
│   ├── types.rs        Symbol, Order, Quote, … (see §3)
│   ├── exchange/
│   │   ├── mod.rs      Exchange trait, ExchangeError, ExchangeId
│   │   ├── paper.rs    in-process simulated exchange (paper mode + tests)
│   │   ├── binance/    client.rs · rest.rs · ws.rs · mapper.rs
│   │   ├── bybit/      (same shape)
│   │   └── okx/        (same shape)
│   ├── market_data/    manager.rs · orderbook.rs
│   ├── strategy/       mod.rs (trait) · basic_mm.rs · fair_price.rs · inventory.rs
│   ├── quote.rs
│   ├── orders/         manager.rs · state.rs · reconcile.rs
│   ├── risk/           manager.rs · kill_switch.rs
│   ├── portfolio/      balances.rs · position.rs · pnl.rs
│   ├── persistence.rs
│   └── bot.rs          controller: startup, shutdown, task supervision
└── tests/              strategy.rs · risk.rs · reconciliation.rs
```

Single crate. Split into a workspace only if adapter compile times become a problem.

### Cargo.toml

```toml
[package]
name    = "bookend"
version = "0.1.0"
edition = "2024"

[dependencies]
tokio              = { version = "1", features = ["rt-multi-thread", "macros", "sync", "time", "net", "signal"] }
reqwest            = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
tokio-tungstenite  = { version = "0.27", features = ["rustls-tls-native-roots"] }
serde              = { version = "1", features = ["derive"] }
serde_json         = "1"
toml               = "0.9"
tracing            = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt", "json"] }
thiserror          = "2"
anyhow             = "1"
uuid               = { version = "1", features = ["v4", "serde"] }
chrono             = { version = "0.4", features = ["serde"] }
rust_decimal       = { version = "1", features = ["serde"] }
async-trait        = "0.1"
clap               = { version = "4", features = ["derive", "env"] }
```

Inline tables must stay on one line (TOML 1.0). Check versions when implementation starts.

---

## 3. Domain types

Exchange-agnostic. Adapters map to and from these; nothing else touches exchange payloads.

```rust
pub enum ExchangeId { Binance, Bybit, Okx, Paper }

pub struct Symbol { pub base: String, pub quote: String }   // "BTC"/"USDT"; adapters render BTCUSDT, BTC-USDT, …

pub enum Side      { Buy, Sell }
pub enum OrderType { Limit, Market }

pub struct PriceLevel { pub price: Decimal, pub quantity: Decimal }

pub struct OrderBook {
    pub exchange:  ExchangeId,
    pub symbol:    Symbol,
    pub bids:      Vec<PriceLevel>,      // descending
    pub asks:      Vec<PriceLevel>,      // ascending
    pub sequence:  u64,                  // exchange update id, for gap detection
    pub timestamp: DateTime<Utc>,        // exchange time
    pub received:  Instant,              // local time, for staleness
}

pub struct MarketInfo {
    pub symbol:        Symbol,
    pub native_symbol: String,           // what the exchange calls it
    pub price_tick:    Decimal,
    pub quantity_step: Decimal,
    pub min_quantity:  Decimal,
    pub min_notional:  Decimal,
}

pub struct Balance { pub asset: String, pub total: Decimal, pub available: Decimal, pub locked: Decimal }

pub enum OrderId { Exchange(String), Client(String) }   // lookups work with either

pub struct OrderRequest {
    pub symbol:          Symbol,
    pub side:            Side,
    pub order_type:      OrderType,
    pub price:           Option<Decimal>,
    pub quantity:        Decimal,
    pub client_order_id: String,         // always set by the engine (§9)
    pub post_only:       bool,           // default true for a market maker
}

pub struct Order {
    pub exchange:          ExchangeId,
    pub exchange_order_id: Option<String>,   // None until the exchange acknowledges
    pub client_order_id:   String,
    pub symbol:            Symbol,
    pub side:              Side,
    pub order_type:        OrderType,
    pub price:             Option<Decimal>,
    pub quantity:          Decimal,
    pub filled_quantity:   Decimal,
    pub status:            OrderStatus,      // §9
    pub created_at:        DateTime<Utc>,
    pub updated_at:        DateTime<Utc>,
}

pub struct Fill {
    pub exchange:        ExchangeId,
    pub client_order_id: String,
    pub trade_id:        String,
    pub side:            Side,
    pub price:           Decimal,
    pub quantity:        Decimal,
    pub fee:             Decimal,
    pub fee_asset:       String,
    pub timestamp:       DateTime<Utc>,
}

pub struct Quote {
    pub exchange:     ExchangeId,        // strategy can quote each exchange differently
    pub symbol:       Symbol,
    pub bid_price:    Decimal,
    pub bid_quantity: Decimal,
    pub ask_price:    Decimal,
    pub ask_quantity: Decimal,
}
```

All prices, quantities, notionals, fees and PnL are `rust_decimal::Decimal`. `f64` is not used for money anywhere.

---

## 4. Exchange trait and adapters

```rust
#[async_trait]
pub trait Exchange: Send + Sync {
    fn id(&self) -> ExchangeId;

    // Market
    async fn get_market(&self, symbol: &Symbol) -> Result<MarketInfo, ExchangeError>;
    async fn get_order_book(&self, symbol: &Symbol) -> Result<OrderBook, ExchangeError>;
    async fn get_fees(&self, symbol: &Symbol) -> Result<Fees, ExchangeError>;

    // Account
    async fn get_balances(&self) -> Result<Vec<Balance>, ExchangeError>;
    async fn get_open_orders(&self, symbol: &Symbol) -> Result<Vec<Order>, ExchangeError>;
    async fn get_order(&self, symbol: &Symbol, id: &OrderId) -> Result<Order, ExchangeError>;

    // Execution
    async fn place_order(&self, request: OrderRequest) -> Result<Order, ExchangeError>;
    async fn cancel_order(&self, symbol: &Symbol, id: &OrderId) -> Result<(), ExchangeError>;
    async fn cancel_all_orders(&self, symbol: &Symbol) -> Result<(), ExchangeError>;

    // Streams — the adapter owns the socket, auth, heartbeats, reconnection and resync.
    // The engine only ever sees normalized events.
    async fn subscribe_market_data(&self, symbol: &Symbol) -> Result<mpsc::Receiver<MarketEvent>, ExchangeError>;
    async fn subscribe_user_events(&self) -> Result<mpsc::Receiver<UserEvent>, ExchangeError>;
}
```

The trait stays small. Exchange-specific features (Binance post-only flags, Bybit order flags, OKX account modes) live inside the adapter and are expressed through the common request where possible.

### Adapter responsibilities

Every adapter (`src/exchange/<name>/`) handles, and hides:

- REST auth and request signing, server-time sync / `recvWindow`
- WebSocket connect, auth, heartbeats, subscription management
- Reconnection with exponential backoff: `CONNECTED → DISCONNECTED → BACKOFF → RECONNECT → RESUBSCRIBE → RESYNC → CONNECTED`
- Order-book snapshot + delta assembly and gap detection (§5)
- Symbol, order-type, timestamp and error mapping
- Price/quantity rounding to `MarketInfo` before submission
- Rate limits (request weight, order count, WS limits) — per exchange, inside the adapter
- Client-order-id constraints (§9)

Files: `client.rs` (shared state, signer), `rest.rs`, `ws.rs`, `mapper.rs` (exchange types ↔ internal types).

### Paper exchange

`src/exchange/paper.rs` implements the same trait against a live order book feed with a simple fill model (fill when the book crosses the resting price; maker fee applied). It serves both `paper` mode and tests — there is no separate "mock exchange".

---

## 5. Market data

**Manager responsibilities:** hold the latest `OrderBook` per exchange, validate sequence integrity, track staleness, publish `MarketEvent`s. It never places orders.

**Snapshot + delta sync** (per adapter):

```text
connect → subscribe → buffer deltas → REST snapshot → drop deltas ≤ snapshot seq → apply rest → live
```

On any sequence gap: mark the book invalid, stop quoting on that exchange, resync. Never apply deltas across a gap.

**Staleness.** Before every quote: `now - book.received <= risk.max_market_data_age_ms`, else stop quoting on that exchange. The stale check also feeds the automatic kill conditions (§10).

---

## 6. Strategy

```rust
pub trait Strategy: Send {
    fn on_market_data(&mut self, ctx: &StrategyContext, book: &OrderBook) -> Option<Vec<Quote>>;
    fn on_fill(&mut self, ctx: &StrategyContext, fill: &Fill) -> Option<Vec<Quote>>;
    fn on_order_update(&mut self, ctx: &StrategyContext, order: &Order) -> Option<Vec<Quote>>;
}

pub struct StrategyContext<'a> {
    pub books:     &'a HashMap<ExchangeId, OrderBook>,   // only fresh, valid books
    pub portfolio: &'a PortfolioSnapshot,
    pub fees:      &'a HashMap<ExchangeId, Fees>,
    pub config:    &'a StrategyConfig,
}
```

Strategies are synchronous, pure decision-makers: state in, desired quotes out. They do not know about channels, REST, or throttling. `None` means "no change".

### Fair price

- `local_mid` — `(best_bid + best_ask) / 2` on one exchange.
- `weighted_mid` — weighted average of mids across enabled exchanges. Exchanges whose book is stale or invalid are dropped and the remaining weights renormalised. Weights are configured now, liquidity-derived later.

### Basic market maker (`basic_mm`)

```text
half_spread = fair × spread_bps / 20_000
bid = fair − half_spread − skew
ask = fair + half_spread − skew          (skew > 0 when overweight base, pushes both quotes down)
```

Then round to `price_tick` / `quantity_step`, and enforce `min_quantity` / `min_notional`. Fees must be inside the spread: `spread_bps > 2 × maker_bps` or the quote is not worth posting.

**Levels.** v1 posts one bid and one ask. `strategy.levels.count > 1` adds levels at `± spacing_bps × n`.

Future strategies plug into the same trait: inventory-aware, volatility-aware, multi-level, cross-exchange.

---

## 7. Inventory

```text
inventory_ratio = base_value / (base_value + quote_value)      // e.g. 70,000 / (70,000 + 30,000) = 0.70
```

Tracked per exchange and globally. The strategy pulls quotes toward the target:

| Ratio vs target | Effect |
|---|---|
| above (overweight base) | lower bid and ask → sell more, buy less |
| below | raise bid and ask |
| outside `risk.min/max_inventory_ratio` | risk engine blocks the side that would worsen it |

The skew formula lives in `strategy/inventory.rs` and nowhere else, so it can change without touching the rest of the strategy. `strategy.inventory` holds target and skew factor; the hard bounds belong to `[risk]`.

---

## 8. Quote manager

Turns the stream of desired quotes into a rate-limited stream of order intents.

Requote only when at least one holds:

- price moved ≥ `min_price_change_bps`
- quantity changed ≥ `min_quantity_change`
- inventory changed (fill received)
- an order was filled, cancelled or rejected
- an order exceeded its max age

and never more often than `refresh_interval_ms`. Otherwise the current orders stay — cancelling and replacing on every tick burns rate limit and queue position.

---

## 9. Order manager

Reconciles **desired** vs **actual** orders and drives each order through its state machine.

```text
Desired:  BUY 1000 @ 1.000   SELL 1000 @ 1.010
Actual:   BUY 1000 @ 0.998   SELL 1000 @ 1.010
Action:   cancel BUY, place BUY @ 1.000, keep SELL
```

### State machine

```text
Created ──► Submitting ──► Open ──► PartiallyFilled ──► Filled
                │            │            │
                │            ├────────────┴──► CancelRequested ──► Cancelled
                │            │                       │
                │            │                       └──► Filled      (cancel rejected: already filled)
                │            └──► Expired
                └──► Rejected
Submitting ──► Unknown ──► (reconcile) ──► Open | Filled | Rejected | not found
```

`CancelRequested → Filled` is the most common race in market making and must be handled, not treated as an error. `Unknown` is the state after a timeout on submission.

### Client order ids

The engine generates every id; exchange ids are stored once acknowledged. Format: alphanumeric only, ≤ 32 chars (the OKX limit; Binance allows 36 with `-_`, Bybit 36), e.g. `mmbn<run-id><seq>`. Lookups by client id are what make reconciliation possible.

### Idempotency

```text
submit → response lost → state = Unknown
       → get_order(Client(id))
           found     → adopt it
           not found → safe to retry with the SAME client id
```

Never re-submit with a fresh id on an uncertain result. Never blindly retry.

### Exchange vs local state

The exchange is authoritative for live orders; local state is a cache. After a restart, reconnect, or any unexpected error the manager calls `get_open_orders` and reconciles before quoting resumes. Orders on the exchange that the engine does not recognise (from a previous run) are adopted by client-id prefix or cancelled, per config.

---

## 10. Risk engine

Sits between strategy and quote manager. Every desired quote passes through it; anything rejected is logged with the reason.

**Limits** (all in `[risk]`, see README): `max_order_size`, `max_position`, `max_open_orders`, `min/max_inventory_ratio`, `max_daily_loss`, `max_market_data_age_ms`. Daily loss is persisted (§15) so a crash cannot reset it.

**Kill switch** → stop strategy → cancel all orders → confirm → safe mode (no quoting, connections kept, state observable). Triggered by: config flag, `SIGUSR1`, CLI, and automatically on:

- stale market data past a grace period
- exchange disconnected past a grace period
- balance or position differing from local state beyond tolerance
- daily loss exceeded
- fair price moved more than a safety threshold in one step
- N consecutive order errors
- reconciliation failure or invalid local state

Leaving safe mode requires a restart.

---

## 11. Portfolio

- **Balances** per asset per exchange: total / available / locked. Refreshed from the exchange and updated by `UserEvent::BalanceUpdate`.
- **Position** (spot): base quantity, average cost, exposure in quote, inventory ratio — per exchange and global. Updated **only from confirmed fills**.
- **PnL**: realized, unrealized (at fair price), fees, net. Fees come from the fill, not from config; `[fees]` is a fallback for paper mode and for estimating spread viability.

---

## 12. Errors and retries

```rust
#[derive(Debug, thiserror::Error)]
pub enum ExchangeError {
    #[error("network: {0}")]            Network(#[source] anyhow::Error),
    #[error("timeout")]                 Timeout,          // outcome unknown — never plain-retry an order
    #[error("rate limited, retry in {0:?}")] RateLimited(Option<Duration>),
    #[error("authentication failed")]   Authentication,
    #[error("invalid order: {0}")]       InvalidOrder(String),
    #[error("insufficient balance")]     InsufficientBalance,
    #[error("order not found")]          OrderNotFound,
    #[error("exchange unavailable")]     Unavailable,      // maintenance / 5xx
    #[error("unknown: {0}")]             Unknown(String),
}
```

| Error | Reads | Order submission |
|---|---|---|
| `Network` | retry with backoff | treat as `Unknown` → reconcile (§9) |
| `Timeout` | retry | `Unknown` → reconcile |
| `RateLimited` | wait the given time | wait, then reconcile |
| `Authentication` | stop the exchange | stop |
| `InvalidOrder` | — | do not retry; log, fix rounding/limits |
| `InsufficientBalance` | — | refresh balances, re-evaluate |
| `OrderNotFound` | expected during reconciliation | treat as not placed / already gone |
| `Unavailable` | backoff | safe mode if persistent |

---

## 13. Events and channels

```rust
pub enum MarketEvent { Book(Arc<OrderBook>), Trade(Trade), Connected(ExchangeId), Disconnected(ExchangeId) }
pub enum UserEvent   { Order(Order), Fill(Fill), Balance(Balance) }
```

Tokio channels only — no broker in v1.

| Data | Channel | Why |
|---|---|---|
| latest order book per exchange | `watch` | the strategy only wants the newest book; `broadcast` would lag and drop |
| fills, order updates, balances | `mpsc` | must never be dropped; consumed in order |
| kill switch / shutdown | `watch<bool>` + `CancellationToken` | one writer, many readers |

Books are passed as `Arc<OrderBook>`; no cloning per tick.

---

## 14. Lifecycle

`src/bot.rs` owns startup, supervision and shutdown.

**Startup**

```text
load + validate config → init logging → live-mode gate (ENABLE_LIVE_TRADING, banner)
→ load persisted state → init exchange adapters → server-time sync → load MarketInfo + fees
→ load balances → load open orders → reconcile → subscribe user events → subscribe market data
→ wait for first valid book → start risk → start order manager → start quote manager → start strategy
→ RUNNING
```

Risk and execution are up **before** the strategy emits its first quote.

**Shutdown** (`SIGTERM`, `SIGINT`, kill switch, fatal error)

```text
stop strategy → cancel all open orders → wait for confirmations (≤ 20 s) → persist state → close sockets → exit
```

Cancel-on-shutdown is the default in every mode. The Docker `stop_grace_period` (30 s) exists for this step.

---

## 15. Persistence

v1: one JSON file (`[persistence].path`), written on every fill and on shutdown, read at startup.

Contents: run id, trading day + realized PnL for that day (for `max_daily_loss`), last known open orders by client id (for adopting orders after a restart), position and average cost. Everything else is rebuilt from the exchange. Swap for SQLite when the file gets awkward — not before.

---

## 16. Logging and metrics

`tracing` with structured fields; JSON to stdout in Docker, pretty locally.

```text
INFO  order_submitted exchange=binance symbol=BTC/USDT side=buy price=0.9971 quantity=1000 client_order_id=mmbn…
WARN  book_gap        exchange=binance expected=1042 got=1051 action=resync
ERROR order_rejected  exchange=binance reason="insufficient balance"
```

Never log keys, secrets, tokens, or full signed requests.

**Metrics** (later, Prometheus via `/metrics`): orders submitted/cancelled/filled/rejected, WS disconnects, API errors, market-data age, quote updates, inventory ratio, position value, realized/unrealized PnL, fees, order latency. A `/health` endpoint reports per-exchange connection state, market-data health, strategy state and order sync state.

---

## 17. Testing

| Layer | What | How |
|---|---|---|
| Unit | fair price, spread, skew, rounding, min notional, risk checks, PnL, fees, order state machine | plain `#[test]`, no I/O |
| Integration | adapters: auth, market data, WS, place/cancel/query | against testnet with keys from env; skipped when absent. Record fixtures for mapper tests |
| Simulation | full pipeline with `PaperExchange` on recorded or live books | `tests/strategy.rs` |
| Reconciliation | restart mid-flight, lost responses, cancel-vs-fill race | `tests/reconciliation.rs` with a scripted `PaperExchange` |

The `PaperExchange` is the test double; the same code runs paper mode.

---

## 18. Future

Only after the core is stable in live mode:

- Backtesting: historical books/trades, fill model, PnL/drawdown/quote-uptime report, strategy comparison
- Arbitrage: detect and log in v1; execution is a separate strategy module
- Rebalancing between exchanges: detect and report in v1; transfers add security, fee and latency risk
- Multi-exchange quoting with per-exchange prices and global inventory
- Dashboard / SaaS layer — last
