# Bookend

An exchange-agnostic market-making engine written in Rust.

It connects to centralized exchanges, consumes real-time order books, computes a fair price, quotes both sides, manages orders and inventory, and enforces risk limits. It is a **core trading engine only** — no dashboard, SaaS layer, billing or frontend.

| Document | What it covers |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Components, domain types, exchange trait, order lifecycle, risk, errors, lifecycle |
| [ROADMAP.md](ROADMAP.md) | Milestones, first coding tasks, definition of done |
| [TASKS.md](TASKS.md) | Task board with live status — start here to pick up work |

---

## Scope

**v1 does:**

- Spot markets only, one symbol per process
- Exchanges: Binance first, then Bybit, then OKX — all behind one `Exchange` trait
- Real-time order books → normalized market data → fair price → bid/ask quotes
- Order placement, cancellation, state tracking and reconciliation
- Balance, inventory and PnL tracking
- Risk limits and a kill switch
- Recovery from exchange/network failures and process restarts
- `paper`, `testnet` and `live` modes

**v1 does not do:**

- Perpetuals/derivatives, multiple symbols per process
- Web dashboard, auth, multi-tenancy, billing, mobile, marketing site
- Withdrawals or transfers between exchanges
- Automated arbitrage execution (detect + log only)
- AI-based strategies

> Build a reliable trading engine first. Everything else can come later.

---

## Architecture

```text
              Configuration (TOML + ENV)
                        │
                        ▼
                 Bot Controller
                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
   Market Data ──►  Strategy  ──►     Risk
    Manager          Engine          Engine
                (fair price, spread,   │
                 inventory skew)       ▼
                                  Quote Manager
                                       │
                                       ▼
                                  Order Manager
                                       │
                        ┌──────────────┼──────────────┐
                        ▼              ▼              ▼
                     Binance         Bybit           OKX
                     Adapter        Adapter         Adapter
                    (REST+WS)      (REST+WS)       (REST+WS)
```

The strategy never sees an exchange API. It works against internal types and the `Exchange` trait; adapters translate. Risk sits **between** strategy and execution — nothing reaches an exchange without passing it. Details in [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Stack

Rust (edition 2024), single process, Tokio runtime.

| Crate | Purpose |
|---|---|
| `tokio` | async runtime, channels, signals |
| `reqwest` (rustls) | REST |
| `tokio-tungstenite` (rustls) | WebSockets |
| `serde`, `serde_json`, `toml` | serialization, config |
| `rust_decimal` | all financial arithmetic — never `f64` |
| `tracing`, `tracing-subscriber` | structured logging |
| `thiserror`, `anyhow` | errors |
| `chrono`, `uuid`, `async-trait`, `clap` | time, ids, trait objects, CLI |

Later, only when needed: `sqlx`/`rusqlite`, `prometheus`, `axum`. No message broker in v1.

---

## Quick start

### Cargo

```bash
cp .env.example .env            # fill in API keys (not needed for paper mode)
cargo run -- --config configs/paper.toml
```

### Docker

```bash
cp .env.example .env
docker compose up --build                 # paper mode (default)
MM_MODE=testnet docker compose up --build # testnet
```

The image is a two-stage build (`rust:slim` → `debian:slim`, non-root, ~40 MB). Configs are mounted read-only from `./configs`, persistent state is written to `./data`. `RUST_LOG` controls log level; logs go to stdout as JSON.

Live mode additionally requires `ENABLE_LIVE_TRADING=true` in the environment and `MM_MODE=live`. `stop_grace_period` is set to 30 s so the engine can cancel open orders on `SIGTERM` before the container is killed — do not lower it.

---

## Modes

| Mode | Market data | Orders | Credentials |
|---|---|---|---|
| `paper` | real | simulated in-process | none required |
| `testnet` | testnet | real, on testnet endpoints | testnet keys only |
| `live` | real | real | live keys + `ENABLE_LIVE_TRADING=true` |

Live startup prints a loud banner (`WARNING: LIVE TRADING ENABLED — REAL ORDERS WILL BE SENT`). Production credentials must never be loadable in `paper` or `testnet` mode.

---

## Configuration

One TOML file per mode in `configs/`. Secrets come from the environment only — config references the variable name.

```toml
[bot]
name  = "bookend-btc"
mode  = "paper"             # paper | testnet | live
base  = "BTC"               # symbol is exchange-agnostic; adapters map to BTCUSDT / BTC-USDT / …
quote = "USDT"

[exchanges.binance]
enabled        = true
api_key_env    = "BINANCE_API_KEY"
api_secret_env = "BINANCE_API_SECRET"

[exchanges.bybit]
enabled = false

[exchanges.okx]
enabled            = false
api_passphrase_env = "OKX_API_PASSPHRASE"   # OKX needs a passphrase as well

[strategy]
name       = "basic_mm"
spread_bps = 100
order_size = "0.01"         # base quantity
post_only  = true

[strategy.fair_price]
method = "weighted_mid"     # local_mid | weighted_mid

[strategy.inventory]
target_ratio = 0.50
skew_factor  = 0.50

[strategy.levels]
count       = 1
spacing_bps = 50

[quote]
refresh_interval_ms  = 500
min_price_change_bps = 5
min_quantity_change  = "0.001"

[risk]                      # risk owns every hard limit — no duplicates elsewhere
max_order_size         = "0.1"     # base quantity
max_position           = "10"   # base quantity
max_open_orders        = 20
min_inventory_ratio    = 0.20
max_inventory_ratio    = 0.80
max_daily_loss         = "5000"     # quote currency, persisted across restarts
max_market_data_age_ms = 2000

[fees]                      # fallback only; real tiers are read from the account API at startup
maker_bps = 10
taker_bps = 20

[persistence]
path = "data/state.json"

[logging]
level  = "info"
format = "json"             # json | pretty
```

### Credentials

```text
BINANCE_API_KEY / BINANCE_API_SECRET
BYBIT_API_KEY   / BYBIT_API_SECRET
OKX_API_KEY     / OKX_API_SECRET / OKX_API_PASSPHRASE
ENABLE_LIVE_TRADING=true      # live mode only
```

Keys need **read + trade** only. Never enable withdrawals. Restrict by IP where the exchange supports it. Never commit `.env`. Never log keys, secrets or tokens.

---

## Principles

1. Exchange adapters are replaceable; the strategy works regardless of exchange.
2. The exchange is the source of truth for live orders. Local state is a cache.
3. Never trade on stale market data.
4. Never assume an order was filled without confirmation.
5. Never blindly retry an uncertain order submission.
6. Risk controls are part of the execution path, not a side check.
7. Decimal arithmetic for all financial values.
8. Paper → testnet → live, in that order, every time.
9. One Rust process. No distributed infrastructure until it is actually needed.
10. Correctness and recoverability before latency.

---

## Status

Design phase. Nothing is implemented yet. Next task: [TASKS.md → M1.1](TASKS.md#m1--skeleton).
