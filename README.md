# Bookend

Exchange-agnostic spot market-making engine in Rust.

It connects to centralized exchanges, keeps a validated local order book, computes a fair price, quotes both sides with inventory skew, manages orders through a proper state machine, tracks balances and PnL from confirmed fills, and enforces risk limits. It is a **core trading engine** — no dashboard, no SaaS layer, one process, Tokio.

> **Status: early development.** Paper mode runs end to end against live Binance market data. Testnet/live execution is in progress. Do not point this at real funds.

---

## What it does

| Area | Today |
|---|---|
| Market data | Binance spot depth stream with snapshot + delta sync, sequence-gap detection and resync, dead-socket detection, backoff reconnect |
| Fair price | local mid, or weighted mid across exchanges (weights renormalised over fresh books) |
| Strategy | `basic_mm`: fair ± half spread, shifted by inventory skew, rounded to tick/step, post-only safe, 1–N levels |
| Quoting | requote only on meaningful change (bps / quantity thresholds) or on fills, rate-limited |
| Orders | desired-vs-live diff → cancel/place plan; state machine covering partial fills, cancel/fill races and unknown outcomes; engine-generated client ids |
| Portfolio | balances per exchange, average-cost position, realized / unrealized / fees / daily PnL |
| Execution | paper exchange (simulated account, crossing-based fills, fees) wrapping the real feed |
| Safety | stale-data guard cancels resting orders, cancel-all on shutdown, state persisted across restarts, explicit live-trading gate |
| Modes | `paper` · `testnet` · `live` from the same binary and Docker image |

Planned: Binance order execution + user stream, risk engine and kill switch, Bybit, OKX, multi-exchange quoting, backtesting.

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

The strategy never sees an exchange API. It works on internal types through one `Exchange` trait; adapters translate. Everything financial is `rust_decimal::Decimal` — no floats.

```text
src/
├── main.rs            CLI (--config)
├── lib.rs
├── bot.rs             startup, engine loop, shutdown, live gate
├── config.rs          TOML sections, env secrets, validation
├── types.rs           Symbol, OrderBook, Order, Fill, Quote, MarketInfo, …
├── events.rs          MarketEvent / UserEvent
├── exchange/
│   ├── mod.rs         Exchange trait, ExchangeError
│   ├── paper.rs       simulated exchange (paper mode + tests)
│   └── binance/       client · rest · ws · mapper
├── market_data/       local order book, per-exchange manager + freshness
├── strategy/          Strategy trait, basic_mm, fair_price, inventory
├── quote.rs           requote thresholds and throttle
├── orders/            state machine, order manager
├── portfolio/         balances, position, PnL
├── persistence.rs     atomic JSON state file
└── risk/              (next)
```

---

## Quick start

### Docker (no Rust toolchain needed)

```bash
cp .env.example .env
docker compose up --build                 # paper mode (default)
MM_MODE=testnet docker compose up --build # testnet — needs testnet keys in .env
```

Configs are mounted read-only from `./configs`, state is written to `./data`, logs are JSON on stdout. `stop_grace_period` is 30 s so the engine can cancel its orders on `SIGTERM` — don't lower it.

### Cargo

```bash
cargo run -- --config configs/paper.toml
```

No local toolchain? `scripts/cargo.sh` runs cargo inside the official `rust` image (with clippy and rustfmt):

```bash
scripts/cargo.sh test
scripts/cargo.sh clippy --all-targets -- -D warnings
scripts/cargo.sh test --test binance_live -- --ignored   # hits Binance public API
```

### What you should see

```text
INFO starting            mode=paper symbol=BTC/USDT run_id=x3s8qy
INFO market loaded       exchange=binance tick=0.01 step=0.00001 min_notional=5
INFO balances loaded     exchange=binance base=0.5 quote=50000
INFO ready               strategy=basic_mm status=RUNNING
INFO market data connected exchange=binance
INFO quote               bid=78579.4 ask=78815.48 bid_qty=0.01 ask_qty=0.01
INFO order placed        client_order_id=mmbnx3s8qy000001 side=buy price=78579.4
INFO status              fair=78697.44 inventory_ratio=0.4404 open_orders=2 fresh_books=1
INFO pnl                 position=0 realized=0 unrealized=0 fees=0 net=0 daily_net=0
```

---

## Modes

| Mode | Market data | Orders | Credentials |
|---|---|---|---|
| `paper` | real | simulated in-process | none |
| `testnet` | testnet | real, testnet endpoints | testnet keys |
| `live` | real | real | live keys **and** `ENABLE_LIVE_TRADING=true` |

Endpoints are chosen by mode, never by which keys are present. Live mode refuses to start without the environment flag and prints a banner when it does.

---

## Configuration

One TOML per mode in `configs/`. Secrets are never in the file — each exchange section names the environment variables to read.

```toml
[bot]
name  = "bookend-btc"
mode  = "paper"          # paper | testnet | live
base  = "BTC"            # exchange-agnostic; adapters render BTCUSDT / BTC-USDT / …
quote = "USDT"

[exchanges.binance]
enabled = true           # keys from BINANCE_API_KEY / BINANCE_API_SECRET

[strategy]
name       = "basic_mm"
spread_bps = 100
order_size = "0.01"      # base quantity
post_only  = true

[strategy.fair_price]
method = "local_mid"     # local_mid | weighted_mid

[strategy.inventory]
target_ratio = 0.50
skew_factor  = 0.50

[quote]
refresh_interval_ms  = 500
min_price_change_bps = 5
min_quantity_change  = "0.001"

[risk]                   # every hard limit lives here
max_order_size         = "0.1"
max_position           = "10"
max_open_orders        = 20
min_inventory_ratio    = 0.20
max_inventory_ratio    = 0.80
max_daily_loss         = "200"      # quote currency, persisted across restarts
max_market_data_age_ms = 2000

[fees]                   # fallback until the account API supplies real tiers
maker_bps = 10
taker_bps = 20

[paper]                  # simulated account, paper mode only
initial_base  = "0.5"
initial_quote = "50000"

[persistence]
path = "data/state.json"

[logging]
level  = "info"          # RUST_LOG overrides
format = "json"          # json | pretty
```

Config is validated on start and fails fast: unknown keys, spread that doesn't cover two maker fees, inventory bounds out of order, order size above the risk limit, missing secrets in testnet/live.

### Credentials

```text
BINANCE_API_KEY / BINANCE_API_SECRET
BYBIT_API_KEY   / BYBIT_API_SECRET
OKX_API_KEY     / OKX_API_SECRET / OKX_API_PASSPHRASE
ENABLE_LIVE_TRADING=true      # live only
```

Keys need **read + trade** permissions only. Never enable withdrawals. Restrict by IP where supported. `.env` is git- and docker-ignored; secrets are redacted from every log and debug dump.

---

## Testing

```bash
scripts/cargo.sh test                                   # unit + simulation tests
scripts/cargo.sh test --test binance_live -- --ignored  # live public-API tests
```

- Unit tests cover config validation, rounding, fair price, skew, quote thresholds, the order state machine (including cancel/fill races), order diffing, PnL accounting, the paper exchange and the depth-sync state machine (with real Binance payloads as fixtures).
- `tests/strategy.rs` runs a scripted book sequence through the whole pipeline and asserts quotes, fills, inventory and PnL.
- `tests/binance_live.rs` checks market metadata, the order book and 20 in-sequence stream updates against production (ignored by default).

---

## Principles

1. Exchange adapters are replaceable; the strategy works regardless of exchange.
2. The exchange is the source of truth for live orders. Local state is a cache.
3. Never trade on stale market data.
4. Never assume an order was filled without confirmation.
5. Never blindly retry an uncertain order submission — reconcile by client id.
6. Risk controls sit in the execution path, not beside it.
7. Decimal arithmetic for all financial values.
8. Paper → testnet → live, in that order, every time.
9. One Rust process. No distributed infrastructure until it is actually needed.
10. Correctness and recoverability before latency.

---

## License

Private. All rights reserved.
