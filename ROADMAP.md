# Roadmap

One exchange, one pipeline, paper first. Each milestone is usable on its own before the next starts.

```text
Skeleton → Binance market data → Paper MM → Binance testnet → Risk → Binance live → Bybit → OKX → Multi-exchange → Advanced MM → Backtesting
```

| # | Milestone | Deliverable | Done when |
|---|---|---|---|
| 1 | Skeleton ✅ | Cargo project, config, logging, domain types, `Exchange` trait, Docker build | `cargo run -- --config configs/paper.toml` starts and exits cleanly on Ctrl-C |
| 2 | Binance market data ✅ | REST + WS adapter, order-book sync, `MarketInfo`, staleness | live `BTC/USDT` best bid/ask/mid printed continuously, survives a forced disconnect |
| 3 | Paper market maker | fair price, spread, quotes, `PaperExchange` fills, inventory, PnL, persistence | bot runs for hours in paper mode with sane PnL/inventory logs |
| 4 | Binance testnet | place/cancel/query, user stream, balances, reconciliation, client ids | real orders on testnet; restart mid-run reconciles correctly |
| 5 | Risk | all `[risk]` limits, kill switch, automatic kill conditions | every limit has a test that trips it; kill switch cancels everything |
| 6 | Binance live | live gate, crash recovery, tiny size | runs unattended for a week at minimal exposure without manual intervention |
| 7 | Bybit | full adapter | strategy code unchanged |
| 8 | OKX | full adapter (incl. passphrase, 32-char client ids) | strategy code unchanged |
| 9 | Multi-exchange | weighted fair price, per-exchange quotes, global inventory | quoting on all three from one process |
| 10 | Advanced MM | dynamic spread, multi-level, volatility and imbalance adjustments | measurable improvement over `basic_mm` in paper mode |
| 11 | Backtesting | historical data, fill model, PnL/drawdown/uptime report | strategy comparison on recorded data |

Do not start a milestone until the previous one is stable. Do not start Bybit or OKX before Binance runs live.

---

## Milestone 1 — Skeleton

Tasks, in order:

1. `cargo new bookend`; add the dependencies from [ARCHITECTURE.md §2](ARCHITECTURE.md#cargotoml).
2. Create the module tree: `config`, `types`, `events`, `exchange`, `market_data`, `strategy`, `quote`, `orders`, `risk`, `portfolio`, `persistence`, `bot`.
3. `config.rs`: load TOML from `--config`, resolve `*_env` secrets from the environment, validate (mode, symbol, limits > 0, spread > 2 × maker fee). Fail fast on anything missing.
4. `types.rs`: `ExchangeId`, `Symbol`, `Side`, `OrderType`, `OrderStatus`, `OrderId`, `PriceLevel`, `OrderBook`, `MarketInfo`, `Balance`, `OrderRequest`, `Order`, `Fill`, `Quote`.
5. `exchange/mod.rs`: the `Exchange` trait and `ExchangeError`. No implementations yet.
6. `tracing` setup: JSON when `logging.format = "json"`, pretty otherwise; `RUST_LOG` overrides the config level.
7. `bot.rs`: startup skeleton, `CancellationToken`, `SIGINT`/`SIGTERM` handling, clean exit.
8. Live-mode gate: refuse to start in `live` without `ENABLE_LIVE_TRADING=true`; print the banner when it is set.
9. `docker compose up --build` runs the same binary in paper mode.

Expected output:

```text
INFO  bookend starting version=0.1.0 mode=paper symbol=BTC/USDT
INFO  config loaded path=configs/paper.toml exchanges=[binance]
INFO  ready status=RUNNING
^C
INFO  shutdown signal=SIGINT
INFO  exit code=0
```

## Milestone 2 — Binance market data

1. `exchange/binance/client.rs`: base URLs per mode, server-time offset, signer (unused until M4).
2. `rest.rs`: `exchangeInfo` → `MarketInfo`; depth snapshot.
3. `ws.rs`: depth stream, snapshot + delta sync with sequence validation, backoff reconnection, resync on gap.
4. `mapper.rs`: Binance payloads → `OrderBook`, symbol rendering `BTC/USDT` ↔ `BTCUSDT`.
5. `market_data/manager.rs`: `watch` channel of latest `Arc<OrderBook>`, staleness tracking.
6. Print `bid / ask / mid` on every update; kill the network and confirm resync.

## Milestone 3 — Paper market maker

1. `strategy/fair_price.rs`: `local_mid`. 
2. `strategy/basic_mm.rs`: spread → bid/ask, rounding to `MarketInfo`, fee sanity check.
3. `strategy/inventory.rs`: ratio and skew.
4. `quote.rs`: requote rules and throttle.
5. `exchange/paper.rs`: resting orders, fill when the book crosses, maker fee.
6. `orders/`: state machine and desired-vs-actual diff against the paper exchange.
7. `portfolio/`: balances, position, PnL from fills.
8. `persistence.rs`: state file.

Expected output:

```text
INFO  book     exchange=binance bid=1.0010 ask=1.0030 mid=1.0020 age_ms=12
INFO  quote    exchange=binance bid=0.9970 ask=1.0070 bid_qty=1000 ask_qty=1000 inventory_ratio=0.52
INFO  fill     exchange=paper side=buy price=0.9970 quantity=1000 fee=0.997
INFO  pnl      realized=12.40 unrealized=-3.10 fees=4.20 net=5.10 inventory_ratio=0.55
```

Per-task breakdown and current status for every milestone live in [TASKS.md](TASKS.md).

---

## Definition of done (core engine)

The engine is considered usable when, from a TOML config, it can on **every** enabled exchange:

- receive and validate live order books, detect stale data, survive disconnects and resync
- compute a fair price and post two-sided limit quotes with inventory skew
- submit, cancel, track and reconcile orders, including after a restart and after lost responses
- track balances, position and PnL from confirmed fills
- enforce all `[risk]` limits and execute the kill switch
- run in `paper`, `testnet` and gated `live` mode from the same binary and the same Docker image
- produce structured logs with no secrets in them
