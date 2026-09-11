//! Simulation test: a scripted book sequence through the whole quoting
//! pipeline — strategy → quote manager → order manager → paper exchange →
//! fills → portfolio — asserting quotes, fills, inventory and PnL.
//!
//! The engine loop itself lives in `bot.rs`; this test drives the same
//! components in the same order without the async plumbing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use bookend::config::{
    FairPriceConfig, FairPriceMethod, InventoryConfig, LevelsConfig, QuoteConfig, StrategyConfig,
};
use bookend::events::{MarketEvent, UserEvent};
use bookend::exchange::paper::PaperExchange;
use bookend::exchange::{Exchange, ExchangeError};
use bookend::orders::{OrderManager, Plan};
use bookend::portfolio::Portfolio;
use bookend::quote::{QuoteManager, Trigger};
use bookend::strategy::{self, StrategyContext};
use bookend::types::{
    Balance, ExchangeId, Fees, MarketInfo, Order, OrderBook, OrderId, OrderRequest, OrderStatus,
    PriceLevel, Quote, Side, Symbol,
};

fn d(s: &str) -> Decimal {
    s.parse().unwrap()
}

fn symbol() -> Symbol {
    Symbol::new("BTC", "USDT")
}

fn market() -> MarketInfo {
    MarketInfo {
        symbol: symbol(),
        native_symbol: "BTCUSDT".into(),
        price_tick: d("0.01"),
        quantity_step: d("1"),
        min_quantity: d("1"),
        min_notional: d("10"),
    }
}

fn book(bid: &str, ask: &str) -> Arc<OrderBook> {
    Arc::new(OrderBook {
        exchange: ExchangeId::Binance,
        symbol: symbol(),
        bids: vec![PriceLevel { price: d(bid), quantity: d("1000") }],
        asks: vec![PriceLevel { price: d(ask), quantity: d("1000") }],
        sequence: 1,
        timestamp: Utc::now(),
        received: Instant::now(),
    })
}

/// Market-data source stand-in; the test pushes books straight into the paper exchange.
struct Silent;

#[async_trait]
impl Exchange for Silent {
    fn id(&self) -> ExchangeId {
        ExchangeId::Binance
    }
    async fn get_market(&self, _: &Symbol) -> Result<MarketInfo, ExchangeError> {
        Ok(market())
    }
    async fn get_order_book(&self, _: &Symbol) -> Result<OrderBook, ExchangeError> {
        Err(ExchangeError::Unavailable)
    }
    async fn get_fees(&self, _: &Symbol) -> Result<Fees, ExchangeError> {
        Err(ExchangeError::Unavailable)
    }
    async fn get_balances(&self) -> Result<Vec<Balance>, ExchangeError> {
        Err(ExchangeError::Unavailable)
    }
    async fn get_open_orders(&self, _: &Symbol) -> Result<Vec<Order>, ExchangeError> {
        Err(ExchangeError::Unavailable)
    }
    async fn get_order(&self, _: &Symbol, _: &OrderId) -> Result<Order, ExchangeError> {
        Err(ExchangeError::Unavailable)
    }
    async fn place_order(&self, _: OrderRequest) -> Result<Order, ExchangeError> {
        Err(ExchangeError::Unavailable)
    }
    async fn cancel_order(&self, _: &Symbol, _: &OrderId) -> Result<(), ExchangeError> {
        Err(ExchangeError::Unavailable)
    }
    async fn cancel_all_orders(&self, _: &Symbol) -> Result<(), ExchangeError> {
        Err(ExchangeError::Unavailable)
    }
    async fn subscribe_market_data(
        &self,
        _: &Symbol,
    ) -> Result<mpsc::Receiver<MarketEvent>, ExchangeError> {
        Err(ExchangeError::Unavailable)
    }
    async fn subscribe_user_events(&self) -> Result<mpsc::Receiver<UserEvent>, ExchangeError> {
        Err(ExchangeError::Unavailable)
    }
}

fn strategy_config() -> StrategyConfig {
    StrategyConfig {
        name: "basic_mm".into(),
        spread_bps: 100,
        order_size: d("10"),
        post_only: true,
        fair_price: FairPriceConfig { method: FairPriceMethod::LocalMid, weights: HashMap::new() },
        inventory: InventoryConfig { target_ratio: d("0.5"), skew_factor: d("0.5") },
        levels: LevelsConfig { count: 1, spacing_bps: 50 },
    }
}

/// The pipeline under test, minus the async event plumbing.
struct Pipeline {
    key: ExchangeId,
    exchange: PaperExchange,
    user_rx: mpsc::Receiver<UserEvent>,
    strategy: Box<dyn strategy::Strategy>,
    quotes: QuoteManager,
    orders: OrderManager,
    portfolio: Portfolio,
    books: HashMap<ExchangeId, Arc<OrderBook>>,
    markets: HashMap<ExchangeId, MarketInfo>,
    fees: HashMap<ExchangeId, Fees>,
    config: StrategyConfig,
    fills: Vec<bookend::types::Fill>,
}

impl Pipeline {
    async fn new() -> Self {
        let key = ExchangeId::Binance;
        let fees = Fees::from_bps(10, 20);
        let exchange = PaperExchange::new(Arc::new(Silent), market(), fees, d("100"), d("10000"));
        let user_rx = exchange.subscribe_user_events().await.unwrap();
        let mut portfolio = Portfolio::new(symbol(), Utc::now());
        for b in exchange.get_balances().await.unwrap() {
            portfolio.on_balance(key, &b);
        }
        Self {
            key,
            exchange,
            user_rx,
            strategy: strategy::build(&strategy_config()).unwrap(),
            quotes: QuoteManager::new(&QuoteConfig {
                refresh_interval_ms: 0,
                min_price_change_bps: 5,
                min_quantity_change: d("1"),
            }),
            orders: OrderManager::new(symbol(), "test01", true),
            portfolio,
            books: HashMap::new(),
            markets: HashMap::from([(key, market())]),
            fees: HashMap::from([(key, fees)]),
            config: strategy_config(),
            fills: Vec::new(),
        }
    }

    async fn execute(&mut self, plan: Plan) {
        for o in plan.cancel {
            self.orders.on_cancel_requested(&o.client_order_id);
            match self
                .exchange
                .cancel_order(&symbol(), &OrderId::Client(o.client_order_id.clone()))
                .await
            {
                Ok(()) => {}
                Err(e) => self.orders.on_cancel_failed(&o.client_order_id, &e),
            }
        }
        for p in plan.place {
            self.orders.on_submitting(p.exchange, &p.request);
            match self.exchange.place_order(p.request.clone()).await {
                Ok(ack) => self.orders.on_placed(&ack),
                Err(e) => self.orders.on_place_failed(&p.request.client_order_id, &e),
            }
        }
    }

    async fn requote(&mut self, desired: Vec<Quote>, trigger: Trigger) -> Vec<Quote> {
        let Some(q) = self.quotes.filter(desired, trigger, Instant::now()) else { return vec![] };
        let plan = self.orders.plan(&q);
        self.execute(plan).await;
        q
    }

    /// Push a book: strategy → requote, then drain user events the way the engine does.
    async fn step(&mut self, bid: &str, ask: &str) -> Vec<Quote> {
        let book = book(bid, ask);
        self.exchange.on_book(book.clone()).await;
        self.drain_user_events().await;

        self.books.insert(self.key, book);
        let desired = {
            let portfolio = self.portfolio.snapshot();
            let ctx = StrategyContext {
                books: &self.books,
                markets: &self.markets,
                fees: &self.fees,
                portfolio: &portfolio,
                config: &self.config,
            };
            self.strategy.on_market_data(&ctx, self.key).unwrap()
        };
        let sent = self.requote(desired, Trigger::MarketData).await;
        self.drain_user_events().await;
        sent
    }

    async fn drain_user_events(&mut self) {
        while let Ok(ev) = self.user_rx.try_recv() {
            match ev {
                UserEvent::Balance(b) => self.portfolio.on_balance(self.key, &b),
                UserEvent::Order(o) => {
                    self.orders.on_order_update(&o);
                }
                UserEvent::Fill(f) => {
                    self.portfolio.on_fill(&f);
                    self.fills.push(f.clone());
                    let desired = {
                        let portfolio = self.portfolio.snapshot();
                        let ctx = StrategyContext {
                            books: &self.books,
                            markets: &self.markets,
                            fees: &self.fees,
                            portfolio: &portfolio,
                            config: &self.config,
                        };
                        self.strategy.on_fill(&ctx, &f)
                    };
                    if let Some(q) = desired {
                        self.requote(q, Trigger::Fill).await;
                    }
                }
            }
        }
    }

    fn open(&self) -> Vec<Order> {
        let mut v: Vec<Order> = self.orders.live_on(self.key).cloned().collect();
        v.sort_by_key(|o| o.client_order_id.clone());
        v
    }
}

#[tokio::test]
async fn quote_fill_requote_and_pnl_round_trip() {
    let mut p = Pipeline::new().await;

    // 1. Balanced-ish start: 100 BTC + 10 000 USDT at fair 100.10 → ratio ≈ 0.50.
    let sent = p.step("100.00", "100.20").await;
    assert_eq!(sent.len(), 1);
    let q0 = &sent[0];
    // fair 100.10, half spread 0.5005 → bid 99.59 (down), ask 100.61 (up); skew ≈ 0.
    assert_eq!(q0.bid_price, d("99.59"));
    assert_eq!(q0.ask_price, d("100.61"));
    let open = p.open();
    assert_eq!(open.len(), 2, "{open:?}");
    assert!(open.iter().all(|o| o.status == OrderStatus::Open));

    // 2. Small drift (< 5 bps): no requote, orders untouched.
    let before: Vec<String> = p.open().iter().map(|o| o.client_order_id.clone()).collect();
    let sent = p.step("100.01", "100.21").await;
    assert!(sent.is_empty());
    let after: Vec<String> = p.open().iter().map(|o| o.client_order_id.clone()).collect();
    assert_eq!(before, after);

    // 3. Ask collapses onto our bid → paper fills the buy at 99.59.
    let _ = p.step("99.50", "99.59").await;
    assert_eq!(p.fills.len(), 1, "{:?}", p.fills);
    let f = &p.fills[0];
    assert_eq!((f.side, f.price, f.quantity), (Side::Buy, d("99.59"), d("10")));
    assert_eq!(f.fee, d("0.9959"), "10 bps of 995.9");

    let snap = p.portfolio.snapshot();
    assert_eq!(snap.position.quantity, d("10"));
    assert_eq!(snap.position.avg_cost, d("99.59"));
    assert_eq!(snap.position.fees, d("0.9959"));
    assert_eq!(snap.total().base, d("110"));
    assert_eq!(snap.total().quote, d("10000") - d("995.9") - d("0.9959"));

    // Fill triggered a requote around the new (lower) fair with heavier inventory:
    // both sides live again, and the bid is below the new fair's half spread.
    let open = p.open();
    assert_eq!(open.len(), 2, "{open:?}");
    let bid = open.iter().find(|o| o.side == Side::Buy).unwrap();
    let ask = open.iter().find(|o| o.side == Side::Sell).unwrap();
    assert!(bid.price.unwrap() < d("99.545"), "bid {:?} must sit below fair 99.545", bid.price);
    assert!(ask.price.unwrap() > d("99.545"));
    let ask_price = ask.price.unwrap();

    // 4. Bid rallies through our ask → sell fills; realized PnL = (ask − 99.59) × 10 − fees.
    let _ = p.step(&ask_price.to_string(), &(ask_price + d("0.20")).to_string()).await;
    assert_eq!(p.fills.len(), 2, "{:?}", p.fills);
    let s = &p.fills[1];
    assert_eq!((s.side, s.price, s.quantity), (Side::Sell, ask_price, d("10")));

    let snap = p.portfolio.snapshot();
    assert_eq!(snap.position.quantity, Decimal::ZERO);
    assert_eq!(snap.position.realized_pnl, (ask_price - d("99.59")) * d("10"));
    let sell_fee = Fees::from_bps(10, 20).maker_fee(ask_price * d("10"));
    assert_eq!(snap.position.fees, d("0.9959") + sell_fee);
    assert_eq!(snap.daily.net(), snap.position.realized_pnl - snap.position.fees);
    assert_eq!(snap.total().base, d("100"), "flat again");
    // Round trip: bought at 99.59, sold at ask; quote balance reflects it minus both fees.
    assert_eq!(
        snap.total().quote,
        d("10000") - d("995.9") - d("0.9959") + ask_price * d("10") - sell_fee
    );

    // Still quoting two sides afterwards.
    assert_eq!(p.open().len(), 2);
}

#[tokio::test]
async fn empty_quotes_pull_all_orders_and_cancel_all_plan_is_clean() {
    let mut p = Pipeline::new().await;
    p.step("100.00", "100.20").await;
    assert_eq!(p.open().len(), 2);

    let plan = p.orders.cancel_all_plan();
    assert_eq!(plan.cancel.len(), 2);
    p.execute(plan).await;
    p.drain_user_events().await;
    assert!(p.open().is_empty());
    assert!(p.exchange.get_open_orders(&symbol()).await.unwrap().is_empty());
    let bal = p.exchange.get_balances().await.unwrap();
    assert!(bal.iter().all(|b| b.locked.is_zero()), "{bal:?}");
}
