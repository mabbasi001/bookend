//! In-process simulated exchange for `paper` mode and tests.
//!
//! Wraps a real exchange for market data and metadata, keeps its own account
//! (balances, resting orders) and fills orders when the wrapped book crosses
//! their price. Conservative by design: a resting bid fills only when the best
//! ask drops to or below it, so paper PnL understates queue-priority fills
//! rather than overstating them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::{Exchange, ExchangeError};
use crate::events::{MarketEvent, UserEvent};
use crate::types::{
    Balance, ExchangeId, Fees, Fill, MarketInfo, Order, OrderBook, OrderId, OrderRequest,
    OrderStatus, OrderType, Side, Symbol,
};

const USER_CHANNEL_CAPACITY: usize = 1024;

// ---------------------------------------------------------------------------
// Account
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Account {
    totals: HashMap<String, Decimal>,
    /// Reserved by resting orders.
    locked: HashMap<String, Decimal>,
}

impl Account {
    fn total(&self, asset: &str) -> Decimal {
        self.totals.get(asset).copied().unwrap_or_default()
    }

    fn locked(&self, asset: &str) -> Decimal {
        self.locked.get(asset).copied().unwrap_or_default()
    }

    fn available(&self, asset: &str) -> Decimal {
        self.total(asset) - self.locked(asset)
    }

    fn add(&mut self, asset: &str, delta: Decimal) {
        *self.totals.entry(asset.to_owned()).or_default() += delta;
    }

    fn lock(&mut self, asset: &str, delta: Decimal) {
        *self.locked.entry(asset.to_owned()).or_default() += delta;
    }

    fn balance(&self, asset: &str) -> Balance {
        Balance {
            asset: asset.to_owned(),
            total: self.total(asset),
            available: self.available(asset),
            locked: self.locked(asset),
        }
    }
}

struct State {
    account: Account,
    orders: HashMap<String, Order>,
    next_exchange_id: u64,
    next_trade_id: u64,
    last_book: Option<Arc<OrderBook>>,
    user_tx: Option<mpsc::Sender<UserEvent>>,
}

// ---------------------------------------------------------------------------
// Core: account + fill model, shared with the market-data tee task
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Core {
    symbol: Symbol,
    fees: Fees,
    state: Arc<Mutex<State>>,
}

impl Core {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("paper state poisoned")
    }

    async fn on_book(&self, book: Arc<OrderBook>) {
        let events = {
            let mut st = self.lock();
            st.last_book = Some(book.clone());
            self.match_orders(&mut st, &book)
        };
        self.emit(events).await;
    }

    fn match_orders(&self, st: &mut State, book: &OrderBook) -> Vec<UserEvent> {
        let (Some(best_bid), Some(best_ask)) = (book.best_bid(), book.best_ask()) else {
            return Vec::new();
        };
        let mut filled: Vec<String> = st
            .orders
            .values()
            .filter(|o| match (o.side, o.price) {
                (Side::Buy, Some(p)) => best_ask.price <= p,
                (Side::Sell, Some(p)) => best_bid.price >= p,
                _ => false,
            })
            .map(|o| o.client_order_id.clone())
            .collect();
        filled.sort();

        let mut events = Vec::new();
        for id in filled {
            let mut order = st.orders.remove(&id).expect("just listed");
            let price = order.price.expect("limit order");
            let qty = order.remaining_quantity();
            let notional = price * qty;
            let fee = self.fees.maker_fee(notional);
            let now = Utc::now();

            match order.side {
                Side::Buy => {
                    st.account.lock(&self.symbol.quote, -notional);
                    st.account.add(&self.symbol.quote, -notional - fee);
                    st.account.add(&self.symbol.base, qty);
                }
                Side::Sell => {
                    st.account.lock(&self.symbol.base, -qty);
                    st.account.add(&self.symbol.base, -qty);
                    st.account.add(&self.symbol.quote, notional - fee);
                }
            }

            order.filled_quantity = order.quantity;
            order.status = OrderStatus::Filled;
            order.updated_at = now;
            let trade_id = st.next_trade_id;
            st.next_trade_id += 1;

            debug!(client_order_id = %id, side = %order.side, %price, %qty, %fee, "paper fill");
            events.push(UserEvent::Fill(Fill {
                exchange: ExchangeId::Paper,
                client_order_id: id,
                trade_id: format!("P{trade_id}"),
                symbol: self.symbol.clone(),
                side: order.side,
                price,
                quantity: qty,
                fee,
                fee_asset: self.symbol.quote.clone(),
                timestamp: now,
            }));
            events.push(UserEvent::Order(order));
            events.push(UserEvent::Balance(st.account.balance(&self.symbol.base)));
            events.push(UserEvent::Balance(st.account.balance(&self.symbol.quote)));
        }
        events
    }

    async fn emit(&self, events: Vec<UserEvent>) {
        if events.is_empty() {
            return;
        }
        let tx = self.lock().user_tx.clone();
        match tx {
            Some(tx) => {
                for e in events {
                    if tx.send(e).await.is_err() {
                        warn!("paper user-event consumer gone");
                        return;
                    }
                }
            }
            None => debug!(n = events.len(), "paper user events dropped: no subscriber"),
        }
    }

    fn find_client_id(st: &State, id: &OrderId) -> Option<String> {
        match id {
            OrderId::Client(c) => st.orders.contains_key(c).then(|| c.clone()),
            OrderId::Exchange(e) => st
                .orders
                .values()
                .find(|o| o.exchange_order_id.as_deref() == Some(e))
                .map(|o| o.client_order_id.clone()),
        }
    }

    fn cancel_locked(&self, st: &mut State, client_order_id: &str) -> Option<Order> {
        let mut order = st.orders.remove(client_order_id)?;
        let price = order.price.expect("limit order");
        match order.side {
            Side::Buy => st.account.lock(&self.symbol.quote, -(price * order.remaining_quantity())),
            Side::Sell => st.account.lock(&self.symbol.base, -order.remaining_quantity()),
        }
        order.status = OrderStatus::Cancelled;
        order.updated_at = Utc::now();
        Some(order)
    }
}

// ---------------------------------------------------------------------------
// Exchange
// ---------------------------------------------------------------------------

pub struct PaperExchange {
    inner: Arc<dyn Exchange>,
    market: MarketInfo,
    core: Core,
}

impl PaperExchange {
    /// `inner` supplies market data and metadata; the account starts with the
    /// given balances.
    pub fn new(
        inner: Arc<dyn Exchange>,
        market: MarketInfo,
        fees: Fees,
        initial_base: Decimal,
        initial_quote: Decimal,
    ) -> Self {
        let symbol = market.symbol.clone();
        let mut account = Account::default();
        account.add(&symbol.base, initial_base);
        account.add(&symbol.quote, initial_quote);
        let state = State {
            account,
            orders: HashMap::new(),
            next_exchange_id: 1,
            next_trade_id: 1,
            last_book: None,
            user_tx: None,
        };
        Self { inner, market, core: Core { symbol, fees, state: Arc::new(Mutex::new(state)) } }
    }

    /// Feed a book: fills any resting order the book crosses. Public so tests
    /// can drive the fill model without a live stream.
    pub async fn on_book(&self, book: Arc<OrderBook>) {
        self.core.on_book(book).await;
    }

    fn symbol(&self) -> &Symbol {
        &self.core.symbol
    }
}

#[async_trait]
impl Exchange for PaperExchange {
    fn id(&self) -> ExchangeId {
        ExchangeId::Paper
    }

    async fn get_market(&self, symbol: &Symbol) -> Result<MarketInfo, ExchangeError> {
        if symbol != self.symbol() {
            return Err(ExchangeError::InvalidOrder(format!(
                "paper exchange only trades {}",
                self.symbol()
            )));
        }
        Ok(self.market.clone())
    }

    async fn get_order_book(&self, symbol: &Symbol) -> Result<OrderBook, ExchangeError> {
        self.inner.get_order_book(symbol).await
    }

    async fn get_fees(&self, _symbol: &Symbol) -> Result<Fees, ExchangeError> {
        Ok(self.core.fees)
    }

    async fn get_balances(&self) -> Result<Vec<Balance>, ExchangeError> {
        let st = self.core.lock();
        Ok(vec![st.account.balance(&self.symbol().base), st.account.balance(&self.symbol().quote)])
    }

    async fn get_open_orders(&self, _symbol: &Symbol) -> Result<Vec<Order>, ExchangeError> {
        Ok(self.core.lock().orders.values().cloned().collect())
    }

    async fn get_order(&self, _symbol: &Symbol, id: &OrderId) -> Result<Order, ExchangeError> {
        let st = self.core.lock();
        Core::find_client_id(&st, id)
            .and_then(|c| st.orders.get(&c).cloned())
            .ok_or(ExchangeError::OrderNotFound)
    }

    async fn place_order(&self, request: OrderRequest) -> Result<Order, ExchangeError> {
        if request.symbol != *self.symbol() {
            return Err(ExchangeError::InvalidOrder("wrong symbol".into()));
        }
        if request.order_type != OrderType::Limit {
            return Err(ExchangeError::InvalidOrder(
                "paper exchange supports limit orders only".into(),
            ));
        }
        let price = request
            .price
            .ok_or_else(|| ExchangeError::InvalidOrder("limit order needs a price".into()))?;
        if price <= Decimal::ZERO || request.quantity <= Decimal::ZERO {
            return Err(ExchangeError::InvalidOrder("price and quantity must be positive".into()));
        }
        if !self.market.is_price_aligned(price)
            || !self.market.is_quantity_aligned(request.quantity)
        {
            return Err(ExchangeError::InvalidOrder(format!(
                "price {price} / quantity {} not aligned to tick {} / step {}",
                request.quantity, self.market.price_tick, self.market.quantity_step
            )));
        }
        if !self.market.meets_minimums(price, request.quantity) {
            return Err(ExchangeError::InvalidOrder("below min quantity / min notional".into()));
        }

        let order = {
            let mut st = self.core.lock();
            if st.orders.contains_key(&request.client_order_id) {
                return Err(ExchangeError::InvalidOrder("duplicate client order id".into()));
            }
            if request.post_only
                && let Some(book) = &st.last_book
            {
                let crosses = match request.side {
                    Side::Buy => book.best_ask().is_some_and(|a| price >= a.price),
                    Side::Sell => book.best_bid().is_some_and(|b| price <= b.price),
                };
                if crosses {
                    return Err(ExchangeError::InvalidOrder("post-only order would take".into()));
                }
            }
            let (asset, needed) = match request.side {
                Side::Buy => (self.symbol().quote.clone(), price * request.quantity),
                Side::Sell => (self.symbol().base.clone(), request.quantity),
            };
            if st.account.available(&asset) < needed {
                return Err(ExchangeError::InsufficientBalance);
            }
            st.account.lock(&asset, needed);

            let now = Utc::now();
            let exchange_order_id = format!("P{}", st.next_exchange_id);
            st.next_exchange_id += 1;
            let order = Order {
                exchange: ExchangeId::Paper,
                exchange_order_id: Some(exchange_order_id),
                client_order_id: request.client_order_id.clone(),
                symbol: request.symbol,
                side: request.side,
                order_type: request.order_type,
                price: Some(price),
                quantity: request.quantity,
                filled_quantity: Decimal::ZERO,
                status: OrderStatus::Open,
                created_at: now,
                updated_at: now,
            };
            st.orders.insert(order.client_order_id.clone(), order.clone());
            order
        };
        self.core.emit(vec![UserEvent::Order(order.clone())]).await;
        Ok(order)
    }

    async fn cancel_order(&self, _symbol: &Symbol, id: &OrderId) -> Result<(), ExchangeError> {
        let cancelled = {
            let mut st = self.core.lock();
            let client_id = Core::find_client_id(&st, id).ok_or(ExchangeError::OrderNotFound)?;
            self.core.cancel_locked(&mut st, &client_id).ok_or(ExchangeError::OrderNotFound)?
        };
        self.core.emit(vec![UserEvent::Order(cancelled)]).await;
        Ok(())
    }

    async fn cancel_all_orders(&self, _symbol: &Symbol) -> Result<(), ExchangeError> {
        let events = {
            let mut st = self.core.lock();
            let mut ids: Vec<String> = st.orders.keys().cloned().collect();
            ids.sort();
            ids.into_iter()
                .filter_map(|id| self.core.cancel_locked(&mut st, &id))
                .map(UserEvent::Order)
                .collect()
        };
        self.core.emit(events).await;
        Ok(())
    }

    async fn subscribe_market_data(
        &self,
        symbol: &Symbol,
    ) -> Result<mpsc::Receiver<MarketEvent>, ExchangeError> {
        // Tee the wrapped stream: every book drives the fill model, then is forwarded.
        let mut upstream = self.inner.subscribe_market_data(symbol).await?;
        let (tx, rx) = mpsc::channel(256);
        let core = self.core.clone();
        tokio::spawn(async move {
            while let Some(ev) = upstream.recv().await {
                if let MarketEvent::Book(book) = &ev {
                    core.on_book(book.clone()).await;
                }
                if tx.send(ev).await.is_err() {
                    break;
                }
            }
        });
        Ok(rx)
    }

    async fn subscribe_user_events(&self) -> Result<mpsc::Receiver<UserEvent>, ExchangeError> {
        let (tx, rx) = mpsc::channel(USER_CHANNEL_CAPACITY);
        self.core.lock().user_tx = Some(tx);
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::types::PriceLevel;

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
            price_tick: d("0.001"),
            quantity_step: d("1"),
            min_quantity: d("1"),
            min_notional: d("10"),
        }
    }

    fn book(bid: &str, ask: &str) -> Arc<OrderBook> {
        Arc::new(OrderBook {
            exchange: ExchangeId::Paper,
            symbol: symbol(),
            bids: vec![PriceLevel { price: d(bid), quantity: d("1000") }],
            asks: vec![PriceLevel { price: d(ask), quantity: d("1000") }],
            sequence: 1,
            timestamp: Utc::now(),
            received: Instant::now(),
        })
    }

    /// A stand-in for the wrapped real exchange; only market data matters here.
    struct NoExchange;

    #[async_trait]
    impl Exchange for NoExchange {
        fn id(&self) -> ExchangeId {
            ExchangeId::Binance
        }
        async fn get_market(&self, _: &Symbol) -> Result<MarketInfo, ExchangeError> {
            Ok(market())
        }
        async fn get_order_book(&self, _: &Symbol) -> Result<OrderBook, ExchangeError> {
            Ok((*book("1.000", "1.002")).clone())
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
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                let _ = tx.send(MarketEvent::Book(book("1.000", "1.002"))).await;
                let _ = tx.send(MarketEvent::Book(book("1.010", "1.012"))).await;
            });
            Ok(rx)
        }
        async fn subscribe_user_events(&self) -> Result<mpsc::Receiver<UserEvent>, ExchangeError> {
            Err(ExchangeError::Unavailable)
        }
    }

    fn paper() -> PaperExchange {
        PaperExchange::new(
            Arc::new(NoExchange),
            market(),
            Fees::from_bps(10, 20),
            d("100"),
            d("1000"),
        )
    }

    fn req(side: Side, price: &str, qty: &str, id: &str) -> OrderRequest {
        OrderRequest {
            symbol: symbol(),
            side,
            order_type: OrderType::Limit,
            price: Some(d(price)),
            quantity: d(qty),
            client_order_id: id.into(),
            post_only: true,
        }
    }

    async fn drain(rx: &mut mpsc::Receiver<UserEvent>) -> Vec<UserEvent> {
        let mut out = Vec::new();
        while let Ok(e) = rx.try_recv() {
            out.push(e);
        }
        out
    }

    #[tokio::test]
    async fn place_locks_balance_and_reports_open_order() {
        let p = paper();
        let mut rx = p.subscribe_user_events().await.unwrap();
        let o = p.place_order(req(Side::Buy, "0.990", "100", "b1")).await.unwrap();
        assert_eq!(o.status, OrderStatus::Open);
        assert_eq!(o.exchange_order_id.as_deref(), Some("P1"));
        let bal = p.get_balances().await.unwrap();
        let usdt = bal.iter().find(|b| b.asset == "USDT").unwrap();
        assert_eq!(usdt.locked, d("99"));
        assert_eq!(usdt.available, d("901"));
        assert!(
            matches!(drain(&mut rx).await.as_slice(), [UserEvent::Order(o)] if o.client_order_id == "b1")
        );
        assert_eq!(p.get_open_orders(&symbol()).await.unwrap().len(), 1);
        assert!(p.get_order(&symbol(), &OrderId::Exchange("P1".into())).await.is_ok());
        assert!(matches!(
            p.get_order(&symbol(), &OrderId::Client("nope".into())).await,
            Err(ExchangeError::OrderNotFound)
        ));
    }

    #[tokio::test]
    async fn rejects_invalid_requests() {
        let p = paper();
        p.on_book(book("1.000", "1.002")).await;
        let e = |r: Result<Order, ExchangeError>| r.unwrap_err();
        assert!(
            matches!(e(p.place_order(req(Side::Buy, "1.002", "100", "x")).await), ExchangeError::InvalidOrder(m) if m.contains("post-only"))
        );
        assert!(
            matches!(e(p.place_order(req(Side::Sell, "1.000", "10", "x")).await), ExchangeError::InvalidOrder(m) if m.contains("post-only"))
        );
        assert!(
            matches!(e(p.place_order(req(Side::Buy, "0.9995", "100", "x")).await), ExchangeError::InvalidOrder(m) if m.contains("aligned"))
        );
        assert!(
            matches!(e(p.place_order(req(Side::Buy, "0.990", "5", "x")).await), ExchangeError::InvalidOrder(m) if m.contains("min"))
        );
        assert!(matches!(
            e(p.place_order(req(Side::Buy, "0.990", "2000", "x")).await),
            ExchangeError::InsufficientBalance
        ));
        assert!(matches!(
            e(p.place_order(req(Side::Sell, "1.010", "500", "x")).await),
            ExchangeError::InsufficientBalance
        ));
        p.place_order(req(Side::Buy, "0.990", "100", "dup")).await.unwrap();
        assert!(
            matches!(e(p.place_order(req(Side::Buy, "0.990", "100", "dup")).await), ExchangeError::InvalidOrder(m) if m.contains("duplicate"))
        );
    }

    #[tokio::test]
    async fn crossing_book_fills_and_settles_with_fee() {
        let p = paper();
        let mut rx = p.subscribe_user_events().await.unwrap();
        p.on_book(book("1.000", "1.002")).await;
        p.place_order(req(Side::Buy, "0.990", "100", "b1")).await.unwrap();
        p.place_order(req(Side::Sell, "1.010", "50", "s1")).await.unwrap();
        drain(&mut rx).await;

        p.on_book(book("0.985", "0.990")).await; // ask touches our bid
        let ev = drain(&mut rx).await;
        assert_eq!(ev.len(), 4, "fill, order, 2 balances: {ev:?}");
        let UserEvent::Fill(f) = &ev[0] else { panic!("{ev:?}") };
        assert_eq!((f.side, f.price, f.quantity), (Side::Buy, d("0.990"), d("100")));
        assert_eq!(f.fee, d("0.099"), "10 bps of 99");
        assert_eq!(f.fee_asset, "USDT");
        let UserEvent::Order(o) = &ev[1] else { panic!() };
        assert_eq!((o.status, o.filled_quantity), (OrderStatus::Filled, d("100")));

        let bal = p.get_balances().await.unwrap();
        let abc = bal.iter().find(|b| b.asset == "BTC").unwrap();
        let usdt = bal.iter().find(|b| b.asset == "USDT").unwrap();
        assert_eq!(abc.total, d("200"));
        assert_eq!(abc.locked, d("50"), "sell still resting");
        assert_eq!(usdt.total, d("1000") - d("99") - d("0.099"));
        assert_eq!(usdt.locked, Decimal::ZERO);
        assert_eq!(p.get_open_orders(&symbol()).await.unwrap().len(), 1);

        p.on_book(book("1.010", "1.012")).await; // bid reaches our ask
        let ev = drain(&mut rx).await;
        let UserEvent::Fill(f) = &ev[0] else { panic!() };
        assert_eq!((f.side, f.price, f.quantity), (Side::Sell, d("1.010"), d("50")));
        let bal = p.get_balances().await.unwrap();
        assert_eq!(bal.iter().find(|b| b.asset == "BTC").unwrap().total, d("150"));
        assert!(p.get_open_orders(&symbol()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn cancel_unlocks_and_cancel_all_clears() {
        let p = paper();
        let mut rx = p.subscribe_user_events().await.unwrap();
        p.place_order(req(Side::Buy, "0.990", "100", "b1")).await.unwrap();
        p.place_order(req(Side::Buy, "0.980", "100", "b2")).await.unwrap();
        p.place_order(req(Side::Sell, "1.020", "10", "s1")).await.unwrap();
        drain(&mut rx).await;

        p.cancel_order(&symbol(), &OrderId::Client("b1".into())).await.unwrap();
        let ev = drain(&mut rx).await;
        assert!(
            matches!(ev.as_slice(), [UserEvent::Order(o)] if o.status == OrderStatus::Cancelled && o.client_order_id == "b1")
        );
        assert!(matches!(
            p.cancel_order(&symbol(), &OrderId::Client("b1".into())).await,
            Err(ExchangeError::OrderNotFound)
        ));
        let usdt = p.get_balances().await.unwrap().into_iter().find(|b| b.asset == "USDT").unwrap();
        assert_eq!(usdt.locked, d("98"));

        p.cancel_all_orders(&symbol()).await.unwrap();
        assert_eq!(drain(&mut rx).await.len(), 2);
        assert!(p.get_open_orders(&symbol()).await.unwrap().is_empty());
        let bal = p.get_balances().await.unwrap();
        assert!(bal.iter().all(|b| b.locked.is_zero()), "{bal:?}");
    }

    #[tokio::test]
    async fn market_data_tee_drives_fills_and_forwards_books() {
        let p = paper();
        let mut user = p.subscribe_user_events().await.unwrap();
        p.place_order(req(Side::Sell, "1.010", "10", "s1")).await.unwrap();
        drain(&mut user).await;
        let mut md = p.subscribe_market_data(&symbol()).await.unwrap();
        let mut books = 0;
        while let Some(ev) = md.recv().await {
            if matches!(ev, MarketEvent::Book(_)) {
                books += 1;
            }
        }
        assert_eq!(books, 2);
        let ev = drain(&mut user).await;
        assert!(matches!(ev.first(), Some(UserEvent::Fill(f)) if f.side == Side::Sell), "{ev:?}");
    }
}
