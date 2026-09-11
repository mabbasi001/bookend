//! Hits the real Binance public API. Ignored by default:
//!   scripts/cargo.sh test --test binance_live -- --ignored

use bookend::config::Mode;
use bookend::exchange::Exchange;
use bookend::exchange::binance::BinanceExchange;
use bookend::types::Symbol;
use rust_decimal::Decimal;

#[tokio::test]
#[ignore = "needs network"]
async fn market_info_and_order_book_from_production() {
    let ex = BinanceExchange::new(Mode::Paper, None).unwrap();
    let symbol = Symbol::new("BTC", "USDT");

    let m = ex.get_market(&symbol).await.unwrap();
    assert_eq!(m.native_symbol, "BTCUSDT");
    assert!(m.price_tick > Decimal::ZERO && m.quantity_step > Decimal::ZERO);

    let book = ex.get_order_book(&symbol).await.unwrap();
    assert!(book.mid().unwrap() > Decimal::from(1000), "BTC mid looks wrong: {:?}", book.mid());
    assert!(book.best_bid().unwrap().price < book.best_ask().unwrap().price);
    assert!(book.sequence > 0);
    eprintln!(
        "BTC/USDT bid={} ask={} mid={} seq={}",
        book.best_bid().unwrap().price,
        book.best_ask().unwrap().price,
        book.mid().unwrap(),
        book.sequence
    );
}

#[tokio::test]
#[ignore = "needs network"]
async fn unknown_symbol_is_an_invalid_order_error() {
    let ex = BinanceExchange::new(Mode::Paper, None).unwrap();
    let err = ex.get_market(&Symbol::new("NOPE", "NOPE")).await.unwrap_err();
    eprintln!("{err}");
    assert!(matches!(err, bookend::exchange::ExchangeError::InvalidOrder(_)), "{err}");
}

#[tokio::test]
#[ignore = "needs network"]
async fn depth_stream_stays_in_sequence_for_20_books() {
    use bookend::events::MarketEvent;
    use std::time::Duration;

    let ex = BinanceExchange::new(Mode::Paper, None).unwrap();
    let mut rx = ex.subscribe_market_data(&Symbol::new("BTC", "USDT")).await.unwrap();

    let mut last_seq = 0;
    let mut books = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while books < 20 {
        let ev = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .expect("stream stalled")
            .expect("stream ended");
        match ev {
            MarketEvent::Book(b) => {
                assert!(
                    b.sequence > last_seq,
                    "sequence went backwards: {} -> {}",
                    last_seq,
                    b.sequence
                );
                assert!(b.best_bid().unwrap().price < b.best_ask().unwrap().price, "crossed book");
                assert_eq!(b.bids.len(), 20);
                last_seq = b.sequence;
                books += 1;
            }
            MarketEvent::Disconnected(_) => panic!("disconnected during sync test"),
            MarketEvent::Connected(_) | MarketEvent::Trade(_) => {}
        }
    }
    eprintln!("{books} books in sequence, last seq {last_seq}");
}

/// Places a real (tiny, post-only, far-from-market) order on the Binance spot
/// testnet and takes it through lookup, cancel and cancel-all:
///   BINANCE_API_KEY=… BINANCE_API_SECRET=… \
///     scripts/cargo.sh test --test binance_live -- --ignored order_round_trip
///
/// The bid sits far below the market, so it rests and is cancelled rather than
/// filled. Testnet keys only — `Mode::Testnet` pins the endpoints regardless.
#[tokio::test]
#[ignore = "needs testnet credentials"]
async fn order_round_trip_on_testnet() {
    use bookend::config::{Credentials, Secret};
    use bookend::orders::ClientIdGen;
    use bookend::types::{ExchangeId, OrderId, OrderRequest, OrderStatus, OrderType, Side};

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    let (Ok(key), Ok(secret)) =
        (std::env::var("BINANCE_API_KEY"), std::env::var("BINANCE_API_SECRET"))
    else {
        eprintln!("skipped: BINANCE_API_KEY / BINANCE_API_SECRET not set");
        return;
    };

    let credentials = Credentials {
        api_key: Secret::new(key),
        api_secret: Secret::new(secret),
        passphrase: None,
    };
    let ex = BinanceExchange::new(Mode::Testnet, Some(credentials)).unwrap();
    let symbol = Symbol::new("BTC", "USDT");

    let market = ex.get_market(&symbol).await.unwrap();
    let balances = ex.get_balances().await.unwrap();
    assert!(!balances.is_empty(), "testnet account should be funded");
    // The testnet charges nothing, which the adapter reports as no answer so
    // the engine keeps its configured fees.
    match ex.get_fees(&symbol).await {
        Ok(f) => assert!(f.maker_bps > Decimal::ZERO || f.taker_bps > Decimal::ZERO),
        Err(e) => eprintln!("no usable fee tier ({e}); engine falls back to configured fees"),
    }
    eprintln!("{} funded assets", balances.len());

    // 30 % below the mid: deep enough never to trade, and clear of Binance's
    // PERCENT_PRICE_BY_SIDE floor, which rejects a bid under half the trailing
    // 5-minute average price (-1013).
    let mid = ex.get_order_book(&symbol).await.unwrap().mid().unwrap();
    let price = market.round_price_down(mid * dec("0.7"));
    // Smallest quantity that still clears min_notional at that price.
    let quantity = {
        let q = bookend::types::round_up(market.min_notional / price, market.quantity_step);
        q.max(market.min_quantity)
    };
    assert!(market.meets_minimums(price, quantity), "{price} x {quantity}");

    let mut ids = ClientIdGen::new(ClientIdGen::run_id_now());
    let client_order_id = ids.next(ExchangeId::Binance);
    let request = OrderRequest {
        symbol: symbol.clone(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(price),
        quantity,
        client_order_id: client_order_id.clone(),
        post_only: true,
    };

    let placed = ex.place_order(request).await.unwrap();
    eprintln!("placed {} at {price} x {quantity} -> {:?}", placed.client_order_id, placed.status);
    assert_eq!(placed.client_order_id, client_order_id);
    assert_eq!(placed.status, OrderStatus::Open);
    assert_eq!(placed.price, Some(price));
    assert_eq!(placed.quantity, quantity);
    assert!(placed.filled_quantity.is_zero());
    let exchange_order_id = placed.exchange_order_id.clone().expect("acknowledged");

    // Both id forms find the same order.
    let by_client = ex.get_order(&symbol, &OrderId::Client(client_order_id.clone())).await.unwrap();
    let by_exchange =
        ex.get_order(&symbol, &OrderId::Exchange(exchange_order_id.clone())).await.unwrap();
    assert_eq!(by_client, by_exchange);
    assert_eq!(by_client.status, OrderStatus::Open);

    let open = ex.get_open_orders(&symbol).await.unwrap();
    assert!(
        open.iter().any(|o| o.client_order_id == client_order_id),
        "placed order missing from open orders: {open:?}"
    );

    ex.cancel_order(&symbol, &OrderId::Client(client_order_id.clone())).await.unwrap();
    let cancelled = ex.get_order(&symbol, &OrderId::Client(client_order_id.clone())).await.unwrap();
    assert_eq!(cancelled.status, OrderStatus::Cancelled);
    assert!(cancelled.status.is_terminal());

    // Cancelling an already-cancelled order is "not found", not a hard failure.
    let err = ex.cancel_order(&symbol, &OrderId::Client(client_order_id)).await.unwrap_err();
    assert!(matches!(err, bookend::exchange::ExchangeError::OrderNotFound), "{err}");

    // Cancel-all over an empty book answers -2011; the adapter treats that as
    // the desired end state, which the shutdown path depends on.
    ex.cancel_all_orders(&symbol).await.unwrap();
    assert!(ex.get_open_orders(&symbol).await.unwrap().is_empty());
}

/// Drives one order through the private user data stream on the testnet:
///   BINANCE_API_KEY=… BINANCE_API_SECRET=… \
///     scripts/cargo.sh test --test binance_live -- --ignored user_stream
///
/// Covers the listen key, the socket, and `executionReport` → `UserEvent`,
/// including that `get_market` registered `BTCUSDT` so the account-wide stream
/// can resolve it back to `BTC/USDT`.
#[tokio::test]
#[ignore = "needs testnet credentials"]
async fn user_stream_reports_the_order_lifecycle_on_testnet() {
    use bookend::config::{Credentials, Secret};
    use bookend::events::UserEvent;
    use bookend::orders::ClientIdGen;
    use bookend::types::{ExchangeId, OrderId, OrderRequest, OrderStatus, OrderType, Side};
    use std::time::Duration;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    let (Ok(key), Ok(secret)) =
        (std::env::var("BINANCE_API_KEY"), std::env::var("BINANCE_API_SECRET"))
    else {
        eprintln!("skipped: BINANCE_API_KEY / BINANCE_API_SECRET not set");
        return;
    };

    let credentials = Credentials {
        api_key: Secret::new(key),
        api_secret: Secret::new(secret),
        passphrase: None,
    };
    let ex = BinanceExchange::new(Mode::Testnet, Some(credentials)).unwrap();
    let symbol = Symbol::new("BTC", "USDT");

    // Also what teaches the adapter that BTCUSDT is BTC/USDT.
    let market = ex.get_market(&symbol).await.unwrap();
    let mut events = ex.subscribe_user_events().await.unwrap();

    let mid = ex.get_order_book(&symbol).await.unwrap().mid().unwrap();
    let price = market.round_price_down(mid * dec("0.7"));
    let quantity = bookend::types::round_up(market.min_notional / price, market.quantity_step)
        .max(market.min_quantity);

    let client_order_id = ClientIdGen::new(ClientIdGen::run_id_now()).next(ExchangeId::Binance);
    ex.place_order(OrderRequest {
        symbol: symbol.clone(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(price),
        quantity,
        client_order_id: client_order_id.clone(),
        post_only: true,
    })
    .await
    .unwrap();

    // The stream reports the order the engine just placed.
    let open = next_order(&mut events, &client_order_id).await;
    assert_eq!(open.status, OrderStatus::Open);
    assert_eq!(open.symbol, symbol, "native symbol resolved back through get_market");
    assert_eq!(open.price, Some(price));
    assert_eq!(open.quantity, quantity);
    assert!(open.filled_quantity.is_zero());

    ex.cancel_order(&symbol, &OrderId::Client(client_order_id.clone())).await.unwrap();

    let cancelled = next_order(&mut events, &client_order_id).await;
    assert_eq!(cancelled.status, OrderStatus::Cancelled);
    assert!(cancelled.status.is_terminal());
    eprintln!("user stream reported {} open then cancelled", cancelled.client_order_id);

    /// Next order update for `client_order_id`, ignoring balances and any
    /// traffic from other activity on the account.
    async fn next_order(
        events: &mut tokio::sync::mpsc::Receiver<UserEvent>,
        client_order_id: &str,
    ) -> bookend::types::Order {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let event = tokio::time::timeout_at(deadline, events.recv())
                .await
                .unwrap_or_else(|_| panic!("no update for {client_order_id} within 20 s"))
                .expect("user stream ended");
            match event {
                UserEvent::Order(o) if o.client_order_id == client_order_id => return o,
                other => eprintln!("ignoring {other:?}"),
            }
        }
    }
}
