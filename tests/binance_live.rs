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
