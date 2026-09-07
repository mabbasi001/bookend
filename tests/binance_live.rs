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
