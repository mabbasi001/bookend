//! Binance payloads ↔ internal types.

use std::time::Instant;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use super::rest::{DepthSnapshot, Filter, SymbolInfo};
use crate::exchange::ExchangeError;
use crate::types::{ExchangeId, MarketInfo, OrderBook, PriceLevel, Symbol};

/// `BTC/USDT` → `BTCUSDT`
pub fn native_symbol(symbol: &Symbol) -> String {
    format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
}

pub fn market_info(symbol: &Symbol, info: &SymbolInfo) -> Result<MarketInfo, ExchangeError> {
    if info.status != "TRADING" {
        return Err(ExchangeError::Unavailable);
    }
    let mut price_tick = None;
    let mut quantity_step = None;
    let mut min_quantity = None;
    let mut min_notional = None;
    for f in &info.filters {
        match f {
            Filter::Price { tick_size, .. } => price_tick = Some(*tick_size),
            Filter::LotSize { step_size, min_qty, .. } => {
                quantity_step = Some(*step_size);
                min_quantity = Some(*min_qty);
            }
            // NOTIONAL supersedes MIN_NOTIONAL; keep whichever appears, prefer NOTIONAL.
            Filter::Notional { min_notional: n } => min_notional = Some(*n),
            Filter::MinNotional { min_notional: n } => min_notional.get_or_insert(*n).clone_from(n),
            Filter::Other => {}
        }
    }
    let missing = |what: &str| {
        ExchangeError::Unknown(format!("exchangeInfo for {}: missing {what}", info.symbol))
    };
    Ok(MarketInfo {
        symbol: symbol.clone(),
        native_symbol: info.symbol.clone(),
        price_tick: price_tick.ok_or_else(|| missing("PRICE_FILTER"))?.normalize(),
        quantity_step: quantity_step.ok_or_else(|| missing("LOT_SIZE"))?.normalize(),
        min_quantity: min_quantity.ok_or_else(|| missing("LOT_SIZE"))?.normalize(),
        min_notional: min_notional.ok_or_else(|| missing("NOTIONAL"))?.normalize(),
    })
}

pub fn levels(raw: &[(Decimal, Decimal)]) -> Vec<PriceLevel> {
    raw.iter().map(|&(price, quantity)| PriceLevel { price, quantity }).collect()
}

pub fn order_book(symbol: &Symbol, snap: &DepthSnapshot) -> OrderBook {
    OrderBook {
        exchange: ExchangeId::Binance,
        symbol: symbol.clone(),
        bids: levels(&snap.bids),
        asks: levels(&snap.asks),
        sequence: snap.last_update_id,
        timestamp: Utc::now(),
        received: Instant::now(),
    }
}

pub fn timestamp(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::binance::rest::ExchangeInfo;

    const EXCHANGE_INFO: &str =
        include_str!("../../../tests/fixtures/binance/exchange_info_btcusdt.json");
    const DEPTH: &str = include_str!("../../../tests/fixtures/binance/depth_btcusdt.json");

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn native_symbol_is_concatenated_uppercase() {
        assert_eq!(native_symbol(&Symbol::new("btc", "usdt")), "BTCUSDT");
    }

    #[test]
    fn market_info_from_fixture() {
        let info: ExchangeInfo = serde_json::from_str(EXCHANGE_INFO).unwrap();
        let m = market_info(&Symbol::new("BTC", "USDT"), &info.symbols[0]).unwrap();
        assert_eq!(m.native_symbol, "BTCUSDT");
        assert_eq!(m.price_tick, dec("0.01"));
        assert_eq!(m.quantity_step, dec("0.00001"));
        assert_eq!(m.min_quantity, dec("0.00001"));
        assert_eq!(m.min_notional, dec("5"));
    }

    #[test]
    fn market_info_rejects_non_trading_and_missing_filters() {
        let mut info: ExchangeInfo = serde_json::from_str(EXCHANGE_INFO).unwrap();
        info.symbols[0].filters.retain(|f| !matches!(f, Filter::Price { .. }));
        assert!(matches!(
            market_info(&Symbol::new("BTC", "USDT"), &info.symbols[0]),
            Err(ExchangeError::Unknown(m)) if m.contains("PRICE_FILTER")
        ));
        info.symbols[0].status = "BREAK".into();
        assert!(matches!(
            market_info(&Symbol::new("BTC", "USDT"), &info.symbols[0]),
            Err(ExchangeError::Unavailable)
        ));
    }

    #[test]
    fn order_book_from_snapshot() {
        let snap: DepthSnapshot = serde_json::from_str(DEPTH).unwrap();
        let b = order_book(&Symbol::new("BTC", "USDT"), &snap);
        assert_eq!(b.sequence, 99997813480);
        assert_eq!(b.best_bid().unwrap().price, dec("77584.02"));
        assert_eq!(b.best_ask().unwrap().price, dec("77584.03"));
        assert_eq!(b.mid().unwrap(), dec("77584.025"));
    }
}
