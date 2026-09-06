//! Exchange-agnostic domain types. Adapters map to and from these; nothing
//! outside `exchange/<name>/` touches exchange payloads.
//!
//! Every price, quantity, notional, fee and PnL is a [`Decimal`]. `f64` is not
//! used for money anywhere.

use std::fmt;
use std::time::Instant;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExchangeId {
    Binance,
    Bybit,
    Okx,
    /// In-process simulated exchange (paper mode and tests).
    Paper,
}

impl ExchangeId {
    pub const fn as_str(self) -> &'static str {
        match self {
            ExchangeId::Binance => "binance",
            ExchangeId::Bybit => "bybit",
            ExchangeId::Okx => "okx",
            ExchangeId::Paper => "paper",
        }
    }
}

impl fmt::Display for ExchangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `BTC/USDT`. Adapters render the exchange's native form (`BTCUSDT`, `BTC-USDT`, …).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol {
    pub base: String,
    pub quote: String,
}

impl Symbol {
    pub fn new(base: impl Into<String>, quote: impl Into<String>) -> Self {
        Self { base: base.into(), quote: quote.into() }
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.base, self.quote)
    }
}

/// Orders can be looked up by either id; reconciliation relies on the client id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderId {
    Exchange(String),
    Client(String),
}

// ---------------------------------------------------------------------------
// Market
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub const fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

/// A full local view of one exchange's book for one symbol.
#[derive(Debug, Clone)]
pub struct OrderBook {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    /// Descending by price.
    pub bids: Vec<PriceLevel>,
    /// Ascending by price.
    pub asks: Vec<PriceLevel>,
    /// Exchange update id, for gap detection.
    pub sequence: u64,
    /// Exchange time.
    pub timestamp: DateTime<Utc>,
    /// Local receive time, for staleness checks.
    pub received: Instant,
}

impl OrderBook {
    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks.first()
    }

    pub fn mid(&self) -> Option<Decimal> {
        Some((self.best_bid()?.price + self.best_ask()?.price) / Decimal::TWO)
    }
}

/// Precision and size constraints for one market. Adapters round to these
/// before every submission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketInfo {
    pub symbol: Symbol,
    /// What the exchange calls it.
    pub native_symbol: String,
    pub price_tick: Decimal,
    pub quantity_step: Decimal,
    pub min_quantity: Decimal,
    pub min_notional: Decimal,
}

/// Fee tier in basis points.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fees {
    pub maker_bps: Decimal,
    pub taker_bps: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trade {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub price: Decimal,
    pub quantity: Decimal,
    /// Taker side.
    pub side: Side,
    pub timestamp: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Account
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Balance {
    pub asset: String,
    pub total: Decimal,
    pub available: Decimal,
    pub locked: Decimal,
}

/// See ARCHITECTURE.md §9 for the transition diagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Created,
    Submitting,
    /// Submission outcome unknown (timeout / lost response). Must reconcile.
    Unknown,
    Open,
    PartiallyFilled,
    Filled,
    CancelRequested,
    Cancelled,
    Rejected,
    Expired,
}

impl OrderStatus {
    /// No further transitions possible.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            OrderStatus::Filled
                | OrderStatus::Cancelled
                | OrderStatus::Rejected
                | OrderStatus::Expired
        )
    }

    /// Resting on the exchange (or believed to be).
    pub const fn is_live(self) -> bool {
        matches!(
            self,
            OrderStatus::Open | OrderStatus::PartiallyFilled | OrderStatus::CancelRequested
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRequest {
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    /// Always set by the engine; alphanumeric, ≤ 32 chars.
    pub client_order_id: String,
    /// Default `true` for a market maker.
    pub post_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub exchange: ExchangeId,
    /// `None` until the exchange acknowledges.
    pub exchange_order_id: Option<String>,
    pub client_order_id: String,
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    pub filled_quantity: Decimal,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Order {
    pub fn remaining_quantity(&self) -> Decimal {
        self.quantity - self.filled_quantity
    }
}

/// A confirmed execution. The only thing that moves the portfolio.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    pub exchange: ExchangeId,
    pub client_order_id: String,
    pub trade_id: String,
    pub symbol: Symbol,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_asset: String,
    pub timestamp: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Strategy output
// ---------------------------------------------------------------------------

/// One two-sided quote for one exchange. The strategy may quote each exchange differently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quote {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub bid_price: Decimal,
    pub bid_quantity: Decimal,
    pub ask_price: Decimal,
    pub ask_quantity: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book(bids: &[(&str, &str)], asks: &[(&str, &str)]) -> OrderBook {
        let lvl = |(p, q): &(&str, &str)| PriceLevel {
            price: p.parse().unwrap(),
            quantity: q.parse().unwrap(),
        };
        OrderBook {
            exchange: ExchangeId::Paper,
            symbol: Symbol::new("BTC", "USDT"),
            bids: bids.iter().map(lvl).collect(),
            asks: asks.iter().map(lvl).collect(),
            sequence: 1,
            timestamp: Utc::now(),
            received: Instant::now(),
        }
    }

    #[test]
    fn symbol_displays_with_slash() {
        assert_eq!(Symbol::new("BTC", "USDT").to_string(), "BTC/USDT");
    }

    #[test]
    fn mid_is_average_of_best_levels() {
        let b = book(&[("1.0010", "5"), ("1.0000", "9")], &[("1.0030", "3")]);
        assert_eq!(b.mid().unwrap(), "1.0020".parse::<Decimal>().unwrap());
        assert_eq!(b.best_bid().unwrap().quantity, Decimal::from(5));
    }

    #[test]
    fn mid_is_none_on_one_sided_book() {
        assert!(book(&[("1", "1")], &[]).mid().is_none());
        assert!(book(&[], &[]).mid().is_none());
    }

    #[test]
    fn status_classification() {
        assert!(OrderStatus::Filled.is_terminal());
        assert!(!OrderStatus::Filled.is_live());
        assert!(OrderStatus::CancelRequested.is_live());
        assert!(!OrderStatus::Unknown.is_live());
        assert!(!OrderStatus::Unknown.is_terminal());
    }

    #[test]
    fn exchange_id_round_trips_through_serde() {
        let json = serde_json::to_string(&ExchangeId::Okx).unwrap();
        assert_eq!(json, "\"okx\"");
        assert_eq!(serde_json::from_str::<ExchangeId>(&json).unwrap(), ExchangeId::Okx);
        assert_eq!(Side::Buy.opposite(), Side::Sell);
    }
}
