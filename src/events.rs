//! Normalized events flowing between components. Tokio channels only; no broker.

use std::sync::Arc;

use crate::types::{Balance, ExchangeId, Fill, Order, OrderBook, Trade};

/// Public market data, produced by an adapter's market-data stream.
#[derive(Debug, Clone)]
pub enum MarketEvent {
    /// Latest full local book. `Arc` so it is shared, not cloned, per tick.
    Book(Arc<OrderBook>),
    Trade(Trade),
    Connected(ExchangeId),
    /// Also emitted on a sequence gap; the book is invalid until the next `Book`.
    Disconnected(ExchangeId),
}

/// Private account events, produced by an adapter's user stream.
#[derive(Debug, Clone)]
pub enum UserEvent {
    Order(Order),
    Fill(Fill),
    Balance(Balance),
}
