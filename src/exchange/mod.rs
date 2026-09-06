//! The exchange abstraction. Adapters live in submodules and are the only code
//! that knows an exchange's API.

use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::events::{MarketEvent, UserEvent};
use crate::types::{
    Balance, ExchangeId, Fees, MarketInfo, Order, OrderBook, OrderId, OrderRequest, Symbol,
};

#[derive(Debug, thiserror::Error)]
pub enum ExchangeError {
    #[error("network: {0}")]
    Network(#[source] anyhow::Error),
    /// Outcome unknown. An order submission that ends here must be reconciled,
    /// never plainly retried.
    #[error("timeout")]
    Timeout,
    #[error("rate limited, retry after {0:?}")]
    RateLimited(Option<Duration>),
    #[error("authentication failed")]
    Authentication,
    #[error("invalid order: {0}")]
    InvalidOrder(String),
    #[error("insufficient balance")]
    InsufficientBalance,
    #[error("order not found")]
    OrderNotFound,
    /// Maintenance / 5xx.
    #[error("exchange unavailable")]
    Unavailable,
    #[error("unknown exchange error: {0}")]
    Unknown(String),
}

impl ExchangeError {
    /// Safe to retry for *reads*. Order submissions use the reconciliation
    /// path instead (ARCHITECTURE.md §12).
    pub const fn is_retryable_read(&self) -> bool {
        matches!(
            self,
            ExchangeError::Network(_)
                | ExchangeError::Timeout
                | ExchangeError::RateLimited(_)
                | ExchangeError::Unavailable
        )
    }

    /// The order may or may not exist on the exchange.
    pub const fn is_uncertain_submission(&self) -> bool {
        matches!(self, ExchangeError::Network(_) | ExchangeError::Timeout)
    }
}

/// Everything the engine needs from an exchange. Kept deliberately small;
/// exchange-specific features stay inside the adapter.
#[async_trait]
pub trait Exchange: Send + Sync {
    fn id(&self) -> ExchangeId;

    // Market -----------------------------------------------------------------
    async fn get_market(&self, symbol: &Symbol) -> Result<MarketInfo, ExchangeError>;
    async fn get_order_book(&self, symbol: &Symbol) -> Result<OrderBook, ExchangeError>;
    async fn get_fees(&self, symbol: &Symbol) -> Result<Fees, ExchangeError>;

    // Account ----------------------------------------------------------------
    async fn get_balances(&self) -> Result<Vec<Balance>, ExchangeError>;
    async fn get_open_orders(&self, symbol: &Symbol) -> Result<Vec<Order>, ExchangeError>;
    async fn get_order(&self, symbol: &Symbol, id: &OrderId) -> Result<Order, ExchangeError>;

    // Execution --------------------------------------------------------------
    async fn place_order(&self, request: OrderRequest) -> Result<Order, ExchangeError>;
    async fn cancel_order(&self, symbol: &Symbol, id: &OrderId) -> Result<(), ExchangeError>;
    async fn cancel_all_orders(&self, symbol: &Symbol) -> Result<(), ExchangeError>;

    // Streams ----------------------------------------------------------------
    // The adapter owns the socket, auth, heartbeats, reconnection and resync.
    // The engine only ever sees normalized events.
    async fn subscribe_market_data(
        &self,
        symbol: &Symbol,
    ) -> Result<mpsc::Receiver<MarketEvent>, ExchangeError>;
    async fn subscribe_user_events(&self) -> Result<mpsc::Receiver<UserEvent>, ExchangeError>;
}
