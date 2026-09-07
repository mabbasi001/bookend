//! Binance spot adapter. `client.rs` = HTTP + signing + time sync,
//! `rest.rs` = endpoints and raw payloads, `ws.rs` = streams,
//! `mapper.rs` = Binance types ↔ internal types.

pub mod client;
pub mod mapper;
pub mod rest;
pub mod ws;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::config::{Credentials, Mode};
use crate::events::{MarketEvent, UserEvent};
use crate::exchange::{Exchange, ExchangeError};
use crate::types::{
    Balance, ExchangeId, Fees, MarketInfo, Order, OrderBook, OrderId, OrderRequest, Symbol,
};

pub use client::BinanceClient;

pub struct BinanceExchange {
    client: BinanceClient,
}

impl BinanceExchange {
    pub fn new(mode: Mode, credentials: Option<Credentials>) -> Result<Self, ExchangeError> {
        Ok(Self { client: BinanceClient::new(mode, credentials)? })
    }
}

fn not_yet(what: &str) -> ExchangeError {
    ExchangeError::Unknown(format!("binance: {what} not implemented until M4"))
}

#[async_trait]
impl Exchange for BinanceExchange {
    fn id(&self) -> ExchangeId {
        ExchangeId::Binance
    }

    async fn get_market(&self, symbol: &Symbol) -> Result<MarketInfo, ExchangeError> {
        let native = mapper::native_symbol(symbol);
        let info = rest::exchange_info(&self.client, &native).await?;
        let entry = info
            .symbols
            .iter()
            .find(|s| s.symbol == native)
            .ok_or_else(|| ExchangeError::InvalidOrder(format!("unknown symbol {native}")))?;
        mapper::market_info(symbol, entry)
    }

    async fn get_order_book(&self, symbol: &Symbol) -> Result<OrderBook, ExchangeError> {
        let native = mapper::native_symbol(symbol);
        let snap = rest::depth(&self.client, &native, 100).await?;
        Ok(mapper::order_book(symbol, &snap))
    }

    async fn get_fees(&self, _symbol: &Symbol) -> Result<Fees, ExchangeError> {
        Err(not_yet("get_fees"))
    }

    async fn get_balances(&self) -> Result<Vec<Balance>, ExchangeError> {
        Err(not_yet("get_balances"))
    }

    async fn get_open_orders(&self, _symbol: &Symbol) -> Result<Vec<Order>, ExchangeError> {
        Err(not_yet("get_open_orders"))
    }

    async fn get_order(&self, _symbol: &Symbol, _id: &OrderId) -> Result<Order, ExchangeError> {
        Err(not_yet("get_order"))
    }

    async fn place_order(&self, _request: OrderRequest) -> Result<Order, ExchangeError> {
        Err(not_yet("place_order"))
    }

    async fn cancel_order(&self, _symbol: &Symbol, _id: &OrderId) -> Result<(), ExchangeError> {
        Err(not_yet("cancel_order"))
    }

    async fn cancel_all_orders(&self, _symbol: &Symbol) -> Result<(), ExchangeError> {
        Err(not_yet("cancel_all_orders"))
    }

    async fn subscribe_market_data(
        &self,
        symbol: &Symbol,
    ) -> Result<mpsc::Receiver<MarketEvent>, ExchangeError> {
        ws::subscribe_market_data(self.client.clone(), symbol.clone()).await
    }

    async fn subscribe_user_events(&self) -> Result<mpsc::Receiver<UserEvent>, ExchangeError> {
        Err(not_yet("subscribe_user_events"))
    }
}
