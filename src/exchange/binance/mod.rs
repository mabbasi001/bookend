//! Binance spot adapter. `client.rs` = HTTP + signing + time sync,
//! `rest.rs` = endpoints and raw payloads, `ws.rs` = streams,
//! `mapper.rs` = Binance types ↔ internal types.

pub mod client;
pub mod mapper;
pub mod rest;
pub mod ws;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::config::{Credentials, Mode};
use crate::events::{MarketEvent, UserEvent};
use crate::exchange::{Exchange, ExchangeError};
use crate::types::{
    Balance, ExchangeId, Fees, MarketInfo, Order, OrderBook, OrderId, OrderRequest, Symbol,
};

pub use client::BinanceClient;

/// Native symbol (`BTCUSDT`) → internal [`Symbol`] (`BTC/USDT`).
///
/// The user data stream is account-wide and names symbols natively, and
/// `BTCUSDT` cannot be split into base and quote without the exchange's own
/// metadata. [`Exchange::get_market`] already fetches that metadata, so it
/// records the pair here for the stream to look up.
#[derive(Clone, Default)]
pub struct SymbolRegistry(Arc<Mutex<HashMap<String, Symbol>>>);

impl SymbolRegistry {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Symbol>> {
        self.0.lock().expect("symbol registry poisoned")
    }

    pub fn record(&self, native: &str, symbol: &Symbol) {
        self.lock().insert(native.to_owned(), symbol.clone());
    }

    pub fn resolve(&self, native: &str) -> Option<Symbol> {
        self.lock().get(native).cloned()
    }
}

pub struct BinanceExchange {
    client: BinanceClient,
    symbols: SymbolRegistry,
}

impl BinanceExchange {
    pub fn new(mode: Mode, credentials: Option<Credentials>) -> Result<Self, ExchangeError> {
        Ok(Self {
            client: BinanceClient::new(mode, credentials)?,
            symbols: SymbolRegistry::default(),
        })
    }
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
        let market = mapper::market_info(symbol, entry)?;
        // The user stream resolves its native symbols through this.
        self.symbols.record(&market.native_symbol, symbol);
        Ok(market)
    }

    async fn get_order_book(&self, symbol: &Symbol) -> Result<OrderBook, ExchangeError> {
        let native = mapper::native_symbol(symbol);
        let snap = rest::depth(&self.client, &native, 100).await?;
        Ok(mapper::order_book(symbol, &snap))
    }

    async fn get_fees(&self, _symbol: &Symbol) -> Result<Fees, ExchangeError> {
        mapper::fees(&rest::account(&self.client).await?)
    }

    async fn get_balances(&self) -> Result<Vec<Balance>, ExchangeError> {
        let account = rest::account(&self.client).await?;
        if !account.can_trade {
            warn!("binance api key has no trading permission");
        }
        Ok(mapper::balances(&account))
    }

    async fn get_open_orders(&self, symbol: &Symbol) -> Result<Vec<Order>, ExchangeError> {
        let native = mapper::native_symbol(symbol);
        rest::open_orders(&self.client, &native)
            .await?
            .iter()
            .map(|raw| mapper::order(symbol, raw))
            .collect()
    }

    async fn get_order(&self, symbol: &Symbol, id: &OrderId) -> Result<Order, ExchangeError> {
        let (key, value) = mapper::order_id_param(id);
        let raw =
            rest::order(&self.client, &[("symbol", mapper::native_symbol(symbol)), (key, value)])
                .await?;
        mapper::order(symbol, &raw)
    }

    async fn place_order(&self, request: OrderRequest) -> Result<Order, ExchangeError> {
        let native = mapper::native_symbol(&request.symbol);
        let params = mapper::place_order_params(&native, &request)?;
        let raw = rest::place_order(&self.client, &params).await?;
        mapper::order(&request.symbol, &raw)
    }

    async fn cancel_order(&self, symbol: &Symbol, id: &OrderId) -> Result<(), ExchangeError> {
        let (key, value) = mapper::order_id_param(id);
        rest::cancel_order(
            &self.client,
            &[("symbol", mapper::native_symbol(symbol)), (key, value)],
        )
        .await?;
        Ok(())
    }

    async fn cancel_all_orders(&self, symbol: &Symbol) -> Result<(), ExchangeError> {
        let native = mapper::native_symbol(symbol);
        match rest::cancel_open_orders(&self.client, &native).await {
            Ok(n) => {
                debug!(symbol = %native, cancelled = n, "binance cancel-all");
                Ok(())
            }
            // Nothing was open. Binance answers -2011 there; for a cancel-all
            // on shutdown that is the desired end state, not a failure.
            Err(ExchangeError::OrderNotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn subscribe_market_data(
        &self,
        symbol: &Symbol,
    ) -> Result<mpsc::Receiver<MarketEvent>, ExchangeError> {
        ws::subscribe_market_data(self.client.clone(), symbol.clone()).await
    }

    async fn subscribe_user_events(&self) -> Result<mpsc::Receiver<UserEvent>, ExchangeError> {
        ws::subscribe_user_events(self.client.clone(), self.symbols.clone()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_symbol_registry_resolves_only_what_get_market_recorded() {
        let registry = SymbolRegistry::default();
        assert_eq!(registry.resolve("BTCUSDT"), None);

        let btc = Symbol::new("BTC", "USDT");
        registry.record("BTCUSDT", &btc);
        assert_eq!(registry.resolve("BTCUSDT"), Some(btc));
        // An account-wide stream sees symbols this engine never asked for.
        assert_eq!(registry.resolve("ETHUSDT"), None);

        // Clones share one map, so the stream task sees later registrations.
        let clone = registry.clone();
        registry.record("ETHUSDT", &Symbol::new("ETH", "USDT"));
        assert_eq!(clone.resolve("ETHUSDT"), Some(Symbol::new("ETH", "USDT")));
    }
}
