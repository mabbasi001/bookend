//! Market-data stream (M2.6–M2.9). Placeholder until the sync state machine lands.

use tokio::sync::mpsc;

use super::client::BinanceClient;
use crate::events::MarketEvent;
use crate::exchange::ExchangeError;
use crate::types::Symbol;

pub async fn subscribe_market_data(
    _client: BinanceClient,
    _symbol: Symbol,
) -> Result<mpsc::Receiver<MarketEvent>, ExchangeError> {
    Err(ExchangeError::Unknown("binance: market data stream not implemented until M2.6".into()))
}
