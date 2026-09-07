//! REST endpoints and their raw payloads. Nothing here is exposed outside the
//! adapter; `mapper.rs` converts to internal types.

use rust_decimal::Decimal;
use serde::Deserialize;

use super::client::BinanceClient;
use crate::exchange::ExchangeError;

// ---------------------------------------------------------------------------
// Payloads
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerTime {
    pub server_time: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeInfo {
    pub server_time: i64,
    #[serde(default)]
    pub rate_limits: Vec<RateLimit>,
    pub symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateLimit {
    pub rate_limit_type: String,
    pub interval: String,
    pub interval_num: u32,
    pub limit: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SymbolInfo {
    pub symbol: String,
    pub status: String,
    pub base_asset: String,
    pub quote_asset: String,
    #[serde(default)]
    pub filters: Vec<Filter>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "filterType")]
pub enum Filter {
    #[serde(rename = "PRICE_FILTER", rename_all = "camelCase")]
    Price { tick_size: Decimal, min_price: Decimal, max_price: Decimal },
    #[serde(rename = "LOT_SIZE", rename_all = "camelCase")]
    LotSize { step_size: Decimal, min_qty: Decimal, max_qty: Decimal },
    #[serde(rename = "NOTIONAL", rename_all = "camelCase")]
    Notional { min_notional: Decimal },
    #[serde(rename = "MIN_NOTIONAL", rename_all = "camelCase")]
    MinNotional { min_notional: Decimal },
    #[serde(other)]
    Other,
}

/// `GET /api/v3/depth`
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepthSnapshot {
    pub last_update_id: u64,
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
}

/// `<symbol>@depth` stream message.
#[derive(Debug, Deserialize)]
pub struct DepthUpdate {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "U")]
    pub first_update_id: u64,
    #[serde(rename = "u")]
    pub final_update_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<(Decimal, Decimal)>,
    #[serde(rename = "a")]
    pub asks: Vec<(Decimal, Decimal)>,
}

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

pub async fn server_time(client: &BinanceClient) -> Result<i64, ExchangeError> {
    let t: ServerTime = client.get_public("/api/v3/time", &[]).await?;
    client.set_server_time(t.server_time);
    Ok(t.server_time)
}

pub async fn exchange_info(
    client: &BinanceClient,
    native_symbol: &str,
) -> Result<ExchangeInfo, ExchangeError> {
    let info: ExchangeInfo =
        client.get_public("/api/v3/exchangeInfo", &[("symbol", native_symbol.to_owned())]).await?;
    client.set_server_time(info.server_time);
    Ok(info)
}

pub async fn depth(
    client: &BinanceClient,
    native_symbol: &str,
    limit: u16,
) -> Result<DepthSnapshot, ExchangeError> {
    client
        .get_public(
            "/api/v3/depth",
            &[("symbol", native_symbol.to_owned()), ("limit", limit.to_string())],
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    pub const EXCHANGE_INFO: &str =
        include_str!("../../../tests/fixtures/binance/exchange_info_btcusdt.json");
    pub const DEPTH: &str = include_str!("../../../tests/fixtures/binance/depth_btcusdt.json");
    pub const DEPTH_UPDATE: &str =
        include_str!("../../../tests/fixtures/binance/depth_update.json");

    #[test]
    fn exchange_info_fixture_parses() {
        let info: ExchangeInfo = serde_json::from_str(EXCHANGE_INFO).unwrap();
        assert!(info.server_time > 0);
        assert_eq!(info.symbols.len(), 1);
        let s = &info.symbols[0];
        assert_eq!(
            (s.symbol.as_str(), s.base_asset.as_str(), s.quote_asset.as_str()),
            ("BTCUSDT", "BTC", "USDT")
        );
        assert!(s.filters.iter().any(|f| matches!(f, Filter::Price { .. })));
        assert!(s.filters.iter().any(|f| matches!(f, Filter::LotSize { .. })));
        assert!(s.filters.iter().any(|f| matches!(f, Filter::Other)));
        assert!(info.rate_limits.iter().any(|r| r.rate_limit_type == "REQUEST_WEIGHT"));
    }

    #[test]
    fn depth_fixture_parses() {
        let d: DepthSnapshot = serde_json::from_str(DEPTH).unwrap();
        assert_eq!(d.last_update_id, 99997813480);
        assert_eq!(d.bids.len(), 5);
        assert_eq!(d.bids[0].0, "77584.02".parse::<Decimal>().unwrap());
        assert!(d.bids[0].0 > d.bids[1].0, "bids descend");
        assert!(d.asks[0].0 < d.asks[1].0, "asks ascend");
    }

    #[test]
    fn depth_update_fixture_parses() {
        let u: DepthUpdate = serde_json::from_str(DEPTH_UPDATE).unwrap();
        assert_eq!((u.first_update_id, u.final_update_id), (99997813481, 99997813483));
        assert_eq!(u.symbol, "BTCUSDT");
        assert_eq!(u.bids[1].1, Decimal::ZERO, "zero quantity = remove level");
    }
}
