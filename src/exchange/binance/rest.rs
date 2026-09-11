//! REST endpoints and their raw payloads. Nothing here is exposed outside the
//! adapter; `mapper.rs` converts to internal types.

use rust_decimal::Decimal;
use serde::Deserialize;

use super::client::{ApiError, BinanceClient};
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

/// `GET /api/v3/account`
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Account {
    /// Legacy basis-point commission, used when `commission_rates` is absent.
    #[serde(default)]
    pub maker_commission: Option<i64>,
    #[serde(default)]
    pub taker_commission: Option<i64>,
    #[serde(default)]
    pub commission_rates: Option<CommissionRates>,
    pub can_trade: bool,
    pub balances: Vec<AssetBalance>,
}

/// Commission as a rate, not basis points: `"0.001"` = 10 bps.
#[derive(Debug, Deserialize)]
pub struct CommissionRates {
    pub maker: Decimal,
    pub taker: Decimal,
}

#[derive(Debug, Deserialize)]
pub struct AssetBalance {
    pub asset: String,
    pub free: Decimal,
    pub locked: Decimal,
}

/// One order as returned by `POST`/`GET`/`DELETE /api/v3/order` and
/// `GET /api/v3/openOrders`. The three responses differ only in which
/// timestamp they carry and whether the original client id is echoed.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderResponse {
    pub symbol: String,
    pub order_id: i64,
    pub client_order_id: String,
    /// Only on a cancel response, where `client_order_id` is the *cancel*
    /// request's id and this is the id of the order that was cancelled.
    #[serde(default)]
    pub orig_client_order_id: Option<String>,
    #[serde(default)]
    pub price: Decimal,
    pub orig_qty: Decimal,
    #[serde(default)]
    pub executed_qty: Decimal,
    pub status: String,
    #[serde(rename = "type")]
    pub order_type: String,
    pub side: String,
    /// Creation time (`GET /order`, `GET /openOrders`).
    #[serde(default)]
    pub time: Option<i64>,
    /// Time the request was processed (`POST`/`DELETE /order`).
    #[serde(default)]
    pub transact_time: Option<i64>,
    #[serde(default)]
    pub update_time: Option<i64>,
}

/// A frame from the WebSocket API: either the reply to a request we sent or a
/// pushed user data event.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WsApiFrame {
    /// Carries `subscriptionId` and `event`, which a reply never does.
    Push(UserStreamPush),
    Reply(WsApiReply),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserStreamPush {
    pub subscription_id: i64,
    pub event: UserStreamEvent,
}

#[derive(Debug, Deserialize)]
pub struct WsApiReply {
    #[serde(default)]
    pub id: Option<String>,
    pub status: u16,
    #[serde(default)]
    pub error: Option<ApiError>,
}

/// A user data event, as delivered inside a [`UserStreamPush`].
#[derive(Debug, Deserialize)]
#[serde(tag = "e")]
pub enum UserStreamEvent {
    /// Order lifecycle and executions.
    #[serde(rename = "executionReport")]
    ExecutionReport(Box<ExecutionReport>),
    /// Authoritative balances for every asset that changed.
    #[serde(rename = "outboundAccountPosition")]
    AccountPosition(AccountPosition),
    /// A balance *delta* (deposit, withdrawal, transfer).
    #[serde(rename = "balanceUpdate")]
    BalanceUpdate(BalanceUpdate),
    #[serde(other)]
    Other,
}

/// Single-letter fields are Binance's; the names here say what they mean.
#[derive(Debug, Deserialize)]
pub struct ExecutionReport {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub client_order_id: String,
    /// Set on a cancel, where `client_order_id` is the cancel request's own id.
    #[serde(rename = "C", default)]
    pub orig_client_order_id: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "o")]
    pub order_type: String,
    #[serde(rename = "q")]
    pub quantity: Decimal,
    #[serde(rename = "p")]
    pub price: Decimal,
    /// What happened (`NEW`, `TRADE`, `CANCELED`, `REJECTED`, `EXPIRED`).
    #[serde(rename = "x")]
    pub execution_type: String,
    /// Where the order stands now.
    #[serde(rename = "X")]
    pub status: String,
    #[serde(rename = "r")]
    pub reject_reason: String,
    #[serde(rename = "i")]
    pub order_id: i64,
    #[serde(rename = "l")]
    pub last_executed_quantity: Decimal,
    #[serde(rename = "z")]
    pub cumulative_filled_quantity: Decimal,
    #[serde(rename = "L")]
    pub last_executed_price: Decimal,
    #[serde(rename = "n")]
    pub commission: Decimal,
    /// Absent when no commission was charged.
    #[serde(rename = "N")]
    pub commission_asset: Option<String>,
    #[serde(rename = "T")]
    pub transaction_time: i64,
    /// `-1` when the report is not an execution.
    #[serde(rename = "t")]
    pub trade_id: i64,
    #[serde(rename = "O")]
    pub order_creation_time: i64,
}

#[derive(Debug, Deserialize)]
pub struct AccountPosition {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "B")]
    pub balances: Vec<StreamBalance>,
}

#[derive(Debug, Deserialize)]
pub struct StreamBalance {
    #[serde(rename = "a")]
    pub asset: String,
    #[serde(rename = "f")]
    pub free: Decimal,
    #[serde(rename = "l")]
    pub locked: Decimal,
}

#[derive(Debug, Deserialize)]
pub struct BalanceUpdate {
    #[serde(rename = "a")]
    pub asset: String,
    #[serde(rename = "d")]
    pub delta: Decimal,
}

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

pub async fn server_time(client: &BinanceClient) -> Result<i64, ExchangeError> {
    let sent_at = chrono::Utc::now().timestamp_millis();
    let t: ServerTime = client.get_public("/api/v3/time", &[]).await?;
    client.set_server_time(t.server_time, sent_at);
    Ok(t.server_time)
}

pub async fn exchange_info(
    client: &BinanceClient,
    native_symbol: &str,
) -> Result<ExchangeInfo, ExchangeError> {
    let sent_at = chrono::Utc::now().timestamp_millis();
    let info: ExchangeInfo =
        client.get_public("/api/v3/exchangeInfo", &[("symbol", native_symbol.to_owned())]).await?;
    client.set_server_time(info.server_time, sent_at);
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

// --- account ---------------------------------------------------------------

/// `GET /api/v3/account` (`USER_DATA`) — balances plus the account's
/// commission tier.
pub async fn account(client: &BinanceClient) -> Result<Account, ExchangeError> {
    client.get_signed("/api/v3/account", &[]).await
}

// --- orders ----------------------------------------------------------------

/// `POST /api/v3/order` (`TRADE`). `params` comes from
/// [`mapper::place_order_params`](super::mapper::place_order_params).
pub async fn place_order(
    client: &BinanceClient,
    params: &[(&str, String)],
) -> Result<OrderResponse, ExchangeError> {
    client.post_signed("/api/v3/order", params).await
}

/// `GET /api/v3/order` (`USER_DATA`) — lookup by `orderId` or
/// `origClientOrderId`.
pub async fn order(
    client: &BinanceClient,
    params: &[(&str, String)],
) -> Result<OrderResponse, ExchangeError> {
    client.get_signed("/api/v3/order", params).await
}

/// `GET /api/v3/openOrders` (`USER_DATA`) for one symbol.
pub async fn open_orders(
    client: &BinanceClient,
    native_symbol: &str,
) -> Result<Vec<OrderResponse>, ExchangeError> {
    client.get_signed("/api/v3/openOrders", &[("symbol", native_symbol.to_owned())]).await
}

/// `DELETE /api/v3/order` (`TRADE`) — cancel by `orderId` or
/// `origClientOrderId`.
pub async fn cancel_order(
    client: &BinanceClient,
    params: &[(&str, String)],
) -> Result<OrderResponse, ExchangeError> {
    client.delete_signed("/api/v3/order", params).await
}

/// `DELETE /api/v3/openOrders` (`TRADE`) — cancel every open order for the
/// symbol, returning how many the exchange reported.
///
/// The response array mixes plain orders with order-list objects, so it is
/// counted rather than parsed: the call either cancels everything or fails.
pub async fn cancel_open_orders(
    client: &BinanceClient,
    native_symbol: &str,
) -> Result<usize, ExchangeError> {
    let cancelled: serde_json::Value =
        client.delete_signed("/api/v3/openOrders", &[("symbol", native_symbol.to_owned())]).await?;
    Ok(cancelled.as_array().map_or(0, Vec::len))
}

#[cfg(test)]
mod tests {
    use super::*;

    pub const EXCHANGE_INFO: &str =
        include_str!("../../../tests/fixtures/binance/exchange_info_btcusdt.json");
    pub const DEPTH: &str = include_str!("../../../tests/fixtures/binance/depth_btcusdt.json");
    pub const DEPTH_UPDATE: &str =
        include_str!("../../../tests/fixtures/binance/depth_update.json");
    pub const ACCOUNT: &str = include_str!("../../../tests/fixtures/binance/account.json");
    pub const ORDER_NEW: &str = include_str!("../../../tests/fixtures/binance/order_new.json");
    pub const ORDER_CANCELED: &str =
        include_str!("../../../tests/fixtures/binance/order_canceled.json");
    pub const OPEN_ORDERS: &str = include_str!("../../../tests/fixtures/binance/open_orders.json");
    pub const REPORT_NEW: &str =
        include_str!("../../../tests/fixtures/binance/execution_report_new.json");
    pub const REPORT_TRADE: &str =
        include_str!("../../../tests/fixtures/binance/execution_report_trade.json");
    pub const REPORT_CANCELED: &str =
        include_str!("../../../tests/fixtures/binance/execution_report_canceled.json");
    pub const ACCOUNT_POSITION: &str =
        include_str!("../../../tests/fixtures/binance/outbound_account_position.json");

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

    #[test]
    fn account_fixture_parses() {
        let a: Account = serde_json::from_str(ACCOUNT).unwrap();
        assert!(a.can_trade);
        assert_eq!(a.maker_commission, Some(10));
        let rates = a.commission_rates.as_ref().unwrap();
        assert_eq!(rates.maker, "0.001".parse::<Decimal>().unwrap());
        assert_eq!(a.balances.len(), 4);
        assert_eq!(a.balances[0].asset, "BTC");
        assert_eq!(a.balances[0].locked, "0.02".parse::<Decimal>().unwrap());
    }

    #[test]
    fn account_without_commission_rates_still_parses() {
        let a: Account = serde_json::from_str(
            r#"{"makerCommission":15,"takerCommission":15,"canTrade":true,"balances":[]}"#,
        )
        .unwrap();
        assert!(a.commission_rates.is_none());
        assert_eq!(a.taker_commission, Some(15));
    }

    #[test]
    fn place_order_response_fixture_parses() {
        let o: OrderResponse = serde_json::from_str(ORDER_NEW).unwrap();
        assert_eq!(o.order_id, 4059123);
        assert_eq!(o.client_order_id, "mmppabc123000009");
        assert_eq!(o.orig_client_order_id, None, "only a cancel echoes the original id");
        assert_eq!(o.status, "NEW");
        assert_eq!(o.order_type, "LIMIT_MAKER");
        assert_eq!(o.executed_qty, Decimal::ZERO);
        assert_eq!(o.transact_time, Some(1757606400123));
        assert_eq!(o.time, None, "POST carries transactTime, not time");
    }

    #[test]
    fn cancel_response_echoes_the_original_client_id() {
        let o: OrderResponse = serde_json::from_str(ORDER_CANCELED).unwrap();
        assert_eq!(o.orig_client_order_id.as_deref(), Some("mmppabc123000009"));
        assert_eq!(o.client_order_id, "cancelMyOrder1", "this is the cancel request's own id");
        assert_eq!(o.status, "CANCELED");
    }

    #[test]
    fn open_orders_fixture_parses() {
        let orders: Vec<OrderResponse> = serde_json::from_str(OPEN_ORDERS).unwrap();
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[1].status, "PARTIALLY_FILLED");
        assert_eq!(orders[1].executed_qty, "0.004".parse::<Decimal>().unwrap());
        assert_eq!(orders[1].time, Some(1757606401000));
        assert_eq!(orders[1].update_time, Some(1757606402500));
    }

    #[test]
    fn execution_reports_dispatch_on_the_event_field() {
        let e: UserStreamEvent = serde_json::from_str(REPORT_NEW).unwrap();
        let UserStreamEvent::ExecutionReport(r) = e else { panic!("wrong variant") };
        assert_eq!(r.symbol, "BTCUSDT");
        assert_eq!(r.client_order_id, "mmppabc123000009");
        assert_eq!(r.orig_client_order_id, "", "not a cancel");
        assert_eq!((r.execution_type.as_str(), r.status.as_str()), ("NEW", "NEW"));
        assert_eq!(r.trade_id, -1, "no execution");
        assert_eq!(r.commission_asset, None);
        assert_eq!(r.order_creation_time, 1757606400123);
    }

    #[test]
    fn a_trade_report_carries_the_execution_and_its_commission() {
        let e: UserStreamEvent = serde_json::from_str(REPORT_TRADE).unwrap();
        let UserStreamEvent::ExecutionReport(r) = e else { panic!("wrong variant") };
        assert_eq!(r.execution_type, "TRADE");
        assert_eq!(r.status, "PARTIALLY_FILLED");
        assert_eq!(r.last_executed_quantity, "0.004".parse::<Decimal>().unwrap());
        assert_eq!(r.last_executed_price, "77000".parse::<Decimal>().unwrap());
        assert_eq!(r.commission, "0.000004".parse::<Decimal>().unwrap());
        assert_eq!(r.commission_asset.as_deref(), Some("BTC"));
        assert_eq!(r.trade_id, 907711);
    }

    #[test]
    fn a_cancel_report_names_the_cancelled_order_in_the_orig_field() {
        let e: UserStreamEvent = serde_json::from_str(REPORT_CANCELED).unwrap();
        let UserStreamEvent::ExecutionReport(r) = e else { panic!("wrong variant") };
        assert_eq!(r.client_order_id, "cancelMyOrder1");
        assert_eq!(r.orig_client_order_id, "mmppabc123000009");
        assert_eq!(r.status, "CANCELED");
    }

    #[test]
    fn account_position_fixture_parses() {
        let e: UserStreamEvent = serde_json::from_str(ACCOUNT_POSITION).unwrap();
        let UserStreamEvent::AccountPosition(p) = e else { panic!("wrong variant") };
        assert_eq!(p.balances.len(), 2);
        assert_eq!(p.balances[1].asset, "USDT");
        assert_eq!(p.balances[1].locked, "462".parse::<Decimal>().unwrap());
        assert_eq!(p.event_time, 1757606402500);
    }

    #[test]
    fn the_other_stream_events_are_recognised_and_unknown_ones_ignored() {
        let delta: UserStreamEvent = serde_json::from_str(
            r#"{"e":"balanceUpdate","E":1,"a":"BTC","d":"-0.05000000","T":1}"#,
        )
        .unwrap();
        let UserStreamEvent::BalanceUpdate(b) = delta else { panic!("wrong variant") };
        assert_eq!(b.asset, "BTC");
        assert_eq!(b.delta, "-0.05".parse::<Decimal>().unwrap());

        let unknown: UserStreamEvent =
            serde_json::from_str(r#"{"e":"somethingNew","E":1}"#).unwrap();
        assert!(matches!(unknown, UserStreamEvent::Other));
    }

    #[test]
    fn a_push_frame_wraps_the_event_and_a_reply_does_not() {
        let push: WsApiFrame =
            serde_json::from_str(&format!(r#"{{"subscriptionId":0,"event":{REPORT_NEW}}}"#))
                .unwrap();
        let WsApiFrame::Push(p) = push else { panic!("expected a push") };
        assert_eq!(p.subscription_id, 0);
        assert!(matches!(p.event, UserStreamEvent::ExecutionReport(_)));

        let ok: WsApiFrame = serde_json::from_str(
            r#"{"id":"bookend-user-stream","status":200,"result":{"subscriptionId":0}}"#,
        )
        .unwrap();
        let WsApiFrame::Reply(r) = ok else { panic!("expected a reply") };
        assert_eq!(r.status, 200);
        assert_eq!(r.id.as_deref(), Some("bookend-user-stream"));
        assert!(r.error.is_none());
    }

    #[test]
    fn a_rejected_subscribe_carries_the_binance_error() {
        let frame: WsApiFrame = serde_json::from_str(
            r#"{"id":"bookend-user-stream","status":400,
                "error":{"code":-2028,"msg":"HMAC-SHA-256 API key is not supported."}}"#,
        )
        .unwrap();
        let WsApiFrame::Reply(r) = frame else { panic!("expected a reply") };
        assert_eq!(r.status, 400);
        let e = r.error.unwrap();
        assert_eq!(e.code, -2028);
        assert!(e.msg.contains("HMAC"));
    }
}
