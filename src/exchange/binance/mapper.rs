//! Binance payloads ↔ internal types.

use std::time::Instant;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use tracing::warn;

use super::rest::{
    Account, AccountPosition, DepthSnapshot, ExecutionReport, Filter, OrderResponse, SymbolInfo,
};
use crate::events::UserEvent;
use crate::exchange::ExchangeError;
use crate::types::{
    Balance, ExchangeId, Fees, Fill, MarketInfo, Order, OrderBook, OrderId, OrderRequest,
    OrderStatus, OrderType, PriceLevel, Side, Symbol,
};

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

// ---------------------------------------------------------------------------
// Account
// ---------------------------------------------------------------------------

/// Assets with nothing in them are dropped: production accounts list hundreds
/// of them and the portfolio treats an absent asset as zero anyway.
pub fn balances(account: &Account) -> Vec<Balance> {
    account
        .balances
        .iter()
        .filter(|b| !(b.free.is_zero() && b.locked.is_zero()))
        .map(|b| Balance {
            asset: b.asset.clone(),
            total: b.free + b.locked,
            available: b.free,
            locked: b.locked,
        })
        .collect()
}

/// Account-level commission. Binance's per-symbol fee endpoint lives under
/// `/sapi`, which the spot testnet does not serve, so the account default
/// stands in for every symbol.
///
/// Missing *or* zero commission is reported as an error rather than as a free
/// tier, so the caller falls back to the configured fees. Zero fees would make
/// every quote pass the strategy's viability check; the spot testnet really
/// does report them, and taking that at face value would make testnet quoting
/// behave unlike live.
pub fn fees(account: &Account) -> Result<Fees, ExchangeError> {
    let per_bps = Decimal::from(10_000);
    let fees = match (&account.commission_rates, account.maker_commission, account.taker_commission)
    {
        (Some(rates), _, _) => Fees {
            maker_bps: (rates.maker * per_bps).normalize(),
            taker_bps: (rates.taker * per_bps).normalize(),
        },
        (None, Some(maker), Some(taker)) => {
            Fees { maker_bps: Decimal::from(maker), taker_bps: Decimal::from(taker) }
        }
        _ => return Err(ExchangeError::Unknown("account carries no commission rates".into())),
    };
    if fees.maker_bps.is_zero() && fees.taker_bps.is_zero() {
        return Err(ExchangeError::Unknown("account reports zero commission".into()));
    }
    Ok(fees)
}

// ---------------------------------------------------------------------------
// Orders
// ---------------------------------------------------------------------------

pub const fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

fn parse_side(raw: &str) -> Result<Side, ExchangeError> {
    match raw {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        other => Err(ExchangeError::Unknown(format!("unknown order side {other}"))),
    }
}

/// `LIMIT_MAKER` is Binance's post-only limit: the exchange rejects it outright
/// if it would take, which is exactly the guarantee a maker wants.
pub const fn order_type_str(order_type: OrderType, post_only: bool) -> &'static str {
    match (order_type, post_only) {
        (OrderType::Limit, true) => "LIMIT_MAKER",
        (OrderType::Limit, false) => "LIMIT",
        (OrderType::Market, _) => "MARKET",
    }
}

/// An unrecognised status maps to [`OrderStatus::Unknown`] rather than a guess:
/// the order then goes through reconciliation instead of being trusted.
pub fn order_status(raw: &str) -> OrderStatus {
    match raw {
        "NEW" => OrderStatus::Open,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" => OrderStatus::Cancelled,
        "PENDING_CANCEL" => OrderStatus::CancelRequested,
        "REJECTED" => OrderStatus::Rejected,
        // `EXPIRED_IN_MATCH` is self-trade prevention killing the order.
        "EXPIRED" | "EXPIRED_IN_MATCH" => OrderStatus::Expired,
        other => {
            warn!(status = other, "unknown binance order status; treating as unknown");
            OrderStatus::Unknown
        }
    }
}

/// Which query parameter identifies an order, by the id we hold.
pub fn order_id_param(id: &OrderId) -> (&'static str, String) {
    match id {
        OrderId::Exchange(id) => ("orderId", id.clone()),
        OrderId::Client(id) => ("origClientOrderId", id.clone()),
    }
}

/// Query parameters for `POST /api/v3/order`.
pub fn place_order_params(
    native_symbol: &str,
    request: &OrderRequest,
) -> Result<Vec<(&'static str, String)>, ExchangeError> {
    if request.quantity <= Decimal::ZERO {
        return Err(ExchangeError::InvalidOrder("quantity must be positive".into()));
    }
    let mut params = vec![
        ("symbol", native_symbol.to_owned()),
        ("side", side_str(request.side).to_owned()),
        ("type", order_type_str(request.order_type, request.post_only).to_owned()),
        ("quantity", decimal(request.quantity)),
        ("newClientOrderId", request.client_order_id.clone()),
        // RESULT carries the status and filled quantity an `Order` needs;
        // FULL would add a fills array the engine takes from the user stream.
        ("newOrderRespType", "RESULT".to_owned()),
    ];
    match request.order_type {
        OrderType::Limit => {
            let price = request
                .price
                .ok_or_else(|| ExchangeError::InvalidOrder("limit order needs a price".into()))?;
            if price <= Decimal::ZERO {
                return Err(ExchangeError::InvalidOrder("price must be positive".into()));
            }
            params.push(("price", decimal(price)));
            // LIMIT_MAKER rejects `timeInForce` (-1106); it is always GTC.
            if !request.post_only {
                params.push(("timeInForce", "GTC".to_owned()));
            }
        }
        OrderType::Market => {
            if request.price.is_some() {
                return Err(ExchangeError::InvalidOrder(
                    "market order cannot carry a price".into(),
                ));
            }
        }
    }
    Ok(params)
}

pub fn order(symbol: &Symbol, raw: &OrderResponse) -> Result<Order, ExchangeError> {
    // `time` on a lookup, `transactTime` on a placement or cancel.
    let created_at = raw.time.or(raw.transact_time).map_or_else(Utc::now, timestamp);
    let updated_at = raw.update_time.or(raw.transact_time).map_or(created_at, timestamp);
    // Only MARKET has no price; every LIMIT flavour carries one.
    let order_type = if raw.order_type == "MARKET" { OrderType::Market } else { OrderType::Limit };
    Ok(Order {
        exchange: ExchangeId::Binance,
        exchange_order_id: Some(raw.order_id.to_string()),
        // A cancel response puts the cancel request's own id in `clientOrderId`
        // and the order's id in `origClientOrderId`.
        client_order_id: raw
            .orig_client_order_id
            .clone()
            .unwrap_or_else(|| raw.client_order_id.clone()),
        symbol: symbol.clone(),
        side: parse_side(&raw.side)?,
        order_type,
        price: (raw.price > Decimal::ZERO).then_some(raw.price.normalize()),
        quantity: raw.orig_qty.normalize(),
        filled_quantity: raw.executed_qty.normalize(),
        status: order_status(&raw.status),
        created_at,
        updated_at,
    })
}

/// Plain decimal text for the wire: Binance rejects exponent notation, and
/// trailing zeros only make the signed query longer.
fn decimal(value: Decimal) -> String {
    value.normalize().to_string()
}

// ---------------------------------------------------------------------------
// User data stream
// ---------------------------------------------------------------------------

/// A cancel report names the cancelled order in `C` and the cancel request
/// itself in `c`; every other report leaves `C` empty.
fn report_client_order_id(report: &ExecutionReport) -> String {
    if report.orig_client_order_id.is_empty() {
        report.client_order_id.clone()
    } else {
        report.orig_client_order_id.clone()
    }
}

/// Everything one `executionReport` says: the execution first, so the
/// portfolio sees the fill before the order reaches a terminal state.
pub fn user_events(
    symbol: &Symbol,
    report: &ExecutionReport,
) -> Result<Vec<UserEvent>, ExchangeError> {
    let mut events = Vec::with_capacity(2);
    if let Some(fill) = execution_fill(symbol, report)? {
        events.push(UserEvent::Fill(fill));
    }
    events.push(UserEvent::Order(execution_order(symbol, report)?));
    Ok(events)
}

pub fn execution_order(symbol: &Symbol, report: &ExecutionReport) -> Result<Order, ExchangeError> {
    let order_type =
        if report.order_type == "MARKET" { OrderType::Market } else { OrderType::Limit };
    Ok(Order {
        exchange: ExchangeId::Binance,
        exchange_order_id: Some(report.order_id.to_string()),
        client_order_id: report_client_order_id(report),
        symbol: symbol.clone(),
        side: parse_side(&report.side)?,
        order_type,
        price: (report.price > Decimal::ZERO).then_some(report.price.normalize()),
        quantity: report.quantity.normalize(),
        filled_quantity: report.cumulative_filled_quantity.normalize(),
        status: order_status(&report.status),
        created_at: timestamp(report.order_creation_time),
        updated_at: timestamp(report.transaction_time),
    })
}

/// A report is a fill only when it reports an execution; `NEW`, `CANCELED`
/// and the rest move no money.
pub fn execution_fill(
    symbol: &Symbol,
    report: &ExecutionReport,
) -> Result<Option<Fill>, ExchangeError> {
    if report.execution_type != "TRADE" || report.last_executed_quantity <= Decimal::ZERO {
        return Ok(None);
    }
    Ok(Some(Fill {
        exchange: ExchangeId::Binance,
        client_order_id: report_client_order_id(report),
        trade_id: report.trade_id.to_string(),
        symbol: symbol.clone(),
        side: parse_side(&report.side)?,
        price: report.last_executed_price.normalize(),
        quantity: report.last_executed_quantity.normalize(),
        fee: report.commission.normalize(),
        // Binance omits the asset when nothing was charged.
        fee_asset: report.commission_asset.clone().unwrap_or_else(|| symbol.quote.clone()),
        timestamp: timestamp(report.transaction_time),
    }))
}

/// `outboundAccountPosition` carries authoritative totals for every asset that
/// moved, unlike `balanceUpdate`, which is only a delta.
pub fn stream_balances(position: &AccountPosition) -> Vec<Balance> {
    position
        .balances
        .iter()
        .map(|b| Balance {
            asset: b.asset.clone(),
            total: b.free + b.locked,
            available: b.free,
            locked: b.locked,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::binance::rest::ExchangeInfo;

    const EXCHANGE_INFO: &str =
        include_str!("../../../tests/fixtures/binance/exchange_info_btcusdt.json");
    const DEPTH: &str = include_str!("../../../tests/fixtures/binance/depth_btcusdt.json");
    const ACCOUNT: &str = include_str!("../../../tests/fixtures/binance/account.json");
    const ORDER_NEW: &str = include_str!("../../../tests/fixtures/binance/order_new.json");
    const ORDER_CANCELED: &str =
        include_str!("../../../tests/fixtures/binance/order_canceled.json");
    const OPEN_ORDERS: &str = include_str!("../../../tests/fixtures/binance/open_orders.json");
    const REPORT_NEW: &str =
        include_str!("../../../tests/fixtures/binance/execution_report_new.json");
    const REPORT_TRADE: &str =
        include_str!("../../../tests/fixtures/binance/execution_report_trade.json");
    const REPORT_CANCELED: &str =
        include_str!("../../../tests/fixtures/binance/execution_report_canceled.json");
    const ACCOUNT_POSITION: &str =
        include_str!("../../../tests/fixtures/binance/outbound_account_position.json");

    fn report(json: &str) -> ExecutionReport {
        let event: crate::exchange::binance::rest::UserStreamEvent =
            serde_json::from_str(json).unwrap();
        match event {
            crate::exchange::binance::rest::UserStreamEvent::ExecutionReport(r) => *r,
            other => panic!("not an execution report: {other:?}"),
        }
    }

    fn btc() -> Symbol {
        Symbol::new("BTC", "USDT")
    }

    fn request(side: Side, post_only: bool) -> OrderRequest {
        OrderRequest {
            symbol: btc(),
            side,
            order_type: OrderType::Limit,
            price: Some(dec("77000.00")),
            quantity: dec("0.01000"),
            client_order_id: "mmppabc123000009".into(),
            post_only,
        }
    }

    fn param<'a>(params: &'a [(&str, String)], key: &str) -> Option<&'a str> {
        params.iter().find(|(k, _)| *k == key).map(|(_, v)| v.as_str())
    }

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

    #[test]
    fn balances_drop_empty_assets_and_split_free_from_locked() {
        let account: Account = serde_json::from_str(ACCOUNT).unwrap();
        let b = balances(&account);
        assert_eq!(b.len(), 2, "ETH and BNB are empty: {b:?}");
        assert_eq!(b[0].asset, "BTC");
        assert_eq!(b[0].total, dec("0.52"));
        assert_eq!(b[0].available, dec("0.5"));
        assert_eq!(b[0].locked, dec("0.02"));
        assert_eq!(b[1].total, dec("12801.00"));
    }

    #[test]
    fn fees_prefer_commission_rates_over_the_legacy_fields() {
        let account: Account = serde_json::from_str(ACCOUNT).unwrap();
        let f = fees(&account).unwrap();
        assert_eq!(f.maker_bps, dec("10"), "0.001 = 10 bps");
        assert_eq!(f.taker_bps, dec("20"));
    }

    #[test]
    fn fees_fall_back_to_the_legacy_basis_point_fields() {
        let account: Account = serde_json::from_str(
            r#"{"makerCommission":15,"takerCommission":25,"canTrade":true,"balances":[]}"#,
        )
        .unwrap();
        let f = fees(&account).unwrap();
        assert_eq!((f.maker_bps, f.taker_bps), (dec("15"), dec("25")));
    }

    #[test]
    fn an_account_reporting_no_commission_is_an_error_not_a_zero_tier() {
        let account: Account = serde_json::from_str(r#"{"canTrade":true,"balances":[]}"#).unwrap();
        assert!(matches!(fees(&account), Err(ExchangeError::Unknown(_))));
    }

    /// What the spot testnet reports. Free trading would make every quote pass
    /// the viability check, so the caller must fall back to configured fees.
    #[test]
    fn a_zero_commission_tier_is_treated_as_no_answer() {
        let rates: Account = serde_json::from_str(
            r#"{"commissionRates":{"maker":"0.00000000","taker":"0.00000000"},
                "canTrade":true,"balances":[]}"#,
        )
        .unwrap();
        assert!(matches!(fees(&rates), Err(ExchangeError::Unknown(_))));

        let legacy: Account = serde_json::from_str(
            r#"{"makerCommission":0,"takerCommission":0,"canTrade":true,"balances":[]}"#,
        )
        .unwrap();
        assert!(matches!(fees(&legacy), Err(ExchangeError::Unknown(_))));

        // A zero maker rebate against a real taker fee is still a real tier.
        let maker_only: Account = serde_json::from_str(
            r#"{"commissionRates":{"maker":"0.00000000","taker":"0.00100000"},
                "canTrade":true,"balances":[]}"#,
        )
        .unwrap();
        assert_eq!(fees(&maker_only).unwrap().taker_bps, dec("10"));
    }

    #[test]
    fn post_only_becomes_limit_maker_without_time_in_force() {
        let p = place_order_params("BTCUSDT", &request(Side::Buy, true)).unwrap();
        assert_eq!(param(&p, "type"), Some("LIMIT_MAKER"));
        assert_eq!(param(&p, "side"), Some("BUY"));
        assert_eq!(param(&p, "symbol"), Some("BTCUSDT"));
        assert_eq!(param(&p, "newClientOrderId"), Some("mmppabc123000009"));
        assert_eq!(param(&p, "newOrderRespType"), Some("RESULT"));
        assert_eq!(
            param(&p, "timeInForce"),
            None,
            "LIMIT_MAKER is rejected with timeInForce (-1106)"
        );
    }

    #[test]
    fn a_plain_limit_carries_gtc() {
        let p = place_order_params("BTCUSDT", &request(Side::Sell, false)).unwrap();
        assert_eq!(param(&p, "type"), Some("LIMIT"));
        assert_eq!(param(&p, "side"), Some("SELL"));
        assert_eq!(param(&p, "timeInForce"), Some("GTC"));
    }

    #[test]
    fn wire_decimals_are_plain_and_trimmed() {
        let p = place_order_params("BTCUSDT", &request(Side::Buy, true)).unwrap();
        assert_eq!(param(&p, "quantity"), Some("0.01"), "0.01000 loses its trailing zeros");
        assert_eq!(param(&p, "price"), Some("77000"));
        // Binance rejects exponent notation; the smallest BTC lot must survive.
        assert_eq!(decimal(dec("0.00001")), "0.00001");
        assert_eq!(decimal(dec("0.000000010")), "0.00000001");
    }

    #[test]
    fn placement_rejects_unusable_requests_before_the_network() {
        let mut r = request(Side::Buy, true);
        r.price = None;
        assert!(matches!(
            place_order_params("BTCUSDT", &r),
            Err(ExchangeError::InvalidOrder(m)) if m.contains("price")
        ));

        let mut r = request(Side::Buy, true);
        r.quantity = Decimal::ZERO;
        assert!(matches!(place_order_params("BTCUSDT", &r), Err(ExchangeError::InvalidOrder(_))));

        let mut r = request(Side::Buy, true);
        r.price = Some(dec("-1"));
        assert!(matches!(place_order_params("BTCUSDT", &r), Err(ExchangeError::InvalidOrder(_))));

        let mut r = request(Side::Buy, true);
        r.order_type = OrderType::Market;
        assert!(matches!(place_order_params("BTCUSDT", &r), Err(ExchangeError::InvalidOrder(_))));
        r.price = None;
        let p = place_order_params("BTCUSDT", &r).unwrap();
        assert_eq!(param(&p, "type"), Some("MARKET"));
        assert_eq!(param(&p, "price"), None);
    }

    #[test]
    fn an_order_is_looked_up_by_whichever_id_we_hold() {
        assert_eq!(
            order_id_param(&OrderId::Client("mmpp1".into())),
            ("origClientOrderId", "mmpp1".to_owned())
        );
        assert_eq!(
            order_id_param(&OrderId::Exchange("4059123".into())),
            ("orderId", "4059123".to_owned())
        );
    }

    #[test]
    fn statuses_map_and_anything_unrecognised_needs_reconciliation() {
        assert_eq!(order_status("NEW"), OrderStatus::Open);
        assert_eq!(order_status("PARTIALLY_FILLED"), OrderStatus::PartiallyFilled);
        assert_eq!(order_status("FILLED"), OrderStatus::Filled);
        assert_eq!(order_status("CANCELED"), OrderStatus::Cancelled);
        assert_eq!(order_status("PENDING_CANCEL"), OrderStatus::CancelRequested);
        assert_eq!(order_status("REJECTED"), OrderStatus::Rejected);
        assert_eq!(order_status("EXPIRED"), OrderStatus::Expired);
        assert_eq!(order_status("EXPIRED_IN_MATCH"), OrderStatus::Expired);
        assert_eq!(order_status("SOMETHING_NEW"), OrderStatus::Unknown);
    }

    #[test]
    fn placement_response_becomes_an_open_order() {
        let raw: OrderResponse = serde_json::from_str(ORDER_NEW).unwrap();
        let o = order(&btc(), &raw).unwrap();
        assert_eq!(o.exchange, ExchangeId::Binance);
        assert_eq!(o.exchange_order_id.as_deref(), Some("4059123"));
        assert_eq!(o.client_order_id, "mmppabc123000009");
        assert_eq!(o.side, Side::Buy);
        assert_eq!(o.order_type, OrderType::Limit, "LIMIT_MAKER is still a limit order");
        assert_eq!(o.price, Some(dec("77000")));
        assert_eq!(o.quantity, dec("0.01"));
        assert_eq!(o.filled_quantity, Decimal::ZERO);
        assert_eq!(o.status, OrderStatus::Open);
        assert_eq!(o.created_at, timestamp(1757606400123));
        assert_eq!(o.updated_at, o.created_at, "transactTime stands in for updateTime");
        assert_eq!(o.remaining_quantity(), dec("0.01"));
    }

    #[test]
    fn a_cancel_response_is_keyed_by_the_cancelled_order_not_the_cancel_request() {
        let raw: OrderResponse = serde_json::from_str(ORDER_CANCELED).unwrap();
        let o = order(&btc(), &raw).unwrap();
        assert_eq!(o.client_order_id, "mmppabc123000009");
        assert_eq!(o.status, OrderStatus::Cancelled);
        assert!(o.status.is_terminal());
    }

    #[test]
    fn open_orders_carry_their_own_timestamps_and_fills() {
        let raw: Vec<OrderResponse> = serde_json::from_str(OPEN_ORDERS).unwrap();
        let orders: Vec<Order> = raw.iter().map(|r| order(&btc(), r).unwrap()).collect();

        assert_eq!(orders[0].status, OrderStatus::Open);
        let partial = &orders[1];
        assert_eq!(partial.side, Side::Sell);
        assert_eq!(partial.status, OrderStatus::PartiallyFilled);
        assert_eq!(partial.filled_quantity, dec("0.004"));
        assert_eq!(partial.remaining_quantity(), dec("0.006"));
        assert_eq!(partial.created_at, timestamp(1757606401000));
        assert_eq!(partial.updated_at, timestamp(1757606402500));
        assert!(partial.status.is_live());
    }

    #[test]
    fn a_market_order_response_has_no_price() {
        let mut raw: OrderResponse = serde_json::from_str(ORDER_NEW).unwrap();
        raw.order_type = "MARKET".into();
        raw.price = Decimal::ZERO;
        let o = order(&btc(), &raw).unwrap();
        assert_eq!(o.order_type, OrderType::Market);
        assert_eq!(o.price, None);
    }

    #[test]
    fn an_unreadable_side_is_an_error() {
        let mut raw: OrderResponse = serde_json::from_str(ORDER_NEW).unwrap();
        raw.side = "SIDEWAYS".into();
        assert!(matches!(order(&btc(), &raw), Err(ExchangeError::Unknown(_))));
    }

    #[test]
    fn a_new_report_is_an_open_order_and_no_fill() {
        let events = user_events(&btc(), &report(REPORT_NEW)).unwrap();
        assert_eq!(events.len(), 1, "nothing traded: {events:?}");
        let UserEvent::Order(o) = &events[0] else { panic!("expected an order") };
        assert_eq!(o.client_order_id, "mmppabc123000009");
        assert_eq!(o.status, OrderStatus::Open);
        assert_eq!(o.side, Side::Buy);
        assert_eq!(o.price, Some(dec("77000")));
        assert_eq!(o.quantity, dec("0.01"));
        assert!(o.filled_quantity.is_zero());
        assert_eq!(o.exchange_order_id.as_deref(), Some("4059123"));
        assert_eq!(o.created_at, timestamp(1757606400123));
    }

    #[test]
    fn a_trade_report_yields_the_fill_before_the_order_update() {
        let events = user_events(&btc(), &report(REPORT_TRADE)).unwrap();
        assert_eq!(events.len(), 2);

        let UserEvent::Fill(f) = &events[0] else { panic!("fill must come first") };
        assert_eq!(f.client_order_id, "mmppabc123000009");
        assert_eq!(f.trade_id, "907711");
        assert_eq!(f.side, Side::Buy);
        assert_eq!(f.price, dec("77000"), "last executed price, not the order price");
        assert_eq!(f.quantity, dec("0.004"), "this execution, not the cumulative total");
        assert_eq!(f.fee, dec("0.000004"));
        assert_eq!(f.fee_asset, "BTC");
        assert_eq!(f.timestamp, timestamp(1757606402500));

        let UserEvent::Order(o) = &events[1] else { panic!("expected an order") };
        assert_eq!(o.status, OrderStatus::PartiallyFilled);
        assert_eq!(o.filled_quantity, dec("0.004"), "cumulative");
        assert_eq!(o.remaining_quantity(), dec("0.006"));
    }

    #[test]
    fn a_cancel_report_is_keyed_by_the_cancelled_order() {
        let events = user_events(&btc(), &report(REPORT_CANCELED)).unwrap();
        assert_eq!(events.len(), 1, "a cancel moves no money");
        let UserEvent::Order(o) = &events[0] else { panic!("expected an order") };
        assert_eq!(o.client_order_id, "mmppabc123000009", "not the cancel request's own id");
        assert_eq!(o.status, OrderStatus::Cancelled);
        assert_eq!(o.filled_quantity, dec("0.004"), "the part that did trade is kept");
    }

    #[test]
    fn a_commission_free_execution_is_charged_in_the_quote_asset() {
        let mut r = report(REPORT_TRADE);
        r.commission = Decimal::ZERO;
        r.commission_asset = None;
        let f = execution_fill(&btc(), &r).unwrap().unwrap();
        assert!(f.fee.is_zero());
        assert_eq!(f.fee_asset, "USDT", "the portfolio needs a known asset");
    }

    #[test]
    fn only_an_execution_produces_a_fill() {
        for json in [REPORT_NEW, REPORT_CANCELED] {
            assert!(execution_fill(&btc(), &report(json)).unwrap().is_none());
        }
        // A TRADE that executed nothing is not a fill either.
        let mut r = report(REPORT_TRADE);
        r.last_executed_quantity = Decimal::ZERO;
        assert!(execution_fill(&btc(), &r).unwrap().is_none());
    }

    #[test]
    fn account_position_gives_totals_per_asset() {
        let event: crate::exchange::binance::rest::UserStreamEvent =
            serde_json::from_str(ACCOUNT_POSITION).unwrap();
        let crate::exchange::binance::rest::UserStreamEvent::AccountPosition(p) = event else {
            panic!("wrong variant")
        };
        let b = stream_balances(&p);
        assert_eq!(b.len(), 2);
        assert_eq!(b[0].asset, "BTC");
        assert_eq!(b[0].total, dec("1.004"));
        assert_eq!(b[1].total, dec("10154"), "free + locked");
        assert_eq!(b[1].available, dec("9692"));
        assert_eq!(b[1].locked, dec("462"));
    }
}
