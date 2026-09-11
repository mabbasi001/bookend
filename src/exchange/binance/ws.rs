//! Public market-data stream: `<symbol>@depth@100ms` with the documented
//! snapshot + delta synchronisation, gap detection, ping/pong and reconnection.
//!
//! Sync procedure (Binance spot):
//! 1. connect and buffer deltas
//! 2. fetch a REST snapshot (`lastUpdateId`)
//! 3. drop buffered deltas with `u <= lastUpdateId`
//! 4. the first applied delta must satisfy `U <= lastUpdateId + 1 <= u`
//! 5. afterwards any delta with `U > last_u + 1` is a gap → new snapshot
//!
//! [`DepthSync`] is the pure state machine (unit-tested); [`run_stream`] is
//! the I/O loop around it.
//!
//! The private user data stream lives here too. Binance withdrew the REST
//! listen-key endpoints (they answer `410 Gone`), so the stream is opened on
//! the WebSocket API with `userDataStream.subscribe.signature`: one signed
//! request per connection, after which events are pushed down the same socket.
//! There is no key to keep alive, but the connection is capped at 24 hours, so
//! it still re-subscribes on every reconnect.
//!
//! `session.logon` is not an option here: it accepts Ed25519 keys only, and
//! this adapter signs with HMAC-SHA256.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use super::SymbolRegistry;
use super::client::BinanceClient;
use super::mapper;
use super::rest::{self, DepthSnapshot, DepthUpdate, UserStreamEvent, WsApiFrame};
use crate::events::{MarketEvent, UserEvent};
use crate::exchange::ExchangeError;
use crate::market_data::orderbook::LocalBook;
use crate::types::{ExchangeId, OrderBook, Symbol};

/// Levels per side in every published [`OrderBook`].
const PUBLISH_DEPTH: usize = 20;
/// REST snapshot depth. Larger than what we publish so deltas beyond the top
/// levels keep the book consistent.
const SNAPSHOT_LIMIT: u16 = 1000;
const CHANNEL_CAPACITY: usize = 256;
/// `depth@100ms` never goes quiet and Binance pings every 20 s; silence this
/// long means the TCP connection is dead (e.g. network cut without RST).
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// The user stream is silent on a quiet account, so only Binance's own
/// 3-minute pings bound the idle time. Two missed pings means it is dead.
const USER_READ_IDLE_TIMEOUT: Duration = Duration::from_secs(7 * 60);
/// Request id for the subscribe call; one per connection.
const SUBSCRIBE_REQUEST_ID: &str = "bookend-user-stream";

// ---------------------------------------------------------------------------
// Sync state machine
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum SyncState {
    /// No usable snapshot yet; deltas are buffered.
    Buffering(Vec<DepthUpdate>),
    /// Snapshot applied; waiting for the first delta that brackets it.
    AwaitingFirst {
        last_update_id: u64,
    },
    Live {
        last_u: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Stored until a snapshot arrives.
    Buffered,
    /// Book updated; publish it.
    Applied,
    /// Older than what we already have.
    Ignored,
    /// Sequence gap; the book is invalid until a new snapshot is applied.
    Gap,
}

pub struct DepthSync {
    book: LocalBook,
    state: SyncState,
}

impl DepthSync {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            book: LocalBook::new(ExchangeId::Binance, symbol),
            state: SyncState::Buffering(Vec::new()),
        }
    }

    pub fn is_live(&self) -> bool {
        matches!(self.state, SyncState::Live { .. })
    }

    pub fn needs_snapshot(&self) -> bool {
        matches!(self.state, SyncState::Buffering(_))
    }

    pub fn book(&self) -> &LocalBook {
        &self.book
    }

    pub fn snapshot(&self) -> OrderBook {
        self.book.snapshot(PUBLISH_DEPTH)
    }

    /// Feed a stream delta.
    pub fn on_update(&mut self, u: DepthUpdate) -> Outcome {
        match &mut self.state {
            SyncState::Buffering(buf) => {
                buf.push(u);
                Outcome::Buffered
            }
            SyncState::AwaitingFirst { last_update_id } => {
                let last = *last_update_id;
                if u.final_update_id <= last {
                    Outcome::Ignored
                } else if u.first_update_id <= last + 1 {
                    self.apply(&u);
                    self.state = SyncState::Live { last_u: u.final_update_id };
                    Outcome::Applied
                } else {
                    // Snapshot is older than the earliest delta we have: refetch.
                    self.gap()
                }
            }
            SyncState::Live { last_u } => {
                let last = *last_u;
                if u.final_update_id <= last {
                    Outcome::Ignored
                } else if u.first_update_id <= last + 1 {
                    self.apply(&u);
                    self.state = SyncState::Live { last_u: u.final_update_id };
                    Outcome::Applied
                } else {
                    self.gap()
                }
            }
        }
    }

    /// Apply a REST snapshot and drain the buffer through the bracket rule.
    /// Returns the outcome of the last buffered delta (or `Buffered` if the
    /// buffer was empty and we are now awaiting the first live delta).
    pub fn on_snapshot(&mut self, snap: &DepthSnapshot) -> Outcome {
        let buffered = match std::mem::replace(
            &mut self.state,
            SyncState::AwaitingFirst { last_update_id: snap.last_update_id },
        ) {
            SyncState::Buffering(buf) => buf,
            _ => Vec::new(),
        };
        self.book.reset(
            &mapper::levels(&snap.bids),
            &mapper::levels(&snap.asks),
            snap.last_update_id,
            chrono::Utc::now(),
        );
        let mut outcome = Outcome::Buffered;
        for u in buffered {
            outcome = self.on_update(u);
            if outcome == Outcome::Gap {
                break;
            }
        }
        outcome
    }

    fn apply(&mut self, u: &DepthUpdate) {
        self.book.apply(
            &mapper::levels(&u.bids),
            &mapper::levels(&u.asks),
            u.final_update_id,
            mapper::timestamp(u.event_time),
        );
    }

    fn gap(&mut self) -> Outcome {
        self.book.invalidate();
        self.state = SyncState::Buffering(Vec::new());
        Outcome::Gap
    }
}

// ---------------------------------------------------------------------------
// I/O loop
// ---------------------------------------------------------------------------

pub async fn subscribe_market_data(
    client: BinanceClient,
    symbol: Symbol,
) -> Result<mpsc::Receiver<MarketEvent>, ExchangeError> {
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    tokio::spawn(run_stream(client, symbol, tx));
    Ok(rx)
}

/// Reconnect forever (with backoff) until the consumer drops the receiver.
async fn run_stream(client: BinanceClient, symbol: Symbol, tx: mpsc::Sender<MarketEvent>) {
    let mut backoff = Backoff::new(BACKOFF_MIN, BACKOFF_MAX);
    loop {
        match run_session(&client, &symbol, &tx, &mut backoff).await {
            Ok(()) => {
                debug!(exchange = "binance", "market data consumer gone; stream task exiting");
                return;
            }
            Err(e) => {
                warn!(exchange = "binance", error = %e, "market data session ended");
                if tx.send(MarketEvent::Disconnected(ExchangeId::Binance)).await.is_err() {
                    return;
                }
                let delay = backoff.next_delay();
                info!(exchange = "binance", delay_ms = delay.as_millis() as u64, "reconnecting");
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum SessionError {
    #[error("websocket: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("stream closed by server")]
    Closed,
    #[error("no message for {0:?}; connection presumed dead")]
    Idle(Duration),
    #[error("snapshot: {0}")]
    Snapshot(ExchangeError),
    #[error("decode: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("user stream subscribe rejected: status {status} {message}")]
    Subscribe { status: u16, message: String },
}

/// One connection: returns `Ok(())` only when the consumer is gone.
async fn run_session(
    client: &BinanceClient,
    symbol: &Symbol,
    tx: &mpsc::Sender<MarketEvent>,
    backoff: &mut Backoff,
) -> Result<(), SessionError> {
    let native = mapper::native_symbol(symbol);
    let url = format!("{}/ws/{}@depth@100ms", client.endpoints().ws, native.to_ascii_lowercase());
    info!(exchange = "binance", %url, "connecting market data");

    let (ws, _) = connect_async(&url).await?;
    let (mut sink, mut stream) = ws.split();
    backoff.reset();
    if tx.send(MarketEvent::Connected(ExchangeId::Binance)).await.is_err() {
        return Ok(());
    }

    let mut sync = DepthSync::new(symbol.clone());
    let mut snapshot_task: Option<JoinHandle<Result<DepthSnapshot, ExchangeError>>> = None;

    loop {
        tokio::select! {
            msg = tokio::time::timeout(READ_IDLE_TIMEOUT, stream.next()) => {
                let msg = match msg {
                    Ok(Some(Ok(m))) => m,
                    Ok(Some(Err(e))) => return Err(e.into()),
                    Ok(None) => return Err(SessionError::Closed),
                    Err(_) => return Err(SessionError::Idle(READ_IDLE_TIMEOUT)),
                };
                match msg {
                    Message::Text(text) => {
                        let update: DepthUpdate = serde_json::from_str(text.as_str())?;
                        let outcome = sync.on_update(update);
                        match outcome {
                            Outcome::Applied => {
                                if publish(tx, &sync).await.is_err() {
                                    return Ok(());
                                }
                            }
                            Outcome::Gap => {
                                warn!(exchange = "binance", "order book sequence gap; resyncing");
                                if tx.send(MarketEvent::Disconnected(ExchangeId::Binance)).await.is_err() {
                                    return Ok(());
                                }
                            }
                            Outcome::Buffered | Outcome::Ignored => {}
                        }
                        // Fetch a snapshot once deltas are flowing and none is in flight.
                        if sync.needs_snapshot() && snapshot_task.is_none() {
                            let c = client.clone();
                            let n = native.clone();
                            snapshot_task = Some(tokio::spawn(async move {
                                rest::depth(&c, &n, SNAPSHOT_LIMIT).await
                            }));
                        }
                    }
                    Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                    Message::Close(frame) => {
                        debug!(exchange = "binance", ?frame, "close frame");
                        return Err(SessionError::Closed);
                    }
                    Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
            snap = async { snapshot_task.as_mut().expect("guarded").await }, if snapshot_task.is_some() => {
                snapshot_task = None;
                let snap = match snap {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => return Err(SessionError::Snapshot(e)),
                    Err(join) => return Err(SessionError::Snapshot(ExchangeError::Unknown(join.to_string()))),
                };
                debug!(exchange = "binance", last_update_id = snap.last_update_id, "snapshot applied");
                if sync.on_snapshot(&snap) == Outcome::Applied && publish(tx, &sync).await.is_err() {
                    return Ok(());
                }
                // A gap while draining the buffer leaves `needs_snapshot()` true;
                // the next delta triggers another fetch.
            }
        }
    }
}

async fn publish(tx: &mpsc::Sender<MarketEvent>, sync: &DepthSync) -> Result<(), ()> {
    tx.send(MarketEvent::Book(Arc::new(sync.snapshot()))).await.map_err(|_| ())
}

// ---------------------------------------------------------------------------
// User data stream
// ---------------------------------------------------------------------------

/// Opens the stream once before returning, so bad or unpermissioned
/// credentials fail startup instead of looping in a background task.
pub async fn subscribe_user_events(
    client: BinanceClient,
    symbols: SymbolRegistry,
) -> Result<mpsc::Receiver<UserEvent>, ExchangeError> {
    let session = connect_user_stream(&client).await.map_err(|e| match e {
        SessionError::Subscribe { status: 401 | 403, .. } => ExchangeError::Authentication,
        other => ExchangeError::Unknown(format!("user stream: {other}")),
    })?;
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    tokio::spawn(run_user_stream(client, symbols, session, tx));
    Ok(rx)
}

type UserSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Connect to the WebSocket API and subscribe; the returned socket is live.
async fn connect_user_stream(client: &BinanceClient) -> Result<UserSocket, SessionError> {
    let url = client.endpoints().ws_api;
    info!(exchange = "binance", %url, "connecting user stream");
    let (mut ws, _) = connect_async(url).await?;

    // The request carries the signature, so it is never logged.
    let params = client
        .ws_api_signed_params()
        .map_err(|e| SessionError::Subscribe { status: 401, message: e.to_string() })?;
    let request = serde_json::json!({
        "id": SUBSCRIBE_REQUEST_ID,
        "method": "userDataStream.subscribe.signature",
        "params": params,
    });
    ws.send(Message::Text(request.to_string().into())).await?;

    // Events can already be in flight; read until our reply arrives.
    loop {
        let msg = match tokio::time::timeout(USER_READ_IDLE_TIMEOUT, ws.next()).await {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => return Err(e.into()),
            Ok(None) => return Err(SessionError::Closed),
            Err(_) => return Err(SessionError::Idle(USER_READ_IDLE_TIMEOUT)),
        };
        let Message::Text(text) = msg else { continue };
        if let WsApiFrame::Reply(reply) = serde_json::from_str(text.as_str())? {
            if reply.status != 200 {
                let message = reply
                    .error
                    .map_or_else(|| "no detail".to_owned(), |e| format!("{}: {}", e.code, e.msg));
                return Err(SessionError::Subscribe { status: reply.status, message });
            }
            debug!(exchange = "binance", "user stream subscribed");
            return Ok(ws);
        }
        // A pushed event before the reply: dropped, since the caller is not
        // listening yet. Only ever the tail of an already-known state.
    }
}

/// Reconnect forever (with backoff) until the consumer drops the receiver.
async fn run_user_stream(
    client: BinanceClient,
    symbols: SymbolRegistry,
    first_session: UserSocket,
    tx: mpsc::Sender<UserEvent>,
) {
    let mut backoff = Backoff::new(BACKOFF_MIN, BACKOFF_MAX);
    let mut session = Some(first_session);
    loop {
        let ws = match session.take() {
            Some(ws) => ws,
            None => match connect_user_stream(&client).await {
                Ok(ws) => {
                    backoff.reset();
                    ws
                }
                Err(e) => {
                    warn!(exchange = "binance", error = %e, "cannot open the user stream");
                    tokio::time::sleep(backoff.next_delay()).await;
                    continue;
                }
            },
        };

        match run_user_session(&symbols, ws, &tx).await {
            Ok(()) => {
                debug!(exchange = "binance", "user event consumer gone; stream task exiting");
                return;
            }
            Err(e) => {
                warn!(exchange = "binance", error = %e, "user stream session ended");
                let delay = backoff.next_delay();
                info!(
                    exchange = "binance",
                    delay_ms = delay.as_millis() as u64,
                    "reconnecting user stream"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// One connection: returns `Ok(())` only when the consumer is gone.
async fn run_user_session(
    symbols: &SymbolRegistry,
    ws: UserSocket,
    tx: &mpsc::Sender<UserEvent>,
) -> Result<(), SessionError> {
    let (mut sink, mut stream) = ws.split();
    loop {
        let msg = match tokio::time::timeout(USER_READ_IDLE_TIMEOUT, stream.next()).await {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => return Err(e.into()),
            Ok(None) => return Err(SessionError::Closed),
            Err(_) => return Err(SessionError::Idle(USER_READ_IDLE_TIMEOUT)),
        };
        match msg {
            Message::Text(text) => {
                match serde_json::from_str::<WsApiFrame>(text.as_str())? {
                    WsApiFrame::Push(push) => {
                        if !dispatch(symbols, push.event, tx).await {
                            return Ok(());
                        }
                    }
                    // Nothing else is requested on this socket, so a reply here
                    // is the server reporting something about the subscription.
                    WsApiFrame::Reply(reply) if reply.status != 200 => {
                        let message = reply.error.map_or_else(
                            || "no detail".to_owned(),
                            |e| format!("{}: {}", e.code, e.msg),
                        );
                        return Err(SessionError::Subscribe { status: reply.status, message });
                    }
                    WsApiFrame::Reply(_) => {}
                }
            }
            Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
            Message::Close(frame) => {
                debug!(exchange = "binance", ?frame, "user stream close frame");
                return Err(SessionError::Closed);
            }
            Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
}

/// Forwards one user data event. `false` means the consumer is gone.
async fn dispatch(
    symbols: &SymbolRegistry,
    event: UserStreamEvent,
    tx: &mpsc::Sender<UserEvent>,
) -> bool {
    match event {
        UserStreamEvent::ExecutionReport(report) => {
            // Account-wide stream: orders on other symbols are not ours to track.
            let Some(symbol) = symbols.resolve(&report.symbol) else {
                debug!(
                    exchange = "binance",
                    symbol = %report.symbol,
                    "execution report for an untracked symbol; ignored"
                );
                return true;
            };
            if report.status == "REJECTED" {
                warn!(
                    exchange = "binance",
                    client_order_id = %report.client_order_id,
                    reason = %report.reject_reason,
                    "order rejected"
                );
            }
            // One unreadable report must not tear down the connection.
            match mapper::user_events(&symbol, &report) {
                Ok(events) => {
                    for event in events {
                        if tx.send(event).await.is_err() {
                            return false;
                        }
                    }
                }
                Err(e) => warn!(exchange = "binance", error = %e, "unusable execution report"),
            }
        }
        UserStreamEvent::AccountPosition(position) => {
            for balance in mapper::stream_balances(&position) {
                if tx.send(UserEvent::Balance(balance)).await.is_err() {
                    return false;
                }
            }
        }
        // A delta, not a total; `outboundAccountPosition` follows with the truth.
        UserStreamEvent::BalanceUpdate(update) => debug!(
            exchange = "binance",
            asset = %update.asset,
            delta = %update.delta,
            "balance delta"
        ),
        UserStreamEvent::Other => {}
    }
    true
}

// ---------------------------------------------------------------------------
// Backoff
// ---------------------------------------------------------------------------

pub struct Backoff {
    min: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    pub fn new(min: Duration, max: Duration) -> Self {
        Self { min, max, current: min }
    }

    pub fn reset(&mut self) {
        self.current = self.min;
    }

    /// Exponential with up to +25 % jitter, capped at `max`.
    pub fn next_delay(&mut self) -> Duration {
        let base = self.current;
        self.current = (self.current * 2).min(self.max);
        let jitter_pct = chrono::Utc::now().timestamp_subsec_nanos() % 26;
        base + base * jitter_pct / 100
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn upd(first: u64, last: u64, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> DepthUpdate {
        let lv = |v: &[(&str, &str)]| v.iter().map(|(p, q)| (dec(p), dec(q))).collect();
        DepthUpdate {
            event_time: 1_757_600_000_000,
            symbol: "BTCUSDT".into(),
            first_update_id: first,
            final_update_id: last,
            bids: lv(bids),
            asks: lv(asks),
        }
    }

    fn snap(last_update_id: u64) -> DepthSnapshot {
        DepthSnapshot {
            last_update_id,
            bids: vec![(dec("1.00"), dec("5"))],
            asks: vec![(dec("1.02"), dec("3"))],
        }
    }

    fn sync() -> DepthSync {
        DepthSync::new(Symbol::new("BTC", "USDT"))
    }

    #[test]
    fn deltas_are_buffered_until_snapshot_then_drained_by_bracket_rule() {
        let mut s = sync();
        assert!(s.needs_snapshot());
        assert_eq!(s.on_update(upd(95, 98, &[("0.90", "1")], &[])), Outcome::Buffered);
        assert_eq!(s.on_update(upd(99, 101, &[("1.01", "2")], &[])), Outcome::Buffered);
        assert_eq!(s.on_update(upd(102, 103, &[], &[("1.02", "0")])), Outcome::Buffered);

        // Snapshot id 100: first delta (u=98) dropped, second brackets 101, third follows.
        assert_eq!(s.on_snapshot(&snap(100)), Outcome::Applied);
        assert!(s.is_live());
        let b = s.book();
        assert_eq!(b.best_bid().unwrap().price, dec("1.01"));
        assert!(b.best_ask().is_none(), "ask removed by third delta");
        assert!(
            b.snapshot(10).bids.iter().all(|l| l.price != dec("0.90")),
            "stale delta not applied"
        );
        assert_eq!(b.sequence(), 103);
    }

    #[test]
    fn empty_buffer_waits_for_first_bracketing_delta() {
        let mut s = sync();
        assert_eq!(s.on_snapshot(&snap(100)), Outcome::Buffered);
        assert!(!s.is_live());
        assert_eq!(s.on_update(upd(90, 100, &[], &[])), Outcome::Ignored);
        assert_eq!(s.on_update(upd(100, 102, &[("1.00", "6")], &[])), Outcome::Applied);
        assert!(s.is_live());
        assert_eq!(s.book().best_bid().unwrap().quantity, dec("6"));
    }

    #[test]
    fn snapshot_older_than_first_delta_is_a_gap() {
        let mut s = sync();
        s.on_snapshot(&snap(100));
        assert_eq!(s.on_update(upd(105, 106, &[], &[])), Outcome::Gap);
        assert!(s.needs_snapshot());
        assert!(!s.book().is_valid());
    }

    #[test]
    fn live_gap_invalidates_and_rebuffers() {
        let mut s = sync();
        s.on_snapshot(&snap(100));
        s.on_update(upd(101, 101, &[], &[]));
        assert!(s.is_live());
        assert_eq!(s.on_update(upd(102, 102, &[], &[])), Outcome::Applied);
        assert_eq!(s.on_update(upd(102, 102, &[], &[])), Outcome::Ignored, "duplicate");
        assert_eq!(s.on_update(upd(104, 105, &[], &[])), Outcome::Gap);
        assert!(!s.book().is_valid());
        // Subsequent deltas buffer again and a new snapshot recovers.
        assert_eq!(s.on_update(upd(106, 107, &[("1.00", "9")], &[])), Outcome::Buffered);
        assert_eq!(s.on_snapshot(&snap(105)), Outcome::Applied);
        assert!(s.is_live() && s.book().is_valid());
        assert_eq!(s.book().best_bid().unwrap().quantity, dec("9"));
    }

    #[test]
    fn overlapping_delta_is_applied_not_treated_as_gap() {
        let mut s = sync();
        s.on_snapshot(&snap(100));
        s.on_update(upd(101, 103, &[], &[]));
        // U <= last_u + 1 with u > last_u: overlap, apply.
        assert_eq!(s.on_update(upd(103, 105, &[], &[])), Outcome::Applied);
        assert_eq!(s.book().sequence(), 105);
    }

    #[test]
    fn gap_while_draining_buffer_stops_and_requests_snapshot() {
        let mut s = sync();
        s.on_update(upd(101, 101, &[], &[]));
        s.on_update(upd(110, 111, &[], &[]));
        assert_eq!(s.on_snapshot(&snap(100)), Outcome::Gap);
        assert!(s.needs_snapshot());
    }

    #[test]
    fn backoff_doubles_with_jitter_and_caps() {
        let mut b = Backoff::new(Duration::from_secs(1), Duration::from_secs(4));
        let d1 = b.next_delay();
        assert!((Duration::from_secs(1)..=Duration::from_millis(1250)).contains(&d1), "{d1:?}");
        let d2 = b.next_delay();
        assert!((Duration::from_secs(2)..=Duration::from_millis(2500)).contains(&d2), "{d2:?}");
        let d3 = b.next_delay();
        assert!(d3 >= Duration::from_secs(4));
        let d4 = b.next_delay();
        assert!(d4 <= Duration::from_secs(5), "capped: {d4:?}");
        b.reset();
        assert!(b.next_delay() < Duration::from_secs(2));
    }
}
