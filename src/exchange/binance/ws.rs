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

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use super::client::BinanceClient;
use super::mapper;
use super::rest::{self, DepthSnapshot, DepthUpdate};
use crate::events::MarketEvent;
use crate::exchange::ExchangeError;
use crate::market_data::orderbook::LocalBook;
use crate::types::{ExchangeId, OrderBook, Symbol};

/// Levels per side in every published [`OrderBook`].
const PUBLISH_DEPTH: usize = 20;
/// REST snapshot depth. Larger than what we publish so deltas beyond the top
/// levels keep the book consistent.
const SNAPSHOT_LIMIT: u16 = 1000;
const CHANNEL_CAPACITY: usize = 256;
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

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
    #[error("snapshot: {0}")]
    Snapshot(ExchangeError),
    #[error("decode: {0}")]
    Decode(#[from] serde_json::Error),
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
            msg = stream.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return Err(e.into()),
                    None => return Err(SessionError::Closed),
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
        let jitter_pct = (chrono::Utc::now().timestamp_subsec_nanos() % 26) as u32;
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
