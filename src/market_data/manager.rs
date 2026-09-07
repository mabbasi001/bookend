//! Holds the latest valid book per exchange and answers "is this fresh enough
//! to quote on?". Consumes adapter streams; never places orders.
//!
//! Books are published on a `watch` channel: the strategy only wants the newest
//! one, and `watch` never lags or drops the way `broadcast` does.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::events::MarketEvent;
use crate::types::{ExchangeId, OrderBook};

/// Latest book for one exchange; `None` while disconnected or resyncing.
pub type BookWatch = watch::Receiver<Option<Arc<OrderBook>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    Fresh,
    /// Older than `max_age`.
    Stale {
        age: Duration,
    },
    /// No valid book (disconnected, resyncing, never connected).
    Missing,
}

impl Freshness {
    pub fn is_fresh(self) -> bool {
        matches!(self, Freshness::Fresh)
    }
}

pub struct MarketDataManager {
    max_age: Duration,
    senders: HashMap<ExchangeId, watch::Sender<Option<Arc<OrderBook>>>>,
}

impl MarketDataManager {
    pub fn new(max_age: Duration) -> Self {
        Self { max_age, senders: HashMap::new() }
    }

    /// Register an exchange stream. Spawns the consumer task; returns the watch
    /// the strategy reads from.
    pub fn attach(
        &mut self,
        exchange: ExchangeId,
        mut events: mpsc::Receiver<MarketEvent>,
        shutdown: CancellationToken,
    ) -> BookWatch {
        let (tx, rx) = watch::channel(None);
        self.senders.insert(exchange, tx.clone());

        tokio::spawn(async move {
            loop {
                let event = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    e = events.recv() => match e {
                        Some(e) => e,
                        None => {
                            warn!(%exchange, "market data stream ended");
                            break;
                        }
                    },
                };
                match event {
                    MarketEvent::Book(book) => {
                        tx.send_replace(Some(book));
                    }
                    MarketEvent::Connected(id) => info!(exchange = %id, "market data connected"),
                    MarketEvent::Disconnected(id) => {
                        warn!(exchange = %id, "market data disconnected; book invalidated");
                        tx.send_replace(None);
                    }
                    MarketEvent::Trade(_) => {}
                }
            }
            tx.send_replace(None);
        });

        rx
    }

    pub fn watch(&self, exchange: ExchangeId) -> Option<BookWatch> {
        self.senders.get(&exchange).map(watch::Sender::subscribe)
    }

    /// Latest book, regardless of age.
    pub fn latest(&self, exchange: ExchangeId) -> Option<Arc<OrderBook>> {
        self.senders.get(&exchange)?.borrow().clone()
    }

    pub fn freshness(&self, exchange: ExchangeId) -> Freshness {
        self.freshness_at(exchange, Instant::now())
    }

    pub fn freshness_at(&self, exchange: ExchangeId, now: Instant) -> Freshness {
        match self.latest(exchange) {
            None => Freshness::Missing,
            Some(book) => {
                let age = now.saturating_duration_since(book.received);
                if age <= self.max_age { Freshness::Fresh } else { Freshness::Stale { age } }
            }
        }
    }

    /// Latest book only if it is fresh enough to quote on.
    pub fn fresh(&self, exchange: ExchangeId) -> Option<Arc<OrderBook>> {
        self.freshness(exchange).is_fresh().then(|| self.latest(exchange)).flatten()
    }

    pub fn exchanges(&self) -> impl Iterator<Item = ExchangeId> + '_ {
        self.senders.keys().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PriceLevel, Symbol};

    fn book(received: Instant) -> Arc<OrderBook> {
        Arc::new(OrderBook {
            exchange: ExchangeId::Paper,
            symbol: Symbol::new("BTC", "USDT"),
            bids: vec![PriceLevel { price: 1.into(), quantity: 1.into() }],
            asks: vec![PriceLevel { price: 2.into(), quantity: 1.into() }],
            sequence: 1,
            timestamp: chrono::Utc::now(),
            received,
        })
    }

    #[tokio::test]
    async fn publishes_books_and_invalidates_on_disconnect() {
        let mut m = MarketDataManager::new(Duration::from_secs(2));
        let (tx, rx) = mpsc::channel(8);
        let token = CancellationToken::new();
        let mut w = m.attach(ExchangeId::Paper, rx, token.clone());

        assert_eq!(m.freshness(ExchangeId::Paper), Freshness::Missing);
        assert!(m.fresh(ExchangeId::Paper).is_none());

        tx.send(MarketEvent::Book(book(Instant::now()))).await.unwrap();
        w.changed().await.unwrap();
        assert!(w.borrow().is_some());
        assert!(m.fresh(ExchangeId::Paper).is_some());
        assert_eq!(m.freshness(ExchangeId::Paper), Freshness::Fresh);

        tx.send(MarketEvent::Disconnected(ExchangeId::Paper)).await.unwrap();
        w.changed().await.unwrap();
        assert!(w.borrow().is_none());
        assert_eq!(m.freshness(ExchangeId::Paper), Freshness::Missing);

        token.cancel();
    }

    #[tokio::test]
    async fn stale_books_are_not_fresh() {
        let mut m = MarketDataManager::new(Duration::from_millis(500));
        let (tx, rx) = mpsc::channel(8);
        let mut w = m.attach(ExchangeId::Paper, rx, CancellationToken::new());
        let received = Instant::now();
        tx.send(MarketEvent::Book(book(received))).await.unwrap();
        w.changed().await.unwrap();

        assert!(
            m.freshness_at(ExchangeId::Paper, received + Duration::from_millis(400)).is_fresh()
        );
        assert!(matches!(
            m.freshness_at(ExchangeId::Paper, received + Duration::from_millis(600)),
            Freshness::Stale { age } if age >= Duration::from_millis(600)
        ));
        assert!(m.latest(ExchangeId::Paper).is_some(), "latest is age-agnostic");
    }

    #[tokio::test]
    async fn stream_end_clears_the_book() {
        let mut m = MarketDataManager::new(Duration::from_secs(2));
        let (tx, rx) = mpsc::channel(8);
        let mut w = m.attach(ExchangeId::Paper, rx, CancellationToken::new());
        tx.send(MarketEvent::Book(book(Instant::now()))).await.unwrap();
        w.changed().await.unwrap();
        drop(tx);
        w.changed().await.unwrap();
        assert!(w.borrow().is_none());
        assert!(m.watch(ExchangeId::Paper).is_some());
        assert!(m.watch(ExchangeId::Binance).is_none());
    }
}
