//! Local order book: absolute quantity per price level, updated by snapshot or
//! delta. Exchange-agnostic — sequencing rules live in each adapter.

use std::collections::BTreeMap;
use std::time::Instant;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::types::{ExchangeId, OrderBook, PriceLevel, Symbol};

#[derive(Debug, Clone)]
pub struct LocalBook {
    exchange: ExchangeId,
    symbol: Symbol,
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    sequence: u64,
    timestamp: DateTime<Utc>,
    valid: bool,
}

impl LocalBook {
    pub fn new(exchange: ExchangeId, symbol: Symbol) -> Self {
        Self {
            exchange,
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            sequence: 0,
            timestamp: Utc::now(),
            valid: false,
        }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// `false` after a sequence gap or before the first snapshot.
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    pub fn invalidate(&mut self) {
        self.valid = false;
    }

    /// Replace the whole book from a snapshot.
    pub fn reset(
        &mut self,
        bids: &[PriceLevel],
        asks: &[PriceLevel],
        sequence: u64,
        timestamp: DateTime<Utc>,
    ) {
        self.bids.clear();
        self.asks.clear();
        self.apply(bids, asks, sequence, timestamp);
        self.valid = true;
    }

    /// Apply a delta. A zero quantity removes the level. Does not touch `valid`.
    pub fn apply(
        &mut self,
        bids: &[PriceLevel],
        asks: &[PriceLevel],
        sequence: u64,
        timestamp: DateTime<Utc>,
    ) {
        for (side, levels) in [(&mut self.bids, bids), (&mut self.asks, asks)] {
            for l in levels {
                if l.quantity.is_zero() {
                    side.remove(&l.price);
                } else {
                    side.insert(l.price, l.quantity);
                }
            }
        }
        self.sequence = sequence;
        self.timestamp = timestamp;
    }

    pub fn best_bid(&self) -> Option<PriceLevel> {
        self.bids.iter().next_back().map(|(&price, &quantity)| PriceLevel { price, quantity })
    }

    pub fn best_ask(&self) -> Option<PriceLevel> {
        self.asks.iter().next().map(|(&price, &quantity)| PriceLevel { price, quantity })
    }

    pub fn mid(&self) -> Option<Decimal> {
        Some((self.best_bid()?.price + self.best_ask()?.price) / Decimal::TWO)
    }

    /// Bid ≥ ask means the feed is corrupt (or mid-resync); never quote on it.
    pub fn is_crossed(&self) -> bool {
        matches!((self.best_bid(), self.best_ask()), (Some(b), Some(a)) if b.price >= a.price)
    }

    /// Top `depth` levels per side as a shareable snapshot.
    pub fn snapshot(&self, depth: usize) -> OrderBook {
        OrderBook {
            exchange: self.exchange,
            symbol: self.symbol.clone(),
            bids: self
                .bids
                .iter()
                .rev()
                .take(depth)
                .map(|(&price, &quantity)| PriceLevel { price, quantity })
                .collect(),
            asks: self
                .asks
                .iter()
                .take(depth)
                .map(|(&price, &quantity)| PriceLevel { price, quantity })
                .collect(),
            sequence: self.sequence,
            timestamp: self.timestamp,
            received: Instant::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lv(p: &str, q: &str) -> PriceLevel {
        PriceLevel { price: p.parse().unwrap(), quantity: q.parse().unwrap() }
    }

    fn book() -> LocalBook {
        let mut b = LocalBook::new(ExchangeId::Paper, Symbol::new("BTC", "USDT"));
        b.reset(
            &[lv("1.00", "5"), lv("0.99", "7"), lv("0.98", "9")],
            &[lv("1.02", "3"), lv("1.03", "4")],
            10,
            Utc::now(),
        );
        b
    }

    #[test]
    fn snapshot_sets_best_levels_and_validity() {
        let b = book();
        assert!(b.is_valid());
        assert_eq!(b.best_bid().unwrap(), lv("1.00", "5"));
        assert_eq!(b.best_ask().unwrap(), lv("1.02", "3"));
        assert_eq!(b.mid().unwrap(), "1.01".parse::<Decimal>().unwrap());
        assert_eq!(b.sequence(), 10);
        assert!(!b.is_crossed());
    }

    #[test]
    fn delta_updates_inserts_and_removes() {
        let mut b = book();
        b.apply(&[lv("1.00", "0"), lv("1.01", "2")], &[lv("1.02", "1")], 11, Utc::now());
        assert_eq!(b.best_bid().unwrap(), lv("1.01", "2"));
        assert_eq!(b.best_ask().unwrap(), lv("1.02", "1"));
        assert_eq!(b.sequence(), 11);
        b.apply(&[lv("1.01", "0"), lv("0.99", "0")], &[], 12, Utc::now());
        assert_eq!(b.best_bid().unwrap(), lv("0.98", "9"));
    }

    #[test]
    fn snapshot_is_ordered_and_truncated() {
        let s = book().snapshot(2);
        assert_eq!(s.bids, vec![lv("1.00", "5"), lv("0.99", "7")]);
        assert_eq!(s.asks, vec![lv("1.02", "3"), lv("1.03", "4")]);
        assert_eq!(s.exchange, ExchangeId::Paper);
    }

    #[test]
    fn crossed_book_is_detected() {
        let mut b = book();
        b.apply(&[lv("1.02", "1")], &[], 11, Utc::now());
        assert!(b.is_crossed());
    }

    #[test]
    fn reset_clears_previous_levels() {
        let mut b = book();
        b.invalidate();
        assert!(!b.is_valid());
        b.reset(&[lv("2.00", "1")], &[lv("2.10", "1")], 20, Utc::now());
        assert!(b.is_valid());
        assert_eq!(b.snapshot(10).bids.len(), 1);
        assert_eq!(b.best_bid().unwrap().price, "2.00".parse::<Decimal>().unwrap());
    }

    #[test]
    fn empty_book_has_no_mid() {
        let b = LocalBook::new(ExchangeId::Paper, Symbol::new("BTC", "USDT"));
        assert!(b.mid().is_none());
        assert!(!b.is_valid());
        assert!(!b.is_crossed());
    }
}
