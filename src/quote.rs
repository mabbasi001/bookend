//! Quote manager: decides whether a new set of desired quotes is worth acting
//! on. Cancelling and replacing on every tick burns rate limit and queue
//! position, so quotes pass only when something meaningful changed, or a
//! fill/order event forces it, and never more often than `refresh_interval_ms`.

use std::time::{Duration, Instant};

use rust_decimal::Decimal;

use crate::config::QuoteConfig;
use crate::types::Quote;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// Book moved. Subject to thresholds and the refresh interval.
    MarketData,
    /// Inventory changed. Always passes.
    Fill,
    /// An order was cancelled/rejected/expired. Always passes.
    OrderEvent,
    /// Periodic re-evaluation (e.g. stale orders). Subject to thresholds only.
    Timer,
}

pub struct QuoteManager {
    refresh_interval: Duration,
    min_price_change: Decimal, // fraction, e.g. 0.0005 for 5 bps
    min_quantity_change: Decimal,
    last: Option<(Instant, Vec<Quote>)>,
}

impl QuoteManager {
    pub fn new(cfg: &QuoteConfig) -> Self {
        Self {
            refresh_interval: Duration::from_millis(cfg.refresh_interval_ms),
            min_price_change: Decimal::from(cfg.min_price_change_bps) / Decimal::from(10_000),
            min_quantity_change: cfg.min_quantity_change,
            last: None,
        }
    }

    /// Quotes currently considered "live" (last set that passed).
    pub fn current(&self) -> &[Quote] {
        self.last.as_ref().map(|(_, q)| q.as_slice()).unwrap_or(&[])
    }

    /// Forget the last set so the next call passes unconditionally.
    pub fn reset(&mut self) {
        self.last = None;
    }

    /// Returns the quotes to act on, or `None` to keep the current orders.
    pub fn filter(
        &mut self,
        desired: Vec<Quote>,
        trigger: Trigger,
        now: Instant,
    ) -> Option<Vec<Quote>> {
        let pass = match (&self.last, trigger) {
            (None, _) => true,
            (_, Trigger::Fill | Trigger::OrderEvent) => true,
            (Some((sent, _)), Trigger::MarketData)
                if now.duration_since(*sent) < self.refresh_interval =>
            {
                false
            }
            (Some((_, prev)), Trigger::MarketData | Trigger::Timer) => self.changed(prev, &desired),
        };
        if !pass {
            return None;
        }
        self.last = Some((now, desired.clone()));
        Some(desired)
    }

    fn changed(&self, prev: &[Quote], next: &[Quote]) -> bool {
        if prev.len() != next.len() {
            return true;
        }
        prev.iter().zip(next).any(|(a, b)| {
            a.exchange != b.exchange
                || self.price_moved(a.bid_price, b.bid_price)
                || self.price_moved(a.ask_price, b.ask_price)
                || (a.bid_quantity - b.bid_quantity).abs() >= self.min_quantity_change
                || (a.ask_quantity - b.ask_quantity).abs() >= self.min_quantity_change
        })
    }

    fn price_moved(&self, from: Decimal, to: Decimal) -> bool {
        if from.is_zero() {
            return !to.is_zero();
        }
        ((to - from) / from).abs() >= self.min_price_change
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ExchangeId, Symbol};

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn quote(bid: &str, ask: &str, qty: &str) -> Quote {
        Quote {
            exchange: ExchangeId::Paper,
            symbol: Symbol::new("BTC", "USDT"),
            bid_price: d(bid),
            bid_quantity: d(qty),
            ask_price: d(ask),
            ask_quantity: d(qty),
        }
    }

    fn manager() -> QuoteManager {
        QuoteManager::new(&QuoteConfig {
            refresh_interval_ms: 500,
            min_price_change_bps: 5,
            min_quantity_change: d("10"),
        })
    }

    #[test]
    fn first_set_always_passes() {
        let mut m = manager();
        let t0 = Instant::now();
        assert!(
            m.filter(vec![quote("0.9970", "1.0070", "1000")], Trigger::MarketData, t0).is_some()
        );
        assert_eq!(m.current().len(), 1);
    }

    #[test]
    fn market_data_is_throttled_by_refresh_interval() {
        let mut m = manager();
        let t0 = Instant::now();
        m.filter(vec![quote("0.9970", "1.0070", "1000")], Trigger::MarketData, t0);
        // Big move but inside the interval: held.
        assert!(
            m.filter(
                vec![quote("1.0970", "1.1070", "1000")],
                Trigger::MarketData,
                t0 + Duration::from_millis(100)
            )
            .is_none()
        );
        // Same move after the interval: passes.
        assert!(
            m.filter(
                vec![quote("1.0970", "1.1070", "1000")],
                Trigger::MarketData,
                t0 + Duration::from_millis(600)
            )
            .is_some()
        );
    }

    #[test]
    fn small_changes_are_ignored_after_the_interval() {
        let mut m = manager();
        let t0 = Instant::now();
        m.filter(vec![quote("1.0000", "1.0100", "1000")], Trigger::MarketData, t0);
        let later = t0 + Duration::from_secs(1);
        // 2 bps and 5 units: below both thresholds.
        assert!(
            m.filter(vec![quote("1.0002", "1.0102", "1005")], Trigger::MarketData, later).is_none()
        );
        // 4.95 bps on the ask (0.0005 / 1.0100): still below 5 bps.
        assert!(
            m.filter(vec![quote("1.0000", "1.0105", "1000")], Trigger::MarketData, later).is_none()
        );
        // 6 bps on the ask: passes.
        assert!(
            m.filter(vec![quote("1.0000", "1.0106", "1000")], Trigger::MarketData, later).is_some()
        );
        // 10 units of quantity: passes.
        assert!(
            m.filter(
                vec![quote("1.0000", "1.0106", "1010")],
                Trigger::MarketData,
                later + Duration::from_secs(1)
            )
            .is_some()
        );
    }

    #[test]
    fn fills_and_order_events_bypass_throttle_and_thresholds() {
        let mut m = manager();
        let t0 = Instant::now();
        m.filter(vec![quote("1.0000", "1.0100", "1000")], Trigger::MarketData, t0);
        assert!(
            m.filter(
                vec![quote("1.0000", "1.0100", "1000")],
                Trigger::Fill,
                t0 + Duration::from_millis(1)
            )
            .is_some()
        );
        assert!(
            m.filter(
                vec![quote("1.0000", "1.0100", "1000")],
                Trigger::OrderEvent,
                t0 + Duration::from_millis(2)
            )
            .is_some()
        );
    }

    #[test]
    fn timer_ignores_interval_but_respects_thresholds() {
        let mut m = manager();
        let t0 = Instant::now();
        m.filter(vec![quote("1.0000", "1.0100", "1000")], Trigger::MarketData, t0);
        assert!(
            m.filter(
                vec![quote("1.0000", "1.0100", "1000")],
                Trigger::Timer,
                t0 + Duration::from_millis(1)
            )
            .is_none()
        );
        assert!(
            m.filter(vec![], Trigger::Timer, t0 + Duration::from_millis(1)).is_some(),
            "count changed"
        );
    }

    #[test]
    fn reset_forces_next_pass() {
        let mut m = manager();
        let t0 = Instant::now();
        m.filter(vec![quote("1.0000", "1.0100", "1000")], Trigger::MarketData, t0);
        m.reset();
        assert!(m.current().is_empty());
        assert!(
            m.filter(
                vec![quote("1.0000", "1.0100", "1000")],
                Trigger::MarketData,
                t0 + Duration::from_millis(1)
            )
            .is_some()
        );
    }
}
