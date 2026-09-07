//! Reference price used by every strategy.
//!
//! - `local_mid`: average of the mids of all fresh books (== the single
//!   exchange's mid when only one is connected).
//! - `weighted_mid`: configured per-exchange weights, renormalised over the
//!   exchanges that currently have a fresh, two-sided book.

use std::collections::HashMap;
use std::sync::Arc;

use rust_decimal::Decimal;

use crate::config::{FairPriceConfig, FairPriceMethod};
use crate::types::{ExchangeId, OrderBook};

pub fn fair_price(
    cfg: &FairPriceConfig,
    books: &HashMap<ExchangeId, Arc<OrderBook>>,
) -> Option<Decimal> {
    let mut weighted_sum = Decimal::ZERO;
    let mut weight_sum = Decimal::ZERO;

    for (exchange, book) in books {
        let Some(mid) = book.mid() else { continue };
        let weight = match cfg.method {
            FairPriceMethod::LocalMid => Decimal::ONE,
            FairPriceMethod::WeightedMid => {
                cfg.weights.get(exchange.as_str()).copied().unwrap_or(Decimal::ONE)
            }
        };
        if weight <= Decimal::ZERO {
            continue;
        }
        weighted_sum += mid * weight;
        weight_sum += weight;
    }

    (weight_sum > Decimal::ZERO).then(|| weighted_sum / weight_sum)
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::types::{PriceLevel, Symbol};

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn book(exchange: ExchangeId, bid: &str, ask: &str) -> Arc<OrderBook> {
        let lvl = |p: &str| PriceLevel { price: d(p), quantity: Decimal::ONE };
        Arc::new(OrderBook {
            exchange,
            symbol: Symbol::new("BTC", "USDT"),
            bids: if bid.is_empty() { vec![] } else { vec![lvl(bid)] },
            asks: if ask.is_empty() { vec![] } else { vec![lvl(ask)] },
            sequence: 1,
            timestamp: chrono::Utc::now(),
            received: Instant::now(),
        })
    }

    fn cfg(method: FairPriceMethod, weights: &[(&str, &str)]) -> FairPriceConfig {
        FairPriceConfig {
            method,
            weights: weights.iter().map(|(k, v)| ((*k).to_owned(), d(v))).collect(),
        }
    }

    #[test]
    fn local_mid_single_exchange() {
        let books =
            HashMap::from([(ExchangeId::Binance, book(ExchangeId::Binance, "1.0010", "1.0030"))]);
        assert_eq!(fair_price(&cfg(FairPriceMethod::LocalMid, &[]), &books).unwrap(), d("1.0020"));
    }

    #[test]
    fn weighted_mid_uses_weights_and_renormalises_over_present_books() {
        let c = cfg(
            FairPriceMethod::WeightedMid,
            &[("binance", "0.4"), ("bybit", "0.3"), ("okx", "0.3")],
        );
        let mut books = HashMap::from([
            (ExchangeId::Binance, book(ExchangeId::Binance, "1.0000", "1.0000")),
            (ExchangeId::Bybit, book(ExchangeId::Bybit, "1.0020", "1.0020")),
            (ExchangeId::Okx, book(ExchangeId::Okx, "0.9990", "0.9990")),
        ]);
        // 0.4*1.0000 + 0.3*1.0020 + 0.3*0.9990 = 1.0003
        assert_eq!(fair_price(&c, &books).unwrap(), d("1.0003"));

        // OKX drops out: (0.4*1.0000 + 0.3*1.0020) / 0.7
        books.remove(&ExchangeId::Okx);
        let expected = (d("0.4") + d("0.3006")) / d("0.7");
        assert_eq!(fair_price(&c, &books).unwrap(), expected);
    }

    #[test]
    fn unweighted_exchange_defaults_to_one_and_zero_weight_is_excluded() {
        let c = cfg(FairPriceMethod::WeightedMid, &[("okx", "0")]);
        let books = HashMap::from([
            (ExchangeId::Binance, book(ExchangeId::Binance, "2", "2")),
            (ExchangeId::Okx, book(ExchangeId::Okx, "100", "100")),
        ]);
        assert_eq!(fair_price(&c, &books).unwrap(), d("2"));
    }

    #[test]
    fn one_sided_or_missing_books_yield_none() {
        let c = cfg(FairPriceMethod::LocalMid, &[]);
        assert!(fair_price(&c, &HashMap::new()).is_none());
        let books = HashMap::from([(ExchangeId::Binance, book(ExchangeId::Binance, "1", ""))]);
        assert!(fair_price(&c, &books).is_none());
    }
}
