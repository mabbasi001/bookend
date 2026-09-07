//! Inventory skew: the only place the skew formula lives.
//!
//! `skew = (ratio − target) × skew_factor × spread`
//!
//! Positive skew (overweight base) is *subtracted* from both bid and ask,
//! moving the whole quote down: sell more, buy less. At the target the skew
//! is zero. `spread` is the full bid–ask distance in price, so
//! `skew_factor = 1` with a ratio 0.5 above target shifts quotes by half a
//! spread.

use rust_decimal::Decimal;

use crate::config::InventoryConfig;

pub fn skew(ratio: Option<Decimal>, cfg: &InventoryConfig, spread: Decimal) -> Decimal {
    match ratio {
        Some(r) => (r - cfg.target_ratio) * cfg.skew_factor * spread,
        None => Decimal::ZERO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn cfg() -> InventoryConfig {
        InventoryConfig { target_ratio: d("0.5"), skew_factor: d("0.5") }
    }

    #[test]
    fn zero_at_target_and_without_inventory() {
        assert_eq!(skew(Some(d("0.5")), &cfg(), d("0.01")), Decimal::ZERO);
        assert_eq!(skew(None, &cfg(), d("0.01")), Decimal::ZERO);
    }

    #[test]
    fn positive_when_overweight_negative_when_underweight_and_symmetric() {
        let up = skew(Some(d("0.7")), &cfg(), d("0.01"));
        let down = skew(Some(d("0.3")), &cfg(), d("0.01"));
        assert_eq!(up, d("0.001"), "(0.7-0.5)*0.5*0.01");
        assert_eq!(down, -up);
    }

    #[test]
    fn scales_with_factor_and_spread() {
        let c = InventoryConfig { target_ratio: d("0.5"), skew_factor: d("1") };
        assert_eq!(
            skew(Some(d("1")), &c, d("0.02")),
            d("0.01"),
            "fully overweight, factor 1: half a spread"
        );
        let c = InventoryConfig { target_ratio: d("0.5"), skew_factor: Decimal::ZERO };
        assert_eq!(skew(Some(d("1")), &c, d("0.02")), Decimal::ZERO);
    }
}
