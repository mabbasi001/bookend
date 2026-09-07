//! Balances, position, inventory ratio and PnL.
//!
//! Holdings come from exchange balance events; the position (quantity, average
//! cost, realized PnL, fees) is updated **only from confirmed fills**.

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::types::{Balance, ExchangeId, Fill, Side, Symbol};

/// Base and quote holdings on one exchange (totals, including locked).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Holdings {
    pub base: Decimal,
    pub quote: Decimal,
}

impl Holdings {
    pub fn value_in_quote(&self, fair: Decimal) -> Decimal {
        self.base * fair + self.quote
    }

    /// `base_value / (base_value + quote_value)`; `None` when nothing is held.
    pub fn inventory_ratio(&self, fair: Decimal) -> Option<Decimal> {
        let base_value = self.base * fair;
        let total = base_value + self.quote;
        (total > Decimal::ZERO).then(|| base_value / total)
    }
}

/// Average-cost position in the base asset, spot (long-only).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub quantity: Decimal,
    pub avg_cost: Decimal,
    pub realized_pnl: Decimal,
    /// In quote currency.
    pub fees: Decimal,
}

impl Position {
    pub fn unrealized_pnl(&self, fair: Decimal) -> Decimal {
        (fair - self.avg_cost) * self.quantity
    }

    pub fn net_pnl(&self, fair: Decimal) -> Decimal {
        self.realized_pnl + self.unrealized_pnl(fair) - self.fees
    }
}

/// Realized PnL and fees for the current UTC day; feeds `risk.max_daily_loss`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DailyPnl {
    pub day: NaiveDate,
    pub realized: Decimal,
    pub fees: Decimal,
}

impl DailyPnl {
    pub fn new(day: NaiveDate) -> Self {
        Self { day, realized: Decimal::ZERO, fees: Decimal::ZERO }
    }

    pub fn net(&self) -> Decimal {
        self.realized - self.fees
    }
}

/// Read-only copy handed to the strategy and risk engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortfolioSnapshot {
    pub symbol: Symbol,
    pub holdings: HashMap<ExchangeId, Holdings>,
    pub position: Position,
    pub daily: DailyPnl,
}

impl PortfolioSnapshot {
    pub fn total(&self) -> Holdings {
        self.holdings.values().fold(Holdings::default(), |acc, h| Holdings {
            base: acc.base + h.base,
            quote: acc.quote + h.quote,
        })
    }

    pub fn inventory_ratio(&self, fair: Decimal) -> Option<Decimal> {
        self.total().inventory_ratio(fair)
    }

    pub fn holdings_on(&self, exchange: ExchangeId) -> Holdings {
        self.holdings.get(&exchange).copied().unwrap_or_default()
    }
}

pub struct Portfolio {
    symbol: Symbol,
    holdings: HashMap<ExchangeId, Holdings>,
    position: Position,
    daily: DailyPnl,
}

impl Portfolio {
    pub fn new(symbol: Symbol, now: DateTime<Utc>) -> Self {
        Self {
            symbol,
            holdings: HashMap::new(),
            position: Position::default(),
            daily: DailyPnl::new(now.date_naive()),
        }
    }

    /// Continue from persisted state (position and today's PnL).
    pub fn restore(&mut self, position: Position, daily: DailyPnl, now: DateTime<Utc>) {
        self.position = position;
        self.daily =
            if daily.day == now.date_naive() { daily } else { DailyPnl::new(now.date_naive()) };
    }

    pub fn position(&self) -> Position {
        self.position
    }

    pub fn daily(&self) -> DailyPnl {
        self.daily
    }

    /// Balance update from an exchange. Assets other than base/quote are ignored.
    pub fn on_balance(&mut self, exchange: ExchangeId, balance: &Balance) {
        let h = self.holdings.entry(exchange).or_default();
        if balance.asset.eq_ignore_ascii_case(&self.symbol.base) {
            h.base = balance.total;
        } else if balance.asset.eq_ignore_ascii_case(&self.symbol.quote) {
            h.quote = balance.total;
        }
    }

    /// Confirmed execution. Updates position, realized PnL, fees and daily PnL.
    pub fn on_fill(&mut self, fill: &Fill) {
        self.roll_day(fill.timestamp);

        let fee_quote = if fill.fee_asset.eq_ignore_ascii_case(&self.symbol.quote) {
            fill.fee
        } else if fill.fee_asset.eq_ignore_ascii_case(&self.symbol.base) {
            fill.fee * fill.price
        } else {
            warn!(asset = %fill.fee_asset, fee = %fill.fee, "fee in unrelated asset ignored in PnL");
            Decimal::ZERO
        };

        let p = &mut self.position;
        match fill.side {
            Side::Buy => {
                let new_qty = p.quantity + fill.quantity;
                if new_qty > Decimal::ZERO {
                    p.avg_cost = (p.avg_cost * p.quantity + fill.price * fill.quantity) / new_qty;
                }
                p.quantity = new_qty;
            }
            Side::Sell => {
                let closing = fill.quantity.min(p.quantity.max(Decimal::ZERO));
                let realized = (fill.price - p.avg_cost) * closing;
                p.realized_pnl += realized;
                self.daily.realized += realized;
                p.quantity -= fill.quantity;
                if p.quantity <= Decimal::ZERO {
                    p.quantity = Decimal::ZERO;
                    p.avg_cost = Decimal::ZERO;
                }
            }
        }
        p.fees += fee_quote;
        self.daily.fees += fee_quote;
    }

    fn roll_day(&mut self, now: DateTime<Utc>) {
        let today = now.date_naive();
        if today != self.daily.day {
            self.daily = DailyPnl::new(today);
        }
    }

    pub fn snapshot(&self) -> PortfolioSnapshot {
        PortfolioSnapshot {
            symbol: self.symbol.clone(),
            holdings: self.holdings.clone(),
            position: self.position,
            daily: self.daily,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn fill(side: Side, price: &str, qty: &str, fee: &str) -> Fill {
        Fill {
            exchange: ExchangeId::Paper,
            client_order_id: "x".into(),
            trade_id: "t".into(),
            symbol: Symbol::new("BTC", "USDT"),
            side,
            price: d(price),
            quantity: d(qty),
            fee: d(fee),
            fee_asset: "USDT".into(),
            timestamp: Utc::now(),
        }
    }

    fn portfolio() -> Portfolio {
        Portfolio::new(Symbol::new("BTC", "USDT"), Utc::now())
    }

    #[test]
    fn inventory_ratio_from_holdings() {
        let h = Holdings { base: d("70000"), quote: d("30000") };
        assert_eq!(h.inventory_ratio(d("1")).unwrap(), d("0.7"));
        assert_eq!(h.value_in_quote(d("2")), d("170000"));
        assert!(Holdings::default().inventory_ratio(d("1")).is_none());
    }

    #[test]
    fn balances_update_holdings_per_exchange_and_ignore_other_assets() {
        let mut p = portfolio();
        let bal = |asset: &str, total: &str| Balance {
            asset: asset.into(),
            total: d(total),
            available: d(total),
            locked: Decimal::ZERO,
        };
        p.on_balance(ExchangeId::Paper, &bal("BTC", "100"));
        p.on_balance(ExchangeId::Paper, &bal("usdt", "50"));
        p.on_balance(ExchangeId::Paper, &bal("BNB", "9"));
        p.on_balance(ExchangeId::Binance, &bal("BTC", "1"));
        let s = p.snapshot();
        assert_eq!(s.holdings_on(ExchangeId::Paper), Holdings { base: d("100"), quote: d("50") });
        assert_eq!(s.total(), Holdings { base: d("101"), quote: d("50") });
        assert_eq!(s.holdings_on(ExchangeId::Okx), Holdings::default());
    }

    #[test]
    fn average_cost_and_realized_pnl() {
        let mut p = portfolio();
        p.on_fill(&fill(Side::Buy, "10", "10", "0.1"));
        p.on_fill(&fill(Side::Buy, "20", "10", "0.2"));
        let pos = p.position();
        assert_eq!(pos.quantity, d("20"));
        assert_eq!(pos.avg_cost, d("15"));
        assert_eq!(pos.fees, d("0.3"));
        assert_eq!(pos.unrealized_pnl(d("18")), d("60"));

        p.on_fill(&fill(Side::Sell, "18", "5", "0.09"));
        let pos = p.position();
        assert_eq!(pos.realized_pnl, d("15"), "(18-15)*5");
        assert_eq!(pos.quantity, d("15"));
        assert_eq!(pos.avg_cost, d("15"), "selling keeps avg cost");
        assert_eq!(pos.net_pnl(d("18")), d("15") + d("45") - d("0.39"));

        p.on_fill(&fill(Side::Sell, "12", "15", "0.18"));
        let pos = p.position();
        assert_eq!(pos.quantity, Decimal::ZERO);
        assert_eq!(pos.avg_cost, Decimal::ZERO, "flat resets cost");
        assert_eq!(pos.realized_pnl, d("15") + d("-45"));
        assert_eq!(p.daily().net(), d("-30") - d("0.57"));
    }

    #[test]
    fn selling_more_than_held_only_realizes_the_held_part() {
        let mut p = portfolio();
        p.on_fill(&fill(Side::Buy, "10", "5", "0"));
        p.on_fill(&fill(Side::Sell, "12", "8", "0"));
        assert_eq!(p.position().realized_pnl, d("10"), "(12-10)*5");
        assert_eq!(p.position().quantity, Decimal::ZERO);
    }

    #[test]
    fn fee_in_base_is_converted_and_unknown_asset_ignored() {
        let mut p = portfolio();
        let mut f = fill(Side::Buy, "10", "1", "0.01");
        f.fee_asset = "BTC".into();
        p.on_fill(&f);
        assert_eq!(p.position().fees, d("0.1"));
        f.fee_asset = "BNB".into();
        p.on_fill(&f);
        assert_eq!(p.position().fees, d("0.1"));
    }

    #[test]
    fn daily_pnl_rolls_over_at_utc_midnight() {
        let mut p = portfolio();
        let mut f = fill(Side::Buy, "10", "1", "1");
        p.on_fill(&f);
        assert_eq!(p.daily().fees, d("1"));
        f.timestamp += chrono::Duration::days(1);
        f.side = Side::Sell;
        f.price = d("11");
        p.on_fill(&f);
        let daily = p.daily();
        assert_eq!(daily.day, f.timestamp.date_naive());
        assert_eq!(daily.realized, d("1"));
        assert_eq!(daily.fees, d("1"), "only today's fee");
        assert_eq!(p.position().fees, d("2"), "position fees are cumulative");
    }

    #[test]
    fn restore_keeps_todays_pnl_but_not_yesterdays() {
        let now = Utc::now();
        let mut p = portfolio();
        let pos =
            Position { quantity: d("3"), avg_cost: d("9"), realized_pnl: d("4"), fees: d("1") };
        p.restore(pos, DailyPnl { day: now.date_naive(), realized: d("4"), fees: d("1") }, now);
        assert_eq!(p.position(), pos);
        assert_eq!(p.daily().net(), d("3"));
        let yesterday =
            DailyPnl { day: now.date_naive().pred_opt().unwrap(), realized: d("99"), fees: d("0") };
        p.restore(pos, yesterday, now);
        assert_eq!(p.daily().net(), Decimal::ZERO);
    }
}
