//! `basic_mm`: fair price ± half spread, shifted by inventory skew, rounded to
//! the market's tick/step, one or more levels per side, per exchange.
//!
//! ```text
//! half   = fair × spread_bps / 20_000
//! offset = half + level × spacing_bps × fair / 10_000
//! bid    = round_down(fair − offset − skew)
//! ask    = round_up  (fair + offset − skew)
//! ```
//! Post-only safety: a bid is never at or above the best ask, an ask never at
//! or below the best bid. Quotes that violate `min_quantity`/`min_notional`
//! after rounding are dropped.

use rust_decimal::Decimal;

use super::{Strategy, StrategyContext, fair_price, inventory};
use crate::types::{ExchangeId, Fill, MarketInfo, Order, OrderBook, Quote};

#[derive(Debug, Default)]
pub struct BasicMarketMaker {
    last_fair: Option<Decimal>,
}

impl BasicMarketMaker {
    pub fn last_fair(&self) -> Option<Decimal> {
        self.last_fair
    }

    /// Compute quotes for every exchange with a fresh book.
    fn quotes(&mut self, ctx: &StrategyContext) -> Vec<Quote> {
        let Some(fair) = fair_price::fair_price(&ctx.config.fair_price, ctx.books) else {
            self.last_fair = None;
            return Vec::new();
        };
        self.last_fair = Some(fair);

        let cfg = ctx.config;
        let half_spread = fair * Decimal::from(cfg.spread_bps) / Decimal::from(20_000);
        let ratio = ctx.portfolio.inventory_ratio(fair);
        let skew = inventory::skew(ratio, &cfg.inventory, half_spread * Decimal::TWO);

        let mut out = Vec::new();
        for (exchange, book) in ctx.books {
            let Some(market) = ctx.markets.get(exchange) else { continue };
            if let Some(fees) = ctx.fees.get(exchange)
                && Decimal::from(cfg.spread_bps) <= fees.maker_bps * Decimal::TWO
            {
                // Spread does not cover two maker fees on this venue: not worth posting.
                continue;
            }
            for level in 0..cfg.levels.count {
                let offset = half_spread
                    + Decimal::from(level) * Decimal::from(cfg.levels.spacing_bps) * fair
                        / Decimal::from(10_000);
                if let Some(q) =
                    quote_level(*exchange, book, market, fair, offset, skew, cfg.order_size)
                {
                    out.push(q);
                }
            }
        }
        out
    }
}

fn quote_level(
    exchange: ExchangeId,
    book: &OrderBook,
    market: &MarketInfo,
    fair: Decimal,
    offset: Decimal,
    skew: Decimal,
    size: Decimal,
) -> Option<Quote> {
    let (best_bid, best_ask) = (book.best_bid()?.price, book.best_ask()?.price);

    let mut bid = market.round_price_down(fair - offset - skew);
    let mut ask = market.round_price_up(fair + offset - skew);

    // Post-only: never cross the book.
    if bid >= best_ask {
        bid = best_ask - market.price_tick;
    }
    if ask <= best_bid {
        ask = best_bid + market.price_tick;
    }
    if bid <= Decimal::ZERO || bid >= ask {
        return None;
    }

    let quantity = market.round_quantity_down(size);
    if !market.meets_minimums(bid, quantity) || !market.meets_minimums(ask, quantity) {
        return None;
    }

    Some(Quote {
        exchange,
        symbol: market.symbol.clone(),
        bid_price: bid,
        bid_quantity: quantity,
        ask_price: ask,
        ask_quantity: quantity,
    })
}

impl Strategy for BasicMarketMaker {
    fn name(&self) -> &'static str {
        "basic_mm"
    }

    fn on_market_data(
        &mut self,
        ctx: &StrategyContext,
        _exchange: ExchangeId,
    ) -> Option<Vec<Quote>> {
        Some(self.quotes(ctx))
    }

    fn on_fill(&mut self, ctx: &StrategyContext, _fill: &Fill) -> Option<Vec<Quote>> {
        // Inventory changed: requote immediately.
        Some(self.quotes(ctx))
    }

    fn on_order_update(&mut self, _ctx: &StrategyContext, _order: &Order) -> Option<Vec<Quote>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;

    use super::*;
    use crate::config::{
        FairPriceConfig, FairPriceMethod, InventoryConfig, LevelsConfig, StrategyConfig,
    };
    use crate::portfolio::{DailyPnl, Holdings, PortfolioSnapshot, Position};
    use crate::types::{Fees, PriceLevel, Symbol};

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn symbol() -> Symbol {
        Symbol::new("BTC", "USDT")
    }

    fn book(bid: &str, ask: &str) -> Arc<OrderBook> {
        Arc::new(OrderBook {
            exchange: ExchangeId::Paper,
            symbol: symbol(),
            bids: vec![PriceLevel { price: d(bid), quantity: d("100") }],
            asks: vec![PriceLevel { price: d(ask), quantity: d("100") }],
            sequence: 1,
            timestamp: chrono::Utc::now(),
            received: Instant::now(),
        })
    }

    fn market() -> MarketInfo {
        MarketInfo {
            symbol: symbol(),
            native_symbol: "BTCUSDT".into(),
            price_tick: d("0.0001"),
            quantity_step: d("1"),
            min_quantity: d("1"),
            min_notional: d("10"),
        }
    }

    fn config(spread_bps: u32, levels: u8) -> StrategyConfig {
        StrategyConfig {
            name: "basic_mm".into(),
            spread_bps,
            order_size: d("1000"),
            post_only: true,
            fair_price: FairPriceConfig {
                method: FairPriceMethod::LocalMid,
                weights: HashMap::new(),
            },
            inventory: InventoryConfig { target_ratio: d("0.5"), skew_factor: d("0.5") },
            levels: LevelsConfig { count: levels, spacing_bps: 50 },
        }
    }

    fn portfolio(base: &str, quote: &str) -> PortfolioSnapshot {
        PortfolioSnapshot {
            symbol: symbol(),
            holdings: HashMap::from([(
                ExchangeId::Paper,
                Holdings { base: d(base), quote: d(quote) },
            )]),
            position: Position::default(),
            daily: DailyPnl::new(chrono::Utc::now().date_naive()),
        }
    }

    struct Fixture {
        books: HashMap<ExchangeId, Arc<OrderBook>>,
        markets: HashMap<ExchangeId, MarketInfo>,
        fees: HashMap<ExchangeId, Fees>,
        portfolio: PortfolioSnapshot,
        config: StrategyConfig,
    }

    impl Fixture {
        fn new(bid: &str, ask: &str, spread_bps: u32, levels: u8, base: &str, quote: &str) -> Self {
            Self {
                books: HashMap::from([(ExchangeId::Paper, book(bid, ask))]),
                markets: HashMap::from([(ExchangeId::Paper, market())]),
                fees: HashMap::from([(ExchangeId::Paper, Fees::from_bps(10, 20))]),
                portfolio: portfolio(base, quote),
                config: config(spread_bps, levels),
            }
        }

        fn ctx(&self) -> StrategyContext<'_> {
            StrategyContext {
                books: &self.books,
                markets: &self.markets,
                fees: &self.fees,
                portfolio: &self.portfolio,
                config: &self.config,
            }
        }

        fn quotes(&self) -> Vec<Quote> {
            BasicMarketMaker::default().on_market_data(&self.ctx(), ExchangeId::Paper).unwrap()
        }
    }

    #[test]
    fn balanced_inventory_quotes_symmetric_around_fair() {
        // fair 1.0020, 100 bps spread → half = 0.00501; bid rounds down, ask rounds up
        let f = Fixture::new("1.0010", "1.0030", 100, 1, "1000", "1002");
        let q = f.quotes();
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].bid_price, d("0.9969"));
        assert_eq!(q[0].ask_price, d("1.0071"));
        assert_eq!(d("1.0020") - q[0].bid_price, q[0].ask_price - d("1.0020"), "symmetric");
        assert_eq!(q[0].bid_quantity, d("1000"));
        assert_eq!(q[0].exchange, ExchangeId::Paper);
    }

    #[test]
    fn overweight_base_shifts_both_quotes_down() {
        let balanced = Fixture::new("1.0010", "1.0030", 100, 1, "1000", "1002").quotes();
        let heavy = Fixture::new("1.0010", "1.0030", 100, 1, "7000", "3006").quotes();
        assert!(heavy[0].bid_price < balanced[0].bid_price);
        assert!(heavy[0].ask_price < balanced[0].ask_price);
        // ratio 0.7: skew = 0.2 × 0.5 × 0.01002 = 0.001002
        // bid = 1.0020 − 0.00501 − 0.001002 = 0.995988 → 0.9959 (down)
        // ask = 1.0020 + 0.00501 − 0.001002 = 1.006008 → 1.0061 (up)
        assert_eq!(heavy[0].bid_price, d("0.9959"));
        assert_eq!(heavy[0].ask_price, d("1.0061"));
    }

    #[test]
    fn underweight_base_shifts_both_quotes_up() {
        let light = Fixture::new("1.0010", "1.0030", 100, 1, "3000", "7014").quotes();
        assert!(light[0].bid_price > d("0.9969"));
        assert!(light[0].ask_price > d("1.0071"));
    }

    #[test]
    fn quotes_never_cross_the_book() {
        // 2 bps spread on a 20 bps wide book: raw quotes would sit inside the touch.
        let f = Fixture::new("1.0010", "1.0030", 22, 1, "1000", "1002");
        let q = f.quotes();
        assert!(q[0].bid_price < d("1.0030"));
        assert!(q[0].ask_price > d("1.0010"));
    }

    #[test]
    fn spread_not_covering_fees_yields_no_quote() {
        let mut f = Fixture::new("1.0010", "1.0030", 100, 1, "1000", "1002");
        f.fees.insert(ExchangeId::Paper, Fees::from_bps(60, 80));
        assert!(f.quotes().is_empty());
    }

    #[test]
    fn multiple_levels_step_outward() {
        let q = Fixture::new("1.0010", "1.0030", 100, 3, "1000", "1002").quotes();
        assert_eq!(q.len(), 3);
        assert!(q[0].bid_price > q[1].bid_price && q[1].bid_price > q[2].bid_price);
        assert!(q[0].ask_price < q[1].ask_price && q[1].ask_price < q[2].ask_price);
        // level spacing 50 bps of fair ≈ 0.0050
        assert_eq!(q[1].bid_price, q[0].bid_price - d("0.0050"));
    }

    #[test]
    fn below_minimums_is_dropped() {
        let mut f = Fixture::new("1.0010", "1.0030", 100, 1, "1000", "1002");
        f.config.order_size = d("5"); // 5 × ~1.0 < min_notional 10
        assert!(f.quotes().is_empty());
        f.config.order_size = d("0.5"); // rounds to 0 < min_quantity
        assert!(f.quotes().is_empty());
    }

    #[test]
    fn no_fresh_books_means_no_quotes() {
        let mut f = Fixture::new("1.0010", "1.0030", 100, 1, "1000", "1002");
        f.books.clear();
        let mut s = BasicMarketMaker::default();
        assert_eq!(s.on_market_data(&f.ctx(), ExchangeId::Paper), Some(vec![]));
        assert!(s.last_fair().is_none());
    }
}
