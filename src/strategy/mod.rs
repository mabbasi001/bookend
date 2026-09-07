//! Strategy trait and implementations: state in, desired quotes out. No I/O,
//! no channels, no exchange calls — a strategy is a pure decision function
//! over a [`StrategyContext`].

pub mod basic_mm;
pub mod fair_price;
pub mod inventory;

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::StrategyConfig;
use crate::portfolio::PortfolioSnapshot;
use crate::types::{ExchangeId, Fees, Fill, MarketInfo, Order, OrderBook, Quote};

/// Everything a strategy may look at. `books` holds only fresh, valid books.
pub struct StrategyContext<'a> {
    pub books: &'a HashMap<ExchangeId, Arc<OrderBook>>,
    pub markets: &'a HashMap<ExchangeId, MarketInfo>,
    pub fees: &'a HashMap<ExchangeId, Fees>,
    pub portfolio: &'a PortfolioSnapshot,
    pub config: &'a StrategyConfig,
}

/// `None` means "no change to the current quotes". `Some(vec![])` means
/// "pull every quote".
pub trait Strategy: Send {
    fn name(&self) -> &'static str;
    fn on_market_data(&mut self, ctx: &StrategyContext, exchange: ExchangeId)
    -> Option<Vec<Quote>>;
    fn on_fill(&mut self, ctx: &StrategyContext, fill: &Fill) -> Option<Vec<Quote>>;
    fn on_order_update(&mut self, ctx: &StrategyContext, order: &Order) -> Option<Vec<Quote>>;
}

pub fn build(config: &StrategyConfig) -> anyhow::Result<Box<dyn Strategy>> {
    match config.name.as_str() {
        "basic_mm" => Ok(Box::new(basic_mm::BasicMarketMaker::default())),
        other => anyhow::bail!("unknown strategy {other:?} (available: basic_mm)"),
    }
}
