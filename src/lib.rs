//! Bookend — exchange-agnostic market-making engine.
//!
//! Library crate so integration tests and the paper/backtest drivers can reuse
//! every component; `main.rs` is a thin CLI over [`bot::Bot`].

// Most of the domain model is defined ahead of the code that uses it.
// TODO: remove once M3 (paper market maker) wires everything together.
#![allow(dead_code)]

pub mod bot;
pub mod config;
pub mod events;
pub mod exchange;
pub mod logging;
pub mod market_data;
pub mod orders;
pub mod persistence;
pub mod portfolio;
pub mod quote;
pub mod risk;
pub mod strategy;
pub mod types;
