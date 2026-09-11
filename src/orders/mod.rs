//! Order manager: desired-vs-actual reconciliation and the order state machine. (M3/M4)

pub mod manager;
pub mod state;

pub use manager::{ClientIdGen, OrderManager, Placement, Plan};
