//! Order manager: reconciles desired quotes against the orders we believe are
//! live, and tracks every order through its state machine.
//!
//! The manager never decides prices. It turns a set of [`Quote`]s into a
//! [`Plan`] (cancel these, place those), and the caller executes the plan
//! against the exchanges and feeds results back.

use std::collections::HashMap;

use chrono::Utc;
use rust_decimal::Decimal;
use tracing::{debug, warn};

use super::state;
use crate::exchange::ExchangeError;
use crate::types::{ExchangeId, Order, OrderRequest, OrderStatus, OrderType, Quote, Side, Symbol};

// ---------------------------------------------------------------------------
// Client order ids
// ---------------------------------------------------------------------------

/// `mm` + exchange tag + run id + sequence. Alphanumeric, 16 chars — inside
/// every exchange's limit (OKX: 32 alphanumeric; Binance/Bybit: 36).
pub struct ClientIdGen {
    run_id: String,
    seq: u64,
}

impl ClientIdGen {
    pub const PREFIX: &'static str = "mm";

    pub fn new(run_id: impl Into<String>) -> Self {
        let run_id = run_id.into();
        debug_assert!(run_id.chars().all(|c| c.is_ascii_alphanumeric()) && run_id.len() == 6);
        Self { run_id, seq: 0 }
    }

    /// Six base-36 characters derived from the current time.
    pub fn run_id_now() -> String {
        base36(Utc::now().timestamp_millis() as u64 % 36u64.pow(6), 6)
    }

    pub fn next(&mut self, exchange: ExchangeId) -> String {
        self.seq += 1;
        format!("{}{}{}{:06}", Self::PREFIX, exchange_tag(exchange), self.run_id, self.seq)
    }

    /// Does this id belong to *any* run of this engine?
    pub fn is_ours(client_order_id: &str) -> bool {
        client_order_id.len() == 16
            && client_order_id.starts_with(Self::PREFIX)
            && client_order_id.chars().all(|c| c.is_ascii_alphanumeric())
    }
}

fn exchange_tag(exchange: ExchangeId) -> &'static str {
    match exchange {
        ExchangeId::Binance => "bn",
        ExchangeId::Bybit => "by",
        ExchangeId::Okx => "ok",
        ExchangeId::Paper => "pp",
    }
}

fn base36(mut n: u64, width: usize) -> String {
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = vec![b'0'; width];
    for slot in out.iter_mut().rev() {
        *slot = DIGITS[(n % 36) as usize];
        n /= 36;
    }
    String::from_utf8(out).expect("ascii")
}

// ---------------------------------------------------------------------------
// Plan
// ---------------------------------------------------------------------------

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub cancel: Vec<Order>,
    pub place: Vec<OrderRequest>,
}

impl Plan {
    pub fn is_empty(&self) -> bool {
        self.cancel.is_empty() && self.place.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Desired {
    exchange: ExchangeId,
    side: Side,
    price: Decimal,
    quantity: Decimal,
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

pub struct OrderManager {
    symbol: Symbol,
    ids: ClientIdGen,
    post_only: bool,
    /// Every non-terminal order we know about, by client id.
    orders: HashMap<String, Order>,
}

impl OrderManager {
    pub fn new(symbol: Symbol, run_id: impl Into<String>, post_only: bool) -> Self {
        Self { symbol, ids: ClientIdGen::new(run_id), post_only, orders: HashMap::new() }
    }

    pub fn orders(&self) -> impl Iterator<Item = &Order> {
        self.orders.values()
    }

    pub fn get(&self, client_order_id: &str) -> Option<&Order> {
        self.orders.get(client_order_id)
    }

    /// Orders resting (or believed resting) on `exchange`.
    pub fn live_on(&self, exchange: ExchangeId) -> impl Iterator<Item = &Order> {
        self.orders.values().filter(move |o| o.exchange == exchange && o.status.is_live())
    }

    pub fn open_count(&self, exchange: ExchangeId) -> usize {
        self.live_on(exchange).count()
    }

    /// Orders whose outcome we do not know yet; quoting on that side waits.
    fn has_in_flight(&self, exchange: ExchangeId, side: Side) -> bool {
        self.orders.values().any(|o| {
            o.exchange == exchange
                && o.side == side
                && matches!(o.status, OrderStatus::Submitting | OrderStatus::Unknown)
        })
    }

    /// Diff desired quotes against live orders.
    ///
    /// Matching is exact on (exchange, side, price, remaining quantity): an
    /// order that matches stays; anything else live is cancelled; desired
    /// entries without a match are placed — unless that side already has an
    /// order in flight (`Submitting`/`Unknown`), in which case we wait.
    pub fn plan(&mut self, quotes: &[Quote]) -> Plan {
        let mut wanted: Vec<Desired> = Vec::with_capacity(quotes.len() * 2);
        for q in quotes {
            wanted.push(Desired {
                exchange: q.exchange,
                side: Side::Buy,
                price: q.bid_price,
                quantity: q.bid_quantity,
            });
            wanted.push(Desired {
                exchange: q.exchange,
                side: Side::Sell,
                price: q.ask_price,
                quantity: q.ask_quantity,
            });
        }

        let mut plan = Plan::default();
        let mut matched = vec![false; wanted.len()];

        for order in self.orders.values() {
            if !order.status.is_live() || order.status == OrderStatus::CancelRequested {
                continue;
            }
            let hit = wanted.iter().enumerate().find(|(i, w)| {
                !matched[*i]
                    && w.exchange == order.exchange
                    && w.side == order.side
                    && Some(w.price) == order.price
                    && w.quantity == order.remaining_quantity()
            });
            match hit {
                Some((i, _)) => matched[i] = true,
                None => plan.cancel.push(order.clone()),
            }
        }

        for (w, _) in wanted.iter().zip(matched).filter(|(_, m)| !m) {
            if self.has_in_flight(w.exchange, w.side) {
                debug!(exchange = %w.exchange, side = %w.side, "order in flight; not placing another");
                continue;
            }
            plan.place.push(OrderRequest {
                symbol: self.symbol.clone(),
                side: w.side,
                order_type: OrderType::Limit,
                price: Some(w.price),
                quantity: w.quantity,
                client_order_id: self.ids.next(w.exchange),
                post_only: self.post_only,
            });
        }
        plan
    }

    /// Plan that cancels everything live. Used by kill switch and shutdown.
    pub fn cancel_all_plan(&self) -> Plan {
        Plan {
            cancel: self
                .orders
                .values()
                .filter(|o| o.status.is_live() && o.status != OrderStatus::CancelRequested)
                .cloned()
                .collect(),
            place: Vec::new(),
        }
    }

    // --- feedback from execution --------------------------------------------

    /// Record that a placement is being sent.
    pub fn on_submitting(&mut self, exchange: ExchangeId, request: &OrderRequest) {
        let now = Utc::now();
        self.orders.insert(
            request.client_order_id.clone(),
            Order {
                exchange,
                exchange_order_id: None,
                client_order_id: request.client_order_id.clone(),
                symbol: request.symbol.clone(),
                side: request.side,
                order_type: request.order_type,
                price: request.price,
                quantity: request.quantity,
                filled_quantity: Decimal::ZERO,
                status: OrderStatus::Submitting,
                created_at: now,
                updated_at: now,
            },
        );
    }

    /// Exchange acknowledged the placement.
    pub fn on_placed(&mut self, ack: &Order) {
        match self.orders.get_mut(&ack.client_order_id) {
            Some(o) => {
                if let Err(e) = state::apply_update(o, ack) {
                    warn!(client_order_id = %ack.client_order_id, error = %e, "placement ack ignored");
                }
                self.drop_if_terminal(&ack.client_order_id);
            }
            None => {
                // Adopted from a previous run or reconciliation.
                self.orders.insert(ack.client_order_id.clone(), ack.clone());
            }
        }
    }

    /// Placement failed. Uncertain outcomes stay tracked as `Unknown` for
    /// reconciliation; definite rejections are dropped.
    pub fn on_place_failed(&mut self, client_order_id: &str, error: &ExchangeError) {
        let Some(o) = self.orders.get_mut(client_order_id) else { return };
        let to = if error.is_uncertain_submission() {
            OrderStatus::Unknown
        } else {
            OrderStatus::Rejected
        };
        if let Err(e) = state::transition(o, to) {
            warn!(client_order_id, error = %e, "cannot record placement failure");
        }
        self.drop_if_terminal(client_order_id);
    }

    pub fn on_cancel_requested(&mut self, client_order_id: &str) {
        if let Some(o) = self.orders.get_mut(client_order_id)
            && let Err(e) = state::transition(o, OrderStatus::CancelRequested)
        {
            warn!(client_order_id, error = %e, "cannot mark cancel requested");
        }
    }

    /// Cancel failed. `OrderNotFound` means it is already gone (filled or
    /// cancelled) — the user stream will tell us which; drop it if we hear nothing.
    pub fn on_cancel_failed(&mut self, client_order_id: &str, error: &ExchangeError) {
        if matches!(error, ExchangeError::OrderNotFound) {
            if let Some(o) = self.orders.get_mut(client_order_id) {
                let _ = state::transition(o, OrderStatus::Cancelled);
            }
            self.drop_if_terminal(client_order_id);
        } else if let Some(o) = self.orders.get_mut(client_order_id)
            && o.status == OrderStatus::CancelRequested
        {
            // Still live as far as we know; try again on the next plan.
            o.status = if o.filled_quantity.is_zero() {
                OrderStatus::Open
            } else {
                OrderStatus::PartiallyFilled
            };
        }
    }

    /// Order update from the user stream or a reconciliation query.
    /// Returns the updated order when something changed.
    pub fn on_order_update(&mut self, update: &Order) -> Option<Order> {
        let changed = match self.orders.get_mut(&update.client_order_id) {
            Some(o) => match state::apply_update(o, update) {
                Ok(changed) => changed,
                Err(e) => {
                    debug!(client_order_id = %update.client_order_id, error = %e, "stale order update ignored");
                    false
                }
            },
            None if ClientIdGen::is_ours(&update.client_order_id)
                && !update.status.is_terminal() =>
            {
                self.orders.insert(update.client_order_id.clone(), update.clone());
                true
            }
            None => false,
        };
        let result = changed.then(|| self.orders[&update.client_order_id].clone());
        self.drop_if_terminal(&update.client_order_id);
        result
    }

    /// Adopt orders found on the exchange at startup (ours by id prefix).
    pub fn adopt(&mut self, open_orders: Vec<Order>) -> usize {
        let mut n = 0;
        for o in open_orders {
            if ClientIdGen::is_ours(&o.client_order_id) && o.status.is_live() {
                self.orders.insert(o.client_order_id.clone(), o);
                n += 1;
            }
        }
        n
    }

    fn drop_if_terminal(&mut self, client_order_id: &str) {
        if self.orders.get(client_order_id).is_some_and(|o| o.status.is_terminal()) {
            self.orders.remove(client_order_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn manager() -> OrderManager {
        OrderManager::new(Symbol::new("BTC", "USDT"), "run001", true)
    }

    fn ack(req: &OrderRequest, status: OrderStatus) -> Order {
        Order {
            exchange: ExchangeId::Paper,
            exchange_order_id: Some(format!("E-{}", req.client_order_id)),
            client_order_id: req.client_order_id.clone(),
            symbol: req.symbol.clone(),
            side: req.side,
            order_type: req.order_type,
            price: req.price,
            quantity: req.quantity,
            filled_quantity: Decimal::ZERO,
            status,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// Place everything in the plan and acknowledge it as Open.
    fn place_all(m: &mut OrderManager, plan: &Plan) {
        for r in &plan.place {
            m.on_submitting(ExchangeId::Paper, r);
            m.on_placed(&ack(r, OrderStatus::Open));
        }
    }

    #[test]
    fn client_ids_are_alphanumeric_16_chars_and_unique() {
        let mut g = ClientIdGen::new("abc123");
        let a = g.next(ExchangeId::Binance);
        let b = g.next(ExchangeId::Okx);
        assert_eq!(a, "mmbnabc123000001");
        assert_eq!(b, "mmokabc123000002");
        assert!(ClientIdGen::is_ours(&a) && ClientIdGen::is_ours(&b));
        assert!(!ClientIdGen::is_ours("web-12345"));
        assert!(!ClientIdGen::is_ours("mm-bn-abc123-01"));
        let run = ClientIdGen::run_id_now();
        assert_eq!(run.len(), 6);
        assert!(run.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn first_plan_places_both_sides() {
        let mut m = manager();
        let plan = m.plan(&[quote("0.997", "1.007", "1000")]);
        assert!(plan.cancel.is_empty());
        assert_eq!(plan.place.len(), 2);
        assert_eq!(plan.place[0].side, Side::Buy);
        assert_eq!(plan.place[0].price, Some(d("0.997")));
        assert!(plan.place.iter().all(|r| r.post_only && r.order_type == OrderType::Limit));
    }

    #[test]
    fn unchanged_quotes_produce_an_empty_plan() {
        let mut m = manager();
        let plan = m.plan(&[quote("0.997", "1.007", "1000")]);
        place_all(&mut m, &plan);
        assert_eq!(m.open_count(ExchangeId::Paper), 2);
        assert!(m.plan(&[quote("0.997", "1.007", "1000")]).is_empty());
    }

    #[test]
    fn moved_bid_cancels_old_bid_and_keeps_ask() {
        let mut m = manager();
        let plan = m.plan(&[quote("0.997", "1.007", "1000")]);
        place_all(&mut m, &plan);
        let plan = m.plan(&[quote("0.998", "1.007", "1000")]);
        assert_eq!(plan.cancel.len(), 1);
        assert_eq!(plan.cancel[0].side, Side::Buy);
        assert_eq!(plan.place.len(), 1);
        assert_eq!(plan.place[0].price, Some(d("0.998")));
    }

    #[test]
    fn empty_quotes_cancel_everything() {
        let mut m = manager();
        let plan = m.plan(&[quote("0.997", "1.007", "1000")]);
        place_all(&mut m, &plan);
        let plan = m.plan(&[]);
        assert_eq!(plan.cancel.len(), 2);
        assert!(plan.place.is_empty());
        assert_eq!(m.cancel_all_plan().cancel.len(), 2);
    }

    #[test]
    fn in_flight_side_is_not_placed_twice() {
        let mut m = manager();
        let plan = m.plan(&[quote("0.997", "1.007", "1000")]);
        m.on_submitting(ExchangeId::Paper, &plan.place[0]); // bid submitting, no ack yet
        let again = m.plan(&[quote("0.997", "1.007", "1000")]);
        assert_eq!(again.place.len(), 1, "only the ask");
        assert_eq!(again.place[0].side, Side::Sell);
        assert!(again.cancel.is_empty());
    }

    #[test]
    fn uncertain_failure_keeps_order_as_unknown_definite_failure_drops_it() {
        let mut m = manager();
        let plan = m.plan(&[quote("0.997", "1.007", "1000")]);
        m.on_submitting(ExchangeId::Paper, &plan.place[0]);
        m.on_place_failed(&plan.place[0].client_order_id, &ExchangeError::Timeout);
        assert_eq!(m.get(&plan.place[0].client_order_id).unwrap().status, OrderStatus::Unknown);

        m.on_submitting(ExchangeId::Paper, &plan.place[1]);
        m.on_place_failed(&plan.place[1].client_order_id, &ExchangeError::InvalidOrder("x".into()));
        assert!(m.get(&plan.place[1].client_order_id).is_none());
    }

    #[test]
    fn cancel_requested_orders_are_left_alone_and_removed_when_confirmed() {
        let mut m = manager();
        let plan = m.plan(&[quote("0.997", "1.007", "1000")]);
        place_all(&mut m, &plan);
        let id = plan.place[0].client_order_id.clone();
        m.on_cancel_requested(&id);
        // Re-planning the same quote must not cancel it again nor place a duplicate bid
        // (the bid side is still "live", just not matchable).
        let again = m.plan(&[quote("0.997", "1.007", "1000")]);
        assert!(again.cancel.is_empty());
        assert_eq!(again.place.len(), 1, "a fresh bid replaces the cancelling one");

        let mut done = ack(&plan.place[0], OrderStatus::Cancelled);
        done.client_order_id = id.clone();
        assert!(m.on_order_update(&done).is_some());
        assert!(m.get(&id).is_none());
    }

    #[test]
    fn cancel_not_found_drops_the_order() {
        let mut m = manager();
        let plan = m.plan(&[quote("0.997", "1.007", "1000")]);
        place_all(&mut m, &plan);
        let id = plan.place[0].client_order_id.clone();
        m.on_cancel_requested(&id);
        m.on_cancel_failed(&id, &ExchangeError::OrderNotFound);
        assert!(m.get(&id).is_none());
    }

    #[test]
    fn cancel_transport_failure_reverts_to_open_for_retry() {
        let mut m = manager();
        let plan = m.plan(&[quote("0.997", "1.007", "1000")]);
        place_all(&mut m, &plan);
        let id = plan.place[0].client_order_id.clone();
        m.on_cancel_requested(&id);
        m.on_cancel_failed(&id, &ExchangeError::Timeout);
        assert_eq!(m.get(&id).unwrap().status, OrderStatus::Open);
        assert_eq!(m.plan(&[]).cancel.len(), 2);
    }

    #[test]
    fn fills_update_remaining_and_terminal_orders_are_dropped() {
        let mut m = manager();
        let plan = m.plan(&[quote("0.997", "1.007", "1000")]);
        place_all(&mut m, &plan);
        let id = plan.place[0].client_order_id.clone();
        let mut partial = ack(&plan.place[0], OrderStatus::PartiallyFilled);
        partial.filled_quantity = d("400");
        m.on_order_update(&partial).unwrap();
        assert_eq!(m.get(&id).unwrap().remaining_quantity(), d("600"));
        // Remaining 600 ≠ desired 1000 → replaced.
        let plan2 = m.plan(&[quote("0.997", "1.007", "1000")]);
        assert_eq!(plan2.cancel.len(), 1);
        let mut filled = partial.clone();
        filled.status = OrderStatus::Filled;
        filled.filled_quantity = d("1000");
        m.on_order_update(&filled).unwrap();
        assert!(m.get(&id).is_none());
    }

    #[test]
    fn foreign_and_stale_updates_are_ignored() {
        let mut m = manager();
        let plan = m.plan(&[quote("0.997", "1.007", "1000")]);
        place_all(&mut m, &plan);
        let mut foreign = ack(&plan.place[0], OrderStatus::Open);
        foreign.client_order_id = "someone-else".into();
        assert!(m.on_order_update(&foreign).is_none());
        assert_eq!(m.orders().count(), 2);
        let stale = ack(&plan.place[0], OrderStatus::Submitting);
        assert!(m.on_order_update(&stale).is_none());
    }

    #[test]
    fn adopt_takes_only_our_live_orders() {
        let mut m = manager();
        let mut ours = ack(
            &OrderRequest {
                symbol: Symbol::new("BTC", "USDT"),
                side: Side::Buy,
                order_type: OrderType::Limit,
                price: Some(d("1")),
                quantity: d("1"),
                client_order_id: "mmppabc123000009".into(),
                post_only: true,
            },
            OrderStatus::Open,
        );
        let mut theirs = ours.clone();
        theirs.client_order_id = "manual-1".into();
        let mut dead = ours.clone();
        dead.status = OrderStatus::Filled;
        ours.exchange_order_id = Some("E9".into());
        assert_eq!(m.adopt(vec![ours, theirs, dead]), 1);
        assert_eq!(m.open_count(ExchangeId::Paper), 1);
    }
}
