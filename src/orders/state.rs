//! Order state machine (ARCHITECTURE.md §9).
//!
//! ```text
//! Created ──► Submitting ──► Open ──► PartiallyFilled ──► Filled
//!                │            │            │
//!                │            ├────────────┴──► CancelRequested ──► Cancelled
//!                │            │                       │
//!                │            │                       └──► Filled   (cancel lost the race)
//!                │            └──► Expired
//!                ├──► Rejected
//!                └──► Unknown ──► (reconcile) ──► Open | PartiallyFilled | Filled | Cancelled | Rejected | Expired
//! ```

use crate::types::{Order, OrderStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid order transition {from:?} -> {to:?}")]
pub struct InvalidTransition {
    pub from: OrderStatus,
    pub to: OrderStatus,
}

pub fn can_transition(from: OrderStatus, to: OrderStatus) -> bool {
    use OrderStatus::*;
    match from {
        Created => matches!(to, Submitting),
        Submitting => matches!(to, Open | PartiallyFilled | Filled | Rejected | Unknown),
        Unknown => matches!(to, Open | PartiallyFilled | Filled | Cancelled | Rejected | Expired),
        Open => matches!(to, PartiallyFilled | Filled | CancelRequested | Cancelled | Expired),
        // A second partial fill keeps the status; cancels can still race.
        PartiallyFilled => {
            matches!(to, PartiallyFilled | Filled | CancelRequested | Cancelled | Expired)
        }
        // Fills can still land while the cancel is in flight.
        CancelRequested => {
            matches!(to, CancelRequested | PartiallyFilled | Cancelled | Filled | Expired)
        }
        Filled | Cancelled | Rejected | Expired => false,
    }
}

pub fn transition(order: &mut Order, to: OrderStatus) -> Result<(), InvalidTransition> {
    if !can_transition(order.status, to) {
        return Err(InvalidTransition { from: order.status, to });
    }
    order.status = to;
    Ok(())
}

/// Merge an exchange-reported view of the order into ours. Returns `true`
/// when anything changed. An update that would move backwards (e.g. a late
/// `Open` after `Filled`) is rejected rather than applied.
pub fn apply_update(order: &mut Order, update: &Order) -> Result<bool, InvalidTransition> {
    let mut changed = false;

    if order.exchange_order_id.is_none() && update.exchange_order_id.is_some() {
        order.exchange_order_id = update.exchange_order_id.clone();
        changed = true;
    }
    if update.filled_quantity > order.filled_quantity {
        order.filled_quantity = update.filled_quantity;
        changed = true;
    }
    if update.status != order.status {
        // A fill reported while we wait for a cancel keeps CancelRequested
        // (unless it completed the order).
        let target = match (order.status, update.status) {
            (OrderStatus::CancelRequested, OrderStatus::Open | OrderStatus::PartiallyFilled) => {
                OrderStatus::CancelRequested
            }
            (_, s) => s,
        };
        if target != order.status {
            transition(order, target)?;
            changed = true;
        }
    }
    if changed {
        order.updated_at = update.updated_at.max(order.updated_at);
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ExchangeId, OrderType, Side, Symbol};
    use OrderStatus::*;

    const ALL: [OrderStatus; 10] = [
        Created,
        Submitting,
        Unknown,
        Open,
        PartiallyFilled,
        Filled,
        CancelRequested,
        Cancelled,
        Rejected,
        Expired,
    ];

    fn order(status: OrderStatus) -> Order {
        Order {
            exchange: ExchangeId::Paper,
            exchange_order_id: None,
            client_order_id: "c1".into(),
            symbol: Symbol::new("BTC", "USDT"),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(1.into()),
            quantity: 10.into(),
            filled_quantity: 0.into(),
            status,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn terminal_states_have_no_exits() {
        for from in [Filled, Cancelled, Rejected, Expired] {
            for to in ALL {
                assert!(!can_transition(from, to), "{from:?} -> {to:?}");
            }
        }
    }

    #[test]
    fn happy_path() {
        let mut o = order(Created);
        for s in [Submitting, Open, PartiallyFilled, PartiallyFilled, Filled] {
            transition(&mut o, s).unwrap();
        }
        assert_eq!(o.status, Filled);
    }

    #[test]
    fn cancel_race_with_fill_is_legal() {
        assert!(can_transition(CancelRequested, Filled));
        assert!(can_transition(CancelRequested, PartiallyFilled));
        assert!(can_transition(CancelRequested, Cancelled));
        assert!(can_transition(PartiallyFilled, Cancelled));
    }

    #[test]
    fn unknown_can_resolve_anywhere_but_not_stay_created() {
        assert!(can_transition(Submitting, Unknown));
        for s in [Open, PartiallyFilled, Filled, Cancelled, Rejected, Expired] {
            assert!(can_transition(Unknown, s), "Unknown -> {s:?}");
        }
        assert!(!can_transition(Unknown, Created));
        assert!(!can_transition(Unknown, Submitting));
        assert!(!can_transition(Open, Created));
        assert!(!can_transition(Open, Submitting));
    }

    #[test]
    fn transition_rejects_and_leaves_order_untouched() {
        let mut o = order(Filled);
        let err = transition(&mut o, Open).unwrap_err();
        assert_eq!(err, InvalidTransition { from: Filled, to: Open });
        assert_eq!(o.status, Filled);
    }

    #[test]
    fn apply_update_merges_id_fill_and_status() {
        let mut o = order(Submitting);
        let mut u = order(PartiallyFilled);
        u.exchange_order_id = Some("E1".into());
        u.filled_quantity = 4.into();
        assert!(apply_update(&mut o, &u).unwrap());
        assert_eq!(o.exchange_order_id.as_deref(), Some("E1"));
        assert_eq!(o.filled_quantity, 4.into());
        assert_eq!(o.status, PartiallyFilled);
        assert!(!apply_update(&mut o, &u).unwrap(), "idempotent");
    }

    #[test]
    fn apply_update_keeps_cancel_requested_on_partial_fill() {
        let mut o = order(CancelRequested);
        let mut u = order(PartiallyFilled);
        u.filled_quantity = 3.into();
        assert!(apply_update(&mut o, &u).unwrap());
        assert_eq!(o.status, CancelRequested);
        assert_eq!(o.filled_quantity, 3.into());
        let mut done = order(Filled);
        done.filled_quantity = 10.into();
        apply_update(&mut o, &done).unwrap();
        assert_eq!(o.status, Filled);
    }

    #[test]
    fn apply_update_rejects_backwards_status_and_lower_fill() {
        let mut o = order(Filled);
        o.filled_quantity = 10.into();
        let mut stale = order(Open);
        stale.filled_quantity = 2.into();
        assert!(apply_update(&mut o, &stale).is_err());
        assert_eq!(o.filled_quantity, 10.into());
        assert_eq!(o.status, Filled);
    }
}
