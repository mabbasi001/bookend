//! State file: position, today's PnL and our open orders — the things that
//! cannot be rebuilt from the exchange after a restart. One JSON file,
//! written atomically (temp file + rename).

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::portfolio::{DailyPnl, Position};
use crate::types::Order;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    pub run_id: String,
    pub position: Position,
    pub daily: DailyPnl,
    /// Orders we believed live at the last save; adopted or reconciled on start.
    pub open_orders: Vec<Order>,
    pub updated_at: DateTime<Utc>,
}

pub struct Store {
    path: PathBuf,
}

impl Store {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// `Ok(None)` when no state file exists yet.
    pub fn load(&self) -> anyhow::Result<Option<State>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                let state: State = serde_json::from_slice(&bytes).map_err(|e| {
                    anyhow::anyhow!("corrupt state file {}: {e}", self.path.display())
                })?;
                info!(path = %self.path.display(), run_id = %state.run_id, updated_at = %state.updated_at, "state loaded");
                Ok(Some(state))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(anyhow::anyhow!("cannot read state file {}: {e}", self.path.display())),
        }
    }

    pub fn save(&self, state: &State) -> anyhow::Result<()> {
        if let Some(dir) = self.path.parent()
            && !dir.as_os_str().is_empty()
        {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(state)?;
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn state() -> State {
        State {
            run_id: "abc123".into(),
            position: Position {
                quantity: Decimal::from(3),
                avg_cost: Decimal::from(9),
                realized_pnl: Decimal::from(4),
                fees: Decimal::ONE,
            },
            daily: DailyPnl {
                day: Utc::now().date_naive(),
                realized: Decimal::from(4),
                fees: Decimal::ONE,
            },
            open_orders: vec![],
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn round_trip_and_missing_file() {
        let dir = std::env::temp_dir().join(format!("bookend-test-{}", std::process::id()));
        let store = Store::new(dir.join("nested").join("state.json"));
        assert!(store.load().unwrap().is_none());
        let s = state();
        store.save(&s).unwrap();
        assert_eq!(store.load().unwrap().unwrap(), s);
        assert!(!store.path().with_extension("json.tmp").exists(), "temp file renamed away");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn corrupt_file_is_an_error_not_a_silent_reset() {
        let dir = std::env::temp_dir().join(format!("bookend-corrupt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.json");
        std::fs::write(&path, b"{ not json").unwrap();
        let err = Store::new(&path).load().unwrap_err();
        assert!(err.to_string().contains("corrupt"));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
