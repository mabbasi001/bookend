//! Configuration: TOML file + secrets from the environment + validation.
//!
//! Secrets never appear in the TOML. Each exchange section names the
//! environment variables to read (`api_key_env`, …); [`Config::load`] resolves
//! them and fails fast when a required one is missing.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};

use rust_decimal::Decimal;
use serde::Deserialize;

use crate::types::ExchangeId;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot parse config {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("exchange {exchange} is enabled but environment variable {var} is not set")]
    MissingSecret { exchange: ExchangeId, var: String },
    #[error("invalid config: {0}")]
    Invalid(String),
}

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

/// A credential loaded from the environment. `Debug`/`Display` never reveal it.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The only way to read the value; call sites are easy to audit.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(\"***\")")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("***")
    }
}

#[derive(Debug, Clone)]
pub struct Credentials {
    pub api_key: Secret,
    pub api_secret: Secret,
    /// OKX only.
    pub passphrase: Option<Secret>,
}

// ---------------------------------------------------------------------------
// Sections
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Paper,
    Testnet,
    Live,
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Mode::Paper => "paper",
            Mode::Testnet => "testnet",
            Mode::Live => "live",
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BotConfig {
    pub name: String,
    pub mode: Mode,
    pub base: String,
    pub quote: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Env var names. Default to `<EXCHANGE>_API_KEY` etc.
    pub api_key_env: Option<String>,
    pub api_secret_env: Option<String>,
    pub api_passphrase_env: Option<String>,
    /// Filled by [`Config::resolve_secrets`]; never deserialized.
    #[serde(skip)]
    pub credentials: Option<Credentials>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangesConfig {
    #[serde(default)]
    pub binance: ExchangeConfig,
    #[serde(default)]
    pub bybit: ExchangeConfig,
    #[serde(default)]
    pub okx: ExchangeConfig,
}

impl ExchangesConfig {
    pub fn iter(&self) -> impl Iterator<Item = (ExchangeId, &ExchangeConfig)> {
        [
            (ExchangeId::Binance, &self.binance),
            (ExchangeId::Bybit, &self.bybit),
            (ExchangeId::Okx, &self.okx),
        ]
        .into_iter()
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = (ExchangeId, &mut ExchangeConfig)> {
        [
            (ExchangeId::Binance, &mut self.binance),
            (ExchangeId::Bybit, &mut self.bybit),
            (ExchangeId::Okx, &mut self.okx),
        ]
        .into_iter()
    }

    pub fn enabled(&self) -> Vec<ExchangeId> {
        self.iter().filter(|(_, c)| c.enabled).map(|(id, _)| id).collect()
    }

    pub fn get(&self, id: ExchangeId) -> Option<&ExchangeConfig> {
        self.iter().find(|(i, _)| *i == id).map(|(_, c)| c)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FairPriceMethod {
    LocalMid,
    WeightedMid,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FairPriceConfig {
    pub method: FairPriceMethod,
    /// Per-exchange weights for `weighted_mid`; equal weights when absent.
    #[serde(default)]
    pub weights: HashMap<String, Decimal>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryConfig {
    pub target_ratio: Decimal,
    pub skew_factor: Decimal,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LevelsConfig {
    pub count: u8,
    pub spacing_bps: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyConfig {
    pub name: String,
    pub spread_bps: u32,
    /// Base-asset quantity per order.
    pub order_size: Decimal,
    #[serde(default = "default_true")]
    pub post_only: bool,
    pub fair_price: FairPriceConfig,
    pub inventory: InventoryConfig,
    pub levels: LevelsConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteConfig {
    pub refresh_interval_ms: u64,
    pub min_price_change_bps: u32,
    pub min_quantity_change: Decimal,
}

/// Every hard limit lives here and nowhere else.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskConfig {
    /// Base quantity.
    pub max_order_size: Decimal,
    /// Base quantity.
    pub max_position: Decimal,
    pub max_open_orders: u32,
    pub min_inventory_ratio: Decimal,
    pub max_inventory_ratio: Decimal,
    /// Quote currency; persisted across restarts.
    pub max_daily_loss: Decimal,
    pub max_market_data_age_ms: u64,
}

/// Fallback fees; real tiers are read from the account API at startup.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeesConfig {
    pub maker_bps: u32,
    pub taker_bps: u32,
}

impl Default for FeesConfig {
    fn default() -> Self {
        Self { maker_bps: 10, taker_bps: 20 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistenceConfig {
    pub path: PathBuf,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self { path: PathBuf::from("data/state.json") }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Json,
    Pretty,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default)]
    pub format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self { level: default_log_level(), format: LogFormat::default() }
    }
}

fn default_true() -> bool {
    true
}

fn default_log_level() -> String {
    "info".to_owned()
}

// ---------------------------------------------------------------------------
// Root
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub bot: BotConfig,
    #[serde(default)]
    pub exchanges: ExchangesConfig,
    pub strategy: StrategyConfig,
    pub quote: QuoteConfig,
    pub risk: RiskConfig,
    #[serde(default)]
    pub fees: FeesConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

impl Config {
    /// Read, resolve secrets from the process environment, validate.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|source| ConfigError::Io { path: path.to_owned(), source })?;
        let mut config = Self::parse(&text)
            .map_err(|source| ConfigError::Parse { path: path.to_owned(), source })?;
        config.resolve_secrets(|var| std::env::var(var).ok())?;
        config.validate()?;
        Ok(config)
    }

    pub fn parse(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }

    /// Fill `credentials` for every enabled exchange from `lookup`.
    ///
    /// In `paper` mode credentials are optional (loaded when present, market
    /// data is public). In `testnet`/`live` a missing variable is an error.
    pub fn resolve_secrets(
        &mut self,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<(), ConfigError> {
        let required = self.bot.mode != Mode::Paper;

        for (id, cfg) in self.exchanges.iter_mut() {
            if !cfg.enabled {
                continue;
            }
            let prefix = id.as_str().to_uppercase();
            let key_var = cfg.api_key_env.clone().unwrap_or_else(|| format!("{prefix}_API_KEY"));
            let secret_var =
                cfg.api_secret_env.clone().unwrap_or_else(|| format!("{prefix}_API_SECRET"));
            let pass_var = cfg
                .api_passphrase_env
                .clone()
                .or_else(|| (id == ExchangeId::Okx).then(|| format!("{prefix}_API_PASSPHRASE")));

            let fetch = |var: &str| -> Result<Option<Secret>, ConfigError> {
                match lookup(var).filter(|v| !v.trim().is_empty()) {
                    Some(v) => Ok(Some(Secret::new(v))),
                    None if required => {
                        Err(ConfigError::MissingSecret { exchange: id, var: var.to_owned() })
                    }
                    None => Ok(None),
                }
            };

            let api_key = fetch(&key_var)?;
            let api_secret = fetch(&secret_var)?;
            let passphrase = match &pass_var {
                Some(var) => fetch(var)?,
                None => None,
            };

            cfg.credentials = match (api_key, api_secret) {
                (Some(api_key), Some(api_secret)) => {
                    Some(Credentials { api_key, api_secret, passphrase })
                }
                _ => None,
            };
        }
        Ok(())
    }

    /// Fail fast on anything that would make quoting unsafe or meaningless.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));
        let one = Decimal::ONE;
        let zero = Decimal::ZERO;

        let b = &self.bot;
        if b.name.trim().is_empty() {
            return invalid("bot.name must not be empty".into());
        }
        for (field, v) in [("bot.base", &b.base), ("bot.quote", &b.quote)] {
            if v.trim().is_empty() || v.contains('/') || v.contains(char::is_whitespace) {
                return invalid(format!("{field} must be a plain asset code, got {v:?}"));
            }
        }
        if b.base == b.quote {
            return invalid("bot.base and bot.quote must differ".into());
        }

        if self.exchanges.enabled().is_empty() {
            return invalid("at least one exchange must be enabled".into());
        }

        let s = &self.strategy;
        if s.name.trim().is_empty() {
            return invalid("strategy.name must not be empty".into());
        }
        if s.spread_bps == 0 {
            return invalid("strategy.spread_bps must be > 0".into());
        }
        if s.spread_bps <= 2 * self.fees.maker_bps {
            return invalid(format!(
                "strategy.spread_bps ({}) must exceed 2 × fees.maker_bps ({}) or quoting loses money",
                s.spread_bps, self.fees.maker_bps
            ));
        }
        if s.order_size <= zero {
            return invalid("strategy.order_size must be > 0".into());
        }
        if s.levels.count == 0 {
            return invalid("strategy.levels.count must be >= 1".into());
        }
        if s.levels.count > 1 && s.levels.spacing_bps == 0 {
            return invalid("strategy.levels.spacing_bps must be > 0 when count > 1".into());
        }
        if s.inventory.skew_factor < zero {
            return invalid("strategy.inventory.skew_factor must be >= 0".into());
        }
        for (name, w) in &s.fair_price.weights {
            if *w < zero {
                return invalid(format!("strategy.fair_price.weights.{name} must be >= 0"));
            }
        }

        let q = &self.quote;
        if q.refresh_interval_ms == 0 {
            return invalid("quote.refresh_interval_ms must be > 0".into());
        }
        if q.min_quantity_change < zero {
            return invalid("quote.min_quantity_change must be >= 0".into());
        }

        let r = &self.risk;
        for (field, v) in [
            ("risk.max_order_size", r.max_order_size),
            ("risk.max_position", r.max_position),
            ("risk.max_daily_loss", r.max_daily_loss),
        ] {
            if v <= zero {
                return invalid(format!("{field} must be > 0"));
            }
        }
        if r.max_open_orders == 0 {
            return invalid("risk.max_open_orders must be > 0".into());
        }
        if r.max_market_data_age_ms == 0 {
            return invalid("risk.max_market_data_age_ms must be > 0".into());
        }
        if s.order_size > r.max_order_size {
            return invalid(format!(
                "strategy.order_size ({}) exceeds risk.max_order_size ({})",
                s.order_size, r.max_order_size
            ));
        }
        let t = s.inventory.target_ratio;
        if !(zero < r.min_inventory_ratio
            && r.min_inventory_ratio < t
            && t < r.max_inventory_ratio
            && r.max_inventory_ratio < one)
        {
            return invalid(format!(
                "inventory ratios must satisfy 0 < risk.min ({}) < strategy.target ({}) < risk.max ({}) < 1",
                r.min_inventory_ratio, t, r.max_inventory_ratio
            ));
        }

        if self.logging.level.trim().is_empty() {
            return invalid("logging.level must not be empty".into());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const PAPER: &str = include_str!("../configs/paper.toml");

    fn paper() -> Config {
        Config::parse(PAPER).expect("paper.toml parses")
    }

    fn env<'a>(vars: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |k| vars.iter().find(|(n, _)| *n == k).map(|(_, v)| (*v).to_owned())
    }

    #[test]
    fn shipped_configs_parse_and_validate() {
        for text in [
            PAPER,
            include_str!("../configs/testnet.toml"),
            include_str!("../configs/live.example.toml"),
        ] {
            let c = Config::parse(text).unwrap();
            c.validate().unwrap();
        }
    }

    #[test]
    fn defaults_apply_for_optional_sections() {
        let mut c = paper();
        c.fees = FeesConfig::default();
        assert_eq!(c.fees.maker_bps, 10);
        assert_eq!(LoggingConfig::default().level, "info");
        assert_eq!(LogFormat::default(), LogFormat::Json);
        assert_eq!(PersistenceConfig::default().path, PathBuf::from("data/state.json"));
        assert!(c.strategy.post_only);
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let text = PAPER.replace("[risk]", "[risk]\nmax_lose = \"1\"");
        assert!(Config::parse(&text).is_err());
    }

    #[test]
    fn paper_mode_secrets_are_optional() {
        let mut c = paper();
        c.resolve_secrets(|_| None).unwrap();
        assert!(c.exchanges.binance.credentials.is_none());
    }

    #[test]
    fn testnet_mode_requires_secrets_for_enabled_exchanges() {
        let mut c = paper();
        c.bot.mode = Mode::Testnet;
        let err = c.resolve_secrets(env(&[("BINANCE_API_KEY", "k")])).unwrap_err();
        assert!(
            matches!(&err, ConfigError::MissingSecret { exchange: ExchangeId::Binance, var } if var == "BINANCE_API_SECRET"),
            "{err}"
        );

        c.resolve_secrets(env(&[("BINANCE_API_KEY", "k"), ("BINANCE_API_SECRET", "s")])).unwrap();
        let creds = c.exchanges.binance.credentials.as_ref().unwrap();
        assert_eq!(creds.api_key.expose(), "k");
        assert!(creds.passphrase.is_none());
    }

    #[test]
    fn disabled_exchanges_are_ignored() {
        let mut c = paper();
        c.bot.mode = Mode::Live;
        c.exchanges.bybit.enabled = false;
        c.resolve_secrets(env(&[("BINANCE_API_KEY", "k"), ("BINANCE_API_SECRET", "s")])).unwrap();
    }

    #[test]
    fn okx_requires_passphrase() {
        let mut c = paper();
        c.bot.mode = Mode::Live;
        c.exchanges.okx.enabled = true;
        let vars = [
            ("BINANCE_API_KEY", "k"),
            ("BINANCE_API_SECRET", "s"),
            ("OKX_API_KEY", "k"),
            ("OKX_API_SECRET", "s"),
        ];
        let err = c.resolve_secrets(env(&vars)).unwrap_err();
        assert!(
            matches!(&err, ConfigError::MissingSecret { var, .. } if var == "OKX_API_PASSPHRASE")
        );
    }

    #[test]
    fn custom_env_var_names_are_honoured() {
        let mut c = paper();
        c.bot.mode = Mode::Live;
        c.exchanges.binance.api_key_env = Some("MY_KEY".into());
        c.exchanges.binance.api_secret_env = Some("MY_SECRET".into());
        c.resolve_secrets(env(&[("MY_KEY", "k"), ("MY_SECRET", "s")])).unwrap();
        assert!(c.exchanges.binance.credentials.is_some());
    }

    #[test]
    fn empty_env_value_counts_as_missing() {
        let mut c = paper();
        c.bot.mode = Mode::Live;
        let err = c
            .resolve_secrets(env(&[("BINANCE_API_KEY", "  "), ("BINANCE_API_SECRET", "s")]))
            .unwrap_err();
        assert!(matches!(err, ConfigError::MissingSecret { .. }));
    }

    #[test]
    fn secrets_never_appear_in_debug_output() {
        let mut c = paper();
        c.bot.mode = Mode::Live;
        c.resolve_secrets(env(&[
            ("BINANCE_API_KEY", "hunter2key"),
            ("BINANCE_API_SECRET", "hunter2sec"),
        ]))
        .unwrap();
        let dump = format!("{c:?}");
        assert!(!dump.contains("hunter2"), "secret leaked: {dump}");
        assert!(dump.contains("Secret(\"***\")"));
        assert_eq!(Secret::new("x").to_string(), "***");
    }

    // --- validation rules ---------------------------------------------------

    fn assert_invalid(c: &Config, needle: &str) {
        match c.validate() {
            Err(ConfigError::Invalid(msg)) => assert!(msg.contains(needle), "got: {msg}"),
            other => panic!("expected Invalid({needle}), got {other:?}"),
        }
    }

    #[test]
    fn rejects_empty_or_slashed_symbol() {
        let mut c = paper();
        c.bot.base = "".into();
        assert_invalid(&c, "bot.base");
        c.bot.base = "BTC/USDT".into();
        assert_invalid(&c, "bot.base");
        c.bot.base = "USDT".into();
        assert_invalid(&c, "must differ");
    }

    #[test]
    fn rejects_no_enabled_exchange() {
        let mut c = paper();
        c.exchanges.binance.enabled = false;
        assert_invalid(&c, "at least one exchange");
    }

    #[test]
    fn rejects_spread_not_covering_fees() {
        let mut c = paper();
        c.strategy.spread_bps = 20;
        c.fees.maker_bps = 10;
        assert_invalid(&c, "2 × fees.maker_bps");
        c.strategy.spread_bps = 0;
        assert_invalid(&c, "spread_bps must be > 0");
    }

    #[test]
    fn rejects_non_positive_limits() {
        let mut c = paper();
        c.risk.max_daily_loss = Decimal::ZERO;
        assert_invalid(&c, "max_daily_loss");
        let mut c = paper();
        c.risk.max_open_orders = 0;
        assert_invalid(&c, "max_open_orders");
        let mut c = paper();
        c.risk.max_market_data_age_ms = 0;
        assert_invalid(&c, "max_market_data_age_ms");
        let mut c = paper();
        c.quote.refresh_interval_ms = 0;
        assert_invalid(&c, "refresh_interval_ms");
        let mut c = paper();
        c.strategy.order_size = Decimal::ZERO;
        assert_invalid(&c, "order_size must be > 0");
    }

    #[test]
    fn rejects_order_size_above_risk_limit() {
        let mut c = paper();
        c.strategy.order_size = c.risk.max_order_size + Decimal::ONE;
        assert_invalid(&c, "exceeds risk.max_order_size");
    }

    #[test]
    fn rejects_bad_inventory_ratio_ordering() {
        let mut c = paper();
        c.strategy.inventory.target_ratio = "0.9".parse().unwrap();
        assert_invalid(&c, "inventory ratios");
        let mut c = paper();
        c.risk.min_inventory_ratio = Decimal::ZERO;
        assert_invalid(&c, "inventory ratios");
        let mut c = paper();
        c.risk.max_inventory_ratio = Decimal::ONE;
        assert_invalid(&c, "inventory ratios");
    }

    #[test]
    fn rejects_levels_without_spacing() {
        let mut c = paper();
        c.strategy.levels.count = 0;
        assert_invalid(&c, "levels.count");
        c.strategy.levels.count = 3;
        c.strategy.levels.spacing_bps = 0;
        assert_invalid(&c, "spacing_bps");
    }

    #[test]
    fn mode_display_matches_toml_values() {
        assert_eq!(Mode::Paper.to_string(), "paper");
        assert_eq!(Mode::Testnet.to_string(), "testnet");
        assert_eq!(Mode::Live.to_string(), "live");
    }
}
