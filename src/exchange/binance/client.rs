//! Shared HTTP client: endpoints per mode, server-time offset, error mapping.
//! Signing is added in M4.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use reqwest::StatusCode;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tracing::{debug, warn};

use crate::config::{Credentials, Mode};
use crate::exchange::ExchangeError;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Endpoints {
    pub rest: &'static str,
    pub ws: &'static str,
}

pub const PROD: Endpoints =
    Endpoints { rest: "https://api.binance.com", ws: "wss://stream.binance.com:9443" };
pub const TESTNET: Endpoints =
    Endpoints { rest: "https://testnet.binance.vision", ws: "wss://stream.testnet.binance.vision" };

/// Production credentials must never be usable in paper/testnet: the mode
/// alone decides the endpoints.
pub const fn endpoints(mode: Mode) -> Endpoints {
    match mode {
        Mode::Testnet => TESTNET,
        Mode::Paper | Mode::Live => PROD,
    }
}

#[derive(Clone)]
pub struct BinanceClient {
    inner: Arc<Inner>,
}

struct Inner {
    http: reqwest::Client,
    endpoints: Endpoints,
    credentials: Option<Credentials>,
    /// `server_time - local_time`, milliseconds. Applied to signed requests.
    time_offset_ms: AtomicI64,
}

/// Binance error body: `{"code":-1121,"msg":"Invalid symbol."}`
#[derive(Debug, Deserialize)]
pub struct ApiError {
    pub code: i64,
    pub msg: String,
}

impl BinanceClient {
    pub fn new(mode: Mode, credentials: Option<Credentials>) -> Result<Self, ExchangeError> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .user_agent(concat!("bookend/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| ExchangeError::Network(e.into()))?;
        Ok(Self {
            inner: Arc::new(Inner {
                http,
                endpoints: endpoints(mode),
                credentials,
                time_offset_ms: AtomicI64::new(0),
            }),
        })
    }

    pub fn endpoints(&self) -> Endpoints {
        self.inner.endpoints
    }

    pub fn has_credentials(&self) -> bool {
        self.inner.credentials.is_some()
    }

    pub fn time_offset_ms(&self) -> i64 {
        self.inner.time_offset_ms.load(Ordering::Relaxed)
    }

    /// Record `server_time - local_now` so signed requests carry a timestamp
    /// Binance accepts even when the host clock drifts.
    pub fn set_server_time(&self, server_time_ms: i64) {
        let offset = server_time_ms - chrono::Utc::now().timestamp_millis();
        if offset.abs() > 1_000 {
            warn!(offset_ms = offset, "binance server time differs from local clock");
        }
        self.inner.time_offset_ms.store(offset, Ordering::Relaxed);
    }

    /// Unauthenticated GET with query parameters.
    pub async fn get_public<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, ExchangeError> {
        let url = format!("{}{}", self.inner.endpoints.rest, path);
        debug!(%url, ?query, "GET");
        let resp = self.inner.http.get(&url).query(query).send().await.map_err(map_transport)?;
        decode(resp).await
    }
}

fn map_transport(e: reqwest::Error) -> ExchangeError {
    if e.is_timeout() { ExchangeError::Timeout } else { ExchangeError::Network(e.into()) }
}

async fn decode<T: DeserializeOwned>(resp: reqwest::Response) -> Result<T, ExchangeError> {
    let status = resp.status();
    let retry_after = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok()?.parse::<u64>().ok())
        .map(Duration::from_secs);
    let body = resp.text().await.map_err(map_transport)?;

    if status.is_success() {
        return serde_json::from_str(&body).map_err(|e| {
            ExchangeError::Unknown(format!("decode {status}: {e}; body={body:.200}"))
        });
    }

    let api = serde_json::from_str::<ApiError>(&body).ok();
    Err(map_status(status, retry_after, api, &body))
}

/// HTTP status + Binance error code → `ExchangeError`.
pub fn map_status(
    status: StatusCode,
    retry_after: Option<Duration>,
    api: Option<ApiError>,
    body: &str,
) -> ExchangeError {
    match status.as_u16() {
        429 | 418 => return ExchangeError::RateLimited(retry_after),
        401 | 403 => return ExchangeError::Authentication,
        503 => return ExchangeError::Unavailable,
        s if s >= 500 => return ExchangeError::Unavailable,
        _ => {}
    }
    match api {
        Some(ApiError { code, msg }) => map_api_code(code, msg),
        None => ExchangeError::Unknown(format!("{status}: {body:.200}")),
    }
}

/// Subset of https://developers.binance.com/docs/binance-spot-api-docs/errors
pub fn map_api_code(code: i64, msg: String) -> ExchangeError {
    match code {
        -1003 => ExchangeError::RateLimited(None),
        -1021 => ExchangeError::InvalidOrder(format!("timestamp outside recvWindow: {msg}")),
        -1022 | -2014 | -2015 => ExchangeError::Authentication,
        -2010 if msg.to_ascii_lowercase().contains("insufficient") => {
            ExchangeError::InsufficientBalance
        }
        -2011 | -2013 => ExchangeError::OrderNotFound,
        -1013 | -1100..=-1099 | -1102 | -1111 | -1121 | -1130 | -2010 => {
            ExchangeError::InvalidOrder(format!("{code}: {msg}"))
        }
        _ => ExchangeError::Unknown(format!("{code}: {msg}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_follow_mode_not_credentials() {
        assert_eq!(endpoints(Mode::Paper), PROD);
        assert_eq!(endpoints(Mode::Live), PROD);
        assert_eq!(endpoints(Mode::Testnet), TESTNET);
    }

    #[test]
    fn status_mapping() {
        let e = |s: u16| map_status(StatusCode::from_u16(s).unwrap(), None, None, "");
        assert!(matches!(e(429), ExchangeError::RateLimited(None)));
        assert!(matches!(e(418), ExchangeError::RateLimited(None)));
        assert!(matches!(e(401), ExchangeError::Authentication));
        assert!(matches!(e(503), ExchangeError::Unavailable));
        assert!(matches!(e(502), ExchangeError::Unavailable));
        assert!(matches!(e(400), ExchangeError::Unknown(_)));
        let ra = map_status(StatusCode::TOO_MANY_REQUESTS, Some(Duration::from_secs(7)), None, "");
        assert!(matches!(ra, ExchangeError::RateLimited(Some(d)) if d == Duration::from_secs(7)));
    }

    #[test]
    fn api_code_mapping() {
        let m = |c: i64, s: &str| map_api_code(c, s.to_owned());
        assert!(matches!(
            m(-2010, "Account has insufficient balance for requested action."),
            ExchangeError::InsufficientBalance
        ));
        assert!(matches!(
            m(-2010, "Order would immediately match and take."),
            ExchangeError::InvalidOrder(_)
        ));
        assert!(matches!(m(-2011, "Unknown order sent."), ExchangeError::OrderNotFound));
        assert!(matches!(m(-2013, "Order does not exist."), ExchangeError::OrderNotFound));
        assert!(matches!(
            m(-1021, "Timestamp for this request is outside of the recvWindow."),
            ExchangeError::InvalidOrder(_)
        ));
        assert!(matches!(
            m(-2015, "Invalid API-key, IP, or permissions for action."),
            ExchangeError::Authentication
        ));
        assert!(matches!(m(-1121, "Invalid symbol."), ExchangeError::InvalidOrder(_)));
        assert!(matches!(m(-1003, "Too many requests."), ExchangeError::RateLimited(None)));
        assert!(matches!(m(-9999, "?"), ExchangeError::Unknown(_)));
    }

    #[test]
    fn body_error_is_parsed_when_status_is_4xx() {
        let e = map_status(
            StatusCode::BAD_REQUEST,
            None,
            serde_json::from_str(r#"{"code":-1121,"msg":"Invalid symbol."}"#).ok(),
            "",
        );
        assert!(matches!(e, ExchangeError::InvalidOrder(m) if m.contains("Invalid symbol")));
    }

    #[test]
    fn server_time_offset_is_recorded() {
        let c = BinanceClient::new(Mode::Paper, None).unwrap();
        assert_eq!(c.time_offset_ms(), 0);
        c.set_server_time(chrono::Utc::now().timestamp_millis() + 5_000);
        assert!((4_500..=5_500).contains(&c.time_offset_ms()));
        assert!(!c.has_credentials());
    }
}
