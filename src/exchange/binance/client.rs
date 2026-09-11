//! Shared HTTP client: endpoints per mode, server-time offset, error mapping,
//! HMAC-SHA256 request signing.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode, Url};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use sha2::Sha256;
use tracing::{debug, warn};

use crate::config::{Credentials, Mode};
use crate::exchange::ExchangeError;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Binance rejects signed requests whose `timestamp` is older than this
/// (`-1021`). 5 s is the Binance default; the server-time offset keeps us
/// well inside it.
pub const RECV_WINDOW_MS: u64 = 5_000;
const API_KEY_HEADER: HeaderName = HeaderName::from_static("x-mbx-apikey");

/// Lowercase-hex HMAC-SHA256 of `payload` keyed with the API secret — the
/// Binance signature scheme (`signature=` over the exact query string).
pub fn sign(secret: &str, payload: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac accepts any key length");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// How a request authenticates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Auth {
    /// `X-MBX-APIKEY` header only (user data stream endpoints).
    Key,
    /// Header plus `timestamp`, `recvWindow` and `signature` query params.
    Signed,
}

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

    /// Signed GET (`USER_DATA` endpoints: open orders, account, order lookup).
    pub async fn get_signed<T: DeserializeOwned>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<T, ExchangeError> {
        self.request(Method::GET, path, params, Auth::Signed).await
    }

    /// Signed POST (`TRADE` endpoints: place order).
    pub async fn post_signed<T: DeserializeOwned>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<T, ExchangeError> {
        self.request(Method::POST, path, params, Auth::Signed).await
    }

    /// Signed DELETE (`TRADE` endpoints: cancel order / cancel all).
    pub async fn delete_signed<T: DeserializeOwned>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<T, ExchangeError> {
        self.request(Method::DELETE, path, params, Auth::Signed).await
    }

    /// API-key-only request without a signature (`USER_STREAM` endpoints:
    /// listenKey create / keepalive / close).
    pub async fn request_keyed<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<T, ExchangeError> {
        self.request(method, path, params, Auth::Key).await
    }

    async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        params: &[(&str, String)],
        auth: Auth,
    ) -> Result<T, ExchangeError> {
        let creds = self.inner.credentials.as_ref().ok_or(ExchangeError::Authentication)?;
        let query = match auth {
            Auth::Key => encode_query(params),
            Auth::Signed => self.signed_query(params)?,
        };
        // Every parameter travels in the query string for all methods; Binance
        // accepts that for POST/DELETE and it keeps one signing path. The
        // query is never logged: it carries the signature.
        let url = format!("{}{}?{}", self.inner.endpoints.rest, path, query);
        debug!(%method, path, ?auth, "request");

        let mut headers = HeaderMap::with_capacity(1);
        headers.insert(
            API_KEY_HEADER,
            HeaderValue::from_str(creds.api_key.expose())
                .map_err(|_| ExchangeError::Authentication)?,
        );
        let resp = self
            .inner
            .http
            .request(method, &url)
            .headers(headers)
            .send()
            .await
            .map_err(map_transport)?;
        decode(resp).await
    }

    /// `params` + `recvWindow` + `timestamp` (server-time adjusted), URL
    /// encoded, with `signature` appended over exactly that string.
    fn signed_query(&self, params: &[(&str, String)]) -> Result<String, ExchangeError> {
        let creds = self.inner.credentials.as_ref().ok_or(ExchangeError::Authentication)?;
        let timestamp = chrono::Utc::now().timestamp_millis() + self.time_offset_ms();
        let mut all: Vec<(&str, String)> = Vec::with_capacity(params.len() + 2);
        all.extend(params.iter().map(|(k, v)| (*k, v.clone())));
        all.push(("recvWindow", RECV_WINDOW_MS.to_string()));
        all.push(("timestamp", timestamp.to_string()));
        let query = encode_query(&all);
        let signature = sign(creds.api_secret.expose(), &query);
        Ok(format!("{query}&signature={signature}"))
    }
}

/// `application/x-www-form-urlencoded` query string, parameter order preserved
/// (the signature is over the bytes as sent, so encoding and order must match).
fn encode_query(params: &[(&str, String)]) -> String {
    let mut url = Url::parse("https://x").expect("static url");
    url.query_pairs_mut().extend_pairs(params.iter().map(|(k, v)| (*k, v.as_str())));
    url.query().unwrap_or_default().to_owned()
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
    use crate::config::Secret;

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

    fn creds() -> Credentials {
        Credentials {
            api_key: Secret::new(
                "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
            ),
            api_secret: Secret::new(
                "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j",
            ),
            passphrase: None,
        }
    }

    /// Reference vector from the Binance spot API docs ("Example 1: as a
    /// query string").
    #[test]
    fn signature_matches_binance_reference_vector() {
        let query = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1\
                     &recvWindow=5000&timestamp=1499827319559";
        assert_eq!(
            sign(creds().api_secret.expose(), query),
            "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
        );
    }

    #[test]
    fn query_encoding_preserves_order_and_escapes() {
        let q = encode_query(&[
            ("symbol", "BTCUSDT".into()),
            ("price", "0.1".into()),
            ("origClientOrderId", "a b&c".into()),
        ]);
        assert_eq!(q, "symbol=BTCUSDT&price=0.1&origClientOrderId=a+b%26c");
        assert_eq!(encode_query(&[]), "");
    }

    #[test]
    fn signed_query_appends_window_timestamp_and_valid_signature() {
        let c = BinanceClient::new(Mode::Testnet, Some(creds())).unwrap();
        c.set_server_time(chrono::Utc::now().timestamp_millis() + 2_000);
        let q = c.signed_query(&[("symbol", "BTCUSDT".into())]).unwrap();

        let (payload, sig) = q.rsplit_once("&signature=").expect("signature last");
        assert_eq!(sig.len(), 64);
        assert!(sig.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(sig, sign(creds().api_secret.expose(), payload));

        assert!(payload.starts_with("symbol=BTCUSDT&recvWindow=5000&timestamp="));
        let ts: i64 = payload.rsplit_once('=').unwrap().1.parse().unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        assert!((ts - now - 2_000).abs() < 1_000, "timestamp carries the server offset");
    }

    #[test]
    fn signed_requests_need_credentials() {
        let c = BinanceClient::new(Mode::Testnet, None).unwrap();
        assert!(matches!(c.signed_query(&[]), Err(ExchangeError::Authentication)));
    }

    #[tokio::test]
    async fn keyed_request_without_credentials_fails_before_the_network() {
        let c = BinanceClient::new(Mode::Testnet, None).unwrap();
        let r: Result<serde_json::Value, _> =
            c.request_keyed(Method::POST, "/api/v3/userDataStream", &[]).await;
        assert!(matches!(r, Err(ExchangeError::Authentication)));
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
