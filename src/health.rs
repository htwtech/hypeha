//! Asking a source where it stands, instead of inferring it from its stream.
//!
//! `order_book_server` answers `GET /health` on the same port it serves
//! websockets from, with the height of the book it has built and how long ago
//! it last applied anything:
//!
//! ```text
//! {"status":"ready","uptime_seconds":16618,"height":1164506857,
//!  "connections":1,"last_event_age_ms":1}
//! ```
//!
//! Three measurements of lag came before this one and all three were wrong in
//! the same way -- they read a side effect rather than the thing:
//!
//! * how recently a source spoke: a node replaying old blocks is the chattiest
//!   source there is;
//! * the block time of frames as our own reader parsed them: a reader held up
//!   by a lock reads exactly like a node that stopped;
//! * the block time of the `bbo` probe: `bbo` is sent only when the top of book
//!   changes, so silence there means the price did not move.
//!
//! A height read straight off the server has none of those ambiguities, and
//! comparing heights between sources needs no clock at all -- which retires the
//! whole question of whether our clock agrees with the nodes'.
//!
//! The request is written by hand over a plain socket rather than pulling in an
//! HTTP client: it is one short GET to a machine-local port, and the endpoint is
//! deliberately lock-free on the server side, so the reply does not wait on
//! anything the server is busy with.

use std::time::Duration;

use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// What the server says about itself.
#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Health {
    pub status: String,
    pub height: u64,
    #[serde(default)]
    pub last_event_age_ms: i64,
}

impl Health {
    /// The server considers itself caught up and serving. Anything else --
    /// `initializing` at startup, `stale` when no batch has been applied for
    /// fifteen seconds -- is the server telling us not to send clients here.
    pub fn is_ready(&self) -> bool {
        self.status == "ready"
    }
}

/// The health endpoint that belongs to a source's websocket address.
///
/// `ws://host:port/ws` and `http://host:port/health` are the same axum router,
/// so only the scheme and the path change. `wss://` would need TLS, which would
/// mean a dependency for one small request; such a source simply goes
/// unpolled (and is therefore never marked behind) with one warning.
pub fn health_url(ws_url: &str) -> Option<String> {
    let rest = ws_url.strip_prefix("ws://")?;
    let authority = rest.split('/').next()?;
    if authority.is_empty() {
        return None;
    }
    Some(format!("http://{authority}/health"))
}

/// Ask one source where it stands. `None` if it could not be reached, did not
/// answer in time, or answered with something unreadable -- all of which mean
/// "no measurement", never "behind": silence is `silent_sources`' business and
/// mixing the two would let one failure mode masquerade as the other.
pub async fn poll(url: &str, timeout: Duration) -> Option<Health> {
    tokio::time::timeout(timeout, fetch(url)).await.ok().flatten()
}

async fn fetch(url: &str) -> Option<Health> {
    let rest = url.strip_prefix("http://")?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };

    let mut sock = TcpStream::connect(authority).await.ok()?;
    // `Connection: close` so the body ends at EOF and there is no chunked or
    // keep-alive framing to implement.
    let req = format!("GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nAccept: application/json\r\n\r\n");
    sock.write_all(req.as_bytes()).await.ok()?;

    // The reply is one short line of JSON; the cap is a guard against reading
    // an unbounded body from something that is not the server we expect.
    let mut buf = Vec::with_capacity(512);
    sock.take(64 * 1024).read_to_end(&mut buf).await.ok()?;
    parse_response(&buf)
}

/// Split an HTTP response and read the JSON body.
fn parse_response(bytes: &[u8]) -> Option<Health> {
    let text = std::str::from_utf8(bytes).ok()?;
    let (head, body) = text.split_once("\r\n\r\n").or_else(|| text.split_once("\n\n"))?;
    // Only 200 carries a body worth reading; anything else is a wrong port or a
    // server that has not come up.
    if !head.lines().next()?.contains(" 200") {
        return None;
    }
    serde_json::from_str(body.trim()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_health_url_is_the_websocket_one_with_another_scheme_and_path() {
        assert_eq!(health_url("ws://localhost:48001/ws").as_deref(), Some("http://localhost:48001/health"));
        assert_eq!(health_url("ws://10.0.0.5:8080/ws").as_deref(), Some("http://10.0.0.5:8080/health"));
        // No path at all is still an address.
        assert_eq!(health_url("ws://localhost:48001").as_deref(), Some("http://localhost:48001/health"));
        // TLS would need a dependency for one small request; such a source goes
        // unpolled rather than half-supported.
        assert_eq!(health_url("wss://example.com/ws"), None);
        assert_eq!(health_url("http://localhost:48001/ws"), None);
        assert_eq!(health_url("ws:///ws"), None);
    }

    #[test]
    fn a_health_reply_is_read_off_the_wire() {
        let body = r#"{"status":"ready","uptime_seconds":16618,"height":1164506857,"connections":1,"last_event_age_ms":1}"#;
        let raw = format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\r\n{body}");
        let h = parse_response(raw.as_bytes()).expect("a 200 with JSON");
        assert_eq!(h.height, 1_164_506_857);
        assert_eq!(h.last_event_age_ms, 1);
        assert!(h.is_ready());
    }

    #[test]
    fn a_server_that_is_not_ready_says_so() {
        for status in ["initializing", "stale"] {
            let raw = format!("HTTP/1.1 200 OK\r\n\r\n{{\"status\":\"{status}\",\"height\":7}}");
            let h = parse_response(raw.as_bytes()).expect("parses");
            assert!(!h.is_ready(), "{status} must not read as ready");
            assert_eq!(h.height, 7);
        }
    }

    #[test]
    fn anything_that_is_not_a_health_reply_is_no_measurement() {
        // Not JSON, no body, an error status, a truncated response: every one
        // of them must come back None rather than a zero height, which would
        // read as "infinitely far behind" and move every client off the source.
        assert_eq!(parse_response(b"HTTP/1.1 200 OK\r\n\r\nnot json"), None);
        assert_eq!(parse_response(b"HTTP/1.1 404 Not Found\r\n\r\n{\"status\":\"ready\",\"height\":7}"), None);
        assert_eq!(parse_response(b"HTTP/1.1 200 OK\r\n\r\n"), None);
        assert_eq!(parse_response(b"garbage"), None);
        assert_eq!(parse_response(b""), None);
        // A body missing the height is not a health reply either.
        assert_eq!(parse_response(b"HTTP/1.1 200 OK\r\n\r\n{\"status\":\"ready\"}"), None);
    }
}
