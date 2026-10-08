//! The network's own height, from outside our nodes.
//!
//! Every other measure of a source's freshness compares it with the other
//! sources (`lagging_sources`) or takes its word for itself (`/health`). Neither
//! can see the case where all of them are behind together: they agree with one
//! another, and each reports itself fine. This is a height that none of our
//! nodes had a hand in, so a source can be measured against the chain itself --
//! block against block, with no clock involved.
//!
//! The source is the public explorer's `explorerBlock` subscription on
//! `wss://rpc.hyperliquid.xyz/ws`: a batch of recent blocks on subscribing,
//! then one message per block, each carrying `height`. Measured at ~14 blocks a
//! second, the same pace as our nodes, and at heights of the same chain. The
//! same subscription on `api.hyperliquid.xyz` is accepted but answered with an
//! empty array.
//!
//! It is not in the public API's documentation -- it is what the explorer
//! itself uses -- so it may change or go away without notice. Hence it is only
//! shown, never acted on, and an outage here costs nothing but the column.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message as TMessage;

use crate::state::AppState;
use crate::upstream::{connect, unix_ms};

pub const DEFAULT_URL: &str = "wss://rpc.hyperliquid.xyz/ws";

const SUBSCRIBE: &str = r#"{"method":"subscribe","subscription":{"type":"explorerBlock"}}"#;
const PING: &str = r#"{"method":"ping"}"#;
/// The public endpoints drop a connection that has gone a minute without
/// hearing from the client.
const PING_EVERY: Duration = Duration::from_secs(20);
/// Blocks come many times a second; this long with none is a dead connection.
const READ_TIMEOUT: Duration = Duration::from_secs(15);
const RECONNECT_EVERY: Duration = Duration::from_secs(5);

/// Beyond this the reference's own height is too old to measure anything by,
/// and the dashboard says so rather than showing every source as ahead.
pub const STALE_AFTER: Duration = Duration::from_secs(10);

/// The latest network height seen, shared with the dashboard.
#[derive(Default)]
pub struct Reference {
    url: OnceLock<String>,
    /// Newest block height seen; zero means never.
    height: AtomicU64,
    /// Wall clock of the last block received, unix ms -- only to judge whether
    /// the reference itself is still alive, never to judge a source by.
    updated_ms: AtomicU64,
    connected: AtomicBool,
}

impl Reference {
    pub fn url(&self) -> Option<&str> {
        self.url.get().map(String::as_str)
    }

    pub fn height(&self) -> Option<u64> {
        match self.height.load(Relaxed) {
            0 => None,
            h => Some(h),
        }
    }

    /// How long ago the last block arrived, or `None` if none ever has.
    pub fn age(&self, now_ms: u64) -> Option<Duration> {
        match self.updated_ms.load(Relaxed) {
            0 => None,
            t => Some(Duration::from_millis(now_ms.saturating_sub(t))),
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Relaxed)
    }

    /// The height to measure by right now: `None` when there is none, or when
    /// the last one is too old to mean anything.
    pub fn current(&self, now_ms: u64) -> Option<u64> {
        let fresh = self.age(now_ms).is_some_and(|a| a <= STALE_AFTER);
        if fresh { self.height() } else { None }
    }

    fn record(&self, height: u64, now_ms: u64) {
        self.height.fetch_max(height, Relaxed);
        self.updated_ms.store(now_ms, Relaxed);
    }
}

#[derive(Deserialize)]
struct Block {
    height: u64,
}

/// The newest height in one message. Anything that is not a list of blocks --
/// the subscription acknowledgement, a pong -- yields nothing.
fn newest_height(text: &str) -> Option<u64> {
    serde_json::from_str::<Vec<Block>>(text).ok()?.iter().map(|b| b.height).max()
}

/// Hold the subscription open for the life of the process, reconnecting as
/// needed.
pub async fn run(state: Arc<AppState>, url: String) {
    let r = &state.reference;
    let _ = r.url.set(url.clone());
    loop {
        match connect(&url).await {
            None => tracing::warn!(%url, "network reference: could not connect"),
            Some(ws) => {
                let (mut write, mut read) = ws.split();
                if write.send(TMessage::Text(SUBSCRIBE.into())).await.is_ok() {
                    r.connected.store(true, Relaxed);
                    tracing::info!(%url, "network reference connected");
                    let mut ping = tokio::time::interval(PING_EVERY);
                    ping.tick().await;
                    loop {
                        tokio::select! {
                            msg = tokio::time::timeout(READ_TIMEOUT, read.next()) => match msg {
                                Ok(Some(Ok(TMessage::Text(t)))) => {
                                    if let Some(h) = newest_height(t.as_str()) {
                                        r.record(h, unix_ms());
                                    }
                                }
                                Ok(Some(Ok(_))) => {}
                                Ok(Some(Err(e))) => {
                                    tracing::warn!(%url, error = %e, "network reference: read failed");
                                    break;
                                }
                                Ok(None) => {
                                    tracing::warn!(%url, "network reference: connection closed");
                                    break;
                                }
                                Err(_) => {
                                    tracing::warn!(%url, secs = READ_TIMEOUT.as_secs(), "network reference: no blocks, reconnecting");
                                    break;
                                }
                            },
                            _ = ping.tick() => {
                                if write.send(TMessage::Text(PING.into())).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    r.connected.store(false, Relaxed);
                }
            }
        }
        tokio::time::sleep(RECONNECT_EVERY).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_newest_height_is_read_off_a_batch() {
        // As the explorer sends it on subscribing: newest first, several at once.
        let batch = r#"[{"height":1176628806,"blockTime":1791489330892,"hash":"0xe7","proposer":"0x4a","numTxs":412},
                        {"height":1176628805,"blockTime":1791489330838,"hash":"0x4e","proposer":"0x57","numTxs":3507}]"#;
        assert_eq!(newest_height(batch), Some(1_176_628_806));
        let one = r#"[{"height":1176628807,"blockTime":1791489330965,"hash":"0xe8","proposer":"0x8a","numTxs":5704}]"#;
        assert_eq!(newest_height(one), Some(1_176_628_807));
    }

    #[test]
    fn anything_but_blocks_is_no_height() {
        assert_eq!(newest_height(r#"{"channel":"subscriptionResponse","data":{}}"#), None);
        assert_eq!(newest_height(r#"{"channel":"pong"}"#), None);
        // What the same subscription gets on api.hyperliquid.xyz.
        assert_eq!(newest_height("[]"), None);
        assert_eq!(newest_height("not json"), None);
    }

    #[test]
    fn a_reference_gone_quiet_measures_nothing() {
        let r = Reference::default();
        assert_eq!(r.current(1_000_000), None, "never heard from");
        r.record(500, 1_000_000);
        assert_eq!(r.current(1_000_000 + 1_000), Some(500));
        // Too old to measure a source by: better no column than every source
        // looking ahead of a frozen reference.
        let late = 1_000_000 + STALE_AFTER.as_millis() as u64 + 1;
        assert_eq!(r.current(late), None);
    }
}
