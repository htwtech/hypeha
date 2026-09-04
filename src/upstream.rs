//! Upstream data-source connections: connect, (re)subscribe, ingest updates,
//! and reconnect with backoff.

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::Utf8Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as TMessage;

use crate::state::{AppState, Seq, Source, SubKey, SubRequest};

const RECONNECT_MIN: Duration = Duration::from_millis(250);
const RECONNECT_MAX: Duration = Duration::from_secs(10);

/// An `l4Book` snapshot runs to ~22 MB for BTC, well past tungstenite's default
/// 16 MiB frame cap, so both limits are raised or the subscription dies on its
/// very first frame.
const MAX_MESSAGE: usize = 256 * 1024 * 1024;

/// How far ahead of the clock a frame's block time may be before it is refused.
///
/// One frame stamped further ahead than this would pin the key's high-water mark
/// there and silence every real frame until wall time caught up, so the check is
/// not optional the way `--max-age` is.
const FUTURE_LIMIT: Duration = Duration::from_secs(5);

/// How long to wait for a late joiner's snapshot before giving up on it.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(120);

/// The block time a frame carries, in unix milliseconds.
///
/// The first `"time":` in a frame is the top-level one — `L4BookUpdates` is
/// `{time, height, order_statuses, …}` — while the `time` fields nested inside
/// `order_statuses` are date strings, so requiring digits settles it.
fn block_time_ms(text: &str) -> Option<u64> {
    const KEY: &str = "\"time\":";
    let i = text.find(KEY)?;
    let j = i + KEY.len();
    let end = text[j..].find(|c: char| !c.is_ascii_digit()).map_or(text.len(), |o| j + o);
    if end == j {
        return None;
    }
    text[j..end].parse().ok()
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

pub fn ws_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    // Assigned rather than built as a literal: the struct is `#[non_exhaustive]`.
    let mut cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
    cfg.max_message_size = Some(MAX_MESSAGE);
    cfg.max_frame_size = Some(MAX_MESSAGE);
    cfg
}

pub type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub async fn connect(url: &str) -> Option<Ws> {
    connect_with(url, None).await.ok()
}

/// Connect, optionally presenting an `x-token` header.
///
/// The sources are reached directly and need no token; the header is for
/// reaching wsarb itself through a gateway that authenticates, which is the
/// only way to measure the path a real client actually takes.
///
/// The failure comes back as text rather than being swallowed: a gateway
/// refuses a bad token with an HTTP status, and "could not connect" would
/// leave that indistinguishable from a dead port.
pub async fn connect_with(url: &str, token: Option<&str>) -> Result<Ws, String> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Error;

    let mut req = url.into_client_request().map_err(|e| e.to_string())?;
    if let Some(t) = token {
        let v: tokio_tungstenite::tungstenite::http::HeaderValue =
            t.parse().map_err(|_| "x-token is not a valid header value".to_string())?;
        req.headers_mut().insert("x-token", v);
    }
    match tokio_tungstenite::connect_async_with_config(req, Some(ws_config()), false).await {
        Ok((ws, _)) => Ok(ws),
        Err(Error::Http(resp)) => {
            let body = resp.body().as_ref()
                .map(|b| String::from_utf8_lossy(b).trim().to_string())
                .unwrap_or_default();
            Err(format!("HTTP {} {}", resp.status().as_u16(), body))
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Run the lifecycle loop for a single upstream source until the process exits.
pub async fn run(state: Arc<AppState>, src: Arc<Source>, mut ctrl_rx: mpsc::UnboundedReceiver<String>) {
    let mut backoff = RECONNECT_MIN;
    loop {
        match tokio_tungstenite::connect_async_with_config(src.url.as_str(), Some(ws_config()), false).await {
            Ok((ws, _resp)) => {
                backoff = RECONNECT_MIN;
                src.stats.connected.store(true, Relaxed);
                tracing::info!(source = src.id, url = %src.url, "connected");

                let (mut write, mut read) = ws.split();

                // Drop anything queued while this source was down. The full
                // resubscribe below already covers it, and sending both draws an
                // "Already subscribed" error back from the upstream for every
                // subscription that was made during the outage.
                while ctrl_rx.try_recv().is_ok() {}

                // Re-subscribe to everything we currently care about.
                for req in state.subscribed_requests() {
                    let _ = write.send(TMessage::Text(req.into())).await;
                }

                loop {
                    tokio::select! {
                        incoming = read.next() => match incoming {
                            Some(Ok(TMessage::Text(text))) => {
                                src.stats.packets.fetch_add(1, Relaxed);
                                handle_text(&state, &src, text.as_str());
                            }
                            Some(Ok(TMessage::Binary(_))) => {
                                src.stats.packets.fetch_add(1, Relaxed);
                            }
                            Some(Ok(TMessage::Ping(p))) => {
                                let _ = write.send(TMessage::Pong(p)).await;
                            }
                            Some(Ok(TMessage::Close(_))) | None => break,
                            Some(Ok(_)) => {}
                            Some(Err(e)) => {
                                tracing::warn!(source = src.id, error = %e, "read error");
                                break;
                            }
                        },
                        Some(req) = ctrl_rx.recv() => {
                            let _ = write.send(TMessage::Text(req.into())).await;
                        }
                        () = src.reconnect.notified() => {
                            tracing::warn!(
                                source = src.id,
                                "silent too long on a healthy socket; reconnecting to re-subscribe"
                            );
                            break;
                        }
                    }
                }

                src.stats.connected.store(false, Relaxed);
                src.stats.disconnects.fetch_add(1, Relaxed);
                tracing::warn!(source = src.id, "disconnected");

                // Anyone this source was mid-block for now has an incomplete
                // book and must be rebuilt from a fresh snapshot.
                for (key, client_id) in state.resync_after_source_loss(src.id) {
                    tokio::spawn(fetch_snapshot(state.clone(), key, client_id));
                }
            }
            Err(e) => {
                tracing::warn!(source = src.id, url = %src.url, error = %e, "connect failed");
            }
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RECONNECT_MAX);
    }
}

/// Fetch a private snapshot for a client that joined an already-running
/// incremental stream, and hand it over.
///
/// A repeat `subscribe` on the shared connection is a no-op upstream (the
/// server dedupes per connection and only snapshots on first insert), so this
/// opens a throwaway connection of its own.
pub async fn fetch_snapshot(state: Arc<AppState>, key: SubKey, client_id: u64) {
    // Freshest source first. A source whose node has died still answers, and
    // answers with a book frozen at its last block - syntactically perfect and
    // silently wrong to build on. Only fall back to a quiet source when none of
    // them is delivering, which means the market is quiet and every book is
    // equally current.
    // The source id travels with the url: whoever answers becomes the leader,
    // so the book's foundation and the increments laid on it come from one node.
    let mut ranked: Vec<(Duration, usize, String)> = state
        .sources
        .iter()
        .filter(|s| s.stats.connected.load(Relaxed))
        .map(|s| (s.stats.idle_for().unwrap_or(Duration::MAX), s.id, s.url.clone()))
        .collect();
    ranked.sort_by_key(|(idle, _, _)| *idle);

    let fresh: Vec<(usize, String)> = ranked
        .iter()
        .filter(|(idle, _, _)| *idle <= crate::stats::SILENCE_LIMIT)
        .map(|(_, id, url)| (*id, url.clone()))
        .collect();
    let urls: Vec<(usize, String)> = if fresh.is_empty() {
        ranked.into_iter().map(|(_, id, url)| (id, url)).collect()
    } else {
        fresh
    };

    for (id, url) in urls {
        match tokio::time::timeout(SNAPSHOT_TIMEOUT, snapshot_from(&url, &key)).await {
            Ok(Some((height, payload))) => {
                state.deliver_snapshot(&key, client_id, id, height, payload);
                return;
            }
            Ok(None) => tracing::warn!(url = %url, sub = %key.label(), "snapshot fetch failed"),
            Err(_) => tracing::warn!(url = %url, sub = %key.label(), "snapshot fetch timed out"),
        }
    }

    state.fail_pending(&key, client_id);
}

/// Open a throwaway connection, subscribe, and return the first snapshot frame
/// together with the height it was taken at.
async fn snapshot_from(url: &str, key: &SubKey) -> Option<(u64, Utf8Bytes)> {
    let ws = connect(url).await?;
    let (mut write, mut read) = ws.split();
    write
        .send(TMessage::Text(SubRequest::Subscribe { subscription: key }.json().into()))
        .await
        .ok()?;

    while let Some(Ok(msg)) = read.next().await {
        let TMessage::Text(text) = msg else { continue };
        let Ok(frame) = serde_json::from_str::<Frame>(text.as_str()) else { continue };
        // Asked of `route` rather than matched channel by channel. This used to
        // look for an l4Book snapshot by name, so when l2Diff arrived it read
        // frames until the timeout and then disconnected the very client it was
        // sent to rebuild -- a channel-specific test inside a path every
        // incremental channel depends on.
        if let Some((found, Seq::Snapshot(height))) = route(frame) {
            if found == *key {
                return Some((height, Utf8Bytes::from(text.as_str().to_string())));
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Frame parsing
//
// Mirrors the upstream's `ServerResponse` enum, but declares only the fields
// needed to route a frame to its subscription and order it against the frames
// already seen. Everything else (the levels, the order bodies) is skipped by
// serde without being materialised.
//
// `bookDiffs` is absent on purpose: those subscriptions are refused downstream,
// so the subscription is never sent upstream and the frames never arrive.
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(tag = "channel", content = "data", rename_all = "camelCase")]
pub enum Frame {
    Bbo(CoinTime),
    L2Book(L2Head),
    Trades(Vec<TradeHead>),
    OrderUpdates(Vec<OrderUpdateHead>),
    L4Book(L4Frame),
    L2Diff(L2DiffFrame),
    Error(String),
}

#[derive(Deserialize)]
pub struct CoinTime {
    pub coin: String,
    pub time: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L2Head {
    coin: String,
    time: u64,
    #[serde(default)]
    n_sig_figs: Option<u32>,
    #[serde(default)]
    n_levels: Option<usize>,
    #[serde(default)]
    mantissa: Option<u64>,
}

#[derive(Deserialize)]
pub struct TradeHead {
    coin: String,
    tid: u64,
}

#[derive(Deserialize)]
pub struct OrderUpdateHead {
    user: String,
    height: u64,
}

#[derive(Deserialize)]
pub struct CoinOnly {
    coin: String,
}

/// `l2Diff` is shaped like `l4Book`: an externally tagged enum in `data`, so
/// the opening snapshot and the increments arrive under different keys. Both
/// carry the same head, and everything past it -- the levels, the changed
/// prices -- is skipped by serde without being materialised.
#[derive(Deserialize)]
pub enum L2DiffFrame {
    Snapshot(L2DiffHead),
    Updates(L2DiffHead),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L2DiffHead {
    coin: String,
    height: u64,
    #[serde(default)]
    n_sig_figs: Option<u32>,
    #[serde(default)]
    n_levels: Option<usize>,
    #[serde(default)]
    mantissa: Option<u64>,
}

impl L2DiffHead {
    fn key(self) -> SubKey {
        SubKey::L2Diff {
            coin: self.coin,
            n_sig_figs: self.n_sig_figs,
            n_levels: self.n_levels,
            mantissa: self.mantissa,
        }
    }
}

/// `l4Book` carries an externally tagged enum in `data`, so the snapshot and
/// the incremental updates arrive under different keys.
#[derive(Deserialize)]
pub enum L4Frame {
    Snapshot { coin: String, height: u64 },
    Updates(L4Updates),
}

#[derive(Deserialize)]
pub struct L4Updates {
    height: u64,
    #[serde(default)]
    order_statuses: Vec<StatusHead>,
    #[serde(default)]
    book_diffs: Vec<CoinOnly>,
}

#[derive(Deserialize)]
pub struct StatusHead {
    order: CoinOnly,
}

/// Parse an upstream text frame and, if it routes to a subscription, apply it.
fn handle_text(state: &AppState, src: &Source, text: &str) {
    // A parse failure here is the normal path for frames we do not route -
    // `subscriptionResponse` and `pong` - so it stays quiet.
    let Ok(frame) = serde_json::from_str::<Frame>(text) else { return };

    if let Frame::Error(msg) = &frame {
        tracing::warn!(source = src.id, error = %msg, "upstream rejected a request");
        return;
    }

    if let Some((key, seq)) = route(frame) {
        // Absolute freshness, which the arbitration cannot supply on its own.
        //
        // Ordering is judged relative to what has already been forwarded, but
        // when the last subscriber to a key leaves, the entry goes with it — and
        // a new subscriber starts from zero, where the first frame to arrive
        // wins however old it is. A node frozen half an hour ago answers a
        // subscribe instantly with its stale book, so it can easily be that
        // first frame.
        if let Some(t) = block_time_ms(text) {
            let now = unix_ms();
            // Ahead of the clock, not behind it. Such a frame would set the key's
            // high-water mark into the future, and then every real frame reads as
            // stale — silently, for as long as the stamp is ahead. Checked before
            // the age gate and without `--max-age`, because the damage does not
            // depend on that setting and disabling it must not open this.
            //
            // Deliberately generous: a small skew between this machine and the
            // chain is normal, and the measured ages run a couple of hundred
            // milliseconds *behind*, so anything seconds ahead is an anomaly.
            if t > now && t - now > FUTURE_LIMIT.as_millis() as u64 {
                src.stats.from_future.fetch_add(1, Relaxed);
                tracing::warn!(
                    source = src.id,
                    sub = %key.label(),
                    ahead_ms = t - now,
                    "frame stamped in the future; not forwarding"
                );
                return;
            }
        }

        if let Some(max) = state.max_age {
            if let Some(t) = block_time_ms(text) {
                let now = unix_ms();
                if now > t && now - t > max.as_millis() as u64 {
                    src.stats.too_old.fetch_add(1, Relaxed);
                    tracing::warn!(
                        source = src.id,
                        sub = %key.label(),
                        age_ms = now - t,
                        "frame older than the freshness limit; not forwarding"
                    );
                    return;
                }
            }
        }
        state.on_update(src, key, seq, Utf8Bytes::from(text));
    }
}

/// Derive the subscription a frame belongs to, and the value it is ordered by.
///
/// The key must come out identical to the one built from the client's original
/// request, or the frame lands in a subscription nobody is listening to.
pub fn route(frame: Frame) -> Option<(SubKey, Seq)> {
    // Both ends must spell the key the same way, or the frame lands in an entry
    // no client is subscribed to and vanishes without a trace. Applied here once
    // rather than per arm so a new channel cannot forget it.
    let (key, seq) = route_raw(frame)?;
    Some((key.normalized(), seq))
}

fn route_raw(frame: Frame) -> Option<(SubKey, Seq)> {
    match frame {
        // `time` here is the *block's* time, not the update's: every top-of-book
        // change within a block carries the same value. Ordering by it as a
        // point would keep only the first of them and discard the rest — some
        // nine updates in ten.
        //
        // Led rather than raced, and that distinction was measured: the upstream
        // suppresses a bbo frame whose values repeat the last one it sent *on
        // that connection*, so how many frames a block contains stops being a
        // property of the data. Two nodes disagreed on the count for 7% of
        // blocks against a 0.6% baseline within one node, and position #k is
        // then not the same event on both. Channels the upstream dedups by
        // value cannot be raced; `l4Book` and `orderUpdates`, which emit every
        // batch that changed anything, can.
        Frame::Bbo(d) => Some((SubKey::Bbo { coin: d.coin }, Seq::Lead(d.time))),

        Frame::L2Book(d) => Some((
            SubKey::L2Book {
                coin: d.coin,
                n_sig_figs: d.n_sig_figs,
                n_levels: d.n_levels,
                mantissa: d.mantissa,
            },
            // Sticky, not Lead: Lead re-elects on every stamp, and with the
            // nodes 15% of levels apart even at depth 20, that swapped the
            // client's book between two of them ten-odd times a second. Every
            // frame is a whole book, so nothing breaks either way -- but a book
            // that stays with one node beats one a few milliseconds fresher.
            Seq::Sticky(d.time),
        )),

        // Batched channels: the coin is per element, and the batch is ordered by
        // the newest id it carries. Two sources may cut batches differently, so
        // ordering on the maximum keeps a short batch from looking newer.
        Frame::Trades(v) => {
            let coin = v.first()?.coin.clone();
            let tid = v.iter().map(|t| t.tid).max()?;
            Some((SubKey::Trades { coin }, Seq::Point(tid)))
        }

        Frame::OrderUpdates(v) => {
            let user = v.first()?.user.to_ascii_lowercase();
            let height = v.iter().map(|u| u.height).max()?;
            // Fed by the same L4Statuses broadcast as `l4Book`, so a block
            // arrives as many batches all stamped with its one height. Newest
            // wins would keep the first batch and drop the rest.
            Some((SubKey::OrderUpdates { user }, Seq::Block(height)))
        }

        Frame::L4Book(L4Frame::Snapshot { coin, height }) => {
            Some((SubKey::L4Book { coin }, Seq::Snapshot(height)))
        }

        // The one channel whose frames are not self-contained: an increment
        // means something only against the book it was computed from. Two nodes
        // measurably do not hold the same book, so the stream is carried by one
        // source from end to end rather than raced -- see `Seq::Sticky`.
        Frame::L2Diff(L2DiffFrame::Snapshot(d)) => {
            let height = d.height;
            Some((d.key(), Seq::Snapshot(height)))
        }
        Frame::L2Diff(L2DiffFrame::Updates(d)) => {
            let height = d.height;
            Some((d.key(), Seq::Sticky(height)))
        }

        // `Updates` carries no coin of its own. The upstream fills exactly one
        // of the two arrays and only enters that branch for a non-empty
        // per-coin group, so one of them always yields the coin.
        Frame::L4Book(L4Frame::Updates(u)) => {
            let coin = u
                .order_statuses
                .first()
                .map(|s| s.order.coin.clone())
                .or_else(|| u.book_diffs.first().map(|d| d.coin.clone()))?;
            Some((SubKey::L4Book { coin }, Seq::Block(u.height)))
        }

        Frame::Error(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_of(text: &str) -> Option<(SubKey, u64, &'static str)> {
        let frame = serde_json::from_str::<Frame>(text).ok()?;
        let (key, seq) = route(frame)?;
        Some(match seq {
            Seq::Point(v) => (key, v, "point"),
            Seq::Block(v) => (key, v, "block"),
            Seq::Snapshot(v) => (key, v, "snapshot"),
            Seq::Lead(v) => (key, v, "lead"),
            Seq::Sticky(v) => (key, v, "sticky"),
        })
    }

    #[test]
    fn l2diff_frames_route_by_key_and_kind() {
        // `nLevels` 20 is the upstream's own default, stamped into every frame
        // it sends back, so it has to fold away exactly as it does for l2Book --
        // otherwise the frame's key misses the subscription's and the client is
        // acknowledged and then hears nothing.
        let snap = r#"{"channel":"l2Diff","data":{"Snapshot":{"coin":"BTC","time":1,"height":7,"nLevels":20,"levels":[[],[]]}}}"#;
        let (key, h, kind) = key_of(snap).unwrap();
        assert_eq!(key, SubKey::L2Diff { coin: "BTC".into(), n_sig_figs: None, n_levels: None, mantissa: None });
        assert_eq!((h, kind), (7, "snapshot"));

        let upd = r#"{"channel":"l2Diff","data":{"Updates":{"coin":"BTC","time":2,"height":8,"prevHeight":7,"nLevels":1000,"bids":{"upd":[],"del":[]},"asks":{"upd":[],"del":[]}}}}"#;
        let (key, h, kind) = key_of(upd).unwrap();
        assert_eq!(key, SubKey::L2Diff { coin: "BTC".into(), n_sig_figs: None, n_levels: Some(1000), mantissa: None });
        assert_eq!((h, kind), (8, "sticky"));
    }

    #[test]
    fn a_stamp_in_the_future_is_refused() {
        // The guard is unconditional, so this only checks the arithmetic: the
        // gate itself lives in `handle_text` and is exercised live.
        let now = unix_ms();
        let ahead = format!(r#"{{"channel":"bbo","data":{{"coin":"BTC","time":{},"bid":null,"ask":null}}}}"#, now + 60_000);
        let t = block_time_ms(&ahead).expect("a numeric time");
        assert!(t > now && t - now > FUTURE_LIMIT.as_millis() as u64);

        // A frame a couple of hundred milliseconds behind — the normal case —
        // must not trip it.
        let normal = format!(r#"{{"channel":"bbo","data":{{"coin":"BTC","time":{},"bid":null,"ask":null}}}}"#, now - 200);
        let t = block_time_ms(&normal).expect("a numeric time");
        assert!(t <= now);
    }

    #[test]
    fn bbo_routes_by_coin_and_time() {
        let (key, seq, kind) =
            key_of(r#"{"channel":"bbo","data":{"coin":"BTC","time":7,"bid":null,"ask":null}}"#).unwrap();
        assert_eq!(key, SubKey::Bbo { coin: "BTC".into() });
        assert_eq!(seq, 7);
        // Led, not raced: see `route` for why value-deduped channels cannot
        // have their positions matched up across sources.
        assert_eq!(kind, "lead");
    }

    #[test]
    fn l2book_frame_rebuilds_the_exact_subscription_key() {
        // Captured from a live node, not imagined: subscribing without
        // `nLevels` still gets `"nLevels":20` back, because the upstream stamps
        // its default into every frame. The earlier version of this test used a
        // frame with the field absent — a shape the server never emits — so it
        // passed while every l2Book subscription silently received nothing.
        let plain = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"nLevels":20,"levels":[[],[]]}}"#;
        let (key, _, _) = key_of(plain).unwrap();
        assert_eq!(key, SubKey::L2Book { coin: "BTC".into(), n_sig_figs: None, n_levels: None, mantissa: None });

        let banded = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"nSigFigs":3,"nLevels":50,"mantissa":5,"levels":[[],[]]}}"#;
        let (key, _, kind) = key_of(banded).unwrap();
        assert_eq!(
            key,
            SubKey::L2Book { coin: "BTC".into(), n_sig_figs: Some(3), n_levels: Some(50), mantissa: Some(5) }
        );
        assert_eq!(kind, "sticky");
    }

    #[test]
    fn trades_order_by_the_newest_id_in_the_batch() {
        let text = r#"{"channel":"trades","data":[
            {"coin":"BTC","side":"A","px":"1","sz":"1","time":10,"hash":"0x","tid":100,"users":["0x1","0x2"]},
            {"coin":"BTC","side":"A","px":"1","sz":"1","time":11,"hash":"0x","tid":300,"users":["0x1","0x2"]},
            {"coin":"BTC","side":"A","px":"1","sz":"1","time":12,"hash":"0x","tid":200,"users":["0x1","0x2"]}
        ]}"#;
        let (key, seq, _) = key_of(text).unwrap();
        assert_eq!(key, SubKey::Trades { coin: "BTC".into() });
        assert_eq!(seq, 300);
    }

    #[test]
    fn order_updates_route_by_lowercased_user() {
        let text = r#"{"channel":"orderUpdates","data":[{"user":"0xABCD","time":1,"height":42,"order_status":{}}]}"#;
        let (key, seq, kind) = key_of(text).unwrap();
        assert_eq!(key, SubKey::OrderUpdates { user: "0xabcd".into() });
        assert_eq!(seq, 42);
        // `height` is the block's and every batch in it repeats it.
        assert_eq!(kind, "block");
    }

    #[test]
    fn l4book_snapshot_routes_by_its_own_coin() {
        let text = r#"{"channel":"l4Book","data":{"Snapshot":{"coin":"BTC","time":1,"height":900,"levels":[[],[]]}}}"#;
        let (key, seq, kind) = key_of(text).unwrap();
        assert_eq!(key, SubKey::L4Book { coin: "BTC".into() });
        assert_eq!(seq, 900);
        // A snapshot is a reset, not an increment - the distinction is what lets
        // a written off source back into the race.
        assert_eq!(kind, "snapshot");
    }

    #[test]
    fn l4book_updates_recover_the_coin_from_whichever_array_is_filled() {
        // Statuses branch: book_diffs is empty.
        let by_status = r#"{"channel":"l4Book","data":{"Updates":{"time":1,"height":901,
            "order_statuses":[{"time":"t","user":"0x1","status":"open","order":{"coin":"ETH","side":"B"}}],
            "book_diffs":[]}}}"#;
        let (key, seq, kind) = key_of(by_status).unwrap();
        assert_eq!(key, SubKey::L4Book { coin: "ETH".into() });
        assert_eq!(seq, 901);
        assert_eq!(kind, "block");

        // Diffs branch: order_statuses is empty.
        let by_diff = r#"{"channel":"l4Book","data":{"Updates":{"time":1,"height":902,
            "order_statuses":[],
            "book_diffs":[{"user":"0x1","oid":5,"px":"1","coin":"SOL","raw_book_diff":{}}]}}}"#;
        let (key, seq, _) = key_of(by_diff).unwrap();
        assert_eq!(key, SubKey::L4Book { coin: "SOL".into() });
        assert_eq!(seq, 902);
    }

    #[test]
    fn frames_we_do_not_route_are_ignored_quietly() {
        assert!(key_of(r#"{"channel":"pong"}"#).is_none());
        assert!(key_of(
            r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}}"#
        )
        .is_none());
    }
}
