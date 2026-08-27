//! Downstream client websocket handling: accept subscriptions and stream out
//! arbitrated updates.

use std::net::SocketAddr;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;

use crate::state::{AppState, Client, SubKey, SubscribeOutcome};

/// A request frame from a client, mirroring the upstream's own `ClientMessage`.
#[derive(Deserialize)]
#[serde(tag = "method", rename_all = "camelCase")]
enum ClientMessage {
    Subscribe { subscription: SubKey },
    Unsubscribe { subscription: SubKey },
    Ping,
}

/// Drive a single client connection: spawn a writer draining its queue and read
/// subscription requests until the socket closes.
pub async fn handle_socket(socket: WebSocket, state: Arc<AppState>, addr: SocketAddr) {
    let (client, mut rx) = state.register_client(addr.ip().to_string());
    tracing::info!(client = client.id, ip = %client.ip, "client connected");

    let (mut sender, mut receiver) = socket.split();

    // Writer: drain the outbound queue to the socket, tallying bytes sent.
    let wclient = client.clone();
    let mut writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let n = msg.as_str().len() as u64;
            if sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
            wclient.bytes_sent.fetch_add(n, Relaxed);
        }
    });

    // Reader: handle incoming subscription requests.
    let rstate = state.clone();
    let rclient = client.clone();
    let mut reader = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Text(text) => handle_request(&rstate, &rclient, text.as_str()),
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = &mut writer => reader.abort(),
        _ = &mut reader => writer.abort(),
        // Raised when the client's stream has developed a hole it cannot
        // recover from; hanging up is kinder than serving a corrupt book.
        _ = client.kill.notified() => {
            tracing::warn!(client = client.id, "hanging up: stream integrity lost");
            writer.abort();
            reader.abort();
        }
    }

    state.cleanup_client(&client);
    tracing::info!(client = client.id, "client disconnected");
}

/// Parse and act on a client request frame.
fn handle_request(state: &Arc<AppState>, client: &Arc<Client>, text: &str) {
    let req: ClientMessage = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            // Silently dropping this is what made unsupported subscriptions
            // look like they vanished into thin air.
            tracing::warn!(client = client.id, error = %e, request = %truncate(text), "unparsable request");
            return;
        }
    };

    match req {
        ClientMessage::Ping => {
            let _ = client.tx.try_send(Utf8Bytes::from(r#"{"channel":"pong"}"#.to_string()));
        }
        ClientMessage::Subscribe { subscription } => {
            let key = subscription.normalized();
            if let Some(reason) = unsupported(&key) {
                tracing::warn!(client = client.id, sub = %key.label(), "{}", reason);
                return;
            }
            let outcome = state.subscribe(client, key.clone());
            send_ack(client, "subscribe", &key);

            // Joining an incremental stream mid-flight means the snapshot that
            // opened it is long gone. Go fetch one; until it lands the client
            // is parked and its frames are held back.
            if outcome == SubscribeOutcome::Joined && key.is_incremental() {
                tracing::info!(client = client.id, sub = %key.label(), "late joiner, fetching a snapshot");
                tokio::spawn(crate::upstream::fetch_snapshot(state.clone(), key, client.id));
            }
        }
        ClientMessage::Unsubscribe { subscription } => {
            let key = subscription.normalized();
            if state.unsubscribe(client, &key) {
                send_ack(client, "unsubscribe", &key);
            } else {
                tracing::warn!(client = client.id, sub = %key.label(), "unsubscribe for a key the client does not hold");
            }
        }
    }
}

/// Channels the proxy cannot stream correctly, refused loudly rather than
/// served wrong. `bookDiffs` frames carry no time, height or sequence of any
/// kind, so there is nothing to order two sources by; `l4Book` carries the same
/// diffs wrapped in a block height and is the arbitrable way to get them.
fn unsupported(key: &SubKey) -> Option<&'static str> {
    match key {
        SubKey::BookDiffs { .. } => {
            Some("bookDiffs carries no sequence to arbitrate on; subscribe to l4Book for the same diffs")
        }
        _ => None,
    }
}

/// Acknowledge in the same shape the upstream would.
fn send_ack(client: &Arc<Client>, method: &str, key: &SubKey) {
    let ack = serde_json::json!({
        "channel": "subscriptionResponse",
        "data": { "method": method, "subscription": key },
    });
    if let Ok(s) = serde_json::to_string(&ack) {
        let _ = client.tx.try_send(Utf8Bytes::from(s));
    }
}

fn truncate(s: &str) -> String {
    const MAX: usize = 200;
    if s.len() <= MAX {
        return s.to_string();
    }
    let mut end = MAX;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}
