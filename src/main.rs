//! WSARB — websocket arbitration proxy for `order_book_server` feeds.

use wsarb::{client, state, stats, upstream};

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::connect_info::ConnectInfo;
use axum::extract::{State, WebSocketUpgrade};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{any, get};
use axum::Router;
use anyhow::Context;
use clap::Parser;
use std::net::SocketAddr;
use tokio::sync::mpsc;

use state::{AppState, Source};
use stats::SourceStats;

#[derive(Parser)]
#[command(name = "wsarb", about = "Websocket arbitration proxy for Hyperliquid sources")]
struct Args {
    #[arg(short = 's', long = "source", required = true, num_args = 1..)]
    sources: Vec<String>,
    #[arg(short, long, default_value = "0.0.0.0:8080")]
    listen: String,
    #[arg(long = "dashboard-listen", default_value = "0.0.0.0:48090")]
    dashboard_listen: String,
    /// Coin whose `bbo` is held subscribed permanently, so the per-source
    /// arbitration counters keep moving even with no clients connected.
    #[arg(long = "probe-coin", default_value = "BTC")]
    probe_coin: String,
    /// Drop the permanent probe. The dashboard then shows nothing about the
    /// sources while no client is subscribed.
    #[arg(long = "no-probe")]
    no_probe: bool,
    /// Refuse to forward frames whose block time is older than this, in seconds.
    ///
    /// Catches what the arbitration cannot: on a key with no history the first
    /// frame to arrive wins whatever its age, and a node frozen long ago answers
    /// fastest of all. Depends on wsarb and the nodes sharing a clock — set 0 to
    /// disable if they ever do not.
    #[arg(long = "max-age", default_value_t = 60)]
    max_age: u64,
    /// Move clients off a source whose data has fallen this many seconds behind
    /// the freshest source's, in seconds. 0 disables it.
    ///
    /// The other half of the silence watchdog. A node that dies goes quiet and
    /// is caught by silence; a node that restarts and replays blocks stays loud
    /// while serving a book minutes old, and nothing arrival-based can see it.
    /// Judged against the other sources rather than the clock, so a quiet market
    /// — which ages every source at once — is not mistaken for a fault.
    ///
    /// The default is five times the measured p99 age of a healthy node
    /// (~650ms) and well inside `--max-age`.
    #[arg(long = "lag-limit", default_value_t = 3)]
    lag_limit: u64,
}

/// Windows of silence before the connection is bounced once, and how often to
/// retry after that. At a 5s window: first try at 30s, then every 5 minutes —
/// often enough to recover a lost subscription quickly, rare enough that a
/// genuinely dead node does not churn the connection or the disconnect count.
const SILENT_RECONNECT_FIRST: u64 = 6;
const SILENT_RECONNECT_EVERY: u64 = 60;

/// How long the lag watchdog stands down after moving clients.
///
/// Long enough for the rebuild it just ordered to finish and for the readers to
/// settle, so the next verdict is made on a quiet system rather than on the
/// wreckage of the last one.
const LAG_COOLDOWN: Duration = Duration::from_secs(30);

/// Strip a source of the streams it is leading and rebuild its clients from a
/// snapshot taken elsewhere. Shared by the two watchdogs: a source can fail by
/// going quiet or by falling behind, and the cure is the same either way.
fn take_clients_off(state: &Arc<AppState>, id: usize, why: &'static str) -> usize {
    let work = state.resync_after_source_loss(id);
    if work.is_empty() {
        return 0;
    }
    // One fetch per subscription, not per client: everyone parked here wants
    // the same book from the same moment. Ungrouped this opened a connection
    // per client -- 145 at once on a real resync.
    let clients = work.len();
    let keys: HashSet<state::SubKey> = work.into_iter().map(|(key, _)| key).collect();
    tracing::warn!(source = id, clients, subs = keys.len(), "{}", why);
    for key in keys {
        tokio::spawn(upstream::fetch_snapshot(state.clone(), key));
    }
    clients
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();

    let mut sources = Vec::with_capacity(args.sources.len());
    let mut receivers = Vec::with_capacity(args.sources.len());
    for (id, url) in args.sources.iter().enumerate() {
        let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel();
        sources.push(Arc::new(Source {
            id,
            url: url.clone(),
            stats: SourceStats::default(),
            ctrl_tx,
            reconnect: tokio::sync::Notify::new(),
        }));
        receivers.push(ctrl_rx);
    }

    let max_age = (args.max_age > 0).then(|| Duration::from_secs(args.max_age));
    if let Some(d) = max_age {
        tracing::info!(seconds = d.as_secs(), "refusing frames older than this");
    }
    let lag_limit = (args.lag_limit > 0).then(|| Duration::from_secs(args.lag_limit));
    if let Some(d) = lag_limit {
        tracing::info!(seconds = d.as_secs(), "moving clients off a source behind the others by this");
    }
    let state = Arc::new(AppState::new(sources, max_age));

    // Pinned before the sources start, so each one picks the probe up in the
    // resubscribe it sends on connecting rather than as a second request.
    if !args.no_probe {
        state.pin(state::SubKey::Bbo { coin: args.probe_coin.clone() });
    }

    for (src, ctrl_rx) in state.sources.iter().cloned().zip(receivers) {
        let state = state.clone();
        tokio::spawn(upstream::run(state, src, ctrl_rx));
    }

    // Background task: refresh the "last window" deltas, and notice sources
    // that have gone quiet without dropping their socket.
    {
        let state = state.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            loop {
                ticker.tick().await;
                for src in &state.sources {
                    src.stats.roll_window();
                }

                // A source whose node died keeps the websocket open and simply
                // stops speaking, so `connected` still reads true. Left alone,
                // one that was leading an open block would strand its clients
                // holding half a block, forever and silently.
                for id in state.silent_sources(stats::SILENCE_LIMIT) {
                    take_clients_off(
                        &state,
                        id,
                        "connected but silent while others deliver; rebuilding its clients",
                    );

                    // The silence may be a dead node, or a subscription lost
                    // server-side on a socket that stayed up. Only the second is
                    // recoverable and nothing here can tell them apart, so bounce
                    // the connection: reconnecting re-subscribes everything, and
                    // against a genuinely dead node it simply achieves nothing.
                    if let Some(src) = state.sources.iter().find(|s| s.id == id) {
                        let w = src.stats.silent_windows();
                        let due = w == SILENT_RECONNECT_FIRST
                            || (w > SILENT_RECONNECT_FIRST
                                && (w - SILENT_RECONNECT_FIRST) % SILENT_RECONNECT_EVERY == 0);
                        if due {
                            src.reconnect.notify_one();
                        }
                    }
                }
            }
        });
    }

    // Background task: notice a source that keeps talking while its data falls
    // behind the others'.
    //
    // Its own ticker, at one second rather than the five the window above runs
    // at: that one is paced by the silence resolution, and a book minutes stale
    // should not wait on it. The check itself is two atomic loads per source.
    if let Some(limit) = lag_limit {
        let state = state.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            let mut last_move: Option<tokio::time::Instant> = None;
            loop {
                ticker.tick().await;
                // Measured every tick regardless: the run of ticks behind a
                // source is what the verdict is made of, and skipping a tick
                // would lose it.
                let behind = state.lagging_sources(limit);

                // Moving a crowd of clients is itself load, and load is what
                // makes a healthy reader look behind. So after a move, stop
                // looking for a while: without this the first (marginal) verdict
                // manufactured the evidence for the next one, and the two
                // sources took turns being wrong about each other.
                if last_move.is_some_and(|t| t.elapsed() < LAG_COOLDOWN) {
                    continue;
                }
                let mut moved = 0;
                for id in behind {
                    // Only the first pass finds clients to move: `lagging` stays
                    // set until the source catches up, but by then its entries
                    // are led by somebody else and there is nothing to take.
                    moved += take_clients_off(
                        &state,
                        id,
                        "data behind the freshest source; rebuilding its clients elsewhere",
                    );
                }
                if moved > 0 {
                    last_move = Some(tokio::time::Instant::now());
                }
            }
        });
    }

    let ws_app = Router::new()
        .route("/ws", any(ws_handler))
        .with_state(state.clone());
    let dash_app = Router::new()
        .route("/", get(stats_page))
        .route("/stats", get(stats_page))
        .with_state(state);

    // Named binds: two listeners means "address already in use" is otherwise
    // ambiguous, and the dashboard's default port is the one likely taken.
    let ws_listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("binding the client listener on {} (--listen)", args.listen))?;
    let dash_listener = tokio::net::TcpListener::bind(&args.dashboard_listen)
        .await
        .with_context(|| {
            format!("binding the dashboard listener on {} (--dashboard-listen)", args.dashboard_listen)
        })?;
    tracing::info!(ws = %args.listen, dashboard = %args.dashboard_listen, "wsarb listening");
    tokio::spawn(async move {
        let _ = axum::serve(
            dash_listener,
            dash_app.into_make_service_with_connect_info::<SocketAddr>(),
        ).await;
    });
    axum::serve(
        ws_listener,
        ws_app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

async fn stats_page(State(state): State<Arc<AppState>>) -> Html<String> {
    Html(stats::render_page(&state))
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Response {
    ws.on_upgrade(move |socket| client::handle_socket(socket, state, addr))
        .into_response()
}
