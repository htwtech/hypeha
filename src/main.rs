//! WSARB — websocket arbitration proxy for `order_book_server` feeds.

use wsarb::{client, health, reference, state, stats, upstream};

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
    /// Move clients off a source whose book has fallen this many blocks behind
    /// the furthest-ahead source. 0 disables it.
    ///
    /// The other half of the silence watchdog. A node that dies goes quiet and
    /// is caught by silence; a node that restarts and replays blocks stays loud
    /// while serving a book minutes old, and nothing arrival-based can see it.
    ///
    /// Blocks, read from each source's own `GET /health`, because every attempt
    /// to infer this from the stream measured something else. Blocks also mean
    /// no clock is involved: two heights are compared to each other, never to
    /// the time here. Judged against the other sources, so a quiet market —
    /// where nothing advances anywhere — is not mistaken for a fault.
    ///
    /// At ~14 blocks a second the default is about 3.5s. Healthy sources sit
    /// within a block or two of each other.
    #[arg(long = "lag-blocks", default_value_t = 50)]
    lag_blocks: u64,
    /// Where to read the network's own height, for the dashboard's `… vs net`
    /// column: the public explorer's `explorerBlock` subscription.
    ///
    /// The one measure none of our nodes has a hand in, so it shows them all
    /// falling behind together -- which comparing them with one another cannot.
    /// Display only: nothing is moved on it.
    #[arg(long = "reference", default_value = reference::DEFAULT_URL)]
    reference: String,
    /// Do not connect to the network reference at all.
    #[arg(long = "no-reference")]
    no_reference: bool,
    /// Each source's node state file (`…/hyperliquid_data/visor_abci_state.json`),
    /// in the same order as `--source`, for the dashboard's `node height`.
    ///
    /// The node's own height, as opposed to the book height `order_book_server`
    /// reports on `/health`: the two differ exactly while the server catches up
    /// after a restart. Needs wsarb on the same machine as the nodes. Display
    /// only.
    #[arg(long = "node-state", num_args = 1..)]
    node_states: Vec<String>,
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

/// How often each source is asked where it stands, and how long it gets to
/// answer. The endpoint is lock-free on the server side and the reply is one
/// short line, so this costs a socket and a few hundred bytes twice a second;
/// the timeout is generous because a slow answer is not a wrong one, and a
/// missed poll simply means no measurement this round.
const HEALTH_POLL_EVERY: Duration = Duration::from_millis(500);
const HEALTH_POLL_TIMEOUT: Duration = Duration::from_secs(2);

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
        // Rebuilt from somewhere else: the source being taken off is the last
        // one asked, not the first.
        tokio::spawn(upstream::fetch_snapshot_avoiding(state.clone(), key, Some(id)));
    }
    clients
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();

    if !args.node_states.is_empty() && args.node_states.len() != args.sources.len() {
        anyhow::bail!(
            "--node-state takes one path per --source, in the same order: got {} for {} sources",
            args.node_states.len(),
            args.sources.len()
        );
    }

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
    let lag_blocks = (args.lag_blocks > 0).then_some(args.lag_blocks);
    if let Some(n) = lag_blocks {
        tracing::info!(blocks = n, "moving clients off a source behind the others by this");
    }
    let state = Arc::new(AppState::new(sources, max_age));

    // Pinned before the sources start, so each one picks the probe up in the
    // resubscribe it sends on connecting rather than as a second request.
    if !args.no_probe {
        state.pin(state::SubKey::Bbo { coin: args.probe_coin.clone() });
        // And its l2Diff, for the height: `bbo` carries only a block time, and
        // only when the top of book moves, while l2Diff carries the height on
        // almost every block of an active coin. Without it `stream height` on
        // the dashboard would depend on what the clients happen to subscribe
        // to. Default depth, so it costs the nodes next to nothing.
        state.pin(state::SubKey::L2Diff {
            coin: args.probe_coin.clone(),
            n_sig_figs: None,
            n_levels: None,
            mantissa: None,
        });
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
    // The node's own height, straight off its state file. One reader per
    // source, so a file that has gone unreadable on one machine path does not
    // hold up the other.
    for (src, path) in state.sources.iter().cloned().zip(args.node_states.iter().cloned()) {
        tracing::info!(source = src.id, %path, "reading the node's height from its state file");
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(HEALTH_POLL_EVERY);
            let mut failing = false;
            loop {
                ticker.tick().await;
                match health::read_node_height(&path).await {
                    Some(h) => {
                        src.stats.record_node_height(h);
                        if failing {
                            failing = false;
                            tracing::info!(source = src.id, %path, "node state file readable again");
                        }
                    }
                    // Once per outage, not twice a second. A read that lands
                    // mid-write fails too, and costs only that one reading.
                    None if !failing => {
                        failing = true;
                        tracing::warn!(source = src.id, %path, "cannot read the node's height from its state file");
                    }
                    None => {}
                }
            }
        });
    }

    if args.no_reference {
        tracing::info!("network reference disabled");
    } else {
        tokio::spawn(reference::run(state.clone(), args.reference.clone()));
    }

    // The per-subscription watchdog. The two above judge sources as a whole;
    // a source can go quiet on one single-sourced key while carrying on with
    // everything else, and then only this notices. See `stalled_leaders`.
    {
        let state = state.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
                for (key, leader) in state.stalled_leaders(state::STALL_LIMIT) {
                    let clients = state.rebuild_off_leader(&key, leader);
                    if clients == 0 {
                        continue;
                    }
                    tracing::warn!(
                        source = leader,
                        sub = %key.label(),
                        clients,
                        "leader went quiet on this subscription while another source kept sending it; \
                         rebuilding from another source"
                    );
                    tokio::spawn(upstream::fetch_snapshot_avoiding(state.clone(), key, Some(leader)));
                }
            }
        });
    }

    if lag_blocks.is_some() {
        // One poller per source: a slow or unreachable one must not delay the
        // others' measurements, which is the whole point of asking each source
        // directly rather than inferring from a shared stream.
        for src in &state.sources {
            let src = src.clone();
            match health::health_url(&src.url) {
                Some(url) => {
                    tracing::info!(source = src.id, %url, "polling the source for its height");
                    tokio::spawn(async move {
                        let mut ticker = tokio::time::interval(HEALTH_POLL_EVERY);
                        loop {
                            ticker.tick().await;
                            match health::poll(&url, HEALTH_POLL_TIMEOUT).await {
                                Some(h) => src.stats.record_health(h.height, h.is_ready()),
                                None => src.stats.record_health_failure(),
                            }
                        }
                    });
                }
                None => tracing::warn!(
                    source = src.id,
                    url = %src.url,
                    "no health endpoint can be derived from this address (TLS is not supported here); \
                     this source will never be marked behind"
                ),
            }
        }
    }

    if let Some(limit) = lag_blocks {
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
