//! Shared application state: subscription arbitration, the subscription
//! registry, and the upstream/downstream connection bookkeeping.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::ws::Utf8Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::stats::SourceStats;

/// One subscription, mirroring `order_book_server`'s own `Subscription` enum so
/// that a client request round-trips to the upstream unchanged.
///
/// Deriving identity from the whole enum (rather than a channel+coin pair) is
/// what keeps the `l2Book` aggregation parameters part of the key: two clients
/// asking for different `nSigFigs` are two streams, not one.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SubKey {
    Trades {
        coin: String,
    },
    #[serde(rename_all = "camelCase")]
    L2Book {
        coin: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        n_sig_figs: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        n_levels: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mantissa: Option<u64>,
    },
    /// The same book and the same parameters as [`SubKey::L2Book`], but sent as
    /// one snapshot followed by only what changed.
    ///
    /// The rename belongs on the variant: the one on the enum renames variants,
    /// not fields. Without it `nLevels` never reaches `n_levels`, serde fills in
    /// the default, and the client is served a book at some other depth than it
    /// asked for — with an acknowledgement that looks perfectly fine.
    #[serde(rename_all = "camelCase")]
    L2Diff {
        coin: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        n_sig_figs: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        n_levels: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mantissa: Option<u64>,
    },
    L4Book {
        coin: String,
    },
    Bbo {
        coin: String,
    },
    OrderUpdates {
        user: String,
    },
    BookDiffs {
        coin: String,
    },
}

/// The upstream's `nLevels` when a client leaves it out.
///
/// It also *rejects* an explicit 20 ("set n_levels to this by using null"), so
/// no legitimate subscription can carry it and folding it back to `None` is
/// unambiguous. See [`SubKey::canonical`].
const DEFAULT_L2_LEVELS: usize = 20;

impl SubKey {
    pub fn channel(&self) -> &'static str {
        match self {
            Self::Trades { .. } => "trades",
            Self::L2Book { .. } => "l2Book",
            Self::L2Diff { .. } => "l2Diff",
            Self::L4Book { .. } => "l4Book",
            Self::Bbo { .. } => "bbo",
            Self::OrderUpdates { .. } => "orderUpdates",
            Self::BookDiffs { .. } => "bookDiffs",
        }
    }

    /// The one spelling of a key that a client request and an upstream frame
    /// both arrive at. Without it the two disagree and the frames route to an
    /// entry with no subscribers: the client is acknowledged and then hears
    /// nothing at all, with no error anywhere.
    ///
    /// Two ways that happens:
    ///
    /// * addresses — the upstream emits them lower-cased, clients type any case;
    /// * `l2Book` without `nLevels` — the upstream stamps its default into every
    ///   frame it sends back, so the request says `None` and the frames all say
    ///   `Some(20)`.
    pub fn normalized(self) -> Self {
        match self {
            Self::OrderUpdates { user } => Self::OrderUpdates { user: user.to_ascii_lowercase() },
            Self::L2Book { coin, n_sig_figs, n_levels, mantissa } => Self::L2Book {
                coin,
                n_sig_figs,
                n_levels: n_levels.filter(|&n| n != DEFAULT_L2_LEVELS),
                mantissa,
            },
            Self::L2Diff { coin, n_sig_figs, n_levels, mantissa } => Self::L2Diff {
                coin,
                n_sig_figs,
                n_levels: n_levels.filter(|&n| n != DEFAULT_L2_LEVELS),
                mantissa,
            },
            other => other,
        }
    }

    /// Whether the channel streams increments against a prior snapshot, so that
    /// a dropped or out-of-order frame corrupts the client's book for good.
    pub fn is_incremental(&self) -> bool {
        matches!(self, Self::L4Book { .. } | Self::L2Diff { .. })
    }

    /// Compact one-line label for the stats page.
    pub fn label(&self) -> String {
        match self {
            Self::Trades { coin } | Self::L4Book { coin } | Self::Bbo { coin } | Self::BookDiffs { coin } => {
                format!("{}:{coin}", self.channel())
            }
            Self::OrderUpdates { user } => {
                let short = if user.len() > 10 { format!("{}…{}", &user[..6], &user[user.len() - 4..]) } else { user.clone() };
                format!("orderUpdates:{short}")
            }
            Self::L2Book { coin, n_sig_figs, n_levels, mantissa }
            | Self::L2Diff { coin, n_sig_figs, n_levels, mantissa } => {
                let mut s = format!("{}:{coin}", self.channel());
                if let Some(v) = n_sig_figs {
                    s.push_str(&format!("/sf{v}"));
                }
                if let Some(v) = n_levels {
                    s.push_str(&format!("/n{v}"));
                }
                if let Some(v) = mantissa {
                    s.push_str(&format!("/m{v}"));
                }
                s
            }
        }
    }
}

/// A `{"method":..,"subscription":..}` frame, for talking to the upstream.
#[derive(Serialize)]
#[serde(tag = "method", rename_all = "camelCase")]
pub enum SubRequest<'a> {
    Subscribe { subscription: &'a SubKey },
    Unsubscribe { subscription: &'a SubKey },
}

impl SubRequest<'_> {
    pub fn json(&self) -> String {
        serde_json::to_string(self).expect("SubKey is always serializable")
    }
}

/// How an incoming frame orders against the frames already seen for its key.
#[derive(Clone, Copy)]
pub enum Seq {
    /// A self-contained message carrying a monotonic stamp. Newest wins;
    /// anything older is stale and anything equal is a slower source's copy.
    Point(u64),
    /// One message out of a block. Positions within a block are raced
    /// individually: #k goes out as soon as any source reaches it.
    ///
    /// Only sound where the upstream emits per event batch, because then the
    /// batches come straight from chain data and every node cuts a block into
    /// the same messages in the same order.
    Block(u64),
    /// A self-contained snapshot sharing its stamp with the ones that follow
    /// it, where the sources do *not* agree on how many there are.
    ///
    /// `l2Book` is flushed on a 50 ms timer whose phase is each node's own, so
    /// one node may send two snapshots for a block and another three. Racing
    /// positions would then match up messages that are not the same state at
    /// all. Instead the first source to open a stamp carries it to the end:
    /// its own later snapshots go out, everyone else's are duplicates.
    Lead(u64),
    /// One source carries the whole stream, not merely one stamp.
    ///
    /// An increment only means anything against the book it was computed from,
    /// and two nodes measurably do not hold the same book: 17% of levels apart
    /// at depth 100, 36% at 1000, measured on stock binaries and on `l2Book`
    /// itself. Applying one node's increment to another node's book therefore
    /// corrupts it, silently and for good. So the leader changes only when it
    /// dies, and the client is moved to its replacement by a fresh snapshot
    /// rather than by splicing.
    Sticky(u64),
    /// A full snapshot at a block height. A reset rather than an increment, and
    /// the only thing that puts a source back in play after it was written off.
    Snapshot(u64),
}

/// An upstream data connection (one per configured endpoint).
pub struct Source {
    pub id: usize,
    pub url: String,
    pub stats: SourceStats,
    /// Raw `subscribe`/`unsubscribe` frames to send on the upstream socket. The
    /// connection task owns the receiving half.
    pub ctrl_tx: mpsc::UnboundedSender<String>,
    /// Raised to make the connection task drop its socket and start over.
    ///
    /// Silence on a healthy socket is ambiguous: the node behind the upstream
    /// may be down, or the subscription may have been lost server-side with the
    /// connection left intact. Nothing distinguishes the two from here, and only
    /// the second is recoverable — so after a long enough silence the connection
    /// is thrown away, since reconnecting re-subscribes everything anyway.
    pub reconnect: tokio::sync::Notify,
}

/// A connected downstream client.
pub struct Client {
    pub id: u64,
    pub ip: String,
    /// Outbound queue drained by the client's writer task. Bounded so a slow
    /// client cannot stall the hot path. On overflow a self-contained channel
    /// drops the frame and counts it; an incremental one hangs the client up,
    /// since a hole there is not something the next frame repairs.
    pub tx: mpsc::Sender<Utf8Bytes>,
    pub subscriptions: Mutex<HashSet<SubKey>>,
    pub bytes_sent: AtomicU64,
    pub dropped: AtomicU64,
    pub connected_at: Instant,
    /// Raised when the client must be disconnected rather than served a stream
    /// with a hole in it. `handle_socket` waits on this alongside its tasks.
    pub kill: tokio::sync::Notify,
}

impl Client {
    /// Queue a frame, hanging up if the queue has overflowed. Used for the
    /// incremental channels, where a dropped frame corrupts the client's book
    /// for good: reconnecting is the lesser evil.
    fn send_or_hang_up(&self, msg: Utf8Bytes) -> bool {
        if self.tx.try_send(msg).is_err() {
            self.dropped.fetch_add(1, Relaxed);
            self.kill.notify_one();
            return false;
        }
        true
    }
}

/// Cap on what we hold back for one client awaiting its snapshot. `l4Book` has
/// been measured between 1 and 3.5 MB/s per coin depending on market activity,
/// so this buys somewhere between 20 and 60 seconds — comfortably more than the
/// few hundred milliseconds a snapshot fetch actually takes.
const PENDING_MAX_BYTES: usize = 64 * 1024 * 1024;

/// A client waiting for its own snapshot before it may join an incremental
/// stream. Live frames pile up here meanwhile so that nothing is lost between
/// the snapshot's height and the moment the client is attached.
struct Pending {
    client: Arc<Client>,
    held: VecDeque<(u64, Utf8Bytes)>,
    bytes: usize,
}

/// Per-subscription arbitration state and the set of clients subscribed to it.
pub struct SubEntry {
    /// Newest stamp seen across all sources: `time`/`tid` under `Seq::Point`,
    /// block height under `Seq::Block`.
    pub last: u64,
    /// When `last` was first observed, used to measure how late other sources
    /// are for the same update.
    pub last_seen_at: Instant,
    /// Whether we have already subscribed to this on the upstream sources.
    pub upstream_subscribed: bool,
    pub subscribers: Vec<Arc<Client>>,
    /// Held open with no subscribers, as a permanent latency probe. See
    /// `AppState::pin`.
    pub pinned: bool,
    /// Clients held back until their snapshot arrives. Normally empty, so the
    /// hot path pays one emptiness check.
    pending: Vec<Pending>,
    /// `Seq::Block` only: the source leading the currently open block.
    block_leader: Option<usize>,
    /// `Seq::Block` only: bitmask of sources already charged one delay sample
    /// for the open block, so a 200-message block yields one sample per source
    /// instead of 200.
    block_sampled: u64,
    /// How many messages of the current block have gone out to clients.
    ///
    /// Positions within a block are raced individually: message #k is forwarded
    /// the moment any source reaches it. This rests on every node producing the
    /// same messages in the same order for a given block — an assumption the
    /// upstream's design supports (the blocks are a deterministic replay of one
    /// chain) but which nothing here verifies.
    block_sent: u32,
    /// Messages each source has delivered for the current block, by source id.
    block_seen: Vec<u32>,
    /// Bitmask of sources written off after going silent mid-block. Their
    /// increments are ignored until they send a snapshot again.
    ///
    /// Without this a source that stalled while *ahead* of the others would,
    /// on resuming, out-rank the survivor everyone was rebuilt onto and drag
    /// the clients hundreds of blocks forward with no snapshot in between.
    needs_snapshot: u64,
}

impl Default for SubEntry {
    fn default() -> Self {
        Self {
            last: 0,
            last_seen_at: Instant::now(),
            upstream_subscribed: false,
            subscribers: Vec::new(),
            pinned: false,
            pending: Vec::new(),
            block_leader: None,
            block_sampled: 0,
            block_sent: 0,
            block_seen: Vec::new(),
            needs_snapshot: 0,
        }
    }
}

impl SubEntry {
    /// Count one more message of the current block from `id`, returning that
    /// source's running total for the block.
    fn bump_block_count(&mut self, id: usize) -> u32 {
        if self.block_seen.len() <= id {
            self.block_seen.resize(id + 1, 0);
        }
        self.block_seen[id] += 1;
        self.block_seen[id]
    }

    /// Start a new block, with `id` having supplied its first message.
    fn open_block(&mut self, id: usize) {
        self.block_seen.iter_mut().for_each(|c| *c = 0);
        self.block_sent = 1;
        self.bump_block_count(id);
    }
}

/// Bit for a source in the per-entry masks. Sources beyond 64 share bits, which
/// costs a little accuracy in the delay sampling and nothing in correctness.
fn source_bit(id: usize) -> u64 {
    1u64 << (id % 64)
}

/// What happened when a client subscribed.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum SubscribeOutcome {
    /// First subscriber: the upstream subscribe was just issued, so the stream
    /// (and, for `l4Book`, its snapshot) begins fresh for this client.
    Fresh,
    /// Joined a stream that was already running.
    Joined,
    /// The client was already subscribed to exactly this; nothing changed.
    Duplicate,
}

pub struct AppState {
    /// Refuse to forward a frame whose block time is older than this.
    ///
    /// Guards the one case relative ordering cannot: on a key with no history,
    /// the first frame to arrive wins regardless of its age, and a node frozen
    /// long ago answers fastest of all — it has nothing left to compute.
    ///
    /// This makes the data path depend on the clocks agreeing between wsarb and
    /// the nodes. On one machine they do; across machines a skew would make
    /// wsarb discard everything, which is why the limit is generous, the
    /// rejection is logged rather than silent, and `None` turns it off.
    pub max_age: Option<Duration>,
    pub sources: Vec<Arc<Source>>,
    pub subs: DashMap<SubKey, SubEntry>,
    pub clients: DashMap<u64, Arc<Client>>,
    pub next_client_id: AtomicU64,
}

/// How many pending messages we buffer per client.
///
/// Sized for `l4Book`, which does not trickle: a single block can burst several
/// hundred one-event messages at once, and on an incremental channel a queue
/// that overruns costs the client its connection rather than one frame. The
/// payloads behind these slots are refcounted `Bytes` shared with every other
/// subscriber, so a deep queue costs far less than its length suggests.
const CLIENT_QUEUE: usize = 65536;

impl AppState {
    pub fn new(sources: Vec<Arc<Source>>, max_age: Option<Duration>) -> Self {
        Self {
            max_age,
            sources,
            subs: DashMap::new(),
            clients: DashMap::new(),
            next_client_id: AtomicU64::new(1),
        }
    }

    /// Register a new client and return its handle together with the receiver
    /// its writer task should drain.
    pub fn register_client(&self, ip: String) -> (Arc<Client>, mpsc::Receiver<Utf8Bytes>) {
        let (tx, rx) = mpsc::channel(CLIENT_QUEUE);
        let id = self.next_client_id.fetch_add(1, Relaxed);
        let client = Arc::new(Client {
            id,
            ip,
            tx,
            subscriptions: Mutex::new(HashSet::new()),
            bytes_sent: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            connected_at: Instant::now(),
            kill: tokio::sync::Notify::new(),
        });
        self.clients.insert(id, client.clone());
        (client, rx)
    }

    /// Subscribe a client to `key`. On the first ever subscription to a key,
    /// fan the subscription out to every upstream source.
    pub fn subscribe(&self, client: &Arc<Client>, key: SubKey) -> SubscribeOutcome {
        if !client.subscriptions.lock().unwrap().insert(key.clone()) {
            return SubscribeOutcome::Duplicate;
        }

        let mut entry = self.subs.entry(key.clone()).or_default();
        let need_upstream = !entry.upstream_subscribed;

        // On an incremental channel a late joiner must not see diffs before its
        // own snapshot, or it would apply them to an empty book. It waits in
        // `pending` while the frames it will need pile up behind it.
        if !need_upstream && key.is_incremental() {
            if !entry.pending.iter().any(|p| p.client.id == client.id) {
                entry.pending.push(Pending { client: client.clone(), held: VecDeque::new(), bytes: 0 });
            }
        } else if !entry.subscribers.iter().any(|c| c.id == client.id) {
            entry.subscribers.push(client.clone());
        }

        if need_upstream {
            entry.upstream_subscribed = true;
        }
        drop(entry);

        if need_upstream {
            tracing::info!(sub = %key.label(), "new subscription, fanning out to all sources");
            self.broadcast_ctrl(&SubRequest::Subscribe { subscription: &key });
            SubscribeOutcome::Fresh
        } else {
            SubscribeOutcome::Joined
        }
    }

    /// Hold a subscription open forever, with no client behind it.
    ///
    /// The arbitration counters are how the sources get compared against each
    /// other, and they only move while something is streaming — so with no
    /// clients connected there is nothing to judge the nodes by. One cheap
    /// subscription fixes that: `bbo` carries only the top of book, ticks
    /// steadily rather than in bursts, and so makes a better latency probe than
    /// the heavier channels while costing almost nothing.
    pub fn pin(&self, key: SubKey) {
        let mut entry = self.subs.entry(key.clone()).or_default();
        entry.pinned = true;
        let need_upstream = !entry.upstream_subscribed;
        entry.upstream_subscribed = true;
        drop(entry);

        if need_upstream {
            tracing::info!(sub = %key.label(), "pinned as the latency probe");
            self.broadcast_ctrl(&SubRequest::Subscribe { subscription: &key });
        }
    }

    /// Unsubscribe a client from `key` at its own request.
    pub fn unsubscribe(&self, client: &Arc<Client>, key: &SubKey) -> bool {
        if !client.subscriptions.lock().unwrap().remove(key) {
            return false;
        }
        self.detach(client, key);
        true
    }

    /// Detach a client from one key, dropping the upstream subscription once
    /// the last subscriber is gone.
    fn detach(&self, client: &Arc<Client>, key: &SubKey) {
        let mut drop_upstream = false;
        if let Some(mut entry) = self.subs.get_mut(key) {
            entry.subscribers.retain(|c| c.id != client.id);
            entry.pending.retain(|p| p.client.id != client.id);
            if entry.subscribers.is_empty()
                && entry.pending.is_empty()
                && entry.upstream_subscribed
                && !entry.pinned
            {
                entry.upstream_subscribed = false;
                drop_upstream = true;
            }
        }
        // The shard guard is released above; only now touch the ctrl channels
        // and the map itself.
        if drop_upstream {
            tracing::info!(sub = %key.label(), "last subscriber left, unsubscribing upstream");
            self.broadcast_ctrl(&SubRequest::Unsubscribe { subscription: key });
            self.subs.remove(key);
        }
    }

    fn broadcast_ctrl(&self, req: &SubRequest<'_>) {
        let json = req.json();
        for src in &self.sources {
            let _ = src.ctrl_tx.send(json.clone());
        }
    }

    /// Handle a fresh frame for `key` arriving from `src`. Applies the
    /// arbitration rule for `seq` and, on a win, fans the raw payload out.
    pub fn on_update(&self, src: &Source, key: SubKey, seq: Seq, payload: Utf8Bytes) {
        let now = Instant::now();
        // Read off the key, not off `seq`: how a channel is arbitrated and
        // whether it is incremental are separate questions. `bbo` is raced by
        // position like `l4Book`, yet losing one of its frames costs nothing —
        // each carries the whole top of book, and the next one supersedes it.
        let incremental = key.is_incremental();
        let mut entry = self.subs.entry(key).or_default();

        let mut deliver = false;
        let mut win = false;
        let mut late: Option<Duration> = None;
        let mut stale = false;

        match seq {
            Seq::Point(v) => {
                if v > entry.last {
                    entry.last = v;
                    entry.last_seen_at = now;
                    deliver = true;
                    win = true;
                } else if v == entry.last {
                    late = Some(now.saturating_duration_since(entry.last_seen_at));
                } else {
                    stale = true;
                }
            }
            Seq::Lead(v) => {
                if v > entry.last {
                    entry.last = v;
                    entry.last_seen_at = now;
                    entry.block_leader = Some(src.id);
                    deliver = true;
                    win = true;
                } else if v == entry.last {
                    if entry.block_leader == Some(src.id) {
                        // The leader's own next snapshot of the same stamp, and
                        // a fresher one than what went out before. Newest-wins
                        // would drop it for sharing a stamp, which on a channel
                        // flushed twice per block throws away every second
                        // update and leaves the client on the older of the two.
                        deliver = true;
                    } else {
                        late = Some(now.saturating_duration_since(entry.last_seen_at));
                    }
                } else {
                    stale = true;
                }
            }
            Seq::Sticky(v) => match entry.block_leader {
                // Nobody is leading: the first source to speak takes the stream
                // and keeps it. This is also the path back after a resync.
                None => {
                    entry.last = v;
                    entry.last_seen_at = now;
                    entry.block_leader = Some(src.id);
                    deliver = true;
                    win = true;
                }
                Some(id) if id == src.id => {
                    if v >= entry.last {
                        // Equal stamps are ordinary here: the upstream flushes
                        // more than once per block, and each flush is a further
                        // increment on the same book, not a repeat.
                        win = v > entry.last;
                        entry.last = v;
                        entry.last_seen_at = now;
                        deliver = true;
                    } else {
                        // The leader replayed. Its increments no longer line up
                        // with what the client holds.
                        stale = true;
                    }
                }
                // Somebody else's increments are computed against a different
                // book. Not a race to win -- just not usable.
                Some(_) => late = Some(now.saturating_duration_since(entry.last_seen_at)),
            },
            Seq::Snapshot(h) => {
                // A snapshot is a reset, and the one thing that puts a written
                // off source back in play.
                entry.needs_snapshot &= !source_bit(src.id);
                // ...but only from the source we are following, or from anyone
                // when we are following nobody. Every source snapshots when it
                // subscribes and again on every reconnect, so an unguarded
                // reset let a source that happened to be a block ahead seize
                // the stream from a healthy leader -- moving the client onto a
                // different node's book behind its back, which is the one thing
                // `Seq::Sticky` exists to prevent.
                let ours = entry.block_leader.map_or(true, |id| id == src.id);
                if ours && h > entry.last {
                    entry.last = h;
                    entry.last_seen_at = now;
                    entry.block_leader = Some(src.id);
                    entry.block_sampled = source_bit(src.id);
                    entry.open_block(src.id);
                    deliver = true;
                    win = true;
                } else {
                    // Every source snapshots when it subscribes; only the first
                    // one to arrive is of any use.
                    stale = true;
                }
            }
            Seq::Block(h) => {
                if entry.needs_snapshot & source_bit(src.id) != 0 {
                    // Written off after going silent. Its increments cannot be
                    // trusted to line up with whatever the clients were rebuilt
                    // onto, so they wait for a snapshot from it.
                    stale = true;
                } else if h > entry.last {
                    // First message of a newer block.
                    entry.last = h;
                    entry.last_seen_at = now;
                    entry.block_leader = Some(src.id);
                    entry.block_sampled = source_bit(src.id);
                    entry.open_block(src.id);
                    deliver = true;
                    win = true;
                } else if h == entry.last {
                    // Positions inside a block are raced one by one: whoever
                    // reaches position #k first supplies it. Nothing here waits
                    // on the source that opened the block, so if it dies partway
                    // the others simply carry on from where it stopped — the
                    // block is completed rather than left truncated, and without
                    // a snapshot.
                    let seen = entry.bump_block_count(src.id);
                    if seen > entry.block_sent {
                        entry.block_sent = seen;
                        entry.block_leader = Some(src.id);
                        deliver = true;
                    } else {
                        // This position already went out from someone faster.
                        // Charge one delay sample per source per block rather
                        // than one per message.
                        let bit = source_bit(src.id);
                        if entry.block_sampled & bit == 0 {
                            entry.block_sampled |= bit;
                            late = Some(now.saturating_duration_since(entry.last_seen_at));
                        }
                    }
                } else {
                    stale = true;
                }
            }
        }

        // Hold the frame for anyone still waiting on a snapshot, and evict the
        // ones whose backlog has outgrown the cap.
        let mut overflowed: Vec<Arc<Client>> = Vec::new();
        if deliver && !entry.pending.is_empty() {
            if let Seq::Block(h) | Seq::Snapshot(h) = seq {
                let n = payload.as_str().len();
                for p in entry.pending.iter_mut() {
                    p.bytes += n;
                    p.held.push_back((h, payload.clone()));
                }
                entry.pending.retain(|p| {
                    if p.bytes > PENDING_MAX_BYTES {
                        overflowed.push(p.client.clone());
                        false
                    } else {
                        true
                    }
                });
            }
        }

        // Clone the (cheap) Arc handles so we can release the shard lock before
        // touching per-client channels.
        let subscribers = if deliver { Some(entry.subscribers.clone()) } else { None };
        drop(entry);

        if win {
            src.stats.wins.fetch_add(1, Relaxed);
        }
        if let Some(d) = late {
            src.stats.record_delay(d);
        }
        if stale {
            src.stats.stale.fetch_add(1, Relaxed);
        }

        for client in overflowed {
            tracing::warn!(client = client.id, "snapshot backlog overflowed; disconnecting");
            client.kill.notify_one();
        }

        if let Some(subscribers) = subscribers {
            for client in subscribers {
                if incremental {
                    client.send_or_hang_up(payload.clone());
                } else if client.tx.try_send(payload.clone()).is_err() {
                    client.dropped.fetch_add(1, Relaxed);
                }
            }
        }
    }

    /// Hand a freshly fetched snapshot to a waiting client, then release the
    /// frames held behind it and attach it to the live stream.
    ///
    /// Everything held at or below the snapshot's own height is already baked
    /// into it and would be applied twice, so it is discarded.
    pub fn deliver_snapshot(&self, key: &SubKey, client_id: u64, height: u64, snapshot: Utf8Bytes) {
        let mut client = None;
        let mut backlog: Vec<Utf8Bytes> = Vec::new();

        if let Some(mut entry) = self.subs.get_mut(key) {
            if let Some(idx) = entry.pending.iter().position(|p| p.client.id == client_id) {
                let Pending { client: c, held, .. } = entry.pending.remove(idx);
                backlog.push(snapshot);
                backlog.extend(held.into_iter().filter(|(h, _)| *h > height).map(|(_, b)| b));
                entry.subscribers.push(c.clone());
                client = Some(c);
            }
        }

        if let Some(c) = client {
            tracing::info!(
                client = c.id,
                sub = %key.label(),
                height,
                held = backlog.len() - 1,
                "snapshot delivered, client attached to the live stream"
            );
            for msg in backlog {
                if !c.send_or_hang_up(msg) {
                    break;
                }
            }
        }
    }

    /// Sources that have gone quiet while others keep delivering.
    ///
    /// Judged relatively on purpose. A source whose node has died keeps its
    /// websocket open and simply stops speaking, so silence is the only signal
    /// there is — but a quiet market silences every source at once, and that is
    /// not a fault. Requiring somebody else to still be delivering separates
    /// the two, and incidentally covers the idle case where no client is
    /// subscribed and nothing is flowing anywhere.
    pub fn silent_sources(&self, limit: Duration) -> Vec<usize> {
        let freshest = self.sources.iter().filter_map(|s| s.stats.idle_for()).min();
        match freshest {
            // Nobody has delivered lately: quiet market, not a dead source.
            Some(f) if f <= limit => {}
            _ => return Vec::new(),
        }
        self.sources
            .iter()
            .filter(|s| s.stats.connected.load(Relaxed))
            .filter(|s| s.stats.idle_for().is_some_and(|idle| idle > limit))
            .map(|s| s.id)
            .collect()
    }

    /// A source that was leading an open block has gone away. Its subscribers
    /// are holding half a block, and the other sources' copies of that block
    /// were already discarded as trailing — so the book cannot be completed.
    ///
    /// Park those clients and return the snapshots that must be fetched to
    /// rebuild them. Splicing the tail in from another source would be cheaper
    /// but would require assuming both nodes order a block identically, which
    /// is exactly the assumption this design avoids.
    pub fn resync_after_source_loss(&self, source_id: usize) -> Vec<(SubKey, u64)> {
        let mut work = Vec::new();
        for mut entry in self.subs.iter_mut() {
            if !entry.key().is_incremental() || entry.block_leader != Some(source_id) {
                continue;
            }
            let key = entry.key().clone();
            entry.block_leader = None;
            entry.block_sampled = 0;
            // Write the source off until it snapshots again: it stalled while
            // possibly ahead of everyone else, and resuming its increments
            // would drag the clients forward past whatever they get rebuilt on.
            entry.needs_snapshot |= source_bit(source_id);
            // Let go of its height too. The surviving source may well be behind
            // it — with a spare deliberately kept on a slower peer, it usually
            // is — and holding the old height would leave every frame it sends
            // looking stale, forever. The clients are parked and about to be
            // rebuilt from a snapshot anyway, so there is no timeline left to
            // protect here.
            entry.last = 0;
            let orphaned: Vec<Arc<Client>> = entry.subscribers.drain(..).collect();
            for client in orphaned {
                tracing::warn!(client = client.id, sub = %key.label(), "block leader lost mid-block, resyncing");
                work.push((key.clone(), client.id));
                entry.pending.push(Pending { client, held: VecDeque::new(), bytes: 0 });
            }
        }
        work
    }

    /// Give up on a waiting client. Serving an incremental stream with no
    /// snapshot under it would look fine and be silently wrong, so the client
    /// is disconnected instead.
    pub fn fail_pending(&self, key: &SubKey, client_id: u64) {
        let mut client = None;
        if let Some(mut entry) = self.subs.get_mut(key) {
            if let Some(idx) = entry.pending.iter().position(|p| p.client.id == client_id) {
                client = Some(entry.pending.remove(idx).client);
            }
        }
        if let Some(c) = client {
            tracing::warn!(
                client = c.id,
                sub = %key.label(),
                "no snapshot available; disconnecting rather than streaming a book with no base"
            );
            c.kill.notify_one();
        }
    }

    /// Subscribe frames for everything currently live upstream, used to
    /// re-subscribe a source after it reconnects.
    pub fn subscribed_requests(&self) -> Vec<String> {
        self.subs
            .iter()
            .filter(|e| e.upstream_subscribed)
            .map(|e| SubRequest::Subscribe { subscription: e.key() }.json())
            .collect()
    }

    /// Remove a client and detach it from every key it was subscribed to.
    pub fn cleanup_client(&self, client: &Arc<Client>) {
        self.clients.remove(&client.id);
        let keys: Vec<SubKey> = client.subscriptions.lock().unwrap().drain().collect();
        for key in keys {
            self.detach(client, &key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn l2(coin: &str, sf: Option<u32>) -> SubKey {
        SubKey::L2Book { coin: coin.into(), n_sig_figs: sf, n_levels: None, mantissa: None }
    }

    #[test]
    fn l2book_params_are_part_of_identity() {
        assert_ne!(l2("BTC", None), l2("BTC", Some(3)));
        assert_eq!(l2("BTC", Some(3)), l2("BTC", Some(3)));
    }

    #[test]
    fn subscription_round_trips_to_the_upstream_wire_format() {
        let key: SubKey = serde_json::from_str(r#"{"type":"l2Book","coin":"BTC"}"#).unwrap();
        assert_eq!(key, l2("BTC", None));
        assert_eq!(serde_json::to_string(&key).unwrap(), r#"{"type":"l2Book","coin":"BTC"}"#);

        let req = SubRequest::Subscribe { subscription: &key }.json();
        assert_eq!(req, r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}"#);
    }

    #[test]
    fn subscription_parameters_survive_the_round_trip() {
        // The round-trip test above uses the bare form, with no parameters at
        // all, so it cannot see a field-naming mistake. This one can, and it
        // covers both channels: the names on the wire are camelCase and the
        // fields are not, and getting that wrong fails silently -- serde fills
        // in the default and the client is served a book at some depth other
        // than the one it asked for, acknowledged as though nothing happened.
        for channel in ["l2Book", "l2Diff"] {
            let wire =
                format!(r#"{{"type":"{channel}","coin":"BTC","nSigFigs":3,"nLevels":1000,"mantissa":5}}"#);
            let key: SubKey = serde_json::from_str(&wire).expect(channel);
            assert_eq!(serde_json::to_string(&key).unwrap(), wire, "{channel}");
        }
    }

    #[test]
    fn every_channel_name_matches_the_upstream_spelling() {
        let cases = [
            (r#"{"type":"trades","coin":"BTC"}"#, "trades"),
            (r#"{"type":"l2Book","coin":"BTC"}"#, "l2Book"),
            (r#"{"type":"l4Book","coin":"BTC"}"#, "l4Book"),
            (r#"{"type":"bbo","coin":"BTC"}"#, "bbo"),
            (r#"{"type":"bookDiffs","coin":"BTC"}"#, "bookDiffs"),
            (r#"{"type":"orderUpdates","user":"0xabc"}"#, "orderUpdates"),
        ];
        for (json, channel) in cases {
            let key: SubKey = serde_json::from_str(json).unwrap_or_else(|e| panic!("{json}: {e}"));
            assert_eq!(key.channel(), channel);
            assert_eq!(serde_json::to_string(&key).unwrap(), json);
        }
    }

    #[test]
    fn l2book_optional_params_survive_the_round_trip() {
        let json = r#"{"type":"l2Book","coin":"BTC","nSigFigs":3,"nLevels":50,"mantissa":5}"#;
        let key: SubKey = serde_json::from_str(json).unwrap();
        assert_eq!(key, SubKey::L2Book { coin: "BTC".into(), n_sig_figs: Some(3), n_levels: Some(50), mantissa: Some(5) });
        assert_eq!(serde_json::to_string(&key).unwrap(), json);
    }

    #[test]
    fn user_addresses_key_case_insensitively() {
        let typed: SubKey = serde_json::from_str(r#"{"type":"orderUpdates","user":"0xABCdef"}"#).unwrap();
        let from_frame = SubKey::OrderUpdates { user: "0xabcdef".into() };
        assert_eq!(typed.normalized(), from_frame);
    }

    fn test_state(n: usize) -> AppState {
        let sources = (0..n)
            .map(|id| {
                // Nothing drains the ctrl channel here; the sends are ignored.
                let (ctrl_tx, _rx) = mpsc::unbounded_channel();
                Arc::new(Source {
                    id,
                    url: format!("ws://src{id}"),
                    stats: SourceStats::default(),
                    ctrl_tx,
                    reconnect: tokio::sync::Notify::new(),
                })
            })
            .collect();
        AppState::new(sources, None)
    }

    fn drain(rx: &mut mpsc::Receiver<Utf8Bytes>) -> Vec<String> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            out.push(m.as_str().to_string());
        }
        out
    }

    fn msg(s: &str) -> Utf8Bytes {
        Utf8Bytes::from(s.to_string())
    }

    #[test]
    fn the_leader_carries_the_whole_stream_not_just_a_stamp() {
        let state = test_state(2);
        let key = SubKey::L2Diff { coin: "BTC".into(), n_sig_figs: None, n_levels: None, mantissa: None };
        let (client, mut rx) = state.register_client("t".into());
        state.subscribe(&client, key.clone());

        let a = state.sources[0].clone();
        let b = state.sources[1].clone();

        state.on_update(&a, key.clone(), Seq::Sticky(10), msg("10-a"));
        state.on_update(&b, key.clone(), Seq::Sticky(10), msg("10-b")); // other book
        state.on_update(&a, key.clone(), Seq::Sticky(10), msg("10-a2")); // same stamp, next flush
        state.on_update(&b, key.clone(), Seq::Sticky(11), msg("11-b")); // ahead, still not ours
        state.on_update(&a, key.clone(), Seq::Sticky(11), msg("11-a"));
        state.on_update(&a, key.clone(), Seq::Sticky(10), msg("10-a3")); // a replayed

        // Only A. Under Lead, "11-b" would have taken the stamp and the client
        // would have applied B's increment to A's book -- the two nodes hold
        // books that differ by a third at depth, so that silently corrupts it.
        assert_eq!(drain(&mut rx), vec!["10-a", "10-a2", "11-a"]);
        assert_eq!(b.stats.wins.load(Relaxed), 0);
        assert_eq!(a.stats.stale.load(Relaxed), 1);
    }

    #[test]
    fn a_reconnecting_source_does_not_steal_a_healthy_stream() {
        let state = test_state(2);
        let key = SubKey::L2Diff { coin: "BTC".into(), n_sig_figs: None, n_levels: None, mantissa: None };
        let (client, mut rx) = state.register_client("t".into());
        state.subscribe(&client, key.clone());

        let a = state.sources[0].clone();
        let b = state.sources[1].clone();

        state.on_update(&a, key.clone(), Seq::Snapshot(10), msg("snap-a"));
        state.on_update(&a, key.clone(), Seq::Sticky(11), msg("11-a"));

        // B reconnects and snapshots, a block ahead of the leader. Taking that
        // as a reset would hand it the stream and move the client onto B's
        // book -- silently, since both books are internally consistent.
        state.on_update(&b, key.clone(), Seq::Snapshot(12), msg("snap-b"));
        state.on_update(&b, key.clone(), Seq::Sticky(13), msg("13-b"));
        state.on_update(&a, key.clone(), Seq::Sticky(12), msg("12-a"));

        assert_eq!(drain(&mut rx), vec!["snap-a", "11-a", "12-a"]);
        assert_eq!(b.stats.wins.load(Relaxed), 0);
    }

    #[test]
    fn losing_the_leader_parks_l2diff_clients_for_a_rebuild() {
        let state = test_state(2);
        let key = SubKey::L2Diff { coin: "BTC".into(), n_sig_figs: None, n_levels: None, mantissa: None };
        let (client, mut rx) = state.register_client("t".into());
        state.subscribe(&client, key.clone());

        let a = state.sources[0].clone();
        let b = state.sources[1].clone();

        state.on_update(&a, key.clone(), Seq::Snapshot(10), msg("snap-a"));
        state.on_update(&a, key.clone(), Seq::Sticky(12), msg("12-a"));
        assert_eq!(drain(&mut rx), vec!["snap-a", "12-a"]);

        // The channel is incremental, so the loss of its leader has to reach
        // the same machinery l4Book uses rather than passing unnoticed.
        let work = state.resync_after_source_loss(a.id);
        assert_eq!(work.len(), 1);
        assert_eq!(work[0].0, key);

        // Parked until it has a snapshot of its own. B's frames are held, not
        // delivered: applying B's increment to the book A built is exactly the
        // corruption this mode exists to prevent.
        state.on_update(&b, key.clone(), Seq::Snapshot(11), msg("snap-b"));
        state.on_update(&b, key.clone(), Seq::Sticky(12), msg("12-b"));
        assert!(drain(&mut rx).is_empty());
    }

    #[test]
    fn one_source_carries_a_stamp_to_the_end() {
        let state = test_state(2);
        let key = l2("BTC", None);
        let (client, mut rx) = state.register_client("t".into());
        state.subscribe(&client, key.clone());

        let a = state.sources[0].clone();
        let b = state.sources[1].clone();

        state.on_update(&a, key.clone(), Seq::Lead(10), msg("10-a1")); // a takes stamp 10
        state.on_update(&b, key.clone(), Seq::Lead(10), msg("10-b1")); // b's own view, dropped
        state.on_update(&a, key.clone(), Seq::Lead(10), msg("10-a2")); // a's fresher snapshot
        state.on_update(&b, key.clone(), Seq::Lead(11), msg("11-b1")); // b takes stamp 11
        state.on_update(&a, key.clone(), Seq::Lead(11), msg("11-a1")); // a trails now
        state.on_update(&a, key.clone(), Seq::Lead(10), msg("10-a3")); // stale

        // Every snapshot the stamp's owner produced, and nobody else's: the
        // client follows one source through a stamp, so the book only ever
        // moves forward. Newest-wins would have stopped at "10-a1".
        assert_eq!(drain(&mut rx), vec!["10-a1", "10-a2", "11-b1"]);

        assert_eq!(a.stats.wins.load(Relaxed), 1);
        assert_eq!(b.stats.wins.load(Relaxed), 1);
        assert_eq!(a.stats.stale.load(Relaxed), 1);
    }

    #[test]
    fn a_block_never_repeats_or_reorders_a_position() {
        let state = test_state(2);
        let key = SubKey::L4Book { coin: "BTC".into() };
        let (client, mut rx) = state.register_client("t".into());
        state.subscribe(&client, key.clone());

        let a = state.sources[0].clone();
        let b = state.sources[1].clone();

        state.on_update(&a, key.clone(), Seq::Block(10), msg("10-a1")); // a opens block 10
        state.on_update(&b, key.clone(), Seq::Block(10), msg("10-b1")); // b trails, dropped
        state.on_update(&a, key.clone(), Seq::Block(10), msg("10-a2")); // a continues
        state.on_update(&b, key.clone(), Seq::Block(11), msg("11-b1")); // b opens block 11
        state.on_update(&a, key.clone(), Seq::Block(11), msg("11-a1")); // a trails, dropped
        state.on_update(&a, key.clone(), Seq::Block(10), msg("10-a3")); // stale

        // Every position goes out exactly once and in order, whichever source
        // happened to reach it first.
        assert_eq!(drain(&mut rx), vec!["10-a1", "10-a2", "11-b1"]);

        // One win per block won, and one delay sample per trailing source per
        // block rather than one per message.
        assert_eq!(a.stats.wins.load(Relaxed), 1);
        assert_eq!(b.stats.wins.load(Relaxed), 1);
        assert_eq!(a.stats.stale.load(Relaxed), 1);
    }

    #[test]
    fn a_block_is_completed_by_whoever_is_still_alive() {
        let state = test_state(2);
        let key = SubKey::L4Book { coin: "BTC".into() };
        let (client, mut rx) = state.register_client("t".into());
        state.subscribe(&client, key.clone());
        let a = state.sources[0].clone();
        let b = state.sources[1].clone();

        // `a` opens block 10 and gets two positions out; `b` trails on the same
        // two, which are already covered.
        state.on_update(&a, key.clone(), Seq::Block(10), msg("m1"));
        state.on_update(&a, key.clone(), Seq::Block(10), msg("m2"));
        state.on_update(&b, key.clone(), Seq::Block(10), msg("m1"));
        state.on_update(&b, key.clone(), Seq::Block(10), msg("m2"));
        assert_eq!(drain(&mut rx), vec!["m1", "m2"]);

        // Now `a` dies mid-block. `b` carries the block on from position three:
        // no truncation, no snapshot, and no waiting on a timeout to notice.
        state.on_update(&b, key.clone(), Seq::Block(10), msg("m3"));
        state.on_update(&b, key.clone(), Seq::Block(10), msg("m4"));
        assert_eq!(drain(&mut rx), vec!["m3", "m4"]);
    }

    #[test]
    fn a_late_joiner_gets_its_snapshot_before_the_frames_held_behind_it() {
        let state = test_state(1);
        let key = SubKey::L4Book { coin: "BTC".into() };
        let a = state.sources[0].clone();

        let (first, mut first_rx) = state.register_client("first".into());
        assert_eq!(state.subscribe(&first, key.clone()), SubscribeOutcome::Fresh);

        state.on_update(&a, key.clone(), Seq::Block(10), msg("b10"));

        // Joining mid-stream parks the client: it must see nothing live yet.
        let (late, mut late_rx) = state.register_client("late".into());
        assert_eq!(state.subscribe(&late, key.clone()), SubscribeOutcome::Joined);

        state.on_update(&a, key.clone(), Seq::Block(11), msg("b11"));
        state.on_update(&a, key.clone(), Seq::Block(12), msg("b12"));
        assert!(drain(&mut late_rx).is_empty(), "held back until the snapshot lands");

        // Snapshot taken at height 11: block 11 is already baked into it and
        // would be applied twice, block 12 is not.
        state.deliver_snapshot(&key, late.id, 11, msg("snap@11"));
        assert_eq!(drain(&mut late_rx), vec!["snap@11", "b12"]);

        // ...and the client that was there first saw an unbroken stream.
        assert_eq!(drain(&mut first_rx), vec!["b10", "b11", "b12"]);
    }

    #[test]
    fn a_quiet_market_is_not_mistaken_for_dead_sources() {
        let state = test_state(2);
        for src in &state.sources {
            src.stats.connected.store(true, Relaxed);
        }
        // Silence is observed one window at a time, so the limit is one window.
        let limit = crate::stats::WINDOW;
        let deliver = |i: usize| {
            state.sources[i].stats.packets.fetch_add(1, Relaxed);
        };
        let tick = || {
            for src in &state.sources {
                src.stats.roll_window();
            }
        };

        // Nobody has delivered at all yet: startup, or simply no subscribers.
        tick();
        assert!(state.silent_sources(limit).is_empty());

        // Both delivered, then both fell quiet together. That is the market,
        // not a fault - resyncing here would throw 22 MB snapshots around for
        // nothing.
        deliver(0);
        deliver(1);
        tick();
        tick();
        tick();
        assert!(state.silent_sources(limit).is_empty());

        // Only source 0 is still delivering, so source 1 really is the odd one.
        deliver(0);
        tick();
        assert_eq!(state.silent_sources(limit), vec![1]);
    }

    #[test]
    fn a_first_subscriber_is_never_parked() {
        let state = test_state(1);
        let key = SubKey::L4Book { coin: "BTC".into() };
        let (client, mut rx) = state.register_client("t".into());

        // The upstream snapshot that opens the stream is this client's own.
        assert_eq!(state.subscribe(&client, key.clone()), SubscribeOutcome::Fresh);
        state.on_update(&state.sources[0].clone(), key, Seq::Snapshot(10), msg("snap"));
        assert_eq!(drain(&mut rx), vec!["snap"]);
    }

    #[test]
    fn a_stalled_source_cannot_drag_clients_forward_after_a_resync() {
        let state = test_state(2);
        let key = SubKey::L4Book { coin: "BTC".into() };
        let (client, mut rx) = state.register_client("t".into());
        state.subscribe(&client, key.clone());

        let a = state.sources[0].clone();
        let b = state.sources[1].clone();

        // `a` is ahead and leading. `b` lags far behind - which is what a spare
        // deliberately kept on a slower peer looks like.
        state.on_update(&a, key.clone(), Seq::Snapshot(1000), msg("snap@1000"));
        state.on_update(&a, key.clone(), Seq::Block(1001), msg("a-1001"));
        assert_eq!(drain(&mut rx), vec!["snap@1000", "a-1001"]);

        // `a` stalls. Its client is parked and rebuilt from `b`'s book at 950.
        let work = state.resync_after_source_loss(a.id);
        assert_eq!(work.len(), 1);
        state.deliver_snapshot(&key, client.id, 950, msg("snap@950"));
        assert_eq!(drain(&mut rx), vec!["snap@950"]);

        // `b` must now be able to drive the stream even though it is hundreds of
        // blocks behind where `a` had got to. Holding on to `a`'s old height
        // would leave every frame `b` sends looking stale, forever.
        state.on_update(&b, key.clone(), Seq::Block(951), msg("b-951"));
        assert_eq!(drain(&mut rx), vec!["b-951"]);

        // `a` comes back mid-stream. Its increments are far ahead of where the
        // client now sits, and forwarding them would skip fifty blocks with no
        // snapshot in between - silent corruption.
        state.on_update(&a, key.clone(), Seq::Block(1002), msg("a-1002"));
        assert!(drain(&mut rx).is_empty());

        // ...until it snapshots again, which is a reset the client can act on.
        state.on_update(&a, key.clone(), Seq::Snapshot(1003), msg("snap@1003"));
        assert_eq!(drain(&mut rx), vec!["snap@1003"]);
    }

    #[test]
    fn bbo_updates_sharing_a_timestamp_all_reach_the_client() {
        let state = test_state(2);
        let key = SubKey::Bbo { coin: "BTC".into() };
        let (client, mut rx) = state.register_client("t".into());
        state.subscribe(&client, key.clone());

        let a = state.sources[0].clone();
        let b = state.sources[1].clone();

        // Every top-of-book change inside a block carries the block's time, so
        // several updates share one value. Ordering them as points would keep
        // only the first — which is what cost some nine updates in ten.
        state.on_update(&a, key.clone(), Seq::Block(1000), msg("u1"));
        state.on_update(&a, key.clone(), Seq::Block(1000), msg("u2"));
        state.on_update(&a, key.clone(), Seq::Block(1000), msg("u3"));
        assert_eq!(drain(&mut rx), vec!["u1", "u2", "u3"]);

        // The other source's copies of the same three are duplicates.
        state.on_update(&b, key.clone(), Seq::Block(1000), msg("u1"));
        state.on_update(&b, key.clone(), Seq::Block(1000), msg("u2"));
        assert!(drain(&mut rx).is_empty());

        // ...but it takes over the moment it passes what has gone out, which is
        // all the failover this channel needs.
        state.on_update(&b, key.clone(), Seq::Block(1000), msg("u3"));
        state.on_update(&b, key.clone(), Seq::Block(1000), msg("u4"));
        assert_eq!(drain(&mut rx), vec!["u4"]);
    }

    #[test]
    fn bbo_is_never_treated_as_incremental() {
        // It is raced by position like l4Book but is not incremental, and the
        // difference matters twice over: a lost frame is harmless (the next one
        // carries the whole top of book), and — less obviously — only a snapshot
        // clears a source from the blacklist. bbo has no snapshots, so marking
        // it incremental would strand a silenced source there for good.
        assert!(!SubKey::Bbo { coin: "BTC".into() }.is_incremental());

        let state = test_state(2);
        let key = SubKey::Bbo { coin: "BTC".into() };
        let (client, _rx) = state.register_client("t".into());
        state.subscribe(&client, key.clone());
        state.on_update(&state.sources[0].clone(), key.clone(), Seq::Block(1000), msg("u1"));

        assert!(state.resync_after_source_loss(0).is_empty(), "bbo must not be resynced");
    }

    #[test]
    fn a_pinned_probe_outlives_its_clients() {
        let state = test_state(1);
        let key = SubKey::Bbo { coin: "BTC".into() };
        state.pin(key.clone());

        let (client, _rx) = state.register_client("t".into());
        state.subscribe(&client, key.clone());
        state.cleanup_client(&client);

        // The probe exists to keep the arbitration counters moving with nobody
        // connected, so the last client leaving must not take it down.
        assert!(state.subs.get(&key).is_some_and(|e| e.upstream_subscribed));
        assert_eq!(state.subscribed_requests().len(), 1);
    }
}
