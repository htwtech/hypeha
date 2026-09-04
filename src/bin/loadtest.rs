//! Load and continuity test for wsarb.
//!
//! Opens many subscriptions at once and, optionally, the same subscriptions
//! straight to each upstream node. Three questions it answers that nothing else
//! here does:
//!
//!   * **where the data breaks** — a heat strip per subscription, aligned in
//!     time, so a break on every strip at once reads as an upstream outage and a
//!     break on one reads as a proxy bug;
//!   * **what we lose** — every message is identified by a hash of its bytes, so
//!     a message a node emitted and wsarb did not is countable. Dropping a frame
//!     the proxy had already moved past is not loss, and is counted apart;
//!   * **what arbitration is worth** — first-seen times, against a measured
//!     noise floor. Without that floor the figure means nothing: two connections
//!     to the same node disagree by some amount all on their own, and anything
//!     smaller is measuring the test rather than the proxy.
//!
//! Each stream gets its own thread and its own runtime. Sharing one lets a busy
//! stream delay the others, and then the first-seen times record the scheduler.
//!
//! Frames are parsed by `wsarb::upstream::route`, the very code the proxy uses,
//! so the test cannot disagree with wsarb about what a message's key is.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message as TMessage;

use wsarb::state::{Seq, SubKey, SubRequest};
use wsarb::upstream::{connect_with, route, Frame};

/// wsarb, the comparison nodes, and the noise-floor twin. Fixed so that the
/// per-message record is a plain array rather than a heap allocation each.
const MAX_SLOTS: usize = 8;

#[derive(Parser)]
#[command(name = "loadtest", about = "Load and continuity test for wsarb")]
struct Args {
    /// The wsarb endpoint under test.
    #[arg(long, default_value = "ws://localhost:48082/ws")]
    url: String,

    /// Channel for the `--coins` fan-out.
    #[arg(long, default_value = "l4Book")]
    channel: String,

    /// Comma-separated coins to fan `--channel` across.
    #[arg(long, value_delimiter = ',')]
    coins: Vec<String>,

    /// One coin per line; for reaching every market without a mile of argv.
    #[arg(long)]
    coins_file: Option<String>,

    /// Explicit `CHANNEL:KEY` subscription, repeatable, for mixed sets.
    #[arg(long = "sub")]
    subs: Vec<String>,

    /// Upstream node to read directly, bypassing wsarb. Repeatable.
    ///
    /// This is what turns the run from "did it break" into "who broke it".
    #[arg(long = "compare")]
    compare: Vec<String>,

    /// Open a second connection to the first `--compare` node and measure how
    /// far apart two identical connections land.
    ///
    /// That spread is the floor of this whole measurement: any latency figure
    /// smaller than it is the test's own jitter, not the proxy's doing.
    #[arg(long)]
    noise_floor: bool,

    #[arg(long, default_value_t = 600.0)]
    seconds: f64,

    /// Spread the subscribe frames over this long instead of firing them at
    /// once. Subscribing to `l4Book` makes the upstream compute a snapshot while
    /// holding its listener lock, so a few hundred at once can stall it outright
    /// — running with 0 and then with 60 separates that storm from the steady
    /// load underneath it.
    #[arg(long, default_value_t = 0.0)]
    ramp: f64,

    /// Spread the subscriptions over this many connections to wsarb. One
    /// connection tests the per-client queue; many test the fan-out.
    #[arg(long, default_value_t = 1)]
    connections: usize,

    /// A gap this many times the subscription's own 99th-percentile gap counts
    /// as a stall. Against the median it was useless on an illiquid market, where
    /// the median sits inside a block's burst and every normal silence between
    /// blocks then looked like an outage.
    #[arg(long, default_value_t = 5.0)]
    stall_factor: f64,

    /// How long a message hash is kept for cross-stream comparison.
    #[arg(long, default_value_t = 30.0)]
    window: f64,

    #[arg(long, default_value_t = 10.0)]
    progress: f64,

    /// Book depth for `l2Book` and `l2Diff`. Ignored by every other channel.
    #[arg(long, default_value_t = 20)]
    levels: usize,

    /// `x-token` to present on connect, for reaching wsarb through a gateway
    /// that authenticates. Sent on every stream; a node reached directly
    /// ignores the header.
    #[arg(long)]
    token: Option<String>,
}

fn hash_of(text: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut h);
    h.finish()
}

/// The block time the frame carries, in unix milliseconds.
///
/// The first `"time":` in a frame is always the top-level one — `L4BookUpdates`
/// is `{time, height, order_statuses, …}` — while the `time` fields nested in
/// `order_statuses` are date strings, not numbers, so a digit check settles it.
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

/// Where a message was seen and when it was seen first there, plus what it was.
///
/// Slot 0 is wsarb; 1.. are the `--compare` nodes in order; the last is the
/// noise-floor twin when enabled.
struct Seen {
    at: [Option<Instant>; MAX_SLOTS],
    /// How old the frame was, per stream, in milliseconds. `u32::MAX` means the
    /// stream never had it, or it carried no timestamp.
    ///
    /// Kept per message so the age percentiles can be taken over the frames
    /// *every* stream delivered. Taken over each stream's own population they
    /// mislead: a stream that drops a few seconds of data drops precisely its
    /// oldest samples, and so scores better than one that delivered them.
    ages: [u32; MAX_SLOTS],
    born: Instant,
    /// Index into the subscription list, so a verdict can be reached without
    /// carrying the key's name on every message.
    key: u32,
    /// The ordering value, to tell a dropped stale frame from a lost one.
    seq: u64,
    /// The highest value wsarb had delivered for this subscription at the moment
    /// this frame first appeared anywhere.
    ///
    /// Taken here rather than at judgement time, which is a whole window later:
    /// by then wsarb has moved past almost anything, so every dropped frame read
    /// as a correct drop and real losses could not show up at all.
    born_max: u64,
}

/// How many messages each stream carried for one block of one subscription.
///
/// Comparing frames by hash cannot answer the question this exists for: a frame
/// missing from one stream may be a batch cut differently, or may simply be a
/// frame that stream lost in a stall, and the two look identical. Counting
/// messages per block separates them — a stall shows up as a handful of blocks
/// far apart, per-connection framing as a steady fraction of every block.
///
/// It matters because wsarb races positions *inside* a block, taking message #k
/// from whichever source reached it first. That is only sound if every source
/// cuts a given block into the same messages in the same order.
struct Block {
    born: Instant,
    n: [u32; MAX_SLOTS],
    /// Whether this stamp's channel races positions across sources.
    ///
    /// The same disagreement means opposite things in the two modes: on a raced
    /// channel it breaks the assumption the racing rests on, while on a led one
    /// it is the very reason that channel is led instead.
    raced: bool,
}

/// Everything measured about one subscription on one stream.
#[derive(Default)]
struct KeyStats {
    msgs: u64,
    bytes: u64,
    last_at: Option<Instant>,
    gaps: Vec<Duration>,
    /// A sample of the stalls, and the true count. The vectors are capped
    /// because only a handful are ever printed, and an unhealthy run produces
    /// tens of thousands.
    stalls: Vec<(Duration, Duration)>, // (since start, length)
    stall_count: u64,
    violations: Vec<String>,
    violation_count: u64,
    /// Cached stall threshold and how many messages since it was last derived.
    limit: Option<Duration>,
    since_limit: u32,
    last_seq: Option<u64>,
    /// Messages per elapsed second, for the heat strip.
    per_second: Vec<u32>,
    /// How old each message was on arrival, in milliseconds.
    ///
    /// The frame carries the block's own timestamp, and everything here runs on
    /// one machine, so this is an absolute figure: how stale the data was by the
    /// time a client could see it. Unlike a difference of two arrival times it
    /// needs no reference stream and does not drown in per-connection spread.
    ages_ms: Vec<u32>,
}

impl KeyStats {
    fn note(&mut self, now: Instant, start: Instant, len: usize, stall_factor: f64) {
        self.msgs += 1;
        self.bytes += len as u64;

        let sec = now.duration_since(start).as_secs() as usize;
        if self.per_second.len() <= sec {
            self.per_second.resize(sec + 1, 0);
        }
        self.per_second[sec] += 1;

        if let Some(prev) = self.last_at {
            let gap = now.duration_since(prev);
            // The threshold cannot be a constant: l4Book ticks every half a
            // millisecond, trades on an illiquid coin every few seconds. Judge
            // each subscription against its own habits — but cache it. Deriving
            // it per message meant cloning and sorting ten thousand gaps for
            // every frame, and that alone saturated the reading thread, stopped
            // the socket being drained, and got this client hung up on.
            self.since_limit += 1;
            if self.since_limit >= 256 {
                self.since_limit = 0;
                self.limit = self
                    .gap_quantile(0.99)
                    .map(|q| q.mul_f64(stall_factor).max(Duration::from_millis(250)));
            }
            if self.limit.is_some_and(|limit| gap > limit) {
                self.stall_count += 1;
                if self.stalls.len() < 64 {
                    self.stalls.push((prev.duration_since(start), gap));
                }
            }
            if self.gaps.len() < 10_000 {
                self.gaps.push(gap);
            }
        }
        self.last_at = Some(now);
    }

    /// Close the gap still open when the run ended.
    ///
    /// `note` only ever measures a gap when the *next* message arrives, so a
    /// stream that fell silent and never came back leaves its last gap unclosed
    /// and uncounted — the one shape of failure this test exists to catch, and
    /// it was reading as a perfectly healthy stream.
    fn close(&mut self, end: Instant, start: Instant) {
        let Some(prev) = self.last_at else { return };
        let gap = end.saturating_duration_since(prev);
        // The same rule as inside the run, so a quiet market at the end is not
        // mistaken for an outage.
        if self.limit.is_some_and(|limit| gap > limit) {
            self.stall_count += 1;
            if self.stalls.len() < 64 {
                self.stalls.push((prev.duration_since(start), gap));
            }
        }
    }

    /// A quantile of the observed gaps, used to set the stall threshold.
    ///
    /// Deliberately not the median. On an illiquid market the gaps are bimodal:
    /// a burst within one block, then a long and entirely legitimate silence
    /// until the next. A median sits in the burst, so every normal silence
    /// looked like a stall — which is where the twenty-six thousand of them in
    /// the 176-market run came from.
    fn gap_quantile(&self, p: f64) -> Option<Duration> {
        if self.gaps.len() < 32 {
            return None;
        }
        let mut v = self.gaps.clone();
        v.sort_unstable();
        Some(v[(((v.len() - 1) as f64) * p) as usize])
    }

    fn note_age(&mut self, text: &str, now_ms: u64) -> Option<u32> {
        let t = block_time_ms(text)?;
        // A frame stamped in the future means the clocks are not shared,
        // which the whole measure depends on; drop rather than report a
        // flattering number.
        if now_ms < t {
            return None;
        }
        let age = (now_ms - t).min(u32::MAX as u64 - 1) as u32;
        self.ages_ms.push(age);
        Some(age)
    }

    /// Age at the given percentile, in milliseconds.
    fn age_pct(&self, p: f64) -> Option<u32> {
        if self.ages_ms.is_empty() {
            return None;
        }
        let mut v = self.ages_ms.clone();
        v.sort_unstable();
        Some(v[(((v.len() - 1) as f64) * p) as usize])
    }

    /// Order the frame against what came before, allowing for the fact that a
    /// snapshot is a reset rather than an increment.
    fn order(&mut self, seq: Seq, label: &str) {
        match seq {
            Seq::Snapshot(_) => self.last_seq = None,
            Seq::Point(v) | Seq::Block(v) | Seq::Lead(v) | Seq::Sticky(v) => {
                if let Some(prev) = self.last_seq {
                    // Gaps are not violations: with nothing happening for a coin
                    // in a block the upstream sends nothing for it at all.
                    if v < prev {
                        self.violation_count += 1;
                        if self.violations.len() < 64 {
                            self.violations.push(format!("{label}: {prev} -> {v}"));
                        }
                    }
                }
                self.last_seq = Some(v);
            }
        }
    }
}

#[derive(Default)]
struct Stream {
    name: String,
    keys: Mutex<HashMap<String, KeyStats>>,
    rejected: AtomicU64,
    /// Age of arriving frames, summed per elapsed second.
    ///
    /// Aggregate percentiles say how bad a run was but not *when*. A subscribe
    /// storm and a sick upstream produce similar totals and are told apart only
    /// by whether the damage was confined to the opening seconds.
    age_by_sec: Mutex<Vec<(u64, u32)>>,
    /// Nanoseconds this stream's thread spent processing rather than waiting.
    ///
    /// The figure that matters at scale, and the one the sleep-overshoot
    /// watchdog cannot see: with spare cores, a thread pinned at 100% delays no
    /// sleeper at all, so `own lag` stayed at 1 ms while the reader was too busy
    /// to drain its socket and was hung up on for it.
    busy_ns: AtomicU64,
    /// When and why a connection ended before the run was over.
    ///
    /// A vector because `--connections N` gives one `Stream` several sockets.
    /// Worth keeping apart from everything else: a socket wsarb closed on a full
    /// queue leaves a flat heat strip, which reads as a stall — the opposite
    /// diagnosis, and the one expected first as the number of markets grows.
    ended: Mutex<Vec<(Duration, String)>>,
}

/// What the cross-stream comparison concluded, accumulated as hashes age out.
#[derive(Default)]
struct Verdict {
    matched: AtomicU64,
    /// A node had it, wsarb did not, and wsarb had *not* already moved past it.
    /// This is the number that matters.
    lost: AtomicU64,
    /// A node had it, wsarb did not, but wsarb had already delivered something
    /// newer for that subscription. Discarding it is correct — a replaying node
    /// produces a great many of these and they are not losses.
    stale_dropped: AtomicU64,
    /// A node had it, wsarb did not, but wsarb had already delivered a
    /// *different* frame carrying the same ordering value.
    ///
    /// On a channel of self-contained snapshots that is arbitration working:
    /// the client got a snapshot of that block, just not this source's copy of
    /// it. On an incremental channel the same thing would be a hole, so the two
    /// are counted apart rather than lumped into either neighbour.
    superseded: AtomicU64,
    /// A node had it, wsarb never carried that stamp at all — but by the time
    /// the frame was judged wsarb had already delivered a *newer* one.
    ///
    /// On a channel of full snapshots this is not a loss but the right thing to
    /// do: handing the client the older stamp afterwards would walk its book
    /// backwards. Counted apart from `lost` so that chasing loss to zero does
    /// not chase a number that describes correct behaviour.
    skipped: AtomicU64,
    /// wsarb had it, no node did.
    invented: AtomicU64,
    /// Microseconds by which wsarb trailed the first node to deliver.
    behind_us: AtomicU64,
    behind_n: AtomicU64,
    /// Times wsarb beat every node. Not a triumph: it means the reference
    /// connection was the slower one, which is what the noise floor quantifies.
    ahead_us: AtomicU64,
    ahead_n: AtomicU64,
    /// Arrived too near the end to judge: the other streams had not been given
    /// the full window to produce their copy, so calling it lost would be this
    /// test's own edge rather than wsarb's doing.
    undecided: AtomicU64,
    /// Spread between two connections to the same node — the measurement floor.
    noise_us: AtomicU64,
    noise_n: AtomicU64,
    noise_max_us: AtomicU64,
    /// Which of those two identical connections got there first.
    ///
    /// The control for the whole comparison. Two connections to one node have
    /// no advantage over each other, so this must land near half; whatever it
    /// actually lands on is the baseline wsarb's share has to beat.
    twin_first: AtomicU64,
    twin_second: AtomicU64,
    /// Frames carried by both connections to the same node, and by only one.
    ///
    /// Two connections to one node ought to carry byte-identical frames. Two
    /// things rest on that: this test identifies a message by hashing it, and
    /// wsarb races positions *within* a block, which splices one source's
    /// messages onto another's. If the same node cuts batches differently per
    /// connection, `invented` is an artefact of framing rather than anything
    /// wsarb did — and the splicing is unsound.
    twin_both: AtomicU64,
    node_only: AtomicU64,
    twin_only: AtomicU64,
    /// Blocks every stream carried, and whether the pairs cut them alike. See
    /// [`Block`] — this is the measurement the hash comparison cannot make.
    /// Indexed by mode: 0 = led (`l2Book`), 1 = raced (`l4Book`, `bbo`,
    /// `orderUpdates`). Kept apart because the same disagreement is a fault in
    /// one and a confirmation in the other.
    blk_seen: [AtomicU64; 2],
    blk_same_agree: [AtomicU64; 2],
    blk_same_differ: [AtomicU64; 2],
    /// Stamps where wsarb carried fewer messages than the best node did, and the
    /// total shortfall. The direct answer to "is the proxy dropping updates
    /// inside a stamp", which the hash comparison cannot give on a channel whose
    /// nodes do not emit byte-identical frames.
    blk_short: AtomicU64,
    blk_short_frames: AtomicU64,
    blk_cross_agree: [AtomicU64; 2],
    blk_cross_differ: [AtomicU64; 2],
}

/// One bucket per millisecond, and the last one is everything beyond.
///
/// The range has to cover a sick upstream, not just a healthy one: at 10 s a
/// node replaying a backlog put every single frame in the top bucket and the
/// table then reported 9999 ms as though it were a percentile.
const AGE_BUCKETS: usize = 120_001;

/// Percentile read off a histogram of one bucket per millisecond.
fn pct_of(hist: &[u32], p: f64) -> u32 {
    let total: u64 = hist.iter().map(|&c| c as u64).sum();
    if total == 0 {
        return 0;
    }
    let want = ((total - 1) as f64 * p) as u64;
    let mut seen = 0u64;
    for (i, &c) in hist.iter().enumerate() {
        seen += c as u64;
        if seen > want {
            return i as u32;
        }
    }
    (hist.len() - 1) as u32
}

const RAMP_STEPS: &str = " .:-=+*#";

fn heat(counts: &[u32], secs: usize, others_alive: &[bool]) -> String {
    let peak = counts.iter().copied().max().unwrap_or(0).max(1) as f64;
    let chars: Vec<char> = RAMP_STEPS.chars().collect();
    // Nothing before the first message is a gap in the data — with `--ramp` the
    // subscription simply had not been made yet, and marking those seconds as
    // silence would paint the whole ramp as an outage.
    let first = counts.iter().position(|&n| n > 0);
    (0..secs)
        .map(|i| {
            if first.map_or(true, |f| i < f) {
                return '·';
            }
            let n = counts.get(i).copied().unwrap_or(0);
            if n == 0 {
                // Silence only means something if anybody else had data.
                return if others_alive.get(i).copied().unwrap_or(false) { '!' } else { ' ' };
            }
            let frac = (n as f64).ln_1p() / peak.ln_1p();
            chars[((frac * (chars.len() - 1) as f64).round() as usize).min(chars.len() - 1)]
        })
        .collect()
}

struct StreamCtx {
    url: String,
    subs: Vec<SubKey>,
    slot: usize,
    seen: Arc<DashMap<u64, Seen>>,
    blocks: Arc<DashMap<(u32, u64), Block>>,
    stream: Arc<Stream>,
    /// Highest ordering value wsarb has delivered per subscription, so the
    /// sweeper can tell a correct drop from a real loss.
    wsarb_max: Arc<Vec<AtomicU64>>,
    keys: Arc<HashMap<String, u32>>,
    start: Instant,
    stop: Arc<AtomicBool>,
    ramp: f64,
    stall_factor: f64,
    seconds: f64,
    token: Option<String>,
}

/// Adds the time spent in its scope to a counter, by whatever path the scope is
/// left.
struct Busy<'a>(&'a AtomicU64, Instant);

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(self.1.elapsed().as_nanos() as u64, Relaxed);
    }
}

/// Record a connection that ended before the run did.
///
/// A close in the last couple of seconds is the run winding down and says
/// nothing about the proxy, so it is not recorded.
fn note_end(stream: &Stream, at: Duration, seconds: f64, reason: String) {
    if at.as_secs_f64() + 2.0 < seconds {
        stream.ended.lock().unwrap().push((at, reason));
    }
}

async fn run_stream(mut ctx: StreamCtx) {
    let ws = match connect_with(&ctx.url, ctx.token.as_deref()).await {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("[slot {}] could not connect to {}: {}", ctx.slot, ctx.url, e);
            return;
        }
    };
    let (mut write, mut read) = ws.split();

    // Pace the subscribe frames if asked: firing hundreds at once makes the
    // upstream compute that many snapshots back to back under one lock.
    let gap = if ctx.ramp > 0.0 && ctx.subs.len() > 1 {
        Duration::from_secs_f64(ctx.ramp / ctx.subs.len() as f64)
    } else {
        Duration::ZERO
    };

    // Subscribing runs alongside reading, not before it. Data starts flowing
    // from the first subscription onwards, so a loop that spends the whole ramp
    // sending and never draining the socket gets its queue filled and is hung up
    // on — which then surfaces as "subscribe failed" and hides what happened.
    let subs = std::mem::take(&mut ctx.subs);
    let stream = ctx.stream.clone();
    let (slot, start, seconds) = (ctx.slot, ctx.start, ctx.seconds);
    tokio::spawn(async move {
        for (i, key) in subs.iter().enumerate() {
            let req = SubRequest::Subscribe { subscription: key }.json();
            if write.send(TMessage::Text(req.into())).await.is_err() {
                let n = subs.len();
                eprintln!("[slot {slot}] subscribe failed after {i} of {n}");
                note_end(
                    &stream,
                    start.elapsed(),
                    seconds,
                    format!("subscribe failed after {i} of {n}"),
                );
                return;
            }
            if !gap.is_zero() {
                tokio::time::sleep(gap).await;
            }
        }
    });

    let deadline = ctx.start + Duration::from_secs_f64(ctx.seconds);
    while !ctx.stop.load(Relaxed) {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        // Three different things ended this loop before, all through one silent
        // `break`. Only the timeout is routine; the other two are the socket
        // going away, and that has to be said out loud.
        let msg = match tokio::time::timeout(left, read.next()).await {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => {
                note_end(&ctx.stream, ctx.start.elapsed(), ctx.seconds, e.to_string());
                break;
            }
            Ok(None) => {
                note_end(&ctx.stream, ctx.start.elapsed(), ctx.seconds, "closed by peer".into());
                break;
            }
            Err(_) => break,
        };
        let TMessage::Text(text) = msg else { continue };
        let now = Instant::now();
        // Everything below this point is work, not waiting. Timed through a
        // guard so the several `continue` paths cannot escape the accounting.
        let _busy = Busy(&ctx.stream.busy_ns, now);
        let text = text.as_str();

        if text.starts_with(r#"{"channel":"error"#) {
            ctx.stream.rejected.fetch_add(1, Relaxed);
            continue;
        }

        let Ok(frame) = serde_json::from_str::<Frame>(text) else { continue };
        let Some((key, seq)) = route(frame) else { continue };
        let label = key.label();
        let seq_val = match seq {
            Seq::Point(v) | Seq::Block(v) | Seq::Snapshot(v) | Seq::Lead(v) | Seq::Sticky(v) => v,
        };

        let age = {
            let now_ms = unix_ms();
            let mut keys = ctx.stream.keys.lock().unwrap();
            let st = keys.entry(label.clone()).or_default();
            st.note(now, ctx.start, text.len(), ctx.stall_factor);
            st.order(seq, &label);
            st.note_age(text, now_ms)
        };

        if let Some(age) = age {
            let sec = now.duration_since(ctx.start).as_secs() as usize;
            let mut v = ctx.stream.age_by_sec.lock().unwrap();
            if v.len() <= sec {
                v.resize(sec + 1, (0, 0));
            }
            v[sec].0 += age as u64;
            v[sec].1 += 1;
        }

        let key_idx = ctx.keys.get(&label).copied().unwrap_or(u32::MAX);
        if ctx.slot == 0 && key_idx != u32::MAX {
            ctx.wsarb_max[key_idx as usize].fetch_max(seq_val, Relaxed);
        }

        let mut e = ctx.seen.entry(hash_of(text)).or_insert_with(|| Seen {
            at: [None; MAX_SLOTS],
            ages: [u32::MAX; MAX_SLOTS],
            born: now,
            key: key_idx,
            seq: seq_val,
            born_max: if key_idx == u32::MAX {
                0
            } else {
                ctx.wsarb_max[key_idx as usize].load(Relaxed)
            },
        });
        if e.at[ctx.slot].is_none() {
            e.at[ctx.slot] = Some(now);
            if let Some(age) = age {
                e.ages[ctx.slot] = age;
            }
        }
        drop(e);

        // Every mode where one stamp covers several messages. A snapshot stands
        // alone, and `Point` values rise per message, so neither has anything to
        // count.
        let stamp = match seq {
            Seq::Block(h) => Some((h, true)),
            // Led, not raced: counted for the stamp report, but a divergence
            // there is expected rather than alarming.
            Seq::Lead(h) | Seq::Sticky(h) => Some((h, false)),
            Seq::Point(_) | Seq::Snapshot(_) => None,
        };
        if let (Some((h, raced)), true) = (stamp, key_idx != u32::MAX) {
            ctx.blocks
                .entry((key_idx, h))
                .or_insert_with(|| Block { born: now, n: [0; MAX_SLOTS], raced })
                .n[ctx.slot] += 1;
        }
    }
}

/// Age hashes out of the comparison map, deciding each one as it goes.
///
/// Eviction is the right moment to judge: by then every stream that was going
/// to carry the message has had `--window` to do so.
async fn sweeper(
    seen: Arc<DashMap<u64, Seen>>,
    blocks: Arc<DashMap<(u32, u64), Block>>,
    verdict: Arc<Verdict>,
    wsarb_max: Arc<Vec<AtomicU64>>,
    window: Duration,
    compare_slots: std::ops::Range<usize>,
    noise_slot: Option<usize>,
    used_slots: usize,
    matched_ages: Arc<Mutex<Vec<Vec<u32>>>>,
    stop: Arc<AtomicBool>,
) {
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    loop {
        ticker.tick().await;
        let finishing = stop.load(Relaxed);
        let now = Instant::now();
        // Held across the whole pass rather than taken per message: this is the
        // only thread that writes it, and the report only reads it after the
        // run is over.
        let mut ages = matched_ages.lock().unwrap();
        seen.retain(|_, e| {
            if now.duration_since(e.born) < window {
                // On the last pass these are discarded unjudged: the streams
                // stop at slightly different moments, and a message still in
                // flight has not had its chance to appear everywhere.
                if finishing {
                    verdict.undecided.fetch_add(1, Relaxed);
                    return false;
                }
                return true;
            }

            // Both of these connect to the same node, so a frame on one and not
            // the other means that node framed the same events differently for
            // each — which would explain `invented` without wsarb being at
            // fault, and would also undo the assumption that positions inside a
            // block are the same everywhere.
            if let Some(twin) = noise_slot {
                match (e.at[compare_slots.start], e.at[twin]) {
                    (Some(b), Some(a)) => {
                        verdict.twin_both.fetch_add(1, Relaxed);
                        let d = if a > b { a - b } else { b - a };
                        verdict.noise_us.fetch_add(d.as_micros() as u64, Relaxed);
                        verdict.noise_n.fetch_add(1, Relaxed);
                        verdict.noise_max_us.fetch_max(d.as_micros() as u64, Relaxed);
                        if a < b {
                            verdict.twin_first.fetch_add(1, Relaxed);
                        } else {
                            verdict.twin_second.fetch_add(1, Relaxed);
                        }
                    }
                    (Some(_), None) => {
                        verdict.node_only.fetch_add(1, Relaxed);
                    }
                    (None, Some(_)) => {
                        verdict.twin_only.fetch_add(1, Relaxed);
                    }
                    (None, None) => {}
                }
            }

            // Age percentiles over the frames every stream carried. A stream
            // that stalled and lost data is missing exactly its oldest samples,
            // so comparing each stream's own population rewards losing data.
            if (0..used_slots).all(|s| e.ages[s] != u32::MAX) {
                for s in 0..used_slots {
                    ages[s][(e.ages[s] as usize).min(AGE_BUCKETS - 1)] += 1;
                }
            }

            let ours = e.at[0];
            let theirs = compare_slots.clone().filter_map(|s| e.at[s]).min();
            match (ours, theirs) {
                (Some(o), Some(t)) => {
                    verdict.matched.fetch_add(1, Relaxed);
                    if o >= t {
                        verdict.behind_us.fetch_add((o - t).as_micros() as u64, Relaxed);
                        verdict.behind_n.fetch_add(1, Relaxed);
                    } else {
                        verdict.ahead_us.fetch_add((t - o).as_micros() as u64, Relaxed);
                        verdict.ahead_n.fetch_add(1, Relaxed);
                    }
                }
                (None, Some(_)) => {
                    // Four different things, and only the last is a hole. The
                    // first two are answered from what wsarb had done when the
                    // frame appeared; the third needs what it had done by now,
                    // and the stamp map answers whether it carried that stamp at
                    // all. `blocks` is swept after `seen` in this same tick, so
                    // the entry is still there to ask.
                    let carried = e.key != u32::MAX
                        && blocks.get(&(e.key, e.seq)).is_some_and(|b| b.n[0] > 0);
                    let ahead = e.key != u32::MAX
                        && wsarb_max[e.key as usize].load(Relaxed) > e.seq;
                    let counter = if e.key != u32::MAX && e.born_max > e.seq {
                        &verdict.stale_dropped
                    } else if carried {
                        &verdict.superseded
                    } else if ahead {
                        &verdict.skipped
                    } else {
                        &verdict.lost
                    };
                    counter.fetch_add(1, Relaxed);
                }
                (Some(_), None) => {
                    verdict.invented.fetch_add(1, Relaxed);
                }
                (None, None) => {}
            }
            false
        });
        drop(ages);

        // Whether the pairs cut a block into the same messages. Judged only on
        // blocks every stream carried, so a stream that was not subscribed yet
        // cannot register as a disagreement.
        blocks.retain(|_, b| {
            if now.duration_since(b.born) < window {
                return !finishing;
            }
            if (0..used_slots).any(|s| b.n[s] == 0) {
                return false;
            }
            let mode = usize::from(b.raced);
            verdict.blk_seen[mode].fetch_add(1, Relaxed);
            if let Some(twin) = noise_slot {
                let c = if b.n[compare_slots.start] == b.n[twin] {
                    &verdict.blk_same_agree[mode]
                } else {
                    &verdict.blk_same_differ[mode]
                };
                c.fetch_add(1, Relaxed);
            }
            let best = compare_slots.clone().map(|s| b.n[s]).max().unwrap_or(0);
            if b.n[0] < best {
                verdict.blk_short.fetch_add(1, Relaxed);
                verdict.blk_short_frames.fetch_add(u64::from(best - b.n[0]), Relaxed);
            }
            if compare_slots.end > compare_slots.start + 1 {
                let c = if b.n[compare_slots.start] == b.n[compare_slots.start + 1] {
                    &verdict.blk_cross_agree[mode]
                } else {
                    &verdict.blk_cross_differ[mode]
                };
                c.fetch_add(1, Relaxed);
            }
            false
        });

        if finishing {
            return;
        }
    }
}

fn parse_sub(spec: &str, levels: usize) -> Option<SubKey> {
    let (channel, key) = spec.split_once(':')?;
    // The upstream refuses an explicit 20 -- that is its default, asked for by
    // leaving the field out.
    let n_levels = (levels != 20).then_some(levels);
    Some(match channel {
        "bbo" => SubKey::Bbo { coin: key.into() },
        "l2Book" => SubKey::L2Book {
            coin: key.into(),
            n_sig_figs: None,
            n_levels,
            mantissa: None,
        },
        "l2Diff" => SubKey::L2Diff {
            coin: key.into(),
            n_sig_figs: None,
            n_levels,
            mantissa: None,
        },
        "l4Book" => SubKey::L4Book { coin: key.into() },
        "trades" => SubKey::Trades { coin: key.into() },
        "bookDiffs" => SubKey::BookDiffs { coin: key.into() },
        "orderUpdates" => SubKey::OrderUpdates { user: key.to_ascii_lowercase() },
        _ => return None,
    })
}

/// Run one stream on a thread and runtime of its own.
///
/// A shared runtime lets a busy stream delay the others, and the first-seen
/// times then record the scheduler rather than the network.
fn spawn_stream(ctx: StreamCtx) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(run_stream(ctx));
    })
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut subs: Vec<SubKey> = Vec::new();
    let mut coins = args.coins.clone();
    if let Some(path) = &args.coins_file {
        for line in std::fs::read_to_string(path)?.lines() {
            let c = line.trim();
            if !c.is_empty() && !c.starts_with('#') {
                coins.push(c.to_string());
            }
        }
    }
    for coin in &coins {
        match parse_sub(&format!("{}:{}", args.channel, coin), args.levels) {
            Some(k) => subs.push(k),
            None => anyhow::bail!("unknown channel {}", args.channel),
        }
    }
    for spec in &args.subs {
        match parse_sub(spec, args.levels) {
            Some(k) => subs.push(k),
            None => anyhow::bail!("cannot parse subscription {spec}"),
        }
    }
    if subs.is_empty() {
        anyhow::bail!("nothing to subscribe to: pass --coins or --sub");
    }

    let noise_slot = if args.noise_floor && !args.compare.is_empty() {
        Some(1 + args.compare.len())
    } else {
        None
    };
    let used_slots = 1 + args.compare.len() + usize::from(noise_slot.is_some());
    if used_slots > MAX_SLOTS {
        anyhow::bail!("at most {} streams; got {used_slots}", MAX_SLOTS);
    }

    let key_index: Arc<HashMap<String, u32>> = Arc::new(
        subs.iter().enumerate().map(|(i, k)| (k.label(), i as u32)).collect(),
    );
    let wsarb_max: Arc<Vec<AtomicU64>> =
        Arc::new((0..subs.len()).map(|_| AtomicU64::new(0)).collect());

    println!(
        "{} subscriptions, {} connection(s) to wsarb, {} comparison stream(s){}, {:.0}s",
        subs.len(),
        args.connections,
        args.compare.len(),
        if noise_slot.is_some() { " + noise-floor twin" } else { "" },
        args.seconds
    );
    // Which endpoint was measured, and whether a token was presented.
    // Without this the report cannot say whether a run went through the
    // gateway or straight to the loopback port, and those differ.
    println!(
        "wsarb at {}{}",
        args.url,
        if args.token.is_some() { " (with x-token)" } else { "" }
    );
    println!("each stream on its own thread and runtime\n");

    let start = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let seen: Arc<DashMap<u64, Seen>> = Arc::new(DashMap::new());
    let blocks: Arc<DashMap<(u32, u64), Block>> = Arc::new(DashMap::new());
    let verdict = Arc::new(Verdict::default());

    let mut streams: Vec<Arc<Stream>> = Vec::new();
    let mut threads = Vec::new();

    let mk = |url: String, subs: Vec<SubKey>, slot: usize, stream: Arc<Stream>| StreamCtx {
        url,
        subs,
        slot,
        seen: seen.clone(),
        blocks: blocks.clone(),
        stream,
        wsarb_max: wsarb_max.clone(),
        keys: key_index.clone(),
        start,
        stop: stop.clone(),
        ramp: args.ramp,
        stall_factor: args.stall_factor,
        seconds: args.seconds,
        token: args.token.clone(),
    };

    let matched_ages: Arc<Mutex<Vec<Vec<u32>>>> =
        Arc::new(Mutex::new(vec![vec![0u32; AGE_BUCKETS]; used_slots]));

    let wsarb = Arc::new(Stream { name: "wsarb".into(), ..Default::default() });
    streams.push(wsarb.clone());
    let per = subs.len().div_ceil(args.connections.max(1));
    for chunk in subs.chunks(per) {
        threads.push(spawn_stream(mk(args.url.clone(), chunk.to_vec(), 0, wsarb.clone())));
    }

    for (i, url) in args.compare.iter().enumerate() {
        let s = Arc::new(Stream { name: url.clone(), ..Default::default() });
        streams.push(s.clone());
        threads.push(spawn_stream(mk(url.clone(), subs.clone(), i + 1, s)));
    }

    if let Some(slot) = noise_slot {
        let url = args.compare[0].clone();
        let s = Arc::new(Stream { name: format!("{url} (twin)"), ..Default::default() });
        streams.push(s.clone());
        threads.push(spawn_stream(mk(url, subs.clone(), slot, s)));
    }

    {
        let seen = seen.clone();
        let blocks = blocks.clone();
        let wsarb_max = wsarb_max.clone();
        let verdict = verdict.clone();
        let matched_ages = matched_ages.clone();
        let stop = stop.clone();
        let window = Duration::from_secs_f64(args.window);
        let compare_slots = 1..1 + args.compare.len();
        threads.push(std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(sweeper(
                seen,
                blocks,
                verdict,
                wsarb_max,
                window,
                compare_slots,
                noise_slot,
                used_slots,
                matched_ages,
                stop,
            ));
        }));
    }

    // Whether this process is itself the bottleneck. Reading several heavy
    // streams at once is real work, and once the machine saturates every
    // latency figure below is partly measuring the test. A plain sleep that
    // overshoots is the cheapest honest signal of that.
    let lag_us = Arc::new(AtomicU64::new(0));
    {
        let lag_us = lag_us.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            const TICK: Duration = Duration::from_millis(100);
            while !stop.load(Relaxed) {
                let due = Instant::now() + TICK;
                std::thread::sleep(TICK);
                let over = Instant::now().saturating_duration_since(due);
                lag_us.fetch_max(over.as_micros() as u64, Relaxed);
            }
        });
    }

    if args.progress > 0.0 {
        let wsarb = wsarb.clone();
        let stop = stop.clone();
        let every = Duration::from_secs_f64(args.progress);
        let lag_us = lag_us.clone();
        // The comparison map holds --window seconds of hashes across every
        // stream, so it grows with the number of markets. Shown here because
        // otherwise its growth is only visible from outside the process.
        let seen = seen.clone();
        std::thread::spawn(move || {
            while !stop.load(Relaxed) {
                std::thread::sleep(every);
                if stop.load(Relaxed) {
                    break;
                }
                let keys = wsarb.keys.lock().unwrap();
                let msgs: u64 = keys.values().map(|k| k.msgs).sum();
                let mb: f64 = (keys.values().map(|k| k.bytes as f64).sum::<f64>() / 1e6).max(0.0);
                let stalls: u64 = keys.values().map(|k| k.stall_count).sum();
                drop(keys);
                let el = start.elapsed().as_secs_f64();
                println!(
                    "{:>5.0}s  {:>9} msgs  {:>7.1} MB  {:>6.0} msg/s  {} stalls  \
                     own lag {:.0} ms  seen {}  busy {:.0}%",
                    el,
                    msgs,
                    mb,
                    msgs as f64 / el,
                    stalls,
                    lag_us.load(Relaxed) as f64 / 1000.0,
                    seen.len(),
                    wsarb.busy_ns.load(Relaxed) as f64 / 1e9 / el * 100.0
                );
            }
        });
    }

    std::thread::sleep(Duration::from_secs_f64(args.seconds) + Duration::from_millis(500));
    stop.store(true, Relaxed);
    for t in threads {
        let _ = t.join();
    }

    let ended = Instant::now();
    let secs = start.elapsed().as_secs().max(1) as usize;
    let mut failed = false;

    // A stream that died mid-run has one gap nobody measured: the one running to
    // the end of the run. Close it before anything is reported.
    for stream in &streams {
        for k in stream.keys.lock().unwrap().values_mut() {
            k.close(ended, start);
        }
    }

    let alive: Vec<bool> = {
        let keys = wsarb.keys.lock().unwrap();
        (0..secs)
            .map(|i| keys.values().any(|k| k.per_second.get(i).copied().unwrap_or(0) > 0))
            .collect()
    };

    for stream in &streams {
        println!("\n=== {} ===", stream.name);
        // Before anything else: without this the strips below would be read as
        // stalls, and a socket that went away is a different fault entirely.
        {
            let ended = stream.ended.lock().unwrap();
            for (at, why) in ended.iter() {
                println!("!! closed the connection at +{:.0}s: {why}", at.as_secs_f64());
            }
            if !ended.is_empty() {
                println!("   flat strips after that point are the disconnect, not a stall");
                if Arc::ptr_eq(stream, &wsarb) {
                    failed = true;
                }
            }
        }
        let keys = stream.keys.lock().unwrap();
        let mut names: Vec<&String> = keys.keys().collect();
        names.sort();
        // One strip per subscription is the point of this section, but at every
        // market it is 176 of them per stream and the report stops being
        // readable — or even pasteable. Past the cap, show the ones with
        // something wrong and say how many were left out.
        const MAX_STRIPS: usize = 12;
        let mut shown: Vec<&String> = names.clone();
        if names.len() > MAX_STRIPS {
            shown.sort_by_key(|n| {
                let k = &keys[*n];
                // Worst first: stalls, then staleness.
                (std::cmp::Reverse(k.stall_count), std::cmp::Reverse(k.age_pct(0.50).unwrap_or(0)))
            });
            shown.truncate(MAX_STRIPS);
            shown.sort();
        }
        let width = shown.iter().map(|n| n.len()).max().unwrap_or(0);
        for name in &shown {
            let k = &keys[*name];
            // Per-subscription age matters once there are many coins: a single
            // lagging market is invisible in the aggregate.
            let age = k.age_pct(0.50).map_or_else(|| "    —".into(), |m| format!("{m:>5}"));
            println!("{:width$}  {}  {age} ms", name, heat(&k.per_second, secs, &alive));
        }
        if names.len() > shown.len() {
            println!(
                "{:width$}  ({} more not shown; these are the {} with the most stalls)",
                "",
                names.len() - shown.len(),
                shown.len()
            );
            // The aggregate still has to be there, or a break that hits every
            // subscription at once would vanish along with the strips.
            let mut all = vec![0u32; secs];
            for k in keys.values() {
                for (i, n) in k.per_second.iter().enumerate().take(secs) {
                    all[i] += n;
                }
            }
            println!("{:width$}  {}   all", "ALL", heat(&all, secs, &alive));
        }
        let msgs: u64 = keys.values().map(|k| k.msgs).sum();
        let mb: f64 = (keys.values().map(|k| k.bytes as f64).sum::<f64>() / 1e6).max(0.0);
        let stalls: u64 = keys.values().map(|k| k.stall_count).sum();
        let viol: u64 = keys.values().map(|k| k.violation_count).sum();
        println!(
            "{msgs} messages, {mb:.1} MB, {:.0} msg/s, {stalls} stalls, {viol} ordering violations, \
             {} upstream errors, reader busy {:.0}% of one thread",
            msgs as f64 / secs as f64,
            stream.rejected.load(Relaxed),
            stream.busy_ns.load(Relaxed) as f64 / 1e9 / secs as f64 * 100.0
        );
        // Capped in total, not per subscription: three each across twenty
        // markets runs to a hundred lines a stream and buries the strips.
        const MAX_NOTES: usize = 8;
        let total: u64 = keys.values().map(|k| k.stall_count + k.violation_count).sum();
        let mut shown = 0;
        for (name, k) in keys.iter() {
            for (at, len) in k.stalls.iter().take(2) {
                if shown == MAX_NOTES {
                    break;
                }
                println!("  {name}: stalled {:.1}s at +{:.0}s", len.as_secs_f64(), at.as_secs_f64());
                shown += 1;
            }
            for v in k.violations.iter().take(2) {
                if shown == MAX_NOTES {
                    break;
                }
                println!("  {v}");
                shown += 1;
            }
            if shown == MAX_NOTES {
                break;
            }
        }
        if total > shown as u64 {
            println!("  … and {} more", total - shown as u64);
        }
        if Arc::ptr_eq(stream, &wsarb) && viol > 0 {
            failed = true;
        }
    }

    // How stale the data was when a client could first see it. Absolute, so it
    // needs no reference stream and no noise floor: each stream is measured
    // against the block's own timestamp rather than against another socket.
    {
        // Print this first: it decides whether anything below is worth reading.
        let lag_ms = lag_us.load(Relaxed) as f64 / 1000.0;
        println!("\n=== age of the data on arrival ===");
        println!("this process overshot a 100 ms sleep by up to {lag_ms:.0} ms");
        if lag_ms > 20.0 {
            println!("  the machine was saturated, so differences of that order below");
            println!("  are this test rather than wsarb. Use fewer subscriptions per run.");
        }
        // A handful of frames is not a distribution. A node frozen an hour ago
        // still answers a subscribe with one ancient snapshot, and printing that
        // as a median next to real ones invites reading it as a measurement.
        const MIN_SAMPLES: usize = 100;
        // A stream that stalled delivered fewer frames, and the ones it never
        // delivered were the oldest — so its percentiles flatter it. Say so on
        // the row rather than leave the reader to notice the sample counts.
        let counts: Vec<usize> = streams
            .iter()
            .map(|s| s.keys.lock().unwrap().values().map(|k| k.ages_ms.len()).sum())
            .collect();
        let fullest = counts.iter().copied().max().unwrap_or(0);
        // The name travels with the numbers: streams below the sample floor are
        // skipped, so these indices do not line up with `streams`.
        let mut pcts: Vec<(String, [u32; 3])> = Vec::new();
        for (i, stream) in streams.iter().enumerate() {
            let keys = stream.keys.lock().unwrap();
            let mut all: Vec<u32> = keys.values().flat_map(|k| k.ages_ms.iter().copied()).collect();
            if all.is_empty() {
                println!("{:<30} no timestamped frames", stream.name);
                continue;
            }
            all.sort_unstable();
            let at = |p: f64| all[(((all.len() - 1) as f64) * p) as usize];
            let row = [at(0.50), at(0.95), at(0.99)];
            if all.len() < MIN_SAMPLES {
                println!(
                    "{:<30} only {} sample(s), oldest {} ms — too few to compare, excluded",
                    stream.name,
                    all.len(),
                    row[2]
                );
                // Slot 0 is wsarb; without it there is nothing to compare at all.
                if i == 0 {
                    pcts.clear();
                    break;
                }
                continue;
            }
            let short = fullest.saturating_sub(all.len());
            println!(
                "{:<30} median {:>5} ms   p95 {:>6} ms   p99 {:>6} ms   ({} samples){}",
                stream.name,
                row[0],
                row[1],
                row[2],
                all.len(),
                if short > fullest / 100 {
                    format!("  — {short} fewer than the fullest stream, and the missing ones were its oldest")
                } else {
                    String::new()
                }
            );
            pcts.push((stream.name.clone(), row));
        }

        // When, not just how much. A subscribe storm is confined to the opening
        // seconds and a struggling upstream is not, and the aggregate above
        // cannot tell those apart.
        {
            const BUCKET: usize = 10;
            let mut any = false;
            for stream in streams.iter() {
                let v = stream.age_by_sec.lock().unwrap();
                if v.is_empty() {
                    continue;
                }
                let means: Vec<Option<u64>> = v
                    .chunks(BUCKET)
                    .map(|w| {
                        let (sum, n) = w.iter().fold((0u64, 0u64), |a, b| (a.0 + b.0, a.1 + b.1 as u64));
                        (n > 0).then(|| sum / n)
                    })
                    .collect();
                let cells: Vec<String> = means
                    .iter()
                    .map(|m| m.map_or_else(|| "     -".into(), |v| format!("{v:>6}")))
                    .collect();
                if !any {
                    println!("
mean age per {BUCKET}s, in order:");
                    any = true;
                }
                println!("{:<24}{}", stream.name, cells.join(" "));

                // An age that only ever climbs is a different fault from a bad
                // patch: the source is losing ground against the chain in real
                // time and never catching up, and running longer only makes it
                // worse. Worth failing over, because fifty-second-old book data
                // reads as perfectly healthy in every other number here.
                let known: Vec<u64> = means.iter().flatten().copied().collect();
                let q = known.len() / 4;
                if q > 0 {
                    let head = known[..q].iter().sum::<u64>() / q as u64;
                    let tail = known[known.len() - q..].iter().sum::<u64>() / q as u64;
                    if tail > head.max(1) * 3 && tail > 2_000 {
                        let per = (tail as i64 - head as i64) / (known.len() as i64 - q as i64).max(1);
                        println!(
                            "!! age climbed {head} ms -> {tail} ms and never came back, {per:+} ms per {BUCKET}s."
                        );
                        println!("   The source is losing ground against the chain. While that is");
                        println!("   true nothing in this report measures the proxy.");
                        if Arc::ptr_eq(stream, &wsarb) {
                            failed = true;
                        }
                    }
                }
            }
            if any {
                println!("  a bad opening that settles is the cost of subscribing; bad throughout");
                println!("  is the upstream, and the two look the same in the percentiles above");
            }
        }

        // Against each node on its own, which is the comparison a client
        // actually faces: it connects to one node and lives with whatever that
        // one gives it.
        if pcts.len() > 1 {
            println!();
            // Width from the actual names, and a signed fixed field for the
            // numbers, or the columns stop lining up the moment a value changes
            // width and the rows become hard to read against each other.
            let w = pcts[1..].iter().map(|(n, _)| n.len()).max().unwrap_or(0);
            for (name, row) in pcts.iter().skip(1) {
                let parts: Vec<String> = ["median", "p95", "p99"]
                    .iter()
                    .enumerate()
                    .map(|(j, label)| {
                        format!("{label} {:>+6} ms", pcts[0].1[j] as i64 - row[j] as i64)
                    })
                    .collect();
                println!("vs {name:<w$}   {}", parts.join("   "));
            }
            println!("  (negative is wsarb fresher)");
        }

        // Median and tail answer different questions and can point opposite
        // ways, so report them apart. Collapsing to one number hides whichever
        // of the two happened to be the interesting one.
        if pcts.len() > 1 {
            let names = ["median", "p95", "p99"];
            println!();
            for (i, label) in names.iter().enumerate() {
                let ours = pcts[0].1[i] as i64;
                let best = pcts[1..].iter().map(|(_, r)| r[i] as i64).min().unwrap();
                let d = ours - best;
                match (i, d) {
                    (0, d) if d > 0 => println!(
                        "{label:>6}: wsarb {d} ms staler — the cost of the extra hop"
                    ),
                    (_, d) if d > 0 => println!(
                        "{label:>6}: wsarb {d} ms staler"
                    ),
                    (_, d) if d < 0 => println!(
                        "{label:>6}: wsarb {} ms fresher", -d
                    ),
                    _ => println!("{label:>6}: level"),
                }
            }
            let tail =
                pcts[0].1[2] as i64 - pcts[1..].iter().map(|(_, r)| r[2] as i64).min().unwrap();
            if tail < 0 {
                println!(
                    "\nThe tail is the win: a stale copy the nodes replayed never reached the\n\
                     client at all, so its worst case is {} ms better. Not the same data\n\
                     arriving sooner — data that should not have arrived, withheld.",
                    -tail
                );
            }
        }
    }

    // The same measure over one shared population. The table above gives each
    // stream its own, and that quietly rewards losing data: a stream that
    // stalled for four seconds is missing four seconds of the oldest frames it
    // would otherwise have been charged for.
    if !args.compare.is_empty() {
        let ages = matched_ages.lock().unwrap();
        let total: u64 = ages[0].iter().map(|&c| c as u64).sum();
        println!("\n=== age, over the frames every stream delivered ===");
        // Percentiles over a handful of frames are not a measurement. On a
        // timer-driven channel like l2Book each node flushes on its own phase,
        // so the streams rarely carry the very same bytes and this set can
        // collapse to almost nothing.
        const MIN_MATCHED: u64 = 500;
        if total < MIN_MATCHED {
            println!(
                "only {total} frames reached every stream, too few to compare: they are not emitting the same bytes"
            );
        } else {
            println!("{total} frames, the identical set on every row");
            // Say it rather than let a saturated histogram pass for a reading.
            let over: u64 = ages[0][AGE_BUCKETS - 1] as u64;
            if over * 100 > total {
                println!(
                    "!! {:.0}% were older than {} s: the figures below are floored there, a lower bound",
                    over as f64 * 100.0 / total as f64,
                    (AGE_BUCKETS - 1) / 1000
                );
            }
            let rows: Vec<(String, [u32; 3])> = streams
                .iter()
                .enumerate()
                .map(|(s, stream)| {
                    let row = [
                        pct_of(&ages[s], 0.50),
                        pct_of(&ages[s], 0.95),
                        pct_of(&ages[s], 0.99),
                    ];
                    println!(
                        "{:<30} median {:>5} ms   p95 {:>6} ms   p99 {:>6} ms",
                        stream.name, row[0], row[1], row[2]
                    );
                    (stream.name.clone(), row)
                })
                .collect();

            if rows.len() > 1 {
                println!();
                let w = rows[1..].iter().map(|(n, _)| n.len()).max().unwrap_or(0);
                for (name, row) in rows.iter().skip(1) {
                    let parts: Vec<String> = ["median", "p95", "p99"]
                        .iter()
                        .enumerate()
                        .map(|(j, label)| {
                            format!("{label} {:>+6} ms", rows[0].1[j] as i64 - row[j] as i64)
                        })
                        .collect();
                    println!("vs {name:<w$}   {}", parts.join("   "));
                }
                println!("  (negative is wsarb fresher)");
                // The table carries its own floor, and it is not small: two
                // connections to one node sit in it as separate rows, so the
                // spread between those two is the resolution of every other row.
                if let (Some(twin), true) = (noise_slot, rows.len() == used_slots) {
                    // Slot 1 is the first --compare node, which is the node the
                    // twin is a second connection to.
                    let floor: [i64; 3] =
                        [0, 1, 2].map(|j| (rows[1].1[j] as i64 - rows[twin].1[j] as i64).abs());
                    println!(
                        "\nfloor: the same node on two connections differs by {} ms median, \
                         {} ms p95, {} ms p99",
                        floor[0], floor[1], floor[2]
                    );
                    println!("  rows closer than that say nothing");
                    for (j, label) in ["median", "p95", "p99"].iter().enumerate() {
                        let best = rows[1..].iter().map(|(_, r)| r[j] as i64).min().unwrap();
                        let d = rows[0].1[j] as i64 - best;
                        if d.abs() <= floor[j] {
                            println!("{label:>6}: level within the floor");
                        } else if d > 0 {
                            println!("{label:>6}: wsarb {d} ms staler, above the floor");
                        } else {
                            println!("{label:>6}: wsarb {} ms fresher, above the floor", -d);
                        }
                    }
                }
                println!(
                    "\nThis is the honest one. Where it disagrees with the table above, the\n\
                     difference is which frames each stream had, not how fast it had them."
                );
            }
        }
    }

    // A stall the nodes had too is the upstream's, not wsarb's. Only a stall
    // wsarb had alone is worth failing over — otherwise every upstream hiccup
    // is reported as a proxy fault, and the verdict stops meaning anything.
    if !args.compare.is_empty() {
        const NEAR: f64 = 2.0;
        let ours = wsarb.keys.lock().unwrap();
        let mut alone: Vec<(String, f64, f64)> = Vec::new();
        for (name, k) in ours.iter() {
            for (at, len) in &k.stalls {
                let at = at.as_secs_f64();
                let shared = streams[1..].iter().any(|s| {
                    s.keys.lock().unwrap().get(name).is_some_and(|other| {
                        other.stalls.iter().any(|(o, _)| (o.as_secs_f64() - at).abs() < NEAR)
                    })
                });
                if !shared {
                    alone.push((name.clone(), at, len.as_secs_f64()));
                }
            }
        }
        if alone.is_empty() {
            println!("\nevery stall wsarb had, the nodes had too");
        } else {
            println!("\n{} stall(s) wsarb had and the nodes did not:", alone.len());
            for (name, at, len) in alone.iter().take(10) {
                println!("  {name}: {len:.1}s at +{at:.0}s");
            }
            failed = true;
        }
    }

    if !args.compare.is_empty() {
        println!("\n=== wsarb against the nodes ===");
        let m = verdict.matched.load(Relaxed);
        let lost = verdict.lost.load(Relaxed);
        let stale = verdict.stale_dropped.load(Relaxed);
        let inv = verdict.invented.load(Relaxed);
        let sup = verdict.superseded.load(Relaxed);
        let skip = verdict.skipped.load(Relaxed);
        println!(
            "matched {m}, lost {lost}, superseded {sup}, skipped {skip}, stale {stale}, invented {inv}"
        );
        if sup > 0 || skip > 0 {
            println!("  superseded: that stamp went out, as the other source's copy of it.");
            println!("  skipped: that stamp never went out, but a newer one already had —");
            println!("  on a channel of full snapshots, handing over the older one afterwards");
            println!("  would walk the client's book backwards.");
            println!("  Only `lost` is a stamp the client never saw, with nothing newer either.");
        }
        // Only meaningful when every source wsarb reads is also a --compare
        // stream. With one of two compared, everything wsarb took from the other
        // one lands here and means nothing.
        if inv * 20 > m {
            println!(
                "  invented is {:.0}% of matched, which is what happens when wsarb reads a source",
                inv as f64 * 100.0 / m as f64
            );
            println!("  that is not among the --compare streams. Compare every source, or ignore it.");
        }
        let undecided = verdict.undecided.load(Relaxed);
        if undecided > 0 {
            println!("  ({undecided} arrived too near the end to judge)");
        }

        let noise_n = verdict.noise_n.load(Relaxed);
        if noise_n > 0 {
            let avg = verdict.noise_us.load(Relaxed) as f64 / noise_n as f64 / 1000.0;
            let max = verdict.noise_max_us.load(Relaxed) as f64 / 1000.0;
            println!(
                "noise floor: two connections to the same node differed by {avg:.2} ms on average \
                 (worst {max:.0} ms) over {noise_n} messages"
            );
            println!("  anything below that is this test's jitter, not wsarb's doing");
        } else if args.noise_floor {
            println!("noise floor: no overlap measured");
        } else {
            println!("noise floor not measured — rerun with --noise-floor before trusting the numbers below");
        }

        // Both of those connections are the same node. Whether they carry the
        // same bytes decides how to read `invented` — and more than that,
        // whether racing positions inside a block is sound at all, since that
        // splices one source's messages onto another's.
        let tb = verdict.twin_both.load(Relaxed);
        let node_only = verdict.node_only.load(Relaxed);
        let twin_only = verdict.twin_only.load(Relaxed);
        let odd = node_only + twin_only;
        if tb + odd > 0 {
            println!("\n=== do two connections to one node carry the same bytes? ===");
            println!(
                "on both {tb}, only on the node's own connection {node_only}, \
                 only on the twin {twin_only}"
            );
            println!(
                "  {:.2}% of frames came down one connection and not the other",
                odd as f64 * 100.0 / (tb + odd) as f64
            );
            if odd == 0 && inv == 0 {
                // Nothing to explain: the connections agree and wsarb invented
                // nothing. Saying more here only invites reading a fault into a
                // clean result.
            } else if odd == 0 {
                println!("  Byte-identical. So invented {inv} cannot be blamed on framing:");
                println!("  wsarb emitted frames no node emitted. Rerun wsarb with a single");
                println!("  --source, where splicing is impossible, to localise it.");
            } else if inv > 0 && odd * 2 >= inv {
                println!("  Same order as invented {inv}, so that is very likely this node");
                println!("  framing the same events differently per connection rather than");
                println!("  wsarb inventing data. The block counts below settle it.");
            }
        }

        // The measure the hash comparison cannot make. A frame absent from one
        // stream may be a batch cut differently or a frame lost in a stall, and
        // by hash the two are indistinguishable. Counting messages per block
        // tells them apart, and that is what decides whether racing positions
        // across sources is sound.
        for mode in [1usize, 0] {
            let seen_blocks = verdict.blk_seen[mode].load(Relaxed);
            if seen_blocks == 0 {
                continue;
            }
            let (what, channels) = if mode == 1 {
                ("raced", "l4Book, bbo, orderUpdates")
            } else {
                ("led", "l2Book, bbo")
            };
            println!("
=== how a stamp is cut, on the {what} channels ({channels}) ===");
            println!("stamps carried by every stream: {seen_blocks}");
            // `None` until a pair has actually been compared. Without this a run
            // with a single --compare and no twin printed the conclusion that
            // everything agreed, having compared nothing at all.
            let pair = |label: &str, agree: u64, differ: u64| -> Option<f64> {
                if agree + differ == 0 {
                    return None;
                }
                let pc = differ as f64 * 100.0 / (agree + differ) as f64;
                println!("{label:<28} agree {agree}, differ {differ}  ({pc:.2}% differ)");
                Some(pc)
            };
            let same = pair(
                "same node, two connections:",
                verdict.blk_same_agree[mode].load(Relaxed),
                verdict.blk_same_differ[mode].load(Relaxed),
            );
            let cross = pair(
                "two different nodes:",
                verdict.blk_cross_agree[mode].load(Relaxed),
                verdict.blk_cross_differ[mode].load(Relaxed),
            );

            // Read against the same-node figure, never against a constant. Two
            // connections to one node see the same events by construction, so
            // whatever they disagree about is the floor of this measurement —
            // and under a struggling upstream that floor climbs into double
            // digits and swamps the thing being looked for.
            match (cross, same, mode) {
                (None, _, _) | (_, None, _) => {
                    println!("  nothing to compare against: this needs two --compare nodes AND");
                    println!("  --noise-floor, or the cross-node figure has no baseline.");
                }
                (Some(_), Some(base), _) if base > 2.0 => {
                    println!("  Two connections to ONE node disagree by {base:.1}%, so the node is not");
                    println!("  serving its connections alike — it is struggling. Nothing here says");
                    println!("  anything about how blocks are cut until that figure is near zero.");
                }
                // Led: the sources are *expected* to disagree.
                (Some(x), Some(base), 0) if x > base * 3.0 => {
                    println!("  The nodes cut this channel differently — as expected, since it is");
                    println!("  flushed on each node's own timer. That is exactly why one source");
                    println!("  carries a stamp to the end here instead of positions being raced.");
                }
                (Some(_), Some(_), 0) => {
                    println!("  The nodes happen to agree closely here, but the mode does not rely");
                    println!("  on it: one source carries each stamp regardless.");
                }
                (Some(x), Some(base), _) if x <= base * 3.0 => {
                    println!("  Cross-node disagreement ({x:.2}%) is within reach of the {base:.2}% floor");
                    println!("  between two connections to one node, so racing positions is sound.");
                }
                (Some(x), Some(base), _) => {
                    println!("  Cross-node disagreement ({x:.2}%) is far above the {base:.2}% floor, so");
                    println!("  position #k is not the same event on two sources and racing them");
                    println!("  splices incompatible batches: these channels must go back to one");
                    println!("  source carrying a whole stamp.");
                }
            }
        }

        // The proxy's own shortfall. The rows above compare nodes with each
        // other and say nothing about what wsarb actually passed on, and on a
        // channel whose nodes do not emit byte-identical frames this is the only
        // way to see it at all.
        let stamps = verdict.blk_seen[0].load(Relaxed) + verdict.blk_seen[1].load(Relaxed);
        if stamps > 0 {
            let short = verdict.blk_short.load(Relaxed);
            println!();
            if short > 0 {
                println!(
                    "wsarb was short of the best node on {short} of {stamps} stamps ({:.1}%),",
                    short as f64 * 100.0 / stamps as f64
                );
                println!("  {} frames behind in total", verdict.blk_short_frames.load(Relaxed));
            } else {
                println!("wsarb matched the best node's message count on every stamp");
            }
        }

        let bn = verdict.behind_n.load(Relaxed);
        let an = verdict.ahead_n.load(Relaxed);

        // The headline. Who got there first is a comparison of order, so the
        // spread between connections cannot distort it the way it distorts a
        // difference of timestamps.
        if an + bn > 0 {
            println!(
                "\nwsarb was first on {:.1}% of {} messages",
                an as f64 * 100.0 / (an + bn) as f64,
                an + bn
            );
            let tf = verdict.twin_first.load(Relaxed);
            let ts = verdict.twin_second.load(Relaxed);
            if tf + ts > 0 {
                println!(
                    "control: one of two identical connections was first on {:.1}% — \
                     no advantage exists there, so that is the coin flip to beat",
                    tf as f64 * 100.0 / (tf + ts) as f64
                );
            } else {
                println!("control not measured — rerun with --noise-floor to know what beating chance looks like");
            }
        }

        println!();
        if bn > 0 {
            println!(
                "wsarb trailed the first node by {:.2} ms on {bn} messages",
                verdict.behind_us.load(Relaxed) as f64 / bn as f64 / 1000.0
            );
        }
        if an > 0 {
            println!(
                "wsarb led the first node by {:.2} ms on {an} messages",
                verdict.ahead_us.load(Relaxed) as f64 / an as f64 / 1000.0
            );
        }
        if bn + an > 0 {
            let net = (verdict.behind_us.load(Relaxed) as f64 - verdict.ahead_us.load(Relaxed) as f64)
                / (bn + an) as f64
                / 1000.0;
            println!("net: wsarb {:+.2} ms against the reference connections", net);
            // Say it outright rather than leave a number to be misread: below
            // the floor there is nothing to read.
            if noise_n > 0 {
                let floor = verdict.noise_us.load(Relaxed) as f64 / noise_n as f64 / 1000.0;
                if net.abs() < floor {
                    println!("  ...which is under the {floor:.2} ms floor, so it says nothing.");
                    println!("  Two connections to one node vary by more than the effect sought.");
                }
            }
        }

        // Loss is wsarb's problem. Inventions are almost always the reference
        // connection having missed something, so they are reported, not judged.
        if lost > 0 {
            failed = true;
        }
    }

    println!("\nRESULT: {}", if failed { "FAILED" } else { "PASSED" });
    std::process::exit(i32::from(failed));
}
