//! Per-source and per-client statistics, plus the HTML stats page renderer.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

/// How often `roll_window` is called, and therefore the resolution at which a
/// source's silence is observed.
pub const WINDOW: Duration = Duration::from_secs(5);

/// How long a source may go without delivering before it is treated as gone.
///
/// Only meaningful relative to the other sources: a quiet market silences all
/// of them at once, which is not a fault. See `AppState::silent_sources`.
pub const SILENCE_LIMIT: Duration = Duration::from_secs(5);

/// Path written by the external block-lag probe (hl_probe_json.sh).
const PROBE_PATH: &str = "/root/hl-tests/test-D/latest.json";
const RACE_PATH: &str = "/root/hl-tests/peer-parsing/race_latest.json";
const RACE_LAG_N_MIN: u64 = 10;
const BAD_DAYS_RED: f64 = 5.0;
const BAD_DAYS_LABEL: f64 = 7.0;

/// Upper edges (exclusive, microseconds) of the delay histogram buckets.
pub const BUCKET_EDGES_US: [u64; 10] =
    [500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000, 500_000];
pub const NUM_BUCKETS: usize = BUCKET_EDGES_US.len() + 1;

const BUCKET_LABELS: [&str; NUM_BUCKETS] = [
    "<0.5ms", "<1ms", "<2ms", "<5ms", "<10ms", "<20ms", "<50ms", "<100ms", "<200ms", "<500ms",
    ">=500ms",
];

/// Statistics for a single upstream data connection.
#[derive(Default)]
pub struct SourceStats {
    pub connected: AtomicBool,
    pub packets: AtomicU64,
    /// Consecutive `roll_window` passes in which this source delivered nothing.
    ///
    /// A connected source can still be delivering nothing — its own node may
    /// have died while the socket stayed up — and that is indistinguishable
    /// from health unless silence is tracked. Counted off the window deltas
    /// that `roll_window` already computes, so the hot path pays nothing for
    /// it; the cost is that silence is only seen at `WINDOW` resolution.
    silent_windows: AtomicU64,
    pub disconnects: AtomicU64,
    pub wins: AtomicU64,
    pub duplicates: AtomicU64,
    pub stale: AtomicU64,
    /// Frames refused outright for being older than the freshness limit, as
    /// opposed to `stale`, which is only older than what we already forwarded.
    pub too_old: AtomicU64,
    /// Frames refused for being stamped in the *future*.
    ///
    /// Counted apart from `too_old` because the damage is different in kind: an
    /// old frame is dropped and forgotten, while one from the future would set
    /// the key's high-water mark ahead of the clock and silence every real frame
    /// behind it until wall time caught up.
    pub from_future: AtomicU64,
    /// Block time of the newest frame seen from this source, in unix ms. Zero
    /// means it has not spoken yet — which is not the same as being behind.
    ///
    /// Diagnostic only, and deliberately no longer the yardstick for lag: it
    /// measures when *our reader* parsed a frame, so a reader held up by a lock
    /// reads exactly like a node that stopped advancing. The dashboard shows it
    /// as `data age` — how old the data a source is sending is — next to the
    /// height polled from the source itself, which is what the verdict is made
    /// on.
    newest_block_ms: AtomicU64,
    /// Book height this source last reported on `GET /health`, and whether that
    /// poll found it ready. Zero height means "never answered": no measurement,
    /// which is not the same as being at block zero.
    health_height: AtomicU64,
    health_ready: AtomicBool,
    /// Polls in a row that failed. A source that cannot be reached is not
    /// declared behind — silence is `silent_sources`' business — but the count
    /// belongs on the dashboard.
    health_failures: AtomicU64,
    /// Highest block height carried by any frame from this source since it
    /// last connected, as our reader parsed them. Zero means none yet.
    ///
    /// The stream's side of the height, next to `health_height` which is the
    /// node's own word. Display only, for the reason the lag verdict moved off
    /// frames: a reader of ours held up reads exactly like a node that stopped.
    /// Which is also what makes the pair worth showing -- the stream behind
    /// `/health` points at our reader, both behind the network at the node.
    stream_height: AtomicU64,
    /// The node's own height, from its `visor_abci_state.json` (`--node-state`).
    /// Zero means not configured or never read. The latest reading rather than
    /// a maximum: a node that restarts comes back lower, and that should show.
    node_height: AtomicU64,
    /// Largest `now - block_time` ever seen from this source, in ms. Diagnostic
    /// only: the lag verdict is relative, this is the absolute worst moment.
    peak_age_ms: AtomicU64,
    /// Set by the watchdog when this source's data has fallen behind the
    /// freshest source's *and stayed there*. Read on the hot paths that hand a
    /// stream to a source, so that a source being written off cannot
    /// immediately take it back.
    lagging: AtomicBool,
    /// Consecutive watchdog ticks this source has measured behind.
    ///
    /// The measurement is taken where the frame is parsed, so it reports our
    /// own reader as much as it reports the node: a reader held up by a lock or
    /// a busy runtime leaves its frames in the socket and looks exactly like a
    /// node that has stopped advancing. Acting on a single tick did exactly
    /// that -- it moved 145 clients at once, which loaded the runtime enough to
    /// stall the *other* reader, which was then marked in its turn, back and
    /// forth. A node that is genuinely behind stays behind and falls further;
    /// a stall clears in a tick or two. So lag has to persist to count.
    lagging_ticks: AtomicU64,
    delay_sum_us: AtomicU64,
    delay_count: AtomicU64,
    hist: [AtomicU64; NUM_BUCKETS],

    // ---- last-window tracking (refreshed by a background task ~every 5s) ----
    prev_packets: AtomicU64,
    prev_wins: AtomicU64,
    prev_dups: AtomicU64,
    prev_stale: AtomicU64,
    prev_delay_sum_us: AtomicU64,
    prev_delay_count: AtomicU64,
    d_packets: AtomicU64,
    d_wins: AtomicU64,
    d_dups: AtomicU64,
    d_stale: AtomicU64,
    d_delay_sum_us: AtomicU64,
    d_delay_count: AtomicU64,
}

impl SourceStats {
    /// Consecutive windows with no data. Zero means it delivered in the last one.
    pub fn silent_windows(&self) -> u64 {
        self.silent_windows.load(Relaxed)
    }

    /// How long this source has been silent, or `None` if it never delivered
    /// at all — "never" must not read as "long ago", since a source that has
    /// yet to say anything cannot have been leading a block.
    ///
    /// Rounded down to whole `WINDOW`s: zero means it delivered during the
    /// most recent window.
    pub fn idle_for(&self) -> Option<Duration> {
        if self.packets.load(Relaxed) == 0 {
            return None;
        }
        Some(WINDOW * self.silent_windows.load(Relaxed) as u32)
    }

    /// Block time of the newest frame from this source, or `None` if it has not
    /// delivered one yet.
    pub fn newest_block_ms(&self) -> Option<u64> {
        match self.newest_block_ms.load(Relaxed) {
            0 => None,
            ms => Some(ms),
        }
    }

    /// Record one arriving frame's block time. `now` is passed in because the
    /// caller has already read the clock for its own checks.
    pub fn record_block_time(&self, block_ms: u64, now_ms: u64) {
        self.newest_block_ms.fetch_max(block_ms, Relaxed);
        if now_ms > block_ms {
            self.peak_age_ms.fetch_max(now_ms - block_ms, Relaxed);
        }
    }

    /// Height this source reported on its last successful health poll, or
    /// `None` if it has never answered one.
    pub fn health_height(&self) -> Option<u64> {
        match self.health_height.load(Relaxed) {
            0 => None,
            h => Some(h),
        }
    }

    pub fn health_ready(&self) -> bool {
        self.health_ready.load(Relaxed)
    }

    pub fn health_failures(&self) -> u64 {
        self.health_failures.load(Relaxed)
    }

    /// A poll came back. The height only moves forward: the endpoint updates it
    /// every thousand applied batches, so two polls can land on the same value,
    /// and a server restarting must not read as having gone backwards.
    pub fn record_health(&self, height: u64, ready: bool) {
        self.health_height.fetch_max(height, Relaxed);
        self.health_ready.store(ready, Relaxed);
        self.health_failures.store(0, Relaxed);
    }

    /// A poll failed. The last known height is kept: it is still the best
    /// answer to "where was this source", and a source that cannot be reached
    /// is judged by `silent_sources`, not here.
    pub fn record_health_failure(&self) {
        self.health_failures.fetch_add(1, Relaxed);
    }

    /// The node's own height, as last read from its state file.
    pub fn node_height(&self) -> Option<u64> {
        match self.node_height.load(Relaxed) {
            0 => None,
            h => Some(h),
        }
    }

    pub fn record_node_height(&self, height: u64) {
        self.node_height.store(height, Relaxed);
    }

    /// Highest height seen in this source's frames since it connected.
    pub fn stream_height(&self) -> Option<u64> {
        match self.stream_height.load(Relaxed) {
            0 => None,
            h => Some(h),
        }
    }

    pub fn record_stream_height(&self, height: u64) {
        self.stream_height.fetch_max(height, Relaxed);
    }

    /// Forget it on reconnecting: a node that restarted comes back lower than
    /// it left, and a maximum kept across that would hide exactly that.
    pub fn reset_stream_height(&self) {
        self.stream_height.store(0, Relaxed);
    }

    /// How old this source's newest data is, against the wall clock. Useful to
    /// look at, but never to judge by: a quiet market ages every source at once.
    pub fn age(&self, now_ms: u64) -> Option<Duration> {
        self.newest_block_ms().map(|t| Duration::from_millis(now_ms.saturating_sub(t)))
    }

    pub fn peak_age(&self) -> Option<Duration> {
        match self.peak_age_ms.load(Relaxed) {
            0 => None,
            ms => Some(Duration::from_millis(ms)),
        }
    }

    pub fn is_lagging(&self) -> bool {
        self.lagging.load(Relaxed)
    }

    /// One more tick measured behind; returns how many in a row that is.
    pub fn note_lagging(&self) -> u64 {
        self.lagging_ticks.fetch_add(1, Relaxed) + 1
    }

    /// Caught up: forget both the mark and the run of ticks behind it.
    pub fn clear_lagging(&self) {
        self.lagging_ticks.store(0, Relaxed);
        self.lagging.store(false, Relaxed);
    }

    pub fn set_lagging(&self, v: bool) {
        self.lagging.store(v, Relaxed);
        if !v {
            self.lagging_ticks.store(0, Relaxed);
        }
    }

    /// Consecutive ticks measured behind, for the dashboard.
    pub fn lagging_ticks(&self) -> u64 {
        self.lagging_ticks.load(Relaxed)
    }

    pub fn record_delay(&self, d: Duration) {
        let us = d.as_micros() as u64;
        self.duplicates.fetch_add(1, Relaxed);
        self.delay_sum_us.fetch_add(us, Relaxed);
        self.delay_count.fetch_add(1, Relaxed);
        let idx = BUCKET_EDGES_US
            .iter()
            .position(|&edge| us < edge)
            .unwrap_or(NUM_BUCKETS - 1);
        self.hist[idx].fetch_add(1, Relaxed);
    }

    pub fn avg_delay_us(&self) -> f64 {
        let n = self.delay_count.load(Relaxed);
        if n == 0 {
            0.0
        } else {
            self.delay_sum_us.load(Relaxed) as f64 / n as f64
        }
    }

    /// Snapshot counters and store the delta since the previous snapshot for
    /// the "last window" table. Uses swap so read+reset is atomic per counter.
    pub fn roll_window(&self) {
        let p = self.packets.load(Relaxed);
        let w = self.wins.load(Relaxed);
        let du = self.duplicates.load(Relaxed);
        let st = self.stale.load(Relaxed);
        let ds = self.delay_sum_us.load(Relaxed);
        let dc = self.delay_count.load(Relaxed);
        let d_packets = p.saturating_sub(self.prev_packets.swap(p, Relaxed));
        // Silence is read off this delta rather than stamped per frame, so the
        // hot path stays untouched.
        if d_packets == 0 {
            self.silent_windows.fetch_add(1, Relaxed);
        } else {
            self.silent_windows.store(0, Relaxed);
        }
        self.d_packets.store(d_packets, Relaxed);
        self.d_wins.store(w.saturating_sub(self.prev_wins.swap(w, Relaxed)), Relaxed);
        self.d_dups.store(du.saturating_sub(self.prev_dups.swap(du, Relaxed)), Relaxed);
        self.d_stale.store(st.saturating_sub(self.prev_stale.swap(st, Relaxed)), Relaxed);
        self.d_delay_sum_us.store(ds.saturating_sub(self.prev_delay_sum_us.swap(ds, Relaxed)), Relaxed);
        self.d_delay_count.store(dc.saturating_sub(self.prev_delay_count.swap(dc, Relaxed)), Relaxed);
    }

    fn window_avg_delay_ms(&self) -> f64 {
        let n = self.d_delay_count.load(Relaxed);
        if n == 0 {
            0.0
        } else {
            self.d_delay_sum_us.load(Relaxed) as f64 / n as f64 / 1000.0
        }
    }

    fn hist_row_html(&self) -> String {
        let mut cells = String::new();
        for (i, label) in BUCKET_LABELS.iter().enumerate() {
            let v = self.hist[i].load(Relaxed);
            if v > 0 {
                cells.push_str(&format!("<span class=b>{label}:{v}</span> "));
            }
        }
        if cells.is_empty() {
            "&mdash;".to_string()
        } else {
            cells
        }
    }
}

/// Timer script: countdown to the next expected probe, or "overdue" in red.
const PROBE_TIMER_JS: &str = r#"<script>
(function(){
  var next = Date.parse("__NEXT__");
  function upd(){
    var el = document.getElementById('probe-timer');
    if(!el || isNaN(next)) return;
    var rem = Math.round((next - Date.now())/1000);
    if(rem >= 0){
      var m=Math.floor(rem/60), s=rem%60;
      el.textContent='expected next in '+m+':'+(s<10?'0':'')+s; el.style.color='#999';
    } else {
      var a=-rem, m=Math.floor(a/60), s=a%60;
      el.textContent='overdue by '+m+':'+(s<10?'0':'')+s; el.style.color='#e53935';
    }
  }
  upd(); setInterval(upd,1000);
})();
</script>"#;

/// Elapsed-since-last-run timer for the peer race section (counts up, hours accumulate).
const RACE_TIMER_JS: &str = r#"<script>
(function(){
  var el = document.getElementById('race-elapsed');
  if(!el) return;
  var upd = Date.parse(el.getAttribute('data-updated'));
  function tick(){
    if(isNaN(upd)){ el.textContent='?'; return; }
    var secs = Math.floor((Date.now()-upd)/1000);
    if(secs<0) secs=0;
    var h=Math.floor(secs/3600), m=Math.floor((secs%3600)/60), s=secs%60;
    function pad(n){return (n<10?'0':'')+n;}
    el.textContent = pad(h)+':'+pad(m)+':'+pad(s)+' ago';
  }
  tick(); setInterval(tick,1000);
})();
</script>"#;

fn read_peer_probe() -> Option<serde_json::Value> {
    let s = std::fs::read_to_string(PROBE_PATH).ok()?;
    serde_json::from_str(&s).ok()
}

fn group(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut out = String::new();
    for (i, c) in bytes.iter().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            out.push(' ');
        }
        out.push(*c as char);
    }
    out
}

fn jstr(v: &serde_json::Value, key: &str) -> String {
    v.get(key).and_then(|x| x.as_str()).unwrap_or("").to_string()
}
fn jnum(v: &serde_json::Value, key: &str) -> String {
    match v.get(key) {
        Some(x) if x.is_number() => x.to_string(),
        _ => "&mdash;".to_string(),
    }
}

fn jf64(v: &serde_json::Value, key: &str) -> f64 {
    v.get(key).and_then(|x| x.as_f64()).unwrap_or(0.0)
}
fn ju64(v: &serde_json::Value, key: &str) -> u64 {
    v.get(key).and_then(|x| x.as_u64()).unwrap_or(0)
}
fn jbool(v: &serde_json::Value, key: &str) -> bool {
    v.get(key).and_then(|x| x.as_bool()).unwrap_or(false)
}
fn read_race() -> Option<serde_json::Value> {
    let s = std::fs::read_to_string(RACE_PATH).ok()?;
    serde_json::from_str(&s).ok()
}
/// Render the "Peer race" section into `out` from race_latest.json.
fn render_race_section(out: &mut String) {
    let race = read_race();
    match &race {
        None => {
            out.push_str("<div class='cap c2'>Peer race &mdash; no data (run start-race.sh)</div>\n");
        }
        Some(v) => {
            let updated = jstr(v, "updated_utc");
            let peers = v.get("peers").and_then(|p| p.as_array());
            // current node peers (turquoise): from the block-lag probe (latest.json),
            // which reflects each node's REAL live peer and refreshes every 5 min (fresher than race).
            let mut node_ips: Vec<String> = Vec::new();
            if let Some(pv) = read_peer_probe() {
                if let Some(parr) = pv.get("peers").and_then(|p| p.as_array()) {
                    for p in parr {
                        let ip = jstr(p, "peer");
                        if !ip.is_empty() { node_ips.push(ip); }
                    }
                }
            }
            let mut green_idx: isize = -1;
            let mut green_leads: u64 = 0;
            let mut lag_idx: isize = -1;
            let mut lag_min = f64::MAX;
            if let Some(arr) = peers {
                for (i, p) in arr.iter().enumerate() {
                    if !jbool(p, "streaming") { continue; }
                    let leads = ju64(p, "leads");
                    if green_idx < 0 || leads > green_leads {
                        green_leads = leads; green_idx = i as isize;
                    }
                    if ju64(p, "lag_n") >= RACE_LAG_N_MIN {
                        let lag = jf64(p, "avg_lag_ms");
                        if lag < lag_min { lag_min = lag; lag_idx = i as isize; }
                    }
                }
            }
            out.push_str(&format!(
                "<div class='cap c2'>Peer race &mdash; last run: <span id=race-elapsed data-updated=\"{}\"></span></div>\n",
                html_escape(&updated)
            ));
            out.push_str("<div class=legend>");
            out.push_str("<span class=sq style='background:#4caf50'></span> leads &mdash; delivers a block first &nbsp;&nbsp;");
            out.push_str("<span class=sq style='background:#aeea00'></span> avg_lag &mdash; winner by delay &nbsp;&nbsp;");
            out.push_str("<span class=sq style='background:#00ced1'></span> current connect &mdash; node's live peer &nbsp;&nbsp;");
            out.push_str("<span class=sq style='background:#e53935'></span> bad peer 5+ days");
            out.push_str("</div>\n<table>\n");
            out.push_str("<tr><th>peer</th><th>state</th><th>leads</th><th>avg_lag (ms)</th><th>lag_n</th><th>recon</th></tr>\n");
            if let Some(arr) = peers {
                let mut order: Vec<usize> = (0..arr.len()).collect();
                order.sort_by(|&a, &b| ju64(&arr[b], "leads").cmp(&ju64(&arr[a], "leads")));
                for i in order {
                    let p = &arr[i];
                    let ip = jstr(p, "peer");
                    let bad_days = jf64(p, "bad_days");
                    // row: red if bad 5+ days (a bad peer never streams, so it can't be the leads leader);
                    // otherwise green for the leads leader.
                    let cls = if bad_days >= BAD_DAYS_RED { " class=badrow" }
                              else if i as isize == green_idx { " class=greenrow" }
                              else { "" };
                    // avg_lag cell: lime for the delay leader
                    let lagcell = if i as isize == lag_idx { " class=limecell" } else { "" };
                    // peer (IP) cell: turquoise if it is a current node peer
                    let peercell = if node_ips.iter().any(|x| x == &ip) { " class=nodecell" } else { "" };
                    let streaming = jbool(p, "streaming");
                    // state cell: append BAD label at 7+ days
                    let state_cell = if bad_days >= BAD_DAYS_LABEL {
                        format!("{} <span class=badlabel>BAD {}d</span>", html_escape(&jstr(p, "state")), bad_days as u64)
                    } else {
                        html_escape(&jstr(p, "state"))
                    };
                    out.push_str(&format!(
                        "<tr{cls}><td{peercell}>{peer}</td><td class={stcls}>{state}</td><td>{leads}</td><td{lagcell}>{lag:.1}</td><td>{lagn}</td><td>{recon}</td></tr>",
                        cls = cls,
                        peercell = peercell,
                        lagcell = lagcell,
                        peer = html_escape(&ip),
                        stcls = if streaming { "up" } else { "down" },
                        state = state_cell,
                        leads = ju64(p, "leads"),
                        lag = jf64(p, "avg_lag_ms"),
                        lagn = ju64(p, "lag_n"),
                        recon = ju64(p, "recon"),
                    ));
                }
            } else {
                out.push_str("<tr><td colspan=6 class=sum>no peers</td></tr>");
            }
            out.push_str("</table>\n");
        }
    }
}

/// Blocks behind the network beyond which a `… vs net` column is shown in red. At ~14
/// blocks a second, about three and a half seconds; the same as `--lag-blocks`'
/// default, so the two columns speak the same language.
const NET_BEHIND_WARN: u64 = 50;

/// One `… vs net` cell: blocks behind, or ahead with a `+`. Ahead is
/// ordinary by a few blocks -- the reference trails the chain too.
fn fmt_vs_network(network: u64, source: u64) -> String {
    if network >= source {
        group(network - source)
    } else {
        format!("+{}", group(source - network))
    }
}

/// One line above the source table: where the network's height comes from and
/// whether it is alive, so empty `… vs net` columns explain themselves.
fn network_line(state: &crate::state::AppState, now_ms: u64) -> String {
    let r = &state.reference;
    let Some(url) = r.url() else {
        return "<div class=sum style='margin:-.2em 0 .5em;font-size:11px'>network reference: off</div>\n".to_string();
    };
    let body = match (r.height(), r.age(now_ms)) {
        (Some(h), Some(a)) if a <= crate::reference::STALE_AFTER => {
            format!("network height <b>{}</b>, last block {:.1}s ago", group(h), a.as_secs_f64())
        }
        (Some(h), Some(a)) => format!(
            "<span class=over>network height {} is {:.0}s old &mdash; not measuring by it</span>",
            group(h),
            a.as_secs_f64()
        ),
        _ if r.is_connected() => "network reference connected, no block yet".to_string(),
        _ => "<span class=over>network reference not connected</span>".to_string(),
    };
    format!(
        "<div class=sum style='margin:-.2em 0 .5em;font-size:11px'>{body} &mdash; via {} explorerBlock</div>\n",
        html_escape(url)
    )
}

/// Render the HTML stats page from a snapshot of the shared state.
pub fn render_page(state: &crate::state::AppState) -> String {
    // Read once for the whole page, so the ages in a row are all measured from
    // the same instant.
    let now_ms = crate::upstream::unix_ms();

    // ---- Table 1: cumulative ----
    let net_height = state.reference.current(now_ms);
    let net_title = match (state.reference.height(), state.reference.age(now_ms)) {
        (Some(h), Some(a)) => format!("network height {} as of {:.1}s ago", group(h), a.as_secs_f64()),
        _ => "no network height".to_string(),
    };
    let mut cum_rows = String::new();
    for src in &state.sources {
        let s = &src.stats;
        let connected = s.connected.load(Relaxed);
        // A live socket is not the same as live data: when a source's own node
        // dies it keeps the connection and simply stops speaking. IDLE is that
        // state, and it is the one worth noticing.
        let idle = s.idle_for();
        let silent = connected && idle.is_some_and(|d| d > SILENCE_LIMIT);
        // Two different failures, two columns. `last data` is when the source
        // last said anything — silence. `data age` is how old what it says is —
        // a node replaying blocks scores perfectly on the first and terribly on
        // the second, and it is the second that reaches the clients.
        let lagging = connected && s.is_lagging();
        cum_rows.push_str(&format!(
            "<tr><td class=nd>{node}</td><td>{url}</td>\
             <td class={cls}>{state_txt}</td><td>{last_data}</td><td title=\"{peak}\">{age}</td><td class=num title=\"{nodetitle}\">{nheight}</td><td class={nodecls} title=\"{ntitle}\">{node_vs_net}</td><td class={hcls}>{height}</td><td class={ncls} title=\"{ntitle}\">{vs_net}</td><td class=num title=\"{stitle}\">{sheight}</td><td class={sncls} title=\"{ntitle}\">{s_vs_net}</td><td>{packets}</td><td>{disc}</td>\
             <td class=num>{wins}</td><td class=num>{dups}</td><td class=num>{stale}</td><td class={oldcls}>{old}</td><td>{avg:.1}</td><td class=hist>{hist}</td></tr>",
            node = format!("node{}", src.id + 1),
            url = html_escape(&src.url),
            cls = if !connected {
                "down"
            } else if silent || lagging {
                "idle"
            } else {
                "up"
            },
            state_txt = if !connected {
                "DOWN"
            } else if silent {
                "IDLE"
            } else if lagging {
                "LAGGING"
            } else {
                "UP"
            },
            last_data = match idle {
                None => "&mdash;".to_string(),
                Some(d) if d > SILENCE_LIMIT => format!("<span class=over>{:.0}s ago</span>", d.as_secs_f64()),
                Some(d) => format!("{:.1}s ago", d.as_secs_f64()),
            },
            age = match s.age(now_ms) {
                None => "&mdash;".to_string(),
                Some(d) if lagging => format!("<span class=over>{:.1}s</span>", d.as_secs_f64()),
                Some(d) if d < Duration::from_secs(1) => format!("{}ms", d.as_millis()),
                Some(d) => format!("{:.1}s", d.as_secs_f64()),
            },
            // The worst this source has ever been, on hover. The column itself
            // is the present moment, which is what the verdict is made on.
            peak = match (s.peak_age(), s.lagging_ticks()) {
                (None, _) => "no data yet".to_string(),
                (Some(d), 0) => format!("worst since start: {:.1}s", d.as_secs_f64()),
                // A run of ticks behind that has not yet reached the threshold
                // is the interesting state: it is what a marginal verdict looks
                // like just before it fires.
                (Some(d), n) => format!("worst since start: {:.1}s; behind for {} tick(s)", d.as_secs_f64(), n),
            },
            // What the source says about itself, which is what the lag
            // verdict is made on. Shown next to `data age` so the two can
            // be compared: they answer different questions and were taken
            // for one another through three rounds of this.
            height = match s.health_height() {
                None => "&mdash;".to_string(),
                Some(h) => group(h),
            },
            hcls = if s.health_height().is_some() && !s.health_ready() { "over" } else { "num" },
            // Blocks behind the network itself, from outside our nodes: the one
            // column that shows every source falling behind together. A few
            // blocks either way is sampling, not lag -- the source's height is
            // polled twice a second, the network's arrives with every block.
            vs_net = match (net_height, s.health_height()) {
                (Some(n), Some(h)) => fmt_vs_network(n, h),
                _ => "&mdash;".to_string(),
            },
            ncls = match (net_height, s.health_height()) {
                (Some(n), Some(h)) if n.saturating_sub(h) > NET_BEHIND_WARN => "over",
                _ => "num",
            },
            ntitle = net_title,
            // The node underneath, from its own state file: where the chain is
            // on this machine. Book behind node is `order_book_server` catching
            // up -- after a restart it replays minutes of history -- while the
            // node is fine. The file trails the node's real head a little, so a
            // few dozen blocks here are normal.
            nheight = match s.node_height() {
                None => "&mdash;".to_string(),
                Some(h) => group(h),
            },
            nodetitle = "the node's own height, from its visor_abci_state.json (--node-state)",
            node_vs_net = match (net_height, s.node_height()) {
                (Some(n), Some(h)) => fmt_vs_network(n, h),
                _ => "&mdash;".to_string(),
            },
            nodecls = match (net_height, s.node_height()) {
                (Some(n), Some(h)) if n.saturating_sub(h) > NET_BEHIND_WARN => "over",
                _ => "num",
            },
            // The same height as read off the frames themselves (l2Diff, l4Book,
            // orderUpdates) -- what the clients actually receive, through our
            // reader. Below `height` means our reader is behind the node; both
            // below the network means the node is.
            sheight = match s.stream_height() {
                None => "&mdash;".to_string(),
                Some(h) => group(h),
            },
            stitle = "highest height in this source's frames since it connected, as our reader parsed them",
            s_vs_net = match (net_height, s.stream_height()) {
                (Some(n), Some(h)) => fmt_vs_network(n, h),
                _ => "&mdash;".to_string(),
            },
            sncls = match (net_height, s.stream_height()) {
                (Some(n), Some(h)) if n.saturating_sub(h) > NET_BEHIND_WARN => "over",
                _ => "num",
            },
            packets = group(s.packets.load(Relaxed)),
            disc = group(s.disconnects.load(Relaxed)),
            wins = group(s.wins.load(Relaxed)),
            dups = group(s.duplicates.load(Relaxed)),
            stale = group(s.stale.load(Relaxed)),
            old = group(s.too_old.load(Relaxed) + s.from_future.load(Relaxed)),
            // Anything here means a node handed us data from the past, which is
            // worth noticing rather than blending into the other counters.
            oldcls = if s.too_old.load(Relaxed) + s.from_future.load(Relaxed) > 0 { "over" } else { "num" },
            avg = s.avg_delay_us() / 1000.0,
            hist = s.hist_row_html(),
        ));
    }

    // ---- Table 2: last window (deltas) ----
    let mut win_rows = String::new();
    for src in &state.sources {
        let s = &src.stats;
        let connected = s.connected.load(Relaxed);
        let silent = connected && s.idle_for().is_some_and(|d| d > SILENCE_LIMIT);
        let lagging = connected && s.is_lagging();
        win_rows.push_str(&format!(
            "<tr><td class=nd>{node}</td><td>{url}</td>\
             <td class={cls}>{state_txt}</td><td>{packets}</td>\
             <td class=win>{wins}</td><td>{dups}</td><td>{stale}</td><td>{avg:.1}</td></tr>",
            node = format!("node{}", src.id + 1),
            url = html_escape(&src.url),
            // Same three-way state as the cumulative table above.
            cls = if !connected {
                "down"
            } else if silent || lagging {
                "idle"
            } else {
                "up"
            },
            state_txt = if !connected {
                "DOWN"
            } else if silent {
                "IDLE"
            } else if lagging {
                "LAGGING"
            } else {
                "UP"
            },
            packets = s.d_packets.load(Relaxed),
            wins = s.d_wins.load(Relaxed),
            dups = s.d_dups.load(Relaxed),
            stale = s.d_stale.load(Relaxed),
            avg = s.window_avg_delay_ms(),
        ));
    }

    // ---- Table 3: peers (from external probe) ----
    let probe = read_peer_probe();
    let mut peer_rows = String::new();
    let mut timer_html = String::from("<span id=probe-timer></span>");
    let mut timer_script = String::new();
    match &probe {
        Some(v) => {
            let updated = jstr(v, "updated_utc");
            let time_only = updated.split('T').nth(1).unwrap_or(updated.as_str()).trim_end_matches('Z');
            if let Some(peers) = v.get("peers").and_then(|p| p.as_array()) {
                for p in peers {
                    // red cell if metric exceeds its threshold (p50>200, p99>1000, stdev>100)
                    let cell = |key: &str, limit: f64| -> String {
                        let num = jnum(p, key);
                        if jf64(p, key) > limit {
                            format!("<td class=over>{}</td>", num)
                        } else {
                            format!("<td>{}</td>", num)
                        }
                    };
                    peer_rows.push_str(&format!(
                        "<tr><td class=nd>{node}</td><td>{peer}</td><td>{loc}</td>\
                         {p50}<td>{p95}</td>{p99}{stdev}<td class=sum>{last}</td></tr>",
                        node = html_escape(&jstr(p, "node")),
                        peer = html_escape(&jstr(p, "peer")),
                        loc = html_escape(&jstr(p, "location")),
                        p50 = cell("p50", 200.0),
                        p95 = jnum(p, "p95"),
                        p99 = cell("p99", 1000.0),
                        stdev = cell("stdev", 100.0),
                        last = html_escape(time_only),
                    ));
                }
            }
            let next_run = jstr(v, "next_run_utc");
            timer_script = PROBE_TIMER_JS.replace("__NEXT__", &next_run);
        }
        None => {
            peer_rows.push_str("<tr><td colspan=8 class=sum>no probe data (is hl_probe_json.sh running?)</td></tr>");
            timer_html = String::from("<span class=sum>no data</span>");
        }
    }
    if peer_rows.is_empty() {
        peer_rows.push_str("<tr><td colspan=8 class=sum>no probe data</td></tr>");
    }

    // ---- Table 4: clients ----
    let mut client_rows = String::new();
    let mut client_count = 0u64;
    for entry in state.clients.iter() {
        let c = entry.value();
        client_count += 1;
        // Tag each subscription with the node carrying it. On a sticky channel
        // that is the whole answer to "whose book is this client seeing", and
        // the per-source win counts cannot give it: they are aggregates over
        // every subscription at once. Raced channels are left untagged -- their
        // leader changes every block, so naming one would misrepresent them.
        let mut subs: Vec<String> = c
            .subscriptions
            .lock()
            .unwrap()
            .iter()
            .map(|k| {
                let label = k.label();
                let Some(e) = state.subs.get(k) else { return label };
                if e.is_rebuilding(c.id) {
                    format!("{label} (rebuilding)")
                } else if !k.single_sourced() {
                    label
                } else {
                    match e.leader() {
                        Some(id) => format!("{label} (node{})", id + 1),
                        None => format!("{label} (no source)"),
                    }
                }
            })
            .collect();
        subs.sort();
        client_rows.push_str(&format!(
            "<tr><td>{id}</td><td>{ip}</td><td>{coins}</td><td>{bytes}</td><td>{dropped}</td><td>{age}s</td></tr>",
            id = c.id,
            ip = html_escape(&c.ip),
            coins = if subs.is_empty() { "&mdash;".to_string() } else { html_escape(&subs.join(", ")) },
            bytes = fmt_bytes(c.bytes_sent.load(Relaxed)),
            dropped = c.dropped.load(Relaxed),
            age = c.connected_at.elapsed().as_secs(),
        ));
    }

    let mut coins: Vec<String> = state
        .subs
        .iter()
        .filter(|e| e.upstream_subscribed)
        // The probe is held open with no client behind it, so mark it rather
        // than let it read as somebody's forgotten subscription.
        .map(|e| if e.pinned { format!("{} (probe)", e.key().label()) } else { e.key().label() })
        .collect();
    coins.sort();

    // ---- assemble ----
    let head = r#"<!doctype html><html><head><meta charset=utf-8>
<meta http-equiv=refresh content=2>
<title>WSARB stats</title>
<style>
body{font:13px/1.4 monospace;margin:1.5em;color:#ddd;background:#111}
h1{margin:0 0 .2em}
.cap{margin:1.2em 0 .4em;font-size:13px}
.c1{color:#8ab4f8}.c2{color:#ffb74d}.c3{color:#81c784}
table{border-collapse:collapse;width:100%}
th,td{border:1px solid #333;padding:3px 7px;text-align:left}
th{background:#1c1c1c}
.up{color:#4caf50;font-weight:bold}
.down{color:#e53935;font-weight:bold}
.idle{color:#ffb74d;font-weight:bold}
.b{color:#8ab4f8;white-space:nowrap}
.nd{color:#8ab4f8}
.win{color:#4caf50;font-weight:bold}
.num{min-width:8.5em;white-space:nowrap}
.hist{white-space:normal;word-break:break-word}
.sum{color:#999}
.legend{margin:.4em 0}
.sq{display:inline-block;width:11px;height:11px;border-radius:2px;vertical-align:middle;margin-right:4px}
.greenrow{background:rgba(76,175,80,.22)}
.bluerow{background:rgba(33,150,243,.22)}
.limecell{background:rgba(174,234,0,.38);font-weight:bold}
.nodecell{background:rgba(0,206,209,.40);font-weight:bold}
.over{color:#e53935;font-weight:bold}
.badrow{background:rgba(229,57,53,.30)}
.badrow td.down{color:#fff;font-weight:bold}
.badlabel{color:#fff;font-weight:bold}
</style></head><body>
"#;

    let mut out = String::with_capacity(8192);
    out.push_str(head);
    out.push_str("<h1>WSARB</h1>\n");
    out.push_str(&format!(
        "<p class=sum>{nsources} sources &middot; {nclients} clients &middot; {ncoins} subscriptions upstream: {coinlist}</p>\n",
        nsources = state.sources.len(),
        nclients = client_count,
        ncoins = coins.len(),
        coinlist = if coins.is_empty() { "&mdash;".to_string() } else { html_escape(&coins.join(", ")) },
    ));

    out.push_str("<div class='cap c1'>Data connections &mdash; cumulative since start</div>\n");
    out.push_str(&network_line(state, now_ms));
    out.push_str("<table>\n");
    out.push_str("<tr><th>node</th><th>endpoint</th><th>state</th><th>last data</th><th>data age</th><th>node height</th><th>node vs net</th><th>book height</th><th>book vs net</th><th>stream height</th><th>stream vs net</th><th>packets</th><th>disc</th><th>wins</th><th>dups</th><th>stale</th><th>too old</th><th>avg delay (ms)</th><th>delay histogram</th></tr>\n");
    out.push_str(&cum_rows);
    out.push_str("</table>\n");

    out.push_str("<div class='cap c1'>Data connections &mdash; last 5s window</div>\n<table>\n");
    out.push_str("<tr><th>node</th><th>endpoint</th><th>state</th><th>packets/5s</th><th>wins/5s</th><th>dups/5s</th><th>stale/5s</th><th>avg delay (ms)</th></tr>\n");
    out.push_str(&win_rows);
    out.push_str("</table>\n");

    out.push_str(&format!(
        "<div class='cap c2'>Peers &mdash; block-lag probe (updated every 5 min &middot; {timer})</div>\n\
         <div class='sum' style='margin:-.2em 0 .5em;font-size:11px'>racer view &mdash; a node's own peer can stream even if shown backoff here</div>\n<table>\n",
        timer = timer_html
    ));
    out.push_str("<tr><th>node</th><th>peer</th><th>location</th><th>p50</th><th>p95</th><th>p99</th><th>stdev</th><th>last run (UTC+0)</th></tr>\n");
    out.push_str(&peer_rows);
    out.push_str("</table>\n");

    out.push_str("<div class='cap c3'>Client connections</div>\n<table>\n");
    out.push_str("<tr><th>id</th><th>ip</th><th>subscriptions</th><th>traffic sent</th><th>dropped</th><th>age</th></tr>\n");
    if client_rows.is_empty() {
        out.push_str("<tr><td colspan=6 class=sum>no clients</td></tr>\n");
    } else {
        out.push_str(&client_rows);
    }
    out.push_str("</table>\n");

    render_race_section(&mut out);
    out.push_str(RACE_TIMER_JS);
    out.push_str(&timer_script);
    out.push_str("</body></html>");
    out
}

fn fmt_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n} B")
    } else {
        format!("{v:.2} {}", UNITS[u])
    }
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}
