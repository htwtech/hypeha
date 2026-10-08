# WSARB

Websocket **arb**itration proxy for [order_book_server](https://github.com/imperator-co/order_book_server)
feeds. It fans a single downstream client base out across several redundant
upstream sources, forwarding each update once from whichever source delivered it
first — so clients see the lowest-latency stream available even when individual
sources lag or drop.

## How it works

- Connects to every configured upstream source on startup and idles.
- Clients connect over websocket (`/ws`) and send the upstream's own subscribe
  frames verbatim:
  ```json
  {"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}
  ```
- The first subscription to a key triggers a subscribe on **all** sources; the
  last client to leave it unsubscribes them again.
- One `bbo` subscription is held open permanently as a latency probe, so the
  per-source counters keep moving with nobody connected — see below.
- Sources reconnect automatically with capped exponential backoff. A dead source
  never blocks the service; on reconnect it re-subscribes to everything live.

## Channels

| Channel | Ordered by | Key |
|---------|-----------|-----|
| `bbo` | block `time`, one source per stamp | coin |
| `l2Book` | block `time`, one source throughout | coin + `nSigFigs`/`nLevels`/`mantissa` |
| `l2Diff` | block `height`, one source throughout | coin + `nSigFigs`/`nLevels`/`mantissa` |
| `trades` | `tid` | coin |
| `orderUpdates` | `height` | user |
| `l4Book` | block `height` | coin |
| `bookDiffs` | — | refused, see below |

The aggregation parameters are part of the `l2Book` identity, so two clients
asking for different `nSigFigs` get two separate streams rather than one shared
one.

`{"method":"ping"}` and `{"method":"unsubscribe"}` are supported. A request that
cannot be parsed or routed is logged with its text — it is never dropped
silently.

### Arbitration

wsarb **never compares message contents** — no hashing, no byte comparison.
Deduplication runs on a single number taken from each frame, or on a message's
position within a block. That keeps it cheap, and it also draws the boundaries
of what it can and cannot catch.

Every frame takes the same path:

```
frame from a source socket
  → parse into Frame (tagged on "channel")
  → "error" → logged, dropped
  → route() → (SubKey, Seq)      subscription key + ordering value
  → future check                 a stamp ahead of the clock is refused
  → record how old this frame is  diagnostic only (`data age`)
  → absolute freshness check     --max-age, counted as "too old"
  → arbitration                  the three modes below
  → fan out, and pile up behind any parked client
```

#### What identifies a subscription, and what orders it

| Channel | Key | Ordering value | Mode |
|---|---|---|---|
| `bbo` | coin | `data.time` | Lead |
| `l2Book` | coin + `nSigFigs`/`nLevels`/`mantissa` | `data.time` | Sticky |
| `l2Diff` / `Snapshot` | coin + params | `height` | Snapshot |
| `l2Diff` / `Updates` | coin + params | `height` | Sticky |
| `trades` | coin | **max** `tid` in the batch | Point |
| `orderUpdates` | user, lower-cased | **max** `height` in the batch | Block |
| `l4Book` / `Snapshot` | coin | `height` | Snapshot |
| `l4Book` / `Updates` | coin, from a nested element | `height` | Block |
| `bookDiffs` | — | nothing to take | refused at subscribe |

Batched channels order on the **maximum**, not the first element: sources cut
batches differently, and a short batch would otherwise look newer than a long
one covering the same ground.

#### Point — newest wins

`trades` only. Its ordering value is a trade id, which rises within a block as
well as across blocks, so an equal value really does mean the same batch twice.
The other channels stamp their frames with the block, where equal values are the
norm and dropping them throws data away — see the two modes below.

| Condition | Action |
|---|---|
| `v > last` | wins: `last = v`, fanned out |
| `v == last` | a slower source's copy: lateness recorded, **not** forwarded |
| `v < last` | stale, dropped |

Each repeat is counted separately.

#### Lead — one source carries a stamp

`bbo` only: a channel the upstream **dedups before sending**.

It stamps its frames with the block, so a block's several frames share a stamp
and differ in content, and newest-wins kept the **first** — the staler of them.
That cost `bbo` some nine updates in ten.

Racing positions, as below, would be wrong here: `bbo` is suppressed whenever
its values repeat what that connection was last sent, so how many frames a block
contains stops being a property of the data and position #k is not the same
state on two sources. Measured, not assumed — two different nodes disagreed on a
stamp's message count for **7%** of `bbo` stamps against a 0.6% baseline within
one node.

`l2Book` was here too until the nodes were measured against each other; it is
now Sticky, below.

| Condition | Action |
|---|---|
| `v > last` | new stamp: `last = v`, **this source takes the stamp**, fanned out |
| `v == last` from the source holding the stamp | its own later, fresher snapshot — fanned out |
| `v == last` from anyone else | lateness recorded, not forwarded |
| `v < last` | stale, dropped |

The client follows one source through a stamp, so the book only ever moves
forward. The stamp is taken afresh by whoever opens the next one, so a source
that dies costs at most the rest of the block it was holding.

#### Sticky — one source carries the whole stream

`l2Diff` and `l2Book` — the same book, sent two ways.

Both are here for one measured reason: **the two nodes do not hold the same
book.** 15% of levels apart at depth 20, 17% at 100, 36% at 1000, on stock
binaries and on `l2Book` itself, so it is a property of the upstream and not of
anything here. Lead re-elects on every stamp, which meant swapping the client's
book between two of them ten-odd times a second.

For `l2Diff` that is fatal rather than merely untidy. Its frames are the only
ones here that are **not self-contained**: an increment means something only
against the book it was computed from, so laying one node's increment on
another's corrupts it silently and for good.

For `l2Book` every frame is a whole book, so nothing breaks either way. It is
Sticky because a book that stays with one node beats one a few milliseconds
fresher — at depth the swapping showed up as liquidity appearing and vanishing
with no trade behind it.

So the leader is not re-elected per stamp as in Lead. It is chosen once and
holds the stream until it dies.

| Condition | Action |
|---|---|
| no leader yet | this source takes the stream, fanned out |
| leader, `v >= last` | fanned out — a further flush of the same block is a further increment, not a repeat |
| leader, `v < last` | it replayed: stale, dropped |
| anyone else | lateness recorded, **never** forwarded |

When the leader dies, `l2Diff` clients are parked and rebuilt from a fresh
snapshot by the same path `l4Book` uses; `l2Book` needs none of that, since the
next frame from whoever takes over is already a whole book — only the leadership
is freed, along with the height, because the surviving source is often the
slower spare and would otherwise look stale for seconds.

Either way the book jumps to the replacement node's version — the jump is real,
but the alternative is not "no jump", it is a book
quietly spliced together from two different ones.

"Dies" is not only the whole source going quiet or falling behind. A leader can
stop sending **one** `l2Diff` subscription while carrying on with everything
else — probe, other coins, a height that keeps rising on `/health` — and then
neither source-level watchdog fires, while the other node's frames for that very
key arrive and are dropped. That was caught in production: one snapshot, then a
minute of nothing. So each subscription also remembers when every source last
sent anything for it, and once a second the leader is checked against the rest:
quiet on the key for 5 s while another source kept sending it, three checks
running, and the clients are rebuilt — from a snapshot asked of the **other**
sources first, since the one just abandoned may well still rank highest. Quiet
on a key that nobody is sending is a coin that is not changing, and is left
alone.

#### Block — positions raced within one stamp

`l4Book` updates and `orderUpdates`: the channels that emit **every** batch which
changed anything, with no dedup in between.

That is what makes them safe to race — the message count of a block follows the
chain data, so every node cuts it the same way and position #k means the same
event everywhere. Measured rather than assumed: two connections to one node, and
two different nodes, agreed on the message count of 99.8% and 99.4% of blocks
respectively, with the remainder accounted for by the stalls in the same run.

They share a stamp across many messages: roughly 200 `l4Book` messages carry one
`height`, and every `orderUpdates` batch of a block repeats its `height`.
Newest-wins would therefore keep the first and discard the rest.

So the count of messages each source has delivered for the current stamp is
tracked, and a frame is forwarded when a source's count **passes** the number of
positions already sent. Message #k goes out the moment any source reaches it.

| Condition | Action |
|---|---|
| source blacklisted | dropped as stale |
| `h > last` | new stamp: position counts reset, forwarded |
| `h == last`, source's count exceeds what was sent | forwarded |
| `h == last`, position already sent | duplicate — **one** lateness sample per source per stamp |
| `h < last` | stale, dropped |

Two things follow. Every message travels at the speed of the fastest source, not
just the first one to open the stamp. And a source that dies partway through
costs nothing: the others carry on from the position it stopped at, with no
timeout to wait out and no detection step — the counter *is* the failover.

Lateness is sampled once per source per stamp rather than once per message, or a
single block would contribute two hundred samples and swamp the histogram.

This rests on every node producing the same messages in the same order for a
given stamp — see the measurement above. `l2Book` and `bbo` are deliberately
kept out of this mode because for them it does not hold: the upstream dedups
both before sending, so a block's message count is not a property of the data.

#### Snapshot — a reset

`l4Book` snapshots only, and they do two things: they **clear the source's
blacklist entry**, which nothing else can do, and they **reset the key** if the
height is ahead of what has been sent. An older one is dropped — every source
snapshots on subscribing, and only the first to arrive is of any use.

#### Two different kinds of "old"

They are counted apart because they mean different things.

**`stale`** is relative: older than what has already been forwarded for this
key. It catches a node replaying blocks it has already sent.

**`too old`** is absolute: the frame's own block timestamp is further in the
past than `--max-age`. It catches what relative ordering structurally cannot —
see [Refusing data from the past](#refusing-data-from-the-past).

**`data age`** is how old the data a source is sending is, against the clock —
not to be confused with `last data`, which is how long ago it last sent
anything. Diagnostic only: it is measured where frames are parsed, so it reports
our own reader as much as the source, and the lag verdict is made on the
`height` column beside it instead. A node replaying blocks scores perfectly on the second and terribly on
the first; see [A source that talks while falling
behind](#a-source-that-talks-while-falling-behind).

#### Around the edges

**The blacklist.** A source that falls silent mid-stamp is marked, and its
increments are ignored until it sends a snapshot. Without this, a node that
dropped behind and came back would drag clients forward past the blocks they
never received. Only channels that are actually incremental are ever
blacklisted: `bbo` has no snapshots, so a mark there could never be cleared.

**Parked clients.** A client subscribing to `l4Book` mid-stream cannot use the
snapshot that opened it, so it is held aside while the live frames pile up
behind it and a private snapshot is fetched over a throwaway connection. It then
receives the snapshot, the held frames above its height, and joins the live
stream. If no snapshot can be had it is disconnected rather than served a book
with nothing under it.

**Queue overflow.** On an incremental channel the connection is closed, because
a hole there is permanent. Elsewhere the frame is dropped and counted.

**`bookDiffs` is refused** at subscribe time. Its frames carry no time, height or
sequence of any kind, so there is nothing to order two sources by. `l4Book`
carries the same diffs wrapped in a block height and is the arbitrable way to
get them.

## Run

```sh
cargo run --release -- \
  --source ws://localhost:48001/ws ws://localhost:48002/ws \
  --listen 0.0.0.0:48080 \
  --dashboard-listen 0.0.0.0:48090
```

- `-s, --source <URL>...` — one or more upstream websocket endpoints (required).
- `-l, --listen <ADDR>` — bind address for clients (default `0.0.0.0:8080`).
- `--dashboard-listen <ADDR>` — bind address for the stats page (default `0.0.0.0:48090`).
- `--probe-coin <COIN>` — coin for the permanent `bbo` probe (default `BTC`).
- `--no-probe` — drop the probe entirely.
- `--max-age <SECS>` — refuse frames whose block time is older than this (default 60; 0 disables).
- `--lag-blocks <N>` — move clients off a source whose book has fallen this many blocks behind the furthest-ahead source (default 50, about 3.5s; 0 disables). Read from each source's own `GET /health`.

### Refusing data from the past

Ordering is judged against what has already been forwarded, which leaves one
case uncovered. When the last subscriber to a key disconnects, its state goes
too; the next subscriber starts from nothing, and there the **first frame to
arrive wins however old it is**. A node frozen half an hour ago answers a
subscribe faster than a healthy one — it has nothing left to compute — so it can
easily be that first frame, and the client would open on a stale book.

`--max-age` closes that: a frame whose own block timestamp is older than the
limit is refused outright, counted under `too old` on the dashboard, and logged.

This makes the data path depend on wsarb and the nodes sharing a clock. On one
machine they do. Across machines a skew would make wsarb discard everything,
which is why the default is generous, every rejection is logged rather than
silent, and `--max-age 0` turns it off.

### The probe

The arbitration counters are the only measure of how the sources compare, and
they move only while something is streaming — so with no clients connected there
is nothing to judge the nodes by, which is precisely when you want to be judging
them. wsarb therefore holds one `bbo` subscription open on its own behalf.

`bbo` is the right channel for it: top of book only, a few hundred bytes an
update, and it ticks steadily rather than in bursts, so it samples latency more
evenly than the heavier channels. It shows on the dashboard marked `(probe)`.

## Endpoints

- `GET /ws` — client websocket, on the `--listen` port.
- `GET /` or `/stats` — auto-refreshing HTML stats page, on the
  `--dashboard-listen` port:
  - **Per source:** state, age of the last data, packets, disconnects, wins
    (delivered an update first), duplicates (slower), stale, average lateness,
    and a delay histogram versus the fastest source.
  - **Per client:** IP, subscriptions, traffic sent, dropped frames, age.

### Live socket vs live data

A source shows `UP` when connected, **`IDLE` when connected but no longer
delivering**, and **`LAGGING` when delivering data that has fallen behind the
other sources**. Three states because there are three things that can be true of
a socket, and only the first one is obvious from the outside.

`order_book_server` reads the files its Hyperliquid node writes, so if that node
dies the server keeps the websocket open and simply stops speaking. That is
`IDLE`. Silence alone is not a fault, though: a quiet market silences every
source at once. A source is only treated as lost when it has gone quiet **while
another source is still delivering**. When that happens its `l4Book` clients are
rebuilt from a fresh snapshot, exactly as on a dropped connection — otherwise a
source that was leading an open block would strand them holding half of it,
forever and without a word in the log.

### A source that talks while falling behind

The other way a source fails, and the one that hides: it keeps sending, at the
wrong height. A server that restarts, or re-syncs its book, replays the node's
files from where the node last persisted its state — minutes of them — so it is
the **busiest** source on the machine while serving a book the chain has long
moved past. Every arrival-based measure reads it as the healthiest source there
is: it never goes idle, it answers a snapshot request first, and it is first to
speak into any stream left without a leader.

On the single-sourced channels (`l2Book`, `l2Diff`) that is worse than useless.
The client follows one source from end to end, so a leader replaying blocks
delivers nothing it can use — its frames are older than what the client already
holds — while the healthy source's frames are refused for not being the leader's.
The stream simply stops, with a working node sitting right there.

`--lag-blocks` closes it. Each source is **asked** where it stands — wsarb polls
its `GET /health` twice a second, which `order_book_server` answers lock-free
with the height of the book it has built — and a source more than that many
blocks behind the furthest-ahead one, or reporting itself not `ready`, is marked
`LAGGING`, has its streams taken away, and has its clients rebuilt from a
snapshot fetched **elsewhere**. It cannot take the stream back while marked, and
it is not asked for the snapshot: the whole point is to stop handing the stream
straight back to the node that just lost it.

Asking rather than inferring matters more than it sounds. Three earlier
versions of this read a side effect instead: how recently a source spoke (a node
replaying old blocks is the chattiest there is), the block time of frames as
*our* reader parsed them (a reader held up by a lock reads exactly like a node
that stopped), and the block time of the `bbo` probe (`bbo` is sent only when
the top of book changes, so silence there means the price did not move). The
third one marked a source that was a single block behind and sending more
packets than the other. A height the server reports about itself has none of
those ambiguities — and comparing two heights needs no clock, which retires the
question of whether ours agrees with the nodes'.

The rule is relative, like silence, and for the same reason — a market that
stops trading ages every source at once and none of them is at fault. Which also
guarantees the freshest source is never the one marked, so there is always
somewhere to move the clients to.

The same rule guards snapshot fetches: a node that has died still answers, with a
book frozen at its last block. Snapshots are taken from the freshest source, and
from a quiet one only when none of them is delivering.

## Limits

- The upstream caps a connection at **256 subscriptions**. wsarb multiplexes
  every client's subscriptions onto one connection per source, so that cap is
  shared across all clients.
- `l4Book` is heavy: the BTC snapshot is ~22 MB, and the diff stream has been
  measured between 1 and 3.5 MB/s per coin per source — it tracks market
  activity, so size the downstream for the peak, not the average. Roughly 200
  messages share one block height.

## Layout

| File | Responsibility |
|------|----------------|
| `src/main.rs` | CLI, wiring, axum servers (client ws + dashboard) |
| `src/state.rs` | `SubKey`, subscription registry, arbitration (`on_update`) |
| `src/upstream.rs` | Per-source connect / (re)subscribe / reconnect, frame routing |
| `src/client.rs` | Downstream client socket handling |
| `src/stats.rs` | Stats counters, delay histogram, HTML rendering |
