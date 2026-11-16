# TODO

## l2Diff: a flush of the snapshot's own block cannot be placed by height

**Where:** `src/state.rs`. Held frames are filtered in `deliver_snapshot_to`
(`*h > height`), and frames that arrive after the snapshot in
`behind_snapshot`.

**What.** A late joiner's `l2Diff` snapshot comes from a connection opened
for it, and its updates come from our shared upstream connection. The server
flushes L2 every 50 ms, so one block is often flushed more than once, and
every flush of a block carries the same `height` (and the same `time`). wsarb
tells the frames already in the snapshot from the ones that are not by
height alone:

- frames held while the client waited for its snapshot: only those with
  `height > snapshot` are released;
- frames arriving after the snapshot: only those with `height < snapshot` are
  withheld (since 85e672f).

A frame at `height == snapshot` can be any of three things:

1. the flush the snapshot was taken from: applying it changes nothing;
2. a later flush of that block: the client needs it;
3. an earlier flush of that block: applying it puts older sizes over newer
   ones, because a diff carries absolute sizes.

A held frame at that height is dropped, which is right for 1 and 3 and wrong
for 2. One arriving after the snapshot is sent, which is right for 1 and 2 and
wrong for 3. In the wrong cases a level stays stale until it next changes, and
nothing tells the client: the `prevHeight` chain is intact.

**How likely.** Not seen live. Through wsarb on MERL at depth 1000, 158 of
158 and 261 of 261 books matched `l2Book` (2026-11-16). It takes our
connection and the throwaway one being a whole flush apart at the moment a
client joins, on a coin that changed in both flushes of that block.

**Fix.** It needs the server (`htwtech/hl-orderbook-server`) to number its L2
flushes: one counter per server process, the same on every connection, sent in
every `l2Diff` frame, `Snapshot` and `Updates` alike (for example `flush`).
Then wsarb compares flush numbers instead of heights: a frame with
`flush <= snapshot.flush` is in the snapshot, any other is not, and both
filters become exact.

- A counter per process is enough: wsarb only combines a snapshot and updates
  from one source (`Seq::Sticky`), and it re-snapshots after a reconnect, so a
  counter restarted along with the server is never compared across the
  restart.
- Keep today's height rule when the field is missing, so wsarb still works
  with servers that do not send it.
- Tests: like `a_frame_held_behind_a_snapshot_is_chained_to_it` in
  `src/state.rs`, with frames at the snapshot's height and flush numbers
  below, at and above the snapshot's.
