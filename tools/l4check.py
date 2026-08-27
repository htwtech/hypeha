#!/usr/bin/env python3
"""Stream checker for the l4Book channel of wsarb.

Reads as fast as it can and validates the stream in flight rather than saving
it: l4Book runs at roughly 3 MB/s per coin, so writing it down is the slow path.

Checks, in order of how badly a failure would hurt:

  * the stream opens with a Snapshot, before any Updates;
  * block heights never go backwards (a stale frame slipped through);
  * a block never reopens once a later one has started, which would mean the
    block was spliced together from two sources - exactly what the
    block-boundary arbitration exists to prevent.

Gaps between heights are NOT failures. If nothing happened for the coin during
a block, the upstream sends nothing for it and the height simply jumps.

With --late N a second client joins N seconds in. It must get a snapshot of its
own and then track the first client exactly; that is the late-joiner path.

    pip install websockets
    python3 l4check.py --seconds 600
    python3 l4check.py --seconds 600 --late 30
"""

import argparse
import asyncio
import json
import sys
import time

import websockets

HEIGHT = '"height":'


def height_of(msg):
    """Pull the block height out without parsing the whole frame.

    `height` sits near the front of both frame kinds, well ahead of the bulky
    `levels` / `order_statuses`, so this stops early even on a 22 MB snapshot.
    """
    i = msg.find(HEIGHT)
    if i < 0:
        return None
    j = k = i + len(HEIGHT)
    while k < len(msg) and msg[k].isdigit():
        k += 1
    return int(msg[j:k]) if k > j else None


class Report:
    def __init__(self, label):
        self.label = label
        self.msgs = 0
        self.bytes = 0
        self.blocks = []  # distinct heights, in the order first seen
        self.counts = {}  # height -> messages received for it
        self.snapshot_height = None
        self.updates_before_snapshot = 0
        self.resyncs = []  # (height before, height of the new snapshot)
        self.violations = []
        self.started = None
        self.ended = None
        self.closed_early = None

    @property
    def seconds(self):
        if self.started is None:
            return 0.0
        return (self.ended or time.monotonic()) - self.started

    def ok(self):
        return (
            not self.violations
            and self.updates_before_snapshot == 0
            and self.snapshot_height is not None
            and not self.closed_early
        )

    def render(self):
        secs = max(self.seconds, 1e-9)
        out = [
            "[{}] {} messages, {} blocks, {:.1f} MB in {:.1f}s ({:.0f} msg/s, {:.2f} MB/s)".format(
                self.label, self.msgs, len(self.blocks), self.bytes / 1e6, secs,
                self.msgs / secs, self.bytes / secs / 1e6,
            )
        ]
        if self.snapshot_height is None:
            out.append("[{}] FAIL: no snapshot ever arrived".format(self.label))
        else:
            out.append("[{}] snapshot at height {}".format(self.label, self.snapshot_height))
        if self.updates_before_snapshot:
            out.append(
                "[{}] FAIL: {} updates arrived before the snapshot - the book would be "
                "built on nothing".format(self.label, self.updates_before_snapshot)
            )
        for before, after in self.resyncs:
            out.append("[{}] resync: new snapshot at {} while at block {}".format(self.label, after, before))
        if self.closed_early:
            out.append("[{}] FAIL: stream ended after {:.1f}s: {}".format(self.label, secs, self.closed_early))
        for v in self.violations[:20]:
            out.append("[{}] FAIL: {}".format(self.label, v))
        if len(self.violations) > 20:
            out.append("[{}] ... and {} more".format(self.label, len(self.violations) - 20))
        if self.ok():
            tail = " across {} resync(s)".format(len(self.resyncs)) if self.resyncs else ""
            out.append(
                "[{}] OK: snapshot first, heights monotonic, no block reopened{}".format(self.label, tail)
            )
        return "\n".join(out)


async def run_client(url, coin, seconds, label, delay=0.0, ping=None, progress=10.0):
    rep = Report(label)
    if delay:
        await asyncio.sleep(delay)

    req = json.dumps({"method": "subscribe", "subscription": {"type": "l4Book", "coin": coin}})
    seen = set()
    last = None

    try:
        # max_size=None: the BTC snapshot is ~22 MB, far past the 1 MB default.
        async with websockets.connect(url, max_size=None, ping_interval=ping) as ws:
            rep.started = time.monotonic()
            await ws.send(req)
            deadline = rep.started + seconds
            next_tick = rep.started + progress if progress else float("inf")

            while True:
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=left)
                except asyncio.TimeoutError:
                    break

                if isinstance(msg, bytes):
                    msg = msg.decode()
                rep.bytes += len(msg)

                now = time.monotonic()
                if now >= next_tick:
                    elapsed = now - rep.started
                    print(
                        "[{}] {:>5.0f}s  {:>8} msgs  {:>6} blocks  {:>7.1f} MB  "
                        "{:>6.0f} msg/s  {} violations".format(
                            label, elapsed, rep.msgs, len(rep.blocks), rep.bytes / 1e6,
                            rep.msgs / max(elapsed, 1e-9), len(rep.violations),
                        ),
                        flush=True,
                    )
                    next_tick = now + progress

                head = msg[:64]
                if '"subscriptionResponse"' in head:
                    continue

                rep.msgs += 1
                is_snapshot = '"Snapshot"' in head
                h = height_of(msg)
                if h is None:
                    continue

                if is_snapshot:
                    if rep.snapshot_height is None:
                        rep.snapshot_height = h
                    else:
                        # A second snapshot means wsarb rebuilt this client's
                        # stream, because a source leading an open block went
                        # away. That is a reset, not an increment: the book
                        # starts over here, so a lower height is correct and the
                        # continuity checks restart with it.
                        rep.resyncs.append((last, h))
                        seen.clear()
                        last = None
                elif rep.snapshot_height is None:
                    rep.updates_before_snapshot += 1

                rep.counts[h] = rep.counts.get(h, 0) + 1

                if last is not None and h == last:
                    continue
                if last is not None:
                    if h < last:
                        rep.violations.append("height went backwards: {} -> {}".format(last, h))
                    elif h in seen:
                        rep.violations.append("block {} reopened after {}".format(h, last))
                rep.blocks.append(h)
                seen.add(h)
                last = h
    except Exception as e:  # any failure is itself a result worth printing
        rep.closed_early = "{}: {}".format(type(e).__name__, e)

    rep.ended = time.monotonic()
    return rep


def compare(first, late):
    """The late joiner must have tracked the first client exactly."""
    if late.snapshot_height is None or not late.blocks:
        return ["FAIL: the late joiner never got a snapshot, nothing to compare"]

    # A resync restarts a client from a fresh snapshot, and the two clients need
    # not resync at the same instant, so only compare where both ran unbroken.
    floor = 0
    for r in first.resyncs + late.resyncs:
        floor = max(floor, r[1])
    if floor:
        first_blocks = [h for h in first.blocks if h >= floor]
        late_blocks = [h for h in late.blocks if h >= floor]
    else:
        first_blocks, late_blocks = first.blocks, late.blocks
    if not late_blocks:
        return ["both clients resynced too near the end to compare"]

    lo, hi = late_blocks[0], late_blocks[-1]
    a = [h for h in first_blocks if lo <= h <= hi]
    b = [h for h in late_blocks if lo <= h <= hi]

    out = []
    if a == b:
        note = " (after resync)" if floor else ""
        out.append("late joiner tracked the first client exactly over {} shared blocks{}".format(len(b), note))

        # Matching heights are not the same as matching content. Losing the tail
        # of a block - which is what a leading source dying mid-block costs -
        # leaves the height sequence perfectly intact, so completeness has to be
        # checked separately. Both clients are fed the same forwarded stream, so
        # any block each received in full must have the same message count.
        # The first and last shared blocks are skipped: those are legitimately
        # partial for whichever client joined or left inside them.
        interior = [h for h in b[1:-1]]
        short = [
            (h, first.counts.get(h, 0), late.counts.get(h, 0))
            for h in interior
            if first.counts.get(h, 0) != late.counts.get(h, 0)
        ]
        if short:
            out.append("FAIL: {} blocks arrived truncated - the two clients got different "
                       "message counts for the same block".format(len(short)))
            for h, n_first, n_late in short[:10]:
                out.append("  block {}: first client {} messages, late joiner {}".format(h, n_first, n_late))
        elif interior:
            out.append("every one of {} fully shared blocks arrived complete".format(len(interior)))
        return out

    only_a = sorted(set(a) - set(b))
    only_b = sorted(set(b) - set(a))
    out = ["FAIL: the two clients disagree over the shared window"]
    if only_a:
        out.append("  only the first client saw: {}".format(only_a[:10]))
    if only_b:
        out.append("  only the late joiner saw: {}".format(only_b[:10]))
    if not only_a and not only_b:
        out.append("  same blocks, different order")
    return out


async def main():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--url", default="ws://localhost:48082/ws")
    p.add_argument("--coin", default="BTC")
    p.add_argument("--seconds", type=float, default=600)
    p.add_argument("--late", type=float, metavar="N",
                   help="also join a second client N seconds in, exercising the snapshot path")
    p.add_argument("--ping", type=float, default=None,
                   help="send websocket pings this often, in seconds (default: off)")
    p.add_argument("--progress", type=float, default=10.0, metavar="SECS",
                   help="print a running line this often; 0 to stay quiet (default: 10)")
    args = p.parse_args()

    jobs = [run_client(args.url, args.coin, args.seconds, "first",
                       ping=args.ping, progress=args.progress)]
    if args.late is not None:
        jobs.append(run_client(args.url, args.coin, max(args.seconds - args.late, 1.0),
                               "late", delay=args.late, ping=args.ping, progress=args.progress))

    reports = await asyncio.gather(*jobs)

    print()
    for r in reports:
        print(r.render())
        print()

    failed = any(not r.ok() for r in reports)
    if len(reports) == 2:
        for line in compare(reports[0], reports[1]):
            print(line)
            if line.startswith("FAIL"):
                failed = True
        print()

    print("RESULT:", "FAILED" if failed else "PASSED")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
