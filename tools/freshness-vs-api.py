#!/usr/bin/env python3
"""How far behind the public Hyperliquid API is each of our sources -- and is
any of them standing still?

Every freshness check so far compared our sources with each other, or took a
node's word for its own height. Neither can see both nodes serving old data at
once: they agree with each other, and each reports itself fine. This one leans
on something that does not depend on our nodes at all.

The reference is the public API's `bbo`. Its `time` is the block time -- the
same quantity our nodes put in their frames -- so the comparison is two block
times against each other, and our clock takes no part in it. That matters more
than it sounds: the first run of this found the clock on the machine it ran on
seven seconds fast, which against the clock would have read as every source,
the API included, being seven seconds behind.

`bbo` rather than `l2Book` because of how often it comes. Measured: the API's
`bbo` arrives about ten times a second, its `l2Book` once every two or three
seconds -- and the bound below is only as tight as the reference is frequent.
`--api-channel l2Book` (or `trades`) is there to cross-check.

Subscribes, all on one coin:

  api            bbo from the public API
  A, B           l2Book straight from each node
  wsarb-l2Book   l2Book through wsarb
  wsarb-l2Diff   l2Diff through wsarb (time from Snapshot / Updates)

Once a second, per source: frames in that second, how far behind the API's
newest block it is, and how long it has stood still while the API moved on.

"Behind" is measured on every frame as it arrives: the API's newest block at
that moment, minus the block the frame carries. Per frame rather than "newest
against newest" on purpose: a channel that sends only when something changes --
`l2Diff` does -- would otherwise look behind for the whole of every pause, and
the first version of this reported a perfectly fresh but infrequent stream as
four seconds behind. The API's newest block itself trails the chain a little,
so this is a lower bound: if the API had already seen a newer block when the
frame arrived, the source was behind by at least that much.

"Stuck" is a source whose newest block has not moved for --stuck seconds while
the API's has. Reliable on an active coin such as BTC, where every channel moves
every block or two; on a quiet coin a change-only channel can be silent for
good reason, and a STUCK there is a question rather than a verdict.

The `api age` column is the API's newest block against the local clock -- a
check on the clock and the network, not on the nodes.

Standard library only, so it runs on the node with nothing installed.

  python3 freshness-vs-api.py --wsarb ws://localhost:48000/ws --coin BTC --seconds 60
  python3 freshness-vs-api.py --levels 1000          # our side at 1000 levels
"""

import argparse
import base64
import json
import os
import socket
import ssl
import struct
import sys
import threading
import time as _time
from urllib.parse import urlparse

# The upstream rejects an explicit 20 -- that is its default, requested by
# omitting the field.
DEFAULT_LEVELS = 20
# The public API drops a connection that has sent nothing for a minute.
PING_EVERY = 20.0


class Ws:
    """Just enough websocket to hold a subscription open and read text frames.

    Unlike the copies in the other tools, a read that times out consumes
    nothing: a frame is taken off the buffer only once it is complete. That is
    what lets the reader wake up every second to send a ping and carry on.
    """

    def __init__(self, url, timeout=30):
        u = urlparse(url)
        secure = u.scheme == "wss"
        port = u.port or (443 if secure else 80)
        self.sock = socket.create_connection((u.hostname, port), timeout=timeout)
        if secure:
            self.sock = ssl.create_default_context().wrap_socket(
                self.sock, server_hostname=u.hostname
            )
        key = base64.b64encode(os.urandom(16)).decode()
        req = [
            "GET {} HTTP/1.1".format(u.path or "/"),
            "Host: {}:{}".format(u.hostname, port),
            "Upgrade: websocket",
            "Connection: Upgrade",
            "Sec-WebSocket-Key: " + key,
            "Sec-WebSocket-Version: 13",
        ]
        self.sock.sendall(("\r\n".join(req) + "\r\n\r\n").encode())

        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise RuntimeError("connection closed during the handshake")
            buf += chunk
        head, _, rest = buf.partition(b"\r\n\r\n")
        status = head.decode(errors="replace").splitlines()[0]
        if "101" not in status:
            raise RuntimeError("handshake refused: " + status)
        self.buf = rest
        self.parts = []

    def _need(self, n):
        """Fill the buffer to at least n bytes without consuming any."""
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise RuntimeError("connection closed by the server")
            self.buf += chunk

    def send_text(self, text):
        payload = text.encode()
        mask = os.urandom(4)
        n = len(payload)
        if n < 126:
            hdr = struct.pack("!BB", 0x81, 0x80 | n)
        elif n < 65536:
            hdr = struct.pack("!BBH", 0x81, 0x80 | 126, n)
        else:
            hdr = struct.pack("!BBQ", 0x81, 0x80 | 127, n)
        self.sock.sendall(hdr + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))

    def _frame(self):
        self._need(2)
        b0, b1 = self.buf[0], self.buf[1]
        fin, opcode, masked = bool(b0 & 0x80), b0 & 0x0F, bool(b1 & 0x80)
        n, off = b1 & 0x7F, 2
        if n == 126:
            self._need(4)
            (n,) = struct.unpack("!H", self.buf[2:4])
            off = 4
        elif n == 127:
            self._need(10)
            (n,) = struct.unpack("!Q", self.buf[2:10])
            off = 10
        if masked:
            off += 4
        self._need(off + n)
        payload = self.buf[off:off + n]
        if masked:
            mask = self.buf[off - 4:off]
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.buf = self.buf[off + n:]
        return fin, opcode, payload

    def recv_text(self):
        """Next text message. Raises socket.timeout with nothing lost."""
        while True:
            fin, opcode, payload = self._frame()
            if opcode == 0x8:
                raise RuntimeError("server closed the connection")
            if opcode == 0x9:
                self.sock.sendall(struct.pack("!BB", 0x8A, 0x80 | len(payload))
                                  + bytes(4) + payload)
                continue
            if opcode == 0xA:
                continue
            if opcode not in (0x0, 0x1, 0x2):
                continue
            self.parts.append(payload)
            if fin:
                text = b"".join(self.parts).decode(errors="replace")
                self.parts = []
                return text


class Source(threading.Thread):
    """One subscription, remembering the newest block time it has delivered."""

    def __init__(self, name, url, sub, deadline, ref=None):
        super().__init__(daemon=True)
        self.name, self.url, self.sub, self.deadline = name, url, sub, deadline
        self.ref = ref              # the source to measure against, if any
        self.behind = []            # ms behind `ref`, one sample per frame
        self.window = None          # worst sample since the last `take_window`
        self.channel = sub["type"]
        self.lock = threading.Lock()
        self.newest = None          # newest block time seen, ms
        self.advanced_at = None     # wall time `newest` last moved
        self.frames = 0
        self.error = None

    def run(self):
        try:
            ws = Ws(self.url)
            ws.send_text(json.dumps({"method": "subscribe", "subscription": self.sub}))
            ws.sock.settimeout(1.0)
            last_ping = _time.time()
            while _time.time() < self.deadline:
                if _time.time() - last_ping > PING_EVERY:
                    ws.send_text(json.dumps({"method": "ping"}))
                    last_ping = _time.time()
                try:
                    msg = json.loads(ws.recv_text())
                except socket.timeout:
                    continue
                if msg.get("channel") == "error":
                    self.error = str(msg.get("data"))
                    return
                if msg.get("channel") != self.channel:
                    continue
                data = msg["data"]
                if self.channel == "l2Diff":
                    data = data.get("Snapshot") or data.get("Updates") or {}
                if isinstance(data, list):
                    # `trades` comes as a batch, each with its own time.
                    t = max((x.get("time") or 0 for x in data), default=0) or None
                else:
                    t = data.get("time")
                if t is None:
                    continue
                now = _time.time()
                r = self.ref.snapshot()[0] if self.ref is not None else None
                with self.lock:
                    self.frames += 1
                    if self.newest is None or t > self.newest:
                        self.newest = t
                        self.advanced_at = now
                    if r is not None:
                        b = max(0, r - t)
                        self.behind.append(b)
                        self.window = b if self.window is None else max(self.window, b)
        except Exception as e:
            self.error = "{}: {}".format(type(e).__name__, e)

    def snapshot(self):
        with self.lock:
            return self.newest, self.advanced_at, self.frames

    def take_window(self):
        with self.lock:
            w, self.window = self.window, None
            return w


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="wss://api.hyperliquid.xyz/ws")
    ap.add_argument("--api-channel", default="bbo", choices=["bbo", "trades", "l2Book"],
                    help="the reference channel; bbo is the most frequent")
    ap.add_argument("--a", default="ws://localhost:48001/ws")
    ap.add_argument("--b", default="ws://localhost:48002/ws")
    ap.add_argument("--wsarb", default="ws://localhost:48000/ws")
    ap.add_argument("--coin", default="BTC")
    ap.add_argument("--levels", type=int, default=DEFAULT_LEVELS,
                    help="nLevels on OUR side; the API is always asked for its default")
    ap.add_argument("--seconds", type=float, default=60.0)
    ap.add_argument("--stuck", type=float, default=5.0,
                    help="seconds without a new block, while the API moved on, that count as stuck")
    ap.add_argument("--behind", type=float, default=2000.0,
                    help="ms behind the API that count as a failure")
    args = ap.parse_args()

    def sub(kind, ours=True):
        s = {"type": kind, "coin": args.coin}
        if ours and args.levels != DEFAULT_LEVELS:
            s["nLevels"] = args.levels
        return s

    deadline = _time.time() + args.seconds
    api = Source("api", args.api, sub(args.api_channel, ours=False), deadline)
    ours = [
        Source("A", args.a, sub("l2Book"), deadline, api),
        Source("B", args.b, sub("l2Book"), deadline, api),
        Source("wsarb-l2Book", args.wsarb, sub("l2Book"), deadline, api),
        Source("wsarb-l2Diff", args.wsarb, sub("l2Diff"), deadline, api),
    ]
    print("freshness against {} {}, {} at {} levels on our side, {:.0f}s".format(
        args.api, args.api_channel, args.coin, args.levels, args.seconds))
    print("per source: frames/s, ms behind the API (a lower bound), and STUCK when it")
    print("has not moved for {:.0f}s while the API has\n".format(args.stuck))
    for s in [api] + ours:
        s.start()

    worst_stuck = {s.name: 0.0 for s in ours}
    prev_frames = {s.name: 0 for s in [api] + ours}
    width = max(len(s.name) for s in ours)

    while _time.time() < deadline:
        _time.sleep(1.0)
        now = _time.time()
        a_new, _, a_frames = api.snapshot()
        cells = []
        if a_new is None:
            head = "api: nothing yet"
        else:
            # Against the local clock: a check on the clock and the network.
            head = "api age {:>5.0f}ms".format(now * 1000 - a_new)
        prev_frames["api"] = a_frames

        for s in ours:
            new, adv, frames = s.snapshot()
            rate = frames - prev_frames[s.name]
            prev_frames[s.name] = frames
            if s.error:
                cells.append("{} ERROR".format(s.name.ljust(width)))
                continue
            if new is None:
                cells.append("{} {:>3}/s  no data".format(s.name.ljust(width), rate))
                continue
            cell = "{} {:>3}/s".format(s.name.ljust(width), rate)
            w = s.take_window()
            # Worst frame of this second; blank when no frame came to measure.
            cell += " {:>6}".format("{:.0f}ms".format(w) if w is not None else "-")
            if a_new is not None:
                still = now - adv
                if a_new > new and still > args.stuck:
                    worst_stuck[s.name] = max(worst_stuck[s.name], still)
                    cell += " STUCK {:.0f}s".format(still)
            cells.append(cell)

        print("{}  {} | {}".format(_time.strftime("%H:%M:%S"), head, " | ".join(cells)))

    for s in [api] + ours:
        s.join(timeout=2)

    print("")
    for s in [api] + ours:
        if s.error:
            print("{} failed: {}".format(s.name, s.error), file=sys.stderr)
    if api.snapshot()[0] is None:
        print("The API delivered nothing -- there is nothing to compare against.")
        print("Check that this machine can reach {}.".format(args.api))
        return 2

    print("--- against the API ---")
    failed = False
    for s in ours:
        _, _, frames = s.snapshot()
        with s.lock:
            xs = list(s.behind)
        reasons = []
        if s.error:
            reasons.append("error: " + s.error)
        elif frames == 0:
            reasons.append("delivered nothing")
        else:
            if worst_stuck[s.name] > 0:
                reasons.append("stood still {:.0f}s while the API moved".format(worst_stuck[s.name]))
            if xs and max(xs) > args.behind:
                reasons.append("{:.0f}ms behind".format(max(xs)))
        failed |= bool(reasons)
        stats = "frames {:>6}".format(frames)
        if xs:
            stats += "   behind p50 {:>5.0f}ms  p99 {:>5.0f}ms  max {:>5.0f}ms".format(
                pct(xs, 0.5), pct(xs, 0.99), max(xs))
        print("{}  {}   {}".format(s.name.ljust(width), stats,
                                   "FAIL: " + "; ".join(reasons) if reasons else "ok"))

    print("")
    print("RESULT: {}".format("FAILED" if failed else "PASSED"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
