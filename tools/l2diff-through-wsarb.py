#!/usr/bin/env python3
"""Does the book wsarb builds from l2Diff match one of the nodes -- and always
the same one?

Three streams at once: `l2Diff` through wsarb, and `l2Book` straight from each
node. The book is rebuilt from wsarb's updates and, at every `time` all three
reported, checked against both nodes.

Two questions in one run:

  correctness -- wsarb's book must equal SOME node's book. Comparing against a
  single node cannot answer this: the nodes measurably hold different books, so
  a mismatch there would be indistinguishable from a bug in the channel.

  stickiness -- it must be the SAME node throughout. wsarb follows one source
  from end to end precisely because an increment only means anything against
  the book it was computed from; if the match hops between nodes, that rule is
  not being kept and a client's book is being spliced from two of them.

Standard library only.

  python3 l2diff-through-wsarb.py --wsarb ws://localhost:48000/ws \
      --a ws://localhost:48001/ws --b ws://localhost:48002/ws \
      --coin BTC --levels 1000 --seconds 60
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

DEFAULT_LEVELS = 20


class Ws:
    """Just enough websocket to hold a subscription open and read text frames."""

    def __init__(self, url, token=None, timeout=30):
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
        if token:
            req.append("x-token: " + token)
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

    def _read(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise RuntimeError("connection closed by the server")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

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
        b0, b1 = self._read(2)
        fin, opcode, masked = bool(b0 & 0x80), b0 & 0x0F, bool(b1 & 0x80)
        n = b1 & 0x7F
        if n == 126:
            (n,) = struct.unpack("!H", self._read(2))
        elif n == 127:
            (n,) = struct.unpack("!Q", self._read(8))
        mask = self._read(4) if masked else None
        payload = self._read(n)
        if mask:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        return fin, opcode, payload

    def recv_text(self):
        parts = []
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
            parts.append(payload)
            if fin:
                return b"".join(parts).decode(errors="replace")


def book_from_levels(levels):
    return [{lv["px"]: (lv["sz"], lv["n"]) for lv in side} for side in levels]


def apply_side(side, diff):
    for px, sz, n in diff.get("upd", []):
        side[px] = (sz, n)
    for px in diff.get("del", []):
        side.pop(px, None)


def signature(book):
    return (tuple(sorted(book[0].items())), tuple(sorted(book[1].items())))


def compare(a, b):
    """Levels differing between two books, for reporting a near miss."""
    return sum(1 for side in (0, 1)
               for px in set(a[side]) | set(b[side])
               if a[side].get(px) != b[side].get(px))


class Stream(threading.Thread):
    """One subscription, recording a book signature per `time`."""

    def __init__(self, name, url, sub, token, deadline):
        super().__init__(daemon=True)
        self.name, self.url, self.sub, self.token, self.deadline = name, url, sub, token, deadline
        self.channel = sub["type"]
        self.books = {}         # time -> whole book
        self.sigs = {}          # time -> signature
        self.frames = self.snapshots = 0
        self.last_t = None      # newest time recorded, for the live readout
        self.error = None

    def run(self):
        try:
            ws = Ws(self.url, self.token)
            ws.send_text(json.dumps({"method": "subscribe", "subscription": self.sub}))
            book, have = [dict(), dict()], False
            while _time.time() < self.deadline:
                ws.sock.settimeout(max(1.0, self.deadline - _time.time()))
                try:
                    msg = json.loads(ws.recv_text())
                except socket.timeout:
                    return
                if msg.get("channel") == "error":
                    self.error = str(msg.get("data"))
                    return
                if msg.get("channel") != self.channel:
                    continue
                self.frames += 1
                data = msg["data"]

                if self.channel == "l2Book":
                    self._record(data["time"], book_from_levels(data["levels"]))
                    continue

                if "Snapshot" in data:
                    d = data["Snapshot"]
                    book, have = book_from_levels(d["levels"]), True
                    self.snapshots += 1
                    self._record(d["time"], book)
                else:
                    d = data["Updates"]
                    if not have:
                        continue
                    apply_side(book[0], d["bids"])
                    apply_side(book[1], d["asks"])
                    self._record(d["time"], book)
        except Exception as e:
            self.error = "{}: {}".format(type(e).__name__, e)

    def _record(self, t, book):
        snap = [dict(book[0]), dict(book[1])]
        self.books[t] = snap
        self.sigs[t] = signature(snap)
        self.last_t = t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wsarb", default="ws://localhost:48000/ws")
    ap.add_argument("--a", default="ws://localhost:48001/ws")
    ap.add_argument("--b", default="ws://localhost:48002/ws")
    ap.add_argument("--coin", default="BTC")
    ap.add_argument("--levels", type=int, default=1000)
    ap.add_argument("--sig-figs", type=int, default=None)
    ap.add_argument("--seconds", type=float, default=60.0)
    ap.add_argument("--token", default=None)
    args = ap.parse_args()

    def sub(kind):
        s = {"type": kind, "coin": args.coin}
        if args.levels != DEFAULT_LEVELS:
            s["nLevels"] = args.levels
        if args.sig_figs is not None:
            s["nSigFigs"] = args.sig_figs
        return s

    deadline = _time.time() + args.seconds
    w = Stream("wsarb", args.wsarb, sub("l2Diff"), args.token, deadline)
    a = Stream("A", args.a, sub("l2Book"), None, deadline)
    b = Stream("B", args.b, sub("l2Book"), None, deadline)
    print("l2Diff through wsarb vs l2Book from both nodes, {} at {} levels, {:.0f}s"
          .format(args.coin, args.levels, args.seconds))
    for s in (w, a, b):
        s.start()

    # Which node wsarb is following, live. Without this the failover test is
    # guesswork: the leader is whoever answered first, so it differs from run to
    # run, and there is no telling which node to stop.
    while _time.time() < deadline:
        _time.sleep(min(5.0, max(0.1, deadline - _time.time())))
        t = w.last_t
        if t is None:
            print("  [{:>3.0f}s] wsarb: nothing yet".format(args.seconds - (deadline - _time.time())))
            continue
        # .get on a dict another thread is writing is safe here; iterating it
        # would not be.
        who = [name for name, s in (("A", a), ("B", b)) if s.sigs.get(t) == w.sigs.get(t)]
        print("  [{:>3.0f}s] wsarb {} frames, following {}".format(
            args.seconds - (deadline - _time.time()),
            w.frames,
            "+".join(who) if who else "NEITHER (or the nodes are behind)"))

    for s in (w, a, b):
        s.join()

    for s in (w, a, b):
        if s.error:
            print("{} failed: {}".format(s.name, s.error), file=sys.stderr)
    if w.error:
        return 1

    print("")
    print("wsarb: {} frame(s), {} snapshot(s), {} distinct time"
          .format(w.frames, w.snapshots, len(w.sigs)))
    print("A:     {} frame(s), {} distinct time".format(a.frames, len(a.sigs)))
    print("B:     {} frame(s), {} distinct time".format(b.frames, len(b.sigs)))
    if not w.sigs:
        print("")
        print("wsarb delivered nothing. Either the subscription never routed, or")
        print("its key does not match the key the frames arrive under.")
        return 1

    common = sorted(set(w.sigs) & (set(a.sigs) | set(b.sigs)))
    only_a = only_b = both = neither = 0
    near = []
    order = []
    for t in common:
        ma = t in a.sigs and a.sigs[t] == w.sigs[t]
        mb = t in b.sigs and b.sigs[t] == w.sigs[t]
        if ma and mb:
            both += 1
        elif ma:
            only_a += 1
            order.append("A")
        elif mb:
            only_b += 1
            order.append("B")
        else:
            neither += 1
            if len(near) < 3:
                d = []
                if t in a.sigs:
                    d.append("A by {}".format(compare(w.books[t], a.books[t])))
                if t in b.sigs:
                    d.append("B by {}".format(compare(w.books[t], b.books[t])))
                near.append("  time {}: off {}".format(t, ", ".join(d)))

    print("")
    print("--- whose book is wsarb serving? ---")
    print("compared {} instants".format(len(common)))
    print("  matched A only:  {}".format(only_a))
    print("  matched B only:  {}".format(only_b))
    print("  matched both:    {}   (the nodes happened to agree)".format(both))
    print("  matched neither: {}".format(neither))
    for line in near:
        print(line)

    print("")
    print("--- did it stay with one of them? ---")
    if not order:
        print("never distinguishable: the nodes agreed whenever we could look")
    else:
        switches = sum(1 for i in range(1, len(order)) if order[i] != order[i - 1])
        print("distinguishable at {} instants, followed {}, switched {} time(s)"
              .format(len(order), "/".join(sorted(set(order))), switches))

    ok = neither == 0 and len(common) > 0
    print("")
    print("RESULT: {}".format("PASSED" if ok else "FAILED"))
    if neither:
        print("A book matching neither node is the channel's own fault: it was")
        print("rebuilt from increments that do not reconstruct any real book.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
