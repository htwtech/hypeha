#!/usr/bin/env python3
"""What an l2Book increment would cost, against what the full snapshot costs.

Subscribes to `l2Book`, collects consecutive snapshots, and for each adjacent
pair builds the price-keyed diff that the planned `l2Diff` channel would send.
Reports the two byte counts side by side -- that ratio is the whole reason the
channel is worth building, and it is the number to quote the client.

Price-keyed rather than position-keyed on purpose: at 1000 levels a new level
at the top shifts all thousand and evicts the last, so a positional diff
degenerates into a full snapshot while a price-keyed one shows one addition and
one eviction.

Standard library only, so it runs on the node with nothing installed.

  python3 l2diff-measure.py --coin BTC --levels 1000 --frames 500
"""

import argparse
import base64
import json
import os
import socket
import ssl
import struct
import sys
from urllib.parse import urlparse

# The upstream rejects an explicit 20 -- that is its default, requested by
# omitting the field (server/src/types/subscription.rs).
DEFAULT_LEVELS = 20


class Ws:
    """Just enough websocket to read text frames from a server."""

    def __init__(self, url, token=None, timeout=30):
        u = urlparse(url)
        secure = u.scheme == "wss"
        port = u.port or (443 if secure else 80)
        path = u.path or "/"

        self.sock = socket.create_connection((u.hostname, port), timeout=timeout)
        if secure:
            self.sock = ssl.create_default_context().wrap_socket(
                self.sock, server_hostname=u.hostname
            )

        key = base64.b64encode(os.urandom(16)).decode()
        req = [
            "GET {} HTTP/1.1".format(path),
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
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(hdr + mask + masked)

    def _frame(self):
        """One raw frame: (fin, opcode, payload). Server frames are unmasked."""
        b0, b1 = self._read(2)
        fin = bool(b0 & 0x80)
        opcode = b0 & 0x0F
        masked = bool(b1 & 0x80)
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
        """Next complete text message, reassembling fragments."""
        parts = []
        expect_continuation = False
        while True:
            fin, opcode, payload = self._frame()
            if opcode == 0x8:
                raise RuntimeError("server closed the connection")
            if opcode == 0x9:  # ping -- answer it, nothing here is worth a timeout
                self.sock.sendall(struct.pack("!BB", 0x8A, 0x80 | len(payload))
                                  + b"\x00\x00\x00\x00" + payload)
                continue
            if opcode == 0xA:  # pong
                continue
            if opcode not in (0x0, 0x1, 0x2):
                continue
            if opcode != 0x0 and expect_continuation:
                parts = []
            parts.append(payload)
            if fin:
                return b"".join(parts).decode(errors="replace")
            expect_continuation = True


def by_px(side):
    return {lv["px"]: (lv["sz"], lv["n"]) for lv in side}


def diff_side(old, new):
    """The price-keyed diff of one side: levels to set, prices to drop."""
    o, n = by_px(old), by_px(new)
    upd = [[px, sz, cnt] for px, (sz, cnt) in n.items() if o.get(px) != (sz, cnt)]
    gone = [px for px in o if px not in n]
    return upd, gone


def pct(values, p):
    if not values:
        return 0
    s = sorted(values)
    i = min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1))))
    return s[i]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="ws://localhost:48001/ws")
    ap.add_argument("--coin", default="BTC")
    ap.add_argument("--levels", type=int, default=1000)
    ap.add_argument("--sig-figs", type=int, default=None)
    ap.add_argument("--frames", type=int, default=500,
                    help="snapshots to collect; pairs compared is one less")
    ap.add_argument("--token", default=None, help="x-token, if going through the gateway")
    args = ap.parse_args()

    sub = {"type": "l2Book", "coin": args.coin}
    if args.levels != DEFAULT_LEVELS:
        sub["nLevels"] = args.levels
    if args.sig_figs is not None:
        sub["nSigFigs"] = args.sig_figs

    ws = Ws(args.url, args.token)
    ws.send_text(json.dumps({"method": "subscribe", "subscription": sub}))
    print("subscribed {} on {}, collecting {} snapshots"
          .format(json.dumps(sub), args.url, args.frames))

    snap_bytes, diff_bytes, changed, prev, first_t, last_t = [], [], [], None, None, None

    while len(snap_bytes) < args.frames:
        text = ws.recv_text()
        msg = json.loads(text)
        if msg.get("channel") != "l2Book":
            if msg.get("channel") == "error":
                print("upstream error: {}".format(msg.get("data")), file=sys.stderr)
                return 1
            continue

        data = msg["data"]
        bids, asks = data["levels"][0], data["levels"][1]
        t = data["time"]
        if first_t is None:
            first_t = t
        last_t = t
        snap_bytes.append(len(text.encode()))

        if prev is not None:
            b_upd, b_del = diff_side(prev[0], bids)
            a_upd, a_del = diff_side(prev[1], asks)
            frame = {
                "channel": "l2Diff",
                "data": {
                    "coin": data["coin"],
                    "time": t,
                    "prevTime": prev[2],
                    "bids": {"upd": b_upd, "del": b_del},
                    "asks": {"upd": a_upd, "del": a_del},
                },
            }
            diff_bytes.append(len(json.dumps(frame, separators=(",", ":")).encode()))
            changed.append(len(b_upd) + len(b_del) + len(a_upd) + len(a_del))

        prev = (bids, asks, t)
        if len(snap_bytes) % 50 == 0:
            print("  {} snapshots".format(len(snap_bytes)))

    pairs = len(diff_bytes)
    if pairs == 0:
        print("not enough snapshots to compare", file=sys.stderr)
        return 1

    depth = len(prev[0]) + len(prev[1])
    snap_mean = sum(snap_bytes[1:]) / float(pairs)
    diff_mean = sum(diff_bytes) / float(pairs)
    span = (last_t - first_t) / 1000.0 if last_t and first_t else 0.0

    print("")
    print("--- {} pairs compared, {} levels per snapshot ---".format(pairs, depth))
    print("changed levels: mean {:.1f}, median {}, p95 {}, max {}  (of {})"
          .format(sum(changed) / float(pairs), pct(changed, 50), pct(changed, 95),
                  max(changed), depth))
    print("")
    print("snapshot: mean {:8.0f} B    total {:9.1f} MB"
          .format(snap_mean, sum(snap_bytes[1:]) / 1e6))
    print("diff:     mean {:8.0f} B    total {:9.1f} MB"
          .format(diff_mean, sum(diff_bytes) / 1e6))
    print("saving:   {:.1f}x smaller".format(snap_mean / diff_mean if diff_mean else 0))

    if span > 0:
        rate = pairs / span
        print("")
        print("at the observed {:.1f} frames/s, per coin:".format(rate))
        print("  snapshots  {:7.1f} KB/s  = {:6.2f} Mbit/s"
              .format(snap_mean * rate / 1e3, snap_mean * rate * 8 / 1e6))
        print("  increments {:7.1f} KB/s  = {:6.2f} Mbit/s"
              .format(diff_mean * rate / 1e3, diff_mean * rate * 8 / 1e6))
    print("")
    print("Uncompressed, both sides -- no permessage-deflate is negotiated on")
    print("this path, so these are the figures that actually go on the wire.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
