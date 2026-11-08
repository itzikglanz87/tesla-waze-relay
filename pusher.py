#!/usr/bin/env python3
"""
pusher.py -- runs on the PHONE, inside Termux.

Pulls JPEG frames out of ScreenStream's local MJPEG endpoint and POSTs them to
the cloud relay, at a rate you choose.  Optionally forwards the Waze overlay
JSON (from wazecast / teslacast) to the same relay.

Pure standard library.  Nothing to install beyond Termux's python.

    python pusher.py --relay https://YOURS.onrender.com --token YOURTOKEN

Useful knobs
    --fps 2              frames per second to upload (default 2)
    --source URL         ScreenStream endpoint (default http://127.0.0.1:8080/stream.mjpeg)
    --data-url URL       where to read the Waze overlay JSON (default
                         http://127.0.0.1:8081/data); pass --no-data to skip
    --quiet              only log errors

It never blocks the reader on the uploader: the reader keeps the newest frame
in a one-slot buffer and the uploader takes whatever is there, so a slow
cellular link costs you frame rate, not lag.
"""

import argparse
import json
import sys
import threading
import time
import urllib.error
import urllib.request

START = time.time()
QUIET = False


def log(msg):
    sys.stderr.write("[%7.1fs] %s\n" % (time.time() - START, msg))
    sys.stderr.flush()


def vlog(msg):
    if not QUIET:
        log(msg)


class Slot:
    """Holds only the newest frame.  Older frames are dropped on the floor."""

    def __init__(self):
        self.lock = threading.Lock()
        self.frame = None
        self.fresh = False
        self.dropped = 0
        self.received = 0

    def put(self, blob):
        with self.lock:
            if self.fresh:
                self.dropped += 1
            self.frame = blob
            self.fresh = True
            self.received += 1

    def take(self):
        """Newest frame + whether it is one we have not sent yet."""
        with self.lock:
            was_fresh = self.fresh
            self.fresh = False
            return self.frame, was_fresh


def reader(source, slot, stop):
    """Read ScreenStream's MJPEG and split it on JPEG markers.

    We look for SOI (ff d8 ff) / EOI (ff d9) rather than parsing the multipart
    envelope -- it works no matter how ScreenStream frames its boundaries.
    """
    while not stop.is_set():
        try:
            req = urllib.request.Request(
                source, headers={"User-Agent": "teslawaze-pusher"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                log("source connected: %s" % source)
                buf = b""
                while not stop.is_set():
                    # read1, NOT read: read(n) blocks until n bytes have piled
                    # up, which at a low bitrate means seconds of added lag.
                    chunk = resp.read1(16384)
                    if not chunk:
                        break
                    buf += chunk
                    while True:
                        i = buf.find(b"\xff\xd8\xff")
                        if i < 0:
                            if len(buf) > 8 * 1024 * 1024:
                                buf = b""      # garbage, not a jpeg stream
                            break
                        j = buf.find(b"\xff\xd9", i + 3)
                        if j < 0:
                            if i:
                                buf = buf[i:]  # drop the header before the SOI
                            break
                        slot.put(buf[i:j + 2])
                        buf = buf[j + 2:]
            log("source stream ended")
        except Exception as exc:
            log("source error: %s" % exc)
        if not stop.is_set():
            stop.wait(2.0)


def post(url, body, token, ctype):
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": ctype, "X-Token": token,
                 "Content-Length": str(len(body)),
                 "User-Agent": "teslawaze-pusher"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        resp.read()


class Counter:
    def __init__(self):
        self.sent = 0
        self.bytes = 0
        self.errors = 0
        self.last_error = ""


def sender(relay, token, fps, slot, stop, count):
    url = relay.rstrip("/") + "/push"
    interval = 1.0 / fps
    keepalive = 8.0          # resend an unchanged frame this often, so the car
    last_sent = 0.0          # does not think the phone went away
    while not stop.is_set():
        t0 = time.time()
        frame, fresh = slot.take()
        due = (time.time() - last_sent) >= keepalive
        if frame is not None and (fresh or due):
            try:
                post(url, frame, token, "image/jpeg")
                count.sent += 1
                count.bytes += len(frame)
                last_sent = time.time()
            except urllib.error.HTTPError as exc:
                count.errors += 1
                count.last_error = "HTTP %s %s" % (exc.code, exc.reason)
                if exc.code == 403:
                    log("relay rejected the token -- check --token")
                    stop.set()
                    return
                stop.wait(1.0)
            except Exception as exc:
                count.errors += 1
                count.last_error = str(exc)
                stop.wait(1.0)
        rest = interval - (time.time() - t0)
        if rest > 0:
            stop.wait(rest)


def data_pump(relay, token, data_url, stop, count):
    url = relay.rstrip("/") + "/push-data"
    warned = [False]
    while not stop.is_set():
        try:
            req = urllib.request.Request(
                data_url, headers={"User-Agent": "teslawaze-pusher"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                raw = resp.read(64 * 1024)
            obj = json.loads(raw.decode("utf-8"))
            if isinstance(obj, dict):
                post(url, json.dumps(obj, ensure_ascii=False).encode("utf-8"),
                     token, "application/json; charset=utf-8")
                warned[0] = False
        except Exception as exc:
            if not warned[0]:
                log("overlay data unavailable (%s) -- map still works, "
                    "text layer will be empty" % exc)
                warned[0] = True
        stop.wait(1.0)


def reporter(slot, count, stop, fps):
    while not stop.is_set():
        stop.wait(30.0)
        if stop.is_set():
            return
        up = time.time() - START
        mb = count.bytes / 1048576.0
        line = ("sent %d frames, %.1f MB (%.2f MB/min, ~%.0f kbps), "
                "%.2f fps out, %d read, %d dropped"
                % (count.sent, mb, mb / (up / 60.0), count.bytes * 8 / 1000.0 / up,
                   count.sent / up, slot.received, slot.dropped))
        if count.errors:
            line += ", %d errors (last: %s)" % (count.errors, count.last_error)
        log(line)


def main():
    global QUIET
    ap = argparse.ArgumentParser()
    ap.add_argument("--relay", required=True,
                    help="https://YOURS.onrender.com")
    ap.add_argument("--token", required=True, help="must match RELAY_TOKEN")
    ap.add_argument("--fps", type=float, default=2.0)
    ap.add_argument("--source",
                    default="http://127.0.0.1:8080/stream.mjpeg",
                    help="ScreenStream's local MJPEG endpoint")
    ap.add_argument("--data-url", default="http://127.0.0.1:8081/data",
                    help="where the Waze overlay JSON is served")
    ap.add_argument("--no-data", action="store_true",
                    help="do not forward the overlay JSON at all")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    QUIET = args.quiet

    if args.fps <= 0 or args.fps > 15:
        sys.stderr.write("--fps must be between 0 and 15\n")
        return 2
    if not args.relay.startswith("http"):
        sys.stderr.write("--relay must be a full URL\n")
        return 2

    # fail fast and loudly if the relay is not up
    try:
        with urllib.request.urlopen(
                args.relay.rstrip("/") + "/health", timeout=30) as r:
            r.read()
        log("relay reachable: %s" % args.relay)
    except Exception as exc:
        log("relay NOT reachable (%s) -- starting anyway and will retry" % exc)

    slot = Slot()
    count = Counter()
    stop = threading.Event()

    threads = [
        threading.Thread(target=reader, args=(args.source, slot, stop), daemon=True),
        threading.Thread(target=sender,
                         args=(args.relay, args.token, args.fps, slot, stop, count),
                         daemon=True),
        threading.Thread(target=reporter, args=(slot, count, stop, args.fps),
                         daemon=True),
    ]
    if not args.no_data:
        threads.append(threading.Thread(
            target=data_pump,
            args=(args.relay, args.token, args.data_url, stop, count),
            daemon=True))

    for t in threads:
        t.start()
    log("pushing at %.1f fps -- Ctrl-C to stop" % args.fps)
    try:
        while not stop.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    stop.set()
    up = time.time() - START
    log("stopped. %d frames, %.1f MB in %.0f s"
        % (count.sent, count.bytes / 1048576.0, up))
    return 0


if __name__ == "__main__":
    sys.exit(main())
