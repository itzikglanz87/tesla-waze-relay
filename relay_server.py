#!/usr/bin/env python3
"""
tesla-waze relay -- the public endpoint that sits between the phone and the car.

The phone (Termux, pusher.py) pulls JPEG frames out of ScreenStream's LOCAL
MJPEG endpoint and POSTs them here.  The Tesla browser opens "/" and pulls them
back down over the car's own internet connection.  Nothing crosses the local
network, so no hotspot is needed.

Pure standard library -- no dependencies.

    RELAY_TOKEN=some-secret python relay_server.py

Endpoints
    POST /push        body = one JPEG, header X-Token         (from the phone)
    POST /push-data   body = overlay JSON, header X-Token     (from the phone)
    GET  /                viewer page                         (for the car)
    GET  /stream.mjpeg    multipart stream of the latest frame
    GET  /frame.jpg       the latest frame, once
    GET  /data            overlay JSON + staleness info
    GET  /status          counters, for debugging
    GET  /health          "ok", no token -- for Render's health check
"""

import hmac
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

TOKEN = os.environ.get("RELAY_TOKEN", "").strip()
PORT = int(os.environ.get("PORT", "10000"))

MAX_FRAME = 4 * 1024 * 1024      # refuse anything bigger; a map frame is ~40 KB
MAX_DATA = 64 * 1024
STREAM_RESEND = 4.0              # if no new frame, resend the last one (keeps the
                                 # connection and the <img> alive)


class State:
    def __init__(self):
        self.cond = threading.Condition()
        self.jpeg = None
        self.seq = 0
        self.frame_ts = 0.0
        self.overlay = {}
        self.data_ts = 0.0
        self.bytes_in = 0
        self.frames_in = 0
        self.bytes_out = 0
        self.viewers = 0
        self.started = time.time()

    def push_frame(self, blob):
        with self.cond:
            self.jpeg = blob
            self.seq += 1
            self.frame_ts = time.time()
            self.bytes_in += len(blob)
            self.frames_in += 1
            self.cond.notify_all()

    def push_data(self, obj):
        with self.cond:
            self.overlay = obj
            self.data_ts = time.time()

    def snapshot(self):
        with self.cond:
            return self.jpeg, self.seq, self.frame_ts

    def wait_for_new(self, last_seq, timeout):
        with self.cond:
            self.cond.wait_for(lambda: self.seq != last_seq, timeout=timeout)
            return self.jpeg, self.seq, self.frame_ts


S = State()


def age(ts):
    return None if not ts else round(time.time() - ts, 2)


VIEWER_HTML = r"""<!doctype html>
<html dir="ltr" lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no">
<title>Tesla Waze</title>
<style>
 html,body{margin:0;height:100%;background:#000;overflow:hidden;color:#fff;
   font-family:system-ui,-apple-system,"Segoe UI",Arial,sans-serif;
   -webkit-user-select:none;user-select:none;-webkit-tap-highlight-color:transparent}
 #stage{position:absolute;inset:0;overflow:hidden;background:#000}
 #v{position:absolute;left:50%;top:50%;height:100%;width:auto;max-width:none;
    transform-origin:center center}
 #dim{position:absolute;inset:0;background:#000;opacity:0;pointer-events:none;
   transition:opacity .25s}
 #ov{position:absolute;left:0;right:0;top:0;padding:12px 18px 18px;
   pointer-events:none;display:flex;align-items:flex-start;gap:16px;
   background:linear-gradient(rgba(0,0,0,.78),rgba(0,0,0,0))}
 #dist{font-size:62px;font-weight:700;line-height:.95;color:#39d353;
   white-space:nowrap;text-shadow:0 2px 8px #000}
 #dist.weak{color:#f0ad4e}
 #man{font-size:34px;font-weight:600;line-height:1.15;flex:1;
   text-shadow:0 2px 8px #000;overflow:hidden}
 #eta{font-size:26px;font-weight:600;color:#cfd8dc;white-space:nowrap;
   text-align:right;text-shadow:0 2px 8px #000}
 #bar{position:absolute;left:0;right:0;bottom:0;display:flex;gap:5px;
   padding:7px;justify-content:center;background:rgba(0,0,0,.62)}
 button{font-size:21px;min-width:50px;height:44px;border:0;border-radius:8px;
   background:#2a2f36;color:#fff;font-weight:600;padding:0 8px}
 button:active{background:#4a525d}
 button.on{background:#2e6fd6}
 #msg{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);
   font-size:26px;background:rgba(0,0,0,.72);padding:14px 22px;border-radius:12px;
   display:none;text-align:center;max-width:70%}
 .hide{display:none!important}
</style>
</head>
<body>
<div id="stage">
  <img id="v" alt="">
  <div id="dim"></div>
</div>
<div id="ov"><div id="dist"></div><div id="man"></div><div id="eta"></div></div>
<div id="msg"></div>
<div id="bar">
  <button id="bzo">&minus;</button>
  <button id="bzi">+</button>
  <button id="bup">&#9650;</button>
  <button id="bdn">&#9660;</button>
  <button id="blf">&#9664;</button>
  <button id="brt">&#9654;</button>
  <button id="bdim">&#9790;</button>
  <button id="bov">A</button>
  <button id="bmode">P</button>
  <button id="brst">&#8962;</button>
  <button id="brld">&#10227;</button>
</div>
<script>
(function(){
  var q = new URLSearchParams(location.search);
  var K = q.get('k') || '';
  var kq = K ? ('k=' + encodeURIComponent(K)) : '';
  var POLL_FPS = 3;

  var v = document.getElementById('v'), dim = document.getElementById('dim'),
      ov = document.getElementById('ov'), msg = document.getElementById('msg'),
      dist = document.getElementById('dist'), man = document.getElementById('man'),
      eta = document.getElementById('eta');

  var DEF = {zoom:1, x:0, y:0, dim:0, ov:1, poll:0};
  var st = DEF;
  try { st = Object.assign({}, DEF, JSON.parse(localStorage.getItem('teslawaze.view')||'{}')); }
  catch(e) {}

  function save(){ try{ localStorage.setItem('teslawaze.view', JSON.stringify(st)); }catch(e){} }

  function apply(){
    v.style.transform = 'translate(-50%,-50%) translate('+st.x+'px,'+st.y+'px) scale('+st.zoom+')';
    dim.style.opacity = st.dim ? 0.45 : 0;
    ov.classList.toggle('hide', !st.ov);
    document.getElementById('bdim').classList.toggle('on', !!st.dim);
    document.getElementById('bov').classList.toggle('on', !!st.ov);
    var bm = document.getElementById('bmode');
    bm.classList.toggle('on', !!st.poll);
    bm.textContent = st.poll ? 'P' : 'S';
    save();
  }

  // ---- pixels -------------------------------------------------------------
  var pollTimer = null;

  function stopVideo(){
    if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; }
    v.src = '';
  }

  function pollOnce(){
    var img = new Image();
    img.onload = function(){
      v.src = img.src;
      pollTimer = setTimeout(pollOnce, 1000 / POLL_FPS);
    };
    img.onerror = function(){ pollTimer = setTimeout(pollOnce, 1500); };
    img.src = '/frame.jpg?' + kq + '&n=' + Date.now();
  }

  function startVideo(){
    stopVideo();
    if (st.poll) { pollOnce(); }
    else { v.src = '/stream.mjpeg?' + kq + '&t=' + Date.now(); }
  }

  // ---- overlay ------------------------------------------------------------
  function pick(o, names){
    for (var i=0;i<names.length;i++){
      var x = o[names[i]];
      if (x !== undefined && x !== null && x !== '') return String(x);
    }
    return '';
  }

  function tick(){
    fetch('/data?' + kq, {cache:'no-store'})
      .then(function(r){ return r.json(); })
      .then(function(d){
        var o = d.overlay || {};
        dist.textContent = pick(o, ['distance','dist','distance_text','turn_distance']);
        man.textContent  = pick(o, ['maneuver','instruction','next_turn','turn','text']);
        eta.textContent  = pick(o, ['eta','arrival','eta_text','summary','remaining']);
        dist.classList.toggle('weak', pick(o,['distance_source']) === 'summary');
        var fa = d.frame_age;
        if (fa === null || fa > 6) {
          msg.style.display = 'block';
          msg.textContent = (fa === null) ? 'אין פריימים — הטלפון לא דוחף'
                                          : 'הפריים האחרון לפני ' + Math.round(fa) + ' שניות';
        } else {
          msg.style.display = 'none';
        }
        setTimeout(tick, 1000);
      })
      .catch(function(){
        msg.style.display = 'block';
        msg.textContent = 'אין קשר לממסר';
        setTimeout(tick, 2000);
      });
  }

  // ---- controls -----------------------------------------------------------
  function on(id, fn){ document.getElementById(id).addEventListener('click', fn); }
  var STEP = 40;
  on('bzi',  function(){ st.zoom = Math.min(6, +(st.zoom + 0.1).toFixed(2)); apply(); });
  on('bzo',  function(){ st.zoom = Math.max(0.3, +(st.zoom - 0.1).toFixed(2)); apply(); });
  on('bup',  function(){ st.y -= STEP; apply(); });
  on('bdn',  function(){ st.y += STEP; apply(); });
  on('blf',  function(){ st.x -= STEP; apply(); });
  on('brt',  function(){ st.x += STEP; apply(); });
  on('bdim', function(){ st.dim = st.dim ? 0 : 1; apply(); });
  on('bov',  function(){ st.ov  = st.ov  ? 0 : 1; apply(); });
  on('bmode',function(){ st.poll = st.poll ? 0 : 1; apply(); startVideo(); });
  on('brst', function(){ st = Object.assign({}, DEF); apply(); startVideo(); });
  on('brld', function(){ startVideo(); });

  apply();
  startVideo();
  tick();

  // if the multipart stream dies silently, fall back to polling once
  v.addEventListener('error', function(){
    if (!st.poll) { st.poll = 1; apply(); startVideo(); }
  });
})();
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "teslawaze-relay"

    # -- plumbing ----------------------------------------------------------
    def log_message(self, fmt, *args):
        pass  # the access log would be one line per frame

    def _query(self):
        return parse_qs(urlparse(self.path).query)

    def _authorized(self):
        """Returns (ok, set_cookie)."""
        k = (self._query().get("k") or [None])[0]
        if k and hmac.compare_digest(k, TOKEN):
            return True, True
        hdr = self.headers.get("X-Token")
        if hdr and hmac.compare_digest(hdr.strip(), TOKEN):
            return True, False
        for part in (self.headers.get("Cookie") or "").split(";"):
            part = part.strip()
            if part.startswith("k=") and hmac.compare_digest(part[2:], TOKEN):
                return True, False
        return False, False

    def _send(self, code, body=b"", ctype="text/plain; charset=utf-8",
              extra=None, set_cookie=False):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        if set_cookie:
            self.send_header("Set-Cookie",
                             "k=%s; Max-Age=31536000; Path=/; SameSite=Lax" % TOKEN)
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def _deny(self):
        self._send(403, "forbidden\n")

    # -- GET ---------------------------------------------------------------
    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/health":
            self._send(200, "ok\n")
            return

        ok, cookie = self._authorized()
        if not ok:
            self._deny()
            return

        if path == "/":
            self._send(200, VIEWER_HTML, "text/html; charset=utf-8",
                       set_cookie=cookie)
        elif path == "/stream.mjpeg":
            self._stream(cookie)
        elif path == "/frame.jpg":
            blob, seq, ts = S.snapshot()
            if blob is None:
                self._send(503, "no frame yet\n", set_cookie=cookie)
            else:
                S.bytes_out += len(blob)
                self._send(200, blob, "image/jpeg",
                           extra={"X-Seq": str(seq)}, set_cookie=cookie)
        elif path == "/data":
            with S.cond:
                payload = {
                    "overlay": S.overlay,
                    "frame_age": age(S.frame_ts),
                    "data_age": age(S.data_ts),
                    "seq": S.seq,
                }
            self._send(200, json.dumps(payload, ensure_ascii=False),
                       "application/json; charset=utf-8", set_cookie=cookie)
        elif path == "/status":
            up = time.time() - S.started
            with S.cond:
                payload = {
                    "uptime_s": round(up, 1),
                    "frames_in": S.frames_in,
                    "mb_in": round(S.bytes_in / 1048576.0, 2),
                    "mb_out": round(S.bytes_out / 1048576.0, 2),
                    "avg_fps_in": round(S.frames_in / up, 2) if up > 0 else 0,
                    "avg_kbps_in": round(S.bytes_in * 8 / 1000.0 / up, 1) if up > 0 else 0,
                    "frame_age": age(S.frame_ts),
                    "data_age": age(S.data_ts),
                    "viewers": S.viewers,
                    "last_frame_bytes": len(S.jpeg) if S.jpeg else 0,
                }
            self._send(200, json.dumps(payload, indent=2, ensure_ascii=False),
                       "application/json; charset=utf-8", set_cookie=cookie)
        else:
            self._send(404, "not found\n")

    def _stream(self, set_cookie):
        boundary = "teslaframe"
        self.send_response(200)
        self.send_header("Content-Type",
                         "multipart/x-mixed-replace; boundary=%s" % boundary)
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Connection", "close")
        if set_cookie:
            self.send_header("Set-Cookie",
                             "k=%s; Max-Age=31536000; Path=/; SameSite=Lax" % TOKEN)
        self.end_headers()
        self.close_connection = True

        with S.cond:
            S.viewers += 1
        last = -1
        try:
            while True:
                blob, seq, ts = S.wait_for_new(last, STREAM_RESEND)
                if blob is None:
                    continue              # nothing has ever been pushed
                last = seq
                head = ("--%s\r\nContent-Type: image/jpeg\r\n"
                        "Content-Length: %d\r\n\r\n" % (boundary, len(blob)))
                self.wfile.write(head.encode("ascii"))
                self.wfile.write(blob)
                self.wfile.write(b"\r\n")
                self.wfile.flush()
                S.bytes_out += len(blob)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            with S.cond:
                S.viewers -= 1

    # -- POST --------------------------------------------------------------
    def do_POST(self):
        path = urlparse(self.path).path
        ok, _ = self._authorized()
        if not ok:
            self._deny()
            return

        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            self._send(400, "bad length\n")
            return

        if path == "/push":
            if length <= 0 or length > MAX_FRAME:
                self._send(413, "bad frame size\n")
                return
            blob = self._read_exactly(length)
            if blob is None:
                return
            if not blob.startswith(b"\xff\xd8"):
                self._send(400, "not a jpeg\n")
                return
            S.push_frame(blob)
            self._send(200, "ok\n")

        elif path == "/push-data":
            if length > MAX_DATA:
                self._send(413, "too big\n")
                return
            raw = self._read_exactly(length) if length else b"{}"
            if raw is None:
                return
            try:
                obj = json.loads(raw.decode("utf-8"))
                if not isinstance(obj, dict):
                    raise ValueError("not an object")
            except Exception as exc:
                self._send(400, "bad json: %s\n" % exc)
                return
            S.push_data(obj)
            self._send(200, "ok\n")

        else:
            self._send(404, "not found\n")

    def _read_exactly(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self.rfile.read(min(65536, n - len(buf)))
            if not chunk:
                self._send(400, "short body\n")
                return None
            buf += chunk
        return bytes(buf)

    def do_HEAD(self):
        self.do_GET()


def main():
    if not TOKEN:
        sys.stderr.write(
            "RELAY_TOKEN is not set.  Refusing to start an open relay --\n"
            "anyone who found the URL would be watching your phone screen.\n"
            "Set RELAY_TOKEN in the service's environment and redeploy.\n")
        return 2
    if len(TOKEN) < 6:
        sys.stderr.write("RELAY_TOKEN is too short; use at least 6 characters.\n")
        return 2

    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    srv.daemon_threads = True
    sys.stderr.write("relay listening on 0.0.0.0:%d\n" % PORT)
    sys.stderr.flush()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
