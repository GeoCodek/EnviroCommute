#!/usr/bin/env python3
"""
Enviro+ logger with GPS (bit-bang) + KY-040 rotary encoder control

Control A -- KY-040 rotary encoder (primary):
- rotate knob            -> move through the page carousel (a "menu")
- 1 click                -> activate the current page if it is an action
                            (e.g. the "Send CSVs" page); no-op on plain pages
- 2 clicks (< ~1 s)      -> toggle recording (start/stop)
- 4 clicks               -> shutdown the Pi (3 s cancellable countdown:
                            rotate or click during it to abort)

Control B -- proximity "cover" gesture over the light sensor (kept as backup):
- quick tap   (< COVER_MIN_S)            -> next page
- hold        (COVER_MIN_S..COVER_MAX_S) -> toggle recording
- hold too long (> COVER_MAX_S)          -> ignored
- Manual/remote: `touch ~/.enviro_toggle` toggles recording once.

Display (ST7735, 160x80):
- a carousel of pages you rotate through: value+graph pages (temp / humidity /
  pressure / light), a gas page, a PMS particulate page, a GPS page, and a
  "Send CSVs" action page. Each sensor page shows a colourful scrolling graph.
- recording status dot (top-right): green = recording, yellow = idle
- transient toast banner for actions; full-screen overlays for the cover
  gesture and the shutdown countdown.

CSV:
- Filename includes YYYY-MM-DD + unix timestamp
- Enviro+ fields + optional PMS5003 particulate fields + GPS fields
"""

import colorsys
import csv
import json
import os
import socket
import subprocess
import sys
import time
from collections import deque
from datetime import datetime

from enviroplus import gas, noise
from smbus2 import SMBus
# --- BME280 import (robust across packaging variants) ---
try:
    # classic Pimoroni layout expected by many Enviro+ examples
    from bme280 import BME280  # type: ignore
except Exception:
    try:
        # some installs expose a different module name
        from pimoroni_bme280 import BME280  # type: ignore
    except Exception:
        try:
            # last resort: package folder might be named "pimoroni_bme280"
            import importlib
            _m = importlib.import_module("pimoroni_bme280")
            BME280 = getattr(_m, "BME280")
        except Exception as e:
            raise ImportError(
                "Cannot import BME280. Your installed pimoroni-bme280 does not expose "
                "a compatible module name. Run the diagnostics (ls/pkgutil/grep) to see the real module."
            ) from e

# Optional PMS5003 support
try:
    from pms5003 import PMS5003  # type: ignore
except Exception:
    PMS5003 = None  # type: ignore

# LTR559 (proximity + light)
try:
    from ltr559 import LTR559
    ltr559 = LTR559()
except Exception:
    import ltr559  # type: ignore

# Optional display
try:
    import st7735  # type: ignore
    from fonts.ttf import RobotoMedium as UserFont  # type: ignore
    from PIL import Image, ImageDraw, ImageFont  # type: ignore
except Exception:
    st7735 = None

# --------- Tuning parameters ---------
SAMPLE_INTERVAL_S = 5.0

CTRL_POLL_S = 0.2          # how often the proximity gesture is evaluated

# -- proximity "cover" gesture (hand over the light sensor) -- kept as backup --
COVER_MIN_S       = 2.0
COVER_MAX_S       = 5.0
PROX_COVER        = 250    # proximity >= this => covered (hand over sensor; idle ~0)
PROX_UNCOVER      = 80     # proximity <= this => released
LUX_MIN_BASELINE  = 20.0
LUX_DARK_RATIO    = 0.25
LUX_LIGHT_RATIO   = 0.50

WARMUP_S            = 4.0
TOGGLE_COOLDOWN_S   = 2.0

TEMP_COMP_FACTOR = 2.25

GPSD_HOST = "127.0.0.1"
GPSD_PORT = 2947
GPSD_TIMEOUT_S = 0.25

# -- display / graphs --
DISP_REFRESH_S    = 2.0    # how often the live sensor values + graph history refresh
PMS_DISP_INTERVAL = 6.0    # how often to read the PMS just for the display (when idle)
HIST_MAXLEN       = 160    # graph history length (~= display width in px)

# -- send latest CSVs over SSH (wired up later; script may not exist yet) --
SEND_SCRIPT   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "send_csvs.sh")
SEND_TIMEOUT_S = 30.0
# -------------------------------------


def _is_num(x) -> bool:
    return x is not None and x == x  # rejects None and NaN


class ToggleController:
    """One proximity 'cover' gesture, action chosen by how long it's held:
        released < COVER_MIN_S    -> 'tap'     (next page)
        released in [MIN .. MAX]  -> 'toggle'  (start/stop recording)
        held    > COVER_MAX_S     -> None      (disqualified)
    """

    def __init__(self, start_t: float):
        self.start_t = start_t
        self.cooldown_until = 0.0
        self.lux_baseline = None
        self.covered = False
        self.cover_since = 0.0
        self.cover_active = False     # hand currently in front
        self.hold_progress = 0.0      # 0..1 toward COVER_MIN_S
        self.phase = ""               # "fill" | "ready" | "toolong"

    def update(self, now: float, lux, prox):
        warming = (now - self.start_t) < WARMUP_S
        prox_v = prox if _is_num(prox) else 0.0

        if _is_num(lux):
            if self.lux_baseline is None:
                self.lux_baseline = lux
            elif not self.covered:
                self.lux_baseline = 0.9 * self.lux_baseline + 0.1 * lux

        base = self.lux_baseline
        bright = base is not None and base >= LUX_MIN_BASELINE
        lux_dark  = bright and _is_num(lux) and lux < base * LUX_DARK_RATIO
        lux_light = (not bright) or (not _is_num(lux)) or (lux > base * LUX_LIGHT_RATIO)
        raw_covered  = (prox_v >= PROX_COVER) or lux_dark
        raw_released = (prox_v <= PROX_UNCOVER) and lux_light

        action = None
        if self.covered:
            held = now - self.cover_since
            if raw_released:
                self.covered = False
                self.cover_active = False
                self.hold_progress = 0.0
                self.phase = ""
                if warming:
                    pass
                elif held < COVER_MIN_S:
                    action = "tap"
                elif held <= COVER_MAX_S and now >= self.cooldown_until:
                    action = "toggle"
                    self.cooldown_until = now + TOGGLE_COOLDOWN_S
            else:
                self.cover_active = True
                if held < COVER_MIN_S:
                    self.hold_progress = held / COVER_MIN_S
                    self.phase = "fill"
                elif held <= COVER_MAX_S:
                    self.hold_progress = 1.0
                    self.phase = "ready"
                else:
                    self.hold_progress = 1.0
                    self.phase = "toolong"
        else:
            self.cover_active = False
            self.hold_progress = 0.0
            self.phase = ""
            if raw_covered:
                self.covered = True
                self.cover_since = now

        return action


def now_iso_seconds() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def today_ymd() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def safe_float(x):
    try:
        if x is None or x == "":
            return ""
        return float(x)
    except Exception:
        return ""


def safe_int(x):
    try:
        if x is None or x == "":
            return ""
        return int(x)
    except Exception:
        return ""


def get_cpu_temperature():
    try:
        with open("/sys/class/thermal/thermal_zone0/temp", "r") as f:
            return int(f.read()) / 1000.0
    except Exception:
        return None


def ensure_data_dir(script_dir: str) -> str:
    data_dir = os.path.join(script_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir


def new_csv_path(data_dir: str) -> str:
    ts = int(time.time())
    return os.path.join(data_dir, f"enviro_log_{today_ymd()}_{ts}.csv")


# ---------- Display: page carousel + colourful graphs ----------
# Each page is rotated to with the encoder. "value" pages show a big number plus
# a scrolling graph; "gas"/"pms"/"gps" are multi-line; "action" pages do
# something on a single click. `hkey` names the graph-history series.
PAGES = [
    {"kind": "value", "key": "temp", "title": "Temperature", "unit": "C",   "fmt": "%.1f", "hkey": "temp"},
    {"kind": "value", "key": "hum",  "title": "Humidity",    "unit": "%",   "fmt": "%.0f", "hkey": "hum"},
    {"kind": "value", "key": "pres", "title": "Pressure",    "unit": "hPa", "fmt": "%.0f", "hkey": "pres"},
    {"kind": "value", "key": "lux",  "title": "Light",       "unit": "lx",  "fmt": "%.0f", "hkey": "lux"},
    {"kind": "gas",   "title": "Gas (kOhm)", "hkey": "gas_ox"},
    {"kind": "pms",   "title": "PM ug/m3",   "hkey": "pm2_5"},
    {"kind": "gps",   "title": "GPS",        "hkey": "gps_nsat"},
    {"kind": "imu",   "title": "IMU",        "hkey": "imu_acc"},
    {"kind": "action", "action": "calib", "title": "Calibrate IMU"},
    {"kind": "action", "action": "send", "title": "Send CSVs"},
]

HIST_KEYS = ("temp", "hum", "pres", "lux", "gas_ox", "pm2_5", "gps_nsat", "imu_acc")


def init_display():
    if st7735 is None:
        return None
    display = st7735.ST7735(
        port=0,
        cs=1,
        dc="GPIO9",
        backlight="GPIO12",
        rotation=270,
        spi_speed_hz=10000000,
    )
    display.begin()
    img = Image.new("RGB", (display.width, display.height), color=(0, 0, 0))
    draw = ImageDraw.Draw(img)
    return {
        "display": display, "img": img, "draw": draw,
        "font": ImageFont.truetype(UserFont, 16),
        "fontbig": ImageFont.truetype(UserFont, 28),
        "fontmed": ImageFont.truetype(UserFont, 20),
        "fontsmall": ImageFont.truetype(UserFont, 12),
        "w": display.width, "h": display.height,
    }


def read_display_sensors(bme):
    """Lightweight read of the values shown on the live pages (no noise/PM)."""
    sv = {}
    for key, fn in (("temp", bme.get_temperature), ("hum", bme.get_humidity),
                    ("pres", bme.get_pressure)):
        try:
            sv[key] = fn()
        except Exception:
            sv[key] = None
    try:
        sv["lux"] = ltr559.get_lux()
    except Exception:
        sv["lux"] = None
    try:
        g = gas.read_all()
        sv["ox"] = getattr(g, "oxidising", None)
        sv["red"] = getattr(g, "reducing", None)
        sv["nh3"] = getattr(g, "nh3", None)
    except Exception:
        sv["ox"] = sv["red"] = sv["nh3"] = None
    return sv


def _fmt(v, fmt):
    try:
        return fmt % v
    except Exception:
        return "--"


def _grad(n):
    """Blue (low) -> green -> red (high) colour for a 0..1 value."""
    if n < 0.0:
        n = 0.0
    elif n > 1.0:
        n = 1.0
    r, g, b = colorsys.hsv_to_rgb(0.66 * (1.0 - n), 1.0, 1.0)
    return (int(r * 255), int(g * 255), int(b * 255))


def render_graph(ctx, region, values, vmin=None, vmax=None):
    """Draw a colourful scrolling bar graph of `values` in the box `region`."""
    draw = ctx["draw"]
    x0, y0, x1, y1 = region
    gw = x1 - x0
    gh = y1 - y0
    draw.rectangle((x0, y0, x1, y1), outline=(40, 40, 40))
    data = [v for v in values if v is not None]
    if len(data) < 2 or gw < 2 or gh < 2:
        return
    data = data[-gw:]
    lo = min(data) if vmin is None else vmin
    hi = max(data) if vmax is None else vmax
    if hi <= lo:
        hi = lo + 1.0
    span = hi - lo
    for i, v in enumerate(data):
        n = (v - lo) / span
        colh = int(max(0.0, min(1.0, n)) * (gh - 2))
        x = x0 + 1 + i
        if x >= x1:
            break
        if colh > 0:
            draw.line((x, y1 - 1, x, y1 - 1 - colh), fill=_grad(n))


def render_ui(ctx, st):
    if not ctx:
        return
    draw = ctx["draw"]; img = ctx["img"]; display = ctx["display"]
    w = ctx["w"]; h = ctx["h"]
    font = ctx["font"]; fontbig = ctx["fontbig"]; fontmed = ctx["fontmed"]; fontsmall = ctx["fontsmall"]
    draw.rectangle((0, 0, w, h), (0, 0, 0))

    # ---- full-screen overlay: shutdown countdown ----
    sr = st.get("shutdown_remaining")
    if sr is not None:
        draw.text((6, 2), "SHUTTING DOWN", font=font, fill=(255, 70, 70))
        draw.text((6, 24), f"{max(0.0, sr):0.1f}s", font=fontbig, fill=(255, 70, 70))
        draw.text((6, h - 14), "rotate/click = cancel", font=fontsmall, fill=(150, 150, 150))
        frac = max(0.0, min(1.0, sr / 3.0))
        bar_w = int((w - 12) * (1.0 - frac))
        draw.rectangle((6, h - 6, w - 6, h - 3), fill=(50, 50, 50))
        if bar_w > 0:
            draw.rectangle((6, h - 6, 6 + bar_w, h - 3), fill=(255, 70, 70))
        display.display(img)
        return

    # ---- full-screen overlay: cover gesture in progress ----
    if st.get("cover_active"):
        phase = st["phase"]; hp = st["hold_progress"]
        if phase == "toolong":
            big, col, sub = "TOO LONG", (255, 70, 70), "let go = no change"
        elif phase == "ready":
            big, col, sub = "RELEASE", (0, 230, 0), "let go to switch"
        else:
            big, col, sub = f"{int(hp * 100)}%", (255, 220, 0), "keep holding"
        draw.text((6, 2), "TOGGLE", font=font, fill=(180, 180, 180))
        draw.text((6, 24), big, font=fontbig, fill=col)
        draw.text((6, h - 14), sub, font=fontsmall, fill=(150, 150, 150))
        bar_w = int((w - 12) * min(1.0, hp))
        draw.rectangle((6, h - 6, w - 6, h - 3), fill=(50, 50, 50))
        if bar_w > 0:
            draw.rectangle((6, h - 6, 6 + bar_w, h - 3), fill=col)
        display.display(img)
        return

    # ---- normal page ----
    page = st["page"]; p = PAGES[page % len(PAGES)]
    kind = p["kind"]; sv = st["sv"]; hist = st["hist"]
    tcol = (120, 160, 255)

    if kind == "value":
        draw.text((4, 1), p["title"], font=fontsmall, fill=tcol)
        draw.text((4, 14), _fmt(sv.get(p["key"]), p["fmt"]) + " " + p["unit"],
                  font=fontmed, fill=(255, 255, 255))
        render_graph(ctx, (4, 40, w - 4, h - 8), hist.get(p["hkey"], ()), p.get("vmin"), p.get("vmax"))
    elif kind == "gas":
        draw.text((4, 1), p["title"], font=fontsmall, fill=tcol)
        draw.text((4, 14), "ox  " + _fmt((sv.get("ox") or 0) / 1000.0, "%.1f"), font=fontsmall, fill=(255, 255, 255))
        draw.text((4, 27), "red " + _fmt((sv.get("red") or 0) / 1000.0, "%.1f"), font=fontsmall, fill=(255, 255, 255))
        draw.text((4, 40), "nh3 " + _fmt((sv.get("nh3") or 0) / 1000.0, "%.1f"), font=fontsmall, fill=(255, 255, 255))
        render_graph(ctx, (4, 53, w - 4, h - 8), hist.get("gas_ox", ()))
    elif kind == "pms":
        pm = st["pm"]
        draw.text((4, 1), p["title"], font=fontsmall, fill=tcol)
        draw.text((4, 14), "1.0  " + _fmt(pm.get("pm1_0"), "%.0f"), font=fontsmall, fill=(255, 255, 255))
        draw.text((4, 27), "2.5  " + _fmt(pm.get("pm2_5"), "%.0f"), font=fontsmall, fill=(255, 255, 255))
        draw.text((4, 40), "10   " + _fmt(pm.get("pm10"), "%.0f"), font=fontsmall, fill=(255, 255, 255))
        render_graph(ctx, (4, 53, w - 4, h - 8), hist.get("pm2_5", ()), 0, None)
    elif kind == "gps":
        gps = st["gps"]; gs = st["gps_state"]
        mode = gps.get("mode")
        modetxt = {None: "NO DATA", 0: "NO FIX", 1: "NO FIX", 2: "2D FIX", 3: "3D FIX"}.get(mode, "?")
        if gs == "NO DATA":
            modetxt = "NO DATA"
        nsat = gps.get("nsat")
        lat = gps.get("lat"); lon = gps.get("lon")
        draw.text((4, 1), "GPS", font=fontsmall, fill=tcol)
        draw.text((4, 14), modetxt + "  sats " + (_fmt(nsat, "%d") if nsat is not None else "--"),
                  font=fontsmall, fill=(255, 255, 255))
        draw.text((4, 27), "lat " + (_fmt(lat, "%.5f") if lat is not None else "--"), font=fontsmall, fill=(255, 255, 255))
        draw.text((4, 40), "lon " + (_fmt(lon, "%.5f") if lon is not None else "--"), font=fontsmall, fill=(255, 255, 255))
        render_graph(ctx, (4, 53, w - 4, h - 8), hist.get("gps_nsat", ()), 0, 12)
    elif kind == "imu":
        imu = st.get("imu")
        draw.text((4, 1), "IMU", font=fontsmall, fill=tcol)
        if imu:
            mag = (imu["ax"] ** 2 + imu["ay"] ** 2 + imu["az"] ** 2) ** 0.5
            draw.text((4, 13), "a %+.2f %+.2f %+.2f" % (imu["ax"], imu["ay"], imu["az"]),
                      font=fontsmall, fill=(255, 255, 255))
            draw.text((4, 26), "g %+.0f %+.0f %+.0f" % (imu["gx"], imu["gy"], imu["gz"]),
                      font=fontsmall, fill=(255, 255, 255))
            draw.text((4, 39), "|a| %.2fg  %.0fC" % (mag, imu["temp"]),
                      font=fontsmall, fill=(180, 180, 180))
        else:
            draw.text((4, 16), "no IMU", font=font, fill=(255, 70, 70))
        render_graph(ctx, (4, 53, w - 4, h - 8), hist.get("imu_acc", ()))
    elif kind == "action":
        draw.text((4, 1), p["title"], font=fontsmall, fill=tcol)
        if p.get("action") == "calib":
            cs = st["calib_status"]; cd = st["calib_detail"]
            if cs == "ok":
                draw.text((4, 16), "CALIBRATED", font=fontmed, fill=(0, 230, 0))
            elif cs == "fail":
                draw.text((4, 16), "MOVED", font=fontmed, fill=(255, 70, 70))
            else:
                draw.text((4, 16), "click to zero", font=font, fill=(200, 200, 200))
            if cd:
                draw.text((4, 44), cd[:28], font=fontsmall, fill=(150, 150, 150))
            draw.text((4, 58), "1 click = zero gyro at rest", font=fontsmall, fill=(110, 110, 110))
        else:
            ss = st["send_status"]; sd = st["send_detail"]
            if ss == "running":
                draw.text((4, 16), "SENDING...", font=fontmed, fill=(255, 220, 0))
            elif ss == "ok":
                draw.text((4, 16), "SENT", font=fontmed, fill=(0, 230, 0))
            elif ss == "fail":
                draw.text((4, 16), "FAILED", font=fontmed, fill=(255, 70, 70))
            else:
                draw.text((4, 16), "click to send", font=font, fill=(200, 200, 200))
            if sd:
                draw.text((4, 44), sd[:28], font=fontsmall, fill=(150, 150, 150))
            draw.text((4, 58), "1 click = send latest CSVs", font=fontsmall, fill=(110, 110, 110))

    # page-indicator dots along the bottom
    n = len(PAGES)
    for i in range(n):
        cx = 4 + i * 9
        c = (255, 255, 255) if i == (page % n) else (70, 70, 70)
        draw.ellipse((cx, h - 5, cx + 4, h - 1), fill=c)

    # transient toast banner (brief; covers the title line)
    toast = st.get("toast")
    if toast:
        draw.rectangle((0, 0, w, 15), fill=(20, 20, 60))
        draw.text((4, 1), toast[:24], font=fontsmall, fill=(255, 255, 255))

    # recording status dot (top-right): red = recording, yellow = idle
    draw.ellipse((w - 15, 3, w - 5, 13), fill=(230, 30, 30) if st["is_recording"] else (240, 200, 0))

    display.display(img)


# ---------- GPS via gpsd (legacy helpers; unused with the bit-bang reader) ----------
def gpsd_read_tpv(timeout=GPSD_TIMEOUT_S):
    try:
        with socket.create_connection((GPSD_HOST, GPSD_PORT), timeout=timeout) as s:
            s.settimeout(timeout)
            s.sendall(b'?WATCH={"enable":true,"json":true};\n')
            buf = b""
            latest_tpv = None
            t_end = time.time() + timeout
            while time.time() < t_end:
                try:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    buf += chunk
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        if not line.strip():
                            continue
                        try:
                            msg = json.loads(line.decode(errors="ignore"))
                        except Exception:
                            continue
                        if msg.get("class") == "TPV":
                            latest_tpv = msg
                except socket.timeout:
                    break
            return latest_tpv
    except Exception:
        return None


# ---------- GPS via pigpio bit-banged serial (hardware UART freed for the PMS5003) ----------
GPS_BB_GPIO = 16       # BCM16 / physical pin 36: bit-banged RX from the GPS TX wire
GPS_BB_BAUD = 9600


def _nmea_checksum_ok(line):
    star = line.rfind("*")
    if star < 1 or star + 3 > len(line):
        return False
    cs = 0
    for ch in line[1:star]:
        cs ^= ord(ch)
    try:
        return cs == int(line[star + 1:star + 3], 16)
    except ValueError:
        return False


def _nmea_coord(val, hemi, deg_digits):
    if not val or len(val) < deg_digits + 2:
        return None
    try:
        deg = float(val[:deg_digits])
        minutes = float(val[deg_digits:])
        dec = deg + minutes / 60.0
        return -dec if hemi in ("S", "W") else dec
    except ValueError:
        return None


class BitBangGPS:
    """NMEA from the GPS TX wire on a spare GPIO via pigpio bit-bang serial."""
    EMPTY = {"mode": None, "lat": None, "lon": None, "alt": None, "speed": None,
             "track": None, "climb": None, "eph": None, "epv": None, "gpstime": None,
             "nsat": None}

    def __init__(self, gpio, baud=9600):
        self.gpio = gpio
        self.buf = b""
        self.latest = dict(self.EMPTY)
        self.last_sentence_t = 0.0
        self.pi = None
        try:
            import pigpio
            pi = pigpio.pi()
            if pi.connected:
                try:
                    pi.bb_serial_read_close(gpio)
                except Exception:
                    pass
                pi.bb_serial_read_open(gpio, baud, 8)
                self.pi = pi
        except Exception:
            self.pi = None

    @property
    def has_data(self):
        return (time.time() - self.last_sentence_t) < 5.0

    def fields(self):
        return self.latest

    def poll(self):
        if self.pi is None:
            return
        try:
            count, data = self.pi.bb_serial_read(self.gpio)
        except Exception:
            return
        if count and data:
            self.buf += bytes(data)
            while b"\n" in self.buf:
                raw, self.buf = self.buf.split(b"\n", 1)
                self._parse(raw.decode("ascii", "ignore").strip())
            if len(self.buf) > 2048:
                self.buf = self.buf[-512:]

    def _parse(self, line):
        if not line.startswith("$") or "*" not in line or not _nmea_checksum_ok(line):
            return
        f = line[1:line.rfind("*")].split(",")
        typ = f[0][2:] if len(f[0]) >= 5 else f[0]
        self.last_sentence_t = time.time()
        L = self.latest
        try:
            if typ == "RMC" and len(f) >= 10:
                if f[2] == "A":
                    L["lat"] = _nmea_coord(f[3], f[4], 2)
                    L["lon"] = _nmea_coord(f[5], f[6], 3)
                    L["speed"] = float(f[7]) * 0.514444 if f[7] else None
                    L["track"] = float(f[8]) if f[8] else None
                    if L["mode"] is None or L["mode"] < 2:
                        L["mode"] = 2
                    if len(f[1]) >= 6 and len(f[9]) == 6:
                        L["gpstime"] = ("20%s-%s-%sT%s:%s:%sZ" %
                                        (f[9][4:6], f[9][2:4], f[9][0:2],
                                         f[1][0:2], f[1][2:4], f[1][4:6]))
                else:
                    L["lat"] = L["lon"] = L["speed"] = L["track"] = None
                    L["mode"] = 1
            elif typ == "GGA" and len(f) >= 10:
                if f[7].isdigit():
                    L["nsat"] = int(f[7])
                if f[6] and f[6] != "0":
                    L["lat"] = _nmea_coord(f[2], f[3], 2)
                    L["lon"] = _nmea_coord(f[4], f[5], 3)
                    L["alt"] = float(f[9]) if f[9] else None
                    if L["mode"] is None or L["mode"] < 3:
                        L["mode"] = 3
                else:
                    L["alt"] = None
            elif typ == "GSA" and len(f) >= 3 and f[2] in ("1", "2", "3"):
                L["mode"] = int(f[2])
        except (ValueError, IndexError):
            pass


# ---------- KY-040 rotary encoder + push button (via pigpio) ----------
# Wiring (BCM):  CLK -> GPIO5 (pin 29)   DT -> GPIO6 (pin 31)   SW -> GPIO13 (pin 33)
#                +   -> 3V3  (pin 17)    GND -> GND  (pin 39)
# NOTE: power from 3V3, NOT 5V (on-board pull-ups would feed 5V into the GPIOs).
#       GPIO22 + GPIO27 are the PMS5003 enable/reset on the Enviro+ HAT -- never use them.
#       Requires pigpiod running with the PWM clock (-t 0); the PCM default collides
#       with the I2S mic and silently kills all pigpio sampling (see project notes).
ENC_CLK_GPIO    = 5
ENC_DT_GPIO     = 6
ENC_SW_GPIO     = 13
BTN_DEBOUNCE_S  = 0.05     # ignore button blips shorter than this
MAX_CLICK_S     = 0.8      # a press longer than this is NOT counted as a click
CLICK_GAP_S     = 0.5      # clicks within this gap group into one burst
SHUTDOWN_CLICKS = 4        # this many clicks -> shutdown
SHUTDOWN_CANCEL_S = 3.0    # cancellable countdown before the actual poweroff


class RotaryControl:
    """KY-040 rotary encoder + shaft button read with pigpio callbacks.

    - take_rotation() returns the net detent count since the last call (+ = CW).
    - take_clicks(now) returns the size of a completed click burst (1, 2, ...)
      once the button has been quiet for CLICK_GAP_S, else 0. Short taps only:
      a press held longer than MAX_CLICK_S is ignored (not a click).

    Degrades gracefully (ok == False) if pigpio/pigpiod is unavailable.
    """

    # Quadrature transitions for a detented encoder (rest state = both high, 0b11)
    _CW  = {(0b11, 0b01), (0b01, 0b00), (0b00, 0b10), (0b10, 0b11)}
    _CCW = {(0b11, 0b10), (0b10, 0b00), (0b00, 0b01), (0b01, 0b11)}

    def __init__(self, clk, dt, sw):
        import threading
        self._lock = threading.Lock()
        self.clk, self.dt, self.sw = clk, dt, sw
        self._steps = 0          # whole detents pending for the caller
        self._accum = 0          # quarter-steps within the current detent
        self._last_code = None
        self._press_t = 0.0      # time the button went down (0 == up)
        self._clicks = 0         # taps in the current burst
        self._last_release_t = 0.0
        self._cbs = []
        self.pi = None
        self.ok = False
        try:
            import pigpio
            pi = pigpio.pi()
            if not pi.connected:
                return
            for g in (clk, dt, sw):
                pi.set_mode(g, pigpio.INPUT)
                pi.set_pull_up_down(g, pigpio.PUD_UP)
            pi.set_glitch_filter(clk, 300)    # microseconds
            pi.set_glitch_filter(dt, 300)
            pi.set_glitch_filter(sw, 2000)
            self._last_code = (pi.read(clk) << 1) | pi.read(dt)
            self._cbs.append(pi.callback(clk, pigpio.EITHER_EDGE, self._on_rotate))
            self._cbs.append(pi.callback(dt,  pigpio.EITHER_EDGE, self._on_rotate))
            self._cbs.append(pi.callback(sw,  pigpio.EITHER_EDGE, self._on_button))
            self.pi = pi
            self.ok = True
        except Exception:
            self.pi = None
            self.ok = False

    def _on_rotate(self, gpio, level, tick):
        if self.pi is None:
            return
        code = (self.pi.read(self.clk) << 1) | self.pi.read(self.dt)
        with self._lock:
            prev = self._last_code
            self._last_code = code
            if prev is None or code == prev:
                return
            if (prev, code) in self._CW:
                self._accum += 1
            elif (prev, code) in self._CCW:
                self._accum -= 1
            if code == 0b11:
                if self._accum >= 2:
                    self._steps += 1
                elif self._accum <= -2:
                    self._steps -= 1
                self._accum = 0

    def _on_button(self, gpio, level, tick):
        # Active-low: level 0 = pressed, level 1 = released.
        if level == 0:
            with self._lock:
                self._press_t = time.time()
        elif level == 1:
            with self._lock:
                if self._press_t:
                    held = time.time() - self._press_t
                    if BTN_DEBOUNCE_S <= held <= MAX_CLICK_S:
                        self._clicks += 1
                        self._last_release_t = time.time()
                self._press_t = 0.0

    def take_rotation(self):
        with self._lock:
            s, self._steps = self._steps, 0
            return s

    def take_clicks(self, now):
        """Return a finished click-burst size, or 0 if none is ready yet."""
        with self._lock:
            if (self._clicks > 0 and self._press_t == 0.0
                    and (now - self._last_release_t) >= CLICK_GAP_S):
                n, self._clicks = self._clicks, 0
                return n
            return 0

    def stop(self):
        for cb in self._cbs:
            try:
                cb.cancel()
            except Exception:
                pass
        if self.pi is not None:
            try:
                self.pi.stop()
            except Exception:
                pass


# ---------- CSV sender (latest files over SSH to the PC; wired up later) ----------
class SendJob:
    """Runs SEND_SCRIPT in the background; surfaces idle/running/ok/fail status."""

    def __init__(self, script, timeout):
        self.script = script
        self.timeout = timeout
        self.proc = None
        self.status = "idle"     # idle | running | ok | fail
        self.detail = ""
        self.t0 = 0.0

    @property
    def running(self):
        return self.proc is not None

    def start(self):
        if self.running:
            return
        if not os.path.exists(self.script):
            self.status, self.detail = "fail", "script not installed"
            return
        try:
            self.proc = subprocess.Popen(
                ["/bin/bash", self.script],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            self.status, self.detail, self.t0 = "running", "", time.time()
        except Exception as e:
            self.status, self.detail, self.proc = "fail", str(e)[:40], None

    def poll(self):
        if self.proc is None:
            return
        rc = self.proc.poll()
        if rc is None:
            if time.time() - self.t0 > self.timeout:
                try:
                    self.proc.kill()
                except Exception:
                    pass
                self.proc, self.status, self.detail = None, "fail", "timeout"
            return
        try:
            out = self.proc.stdout.read() or ""
        except Exception:
            out = ""
        self.proc = None
        lines = [ln for ln in out.strip().splitlines() if ln.strip()]
        self.detail = (lines[-1] if lines else "")[:40]
        self.status = "ok" if rc == 0 else "fail"


# ---------- MPU-6050 (GY-521) accel + gyro over I2C ----------
class MPU6050:
    """Minimal MPU-6050 reader on the shared I2C bus.
    accel in g (+/-2g range), gyro in deg/s (+/-250 range), plus die temp (C).
    Degrades gracefully (ok == False) if the chip isn't present.
    """

    def __init__(self, bus, addr=0x68):
        self.bus = bus
        self.addr = addr
        self.ok = False
        self.who = None
        self._accel_scale = 16384.0   # LSB/g at +/-2g
        self._gyro_scale = 131.0      # LSB/(deg/s) at +/-250 dps
        self.gx_off = 0.0             # gyro zero-rate offsets (set by calibrate())
        self.gy_off = 0.0
        self.gz_off = 0.0
        self.accel_corr = 1.0         # accel gain so |a| at rest -> 1 g
        try:
            self.who = bus.read_byte_data(addr, 0x75)   # WHO_AM_I
            bus.write_byte_data(addr, 0x6B, 0x00)       # PWR_MGMT_1: wake (clear sleep)
            time.sleep(0.05)
            bus.write_byte_data(addr, 0x1A, 0x03)       # CONFIG: DLPF ~44 Hz (less noise)
            bus.write_byte_data(addr, 0x1B, 0x00)       # GYRO_CONFIG: +/-250 dps
            bus.write_byte_data(addr, 0x1C, 0x00)       # ACCEL_CONFIG: +/-2 g
            self.ok = True
        except Exception:
            self.ok = False

    def _raw(self):
        if not self.ok:
            return None
        try:
            d = self.bus.read_i2c_block_data(self.addr, 0x3B, 14)  # accel(6)+temp(2)+gyro(6)
        except Exception:
            return None

        def _c(h, l):
            v = (h << 8) | l
            return v - 65536 if v >= 32768 else v

        return {
            "ax": _c(d[0], d[1]) / self._accel_scale,
            "ay": _c(d[2], d[3]) / self._accel_scale,
            "az": _c(d[4], d[5]) / self._accel_scale,
            "temp": _c(d[6], d[7]) / 340.0 + 36.53,
            "gx": _c(d[8], d[9]) / self._gyro_scale,
            "gy": _c(d[10], d[11]) / self._gyro_scale,
            "gz": _c(d[12], d[13]) / self._gyro_scale,
        }

    def read(self):
        """Calibrated sample: gyro zero-rate removed, accel scaled so |a|~1g."""
        r = self._raw()
        if r is None:
            return None
        r["gx"] -= self.gx_off
        r["gy"] -= self.gy_off
        r["gz"] -= self.gz_off
        r["ax"] *= self.accel_corr
        r["ay"] *= self.accel_corr
        r["az"] *= self.accel_corr
        return r

    def calibrate(self, dur=1.2):
        """Zero the gyro and normalise |accel| to 1 g, assuming the unit is at
        rest. Returns (ok, message); rejects if movement is detected."""
        if not self.ok:
            return (False, "no IMU")
        gx, gy, gz, mags = [], [], [], []
        t0 = time.time()
        while time.time() - t0 < dur:
            r = self._raw()
            if r is not None:
                gx.append(r["gx"]); gy.append(r["gy"]); gz.append(r["gz"])
                mags.append((r["ax"] ** 2 + r["ay"] ** 2 + r["az"] ** 2) ** 0.5)
            time.sleep(0.01)
        n = len(gx)
        if n < 10:
            return (False, "read error")

        def _std(a):
            m = sum(a) / len(a)
            return (sum((x - m) ** 2 for x in a) / len(a)) ** 0.5

        # std-based stillness (robust to the PMS fan vibration / single spikes)
        if max(_std(gx), _std(gy), _std(gz)) > 2.5 or _std(mags) > 0.04:
            return (False, "moving-hold still")
        self.gx_off = sum(gx) / n
        self.gy_off = sum(gy) / n
        self.gz_off = sum(gz) / n
        mean_mag = sum(mags) / n
        self.accel_corr = (1.0 / mean_mag) if mean_mag > 0.5 else 1.0
        return (True, "gyro 0, |a|=1g")


# ---------- Sensors ----------
def init_sensors():
    env_noise = noise.Noise()
    bus = SMBus(1)
    bme = BME280(i2c_dev=bus)
    pms = None
    if PMS5003 is not None:
        try:
            pms = PMS5003()
        except Exception:
            pms = None
    mpu = None
    for _a in (0x68, 0x69):
        m = MPU6050(bus, _a)
        if m.ok:
            mpu = m
            break
    return env_noise, bme, pms, mpu


def _pms_value(pm, attr: str, size, atm_fallback: bool = False):
    src = getattr(pm, attr, None)
    if src is None:
        return None
    if hasattr(src, "get"):
        try:
            v = src.get(size)
            if v is None and isinstance(size, float) and float(size).is_integer():
                v = src.get(int(size))
            return v
        except Exception:
            pass
    if callable(src):
        if atm_fallback:
            try:
                return src(size, True)
            except TypeError:
                pass
            except Exception:
                return None
        try:
            return src(size)
        except Exception:
            return None
    return None


def read_pms_basic(pms):
    """One PMS frame -> the three mass concentrations for the display (or None)."""
    try:
        pm = pms.read()
        return {
            "pm1_0": _pms_value(pm, "pm_ug_per_m3", 1.0),
            "pm2_5": _pms_value(pm, "pm_ug_per_m3", 2.5),
            "pm10": _pms_value(pm, "pm_ug_per_m3", 10),
        }
    except Exception:
        return None


def read_enviro(env_noise, bme, pms, mpu=None):
    try:
        lux = ltr559.get_lux()
    except Exception:
        lux = None
    try:
        prox = ltr559.get_proximity()
    except Exception:
        prox = None

    try:
        temp = bme.get_temperature()
    except Exception:
        temp = None
    try:
        hum = bme.get_humidity()
    except Exception:
        hum = None
    try:
        pres = bme.get_pressure()
    except Exception:
        pres = None
    try:
        alt = bme.get_altitude()
    except Exception:
        alt = None

    cpu = get_cpu_temperature()
    if temp is not None and cpu is not None:
        temp_comp = temp - ((cpu - temp) / TEMP_COMP_FACTOR)
    else:
        temp_comp = None

    try:
        g = gas.read_all()
        ox = getattr(g, "oxidising", None)
        red = getattr(g, "reducing", None)
        nh3 = getattr(g, "nh3", None)
        adc = getattr(g, "adc", None)
    except Exception:
        ox = red = nh3 = adc = None

    try:
        nlow, nmid, nhigh, ntotal = env_noise.get_noise_profile()
    except Exception:
        nlow = nmid = nhigh = ntotal = None

    pm1_0 = pm2_5 = pm10 = None
    pm1_0_atm = pm2_5_atm = pm10_atm = None
    pm0_3_count = pm0_5_count = pm1_0_count = None
    pm2_5_count = pm5_0_count = pm10_count = None
    if pms is not None:
        try:
            pm = pms.read()
            pm1_0 = _pms_value(pm, "pm_ug_per_m3", 1.0)
            pm2_5 = _pms_value(pm, "pm_ug_per_m3", 2.5)
            pm10 = _pms_value(pm, "pm_ug_per_m3", 10)

            pm1_0_atm = _pms_value(pm, "pm_ug_per_m3_atm", 1.0)
            if pm1_0_atm is None:
                pm1_0_atm = _pms_value(pm, "pm_ug_per_m3", 1.0, atm_fallback=True)
            pm2_5_atm = _pms_value(pm, "pm_ug_per_m3_atm", 2.5)
            if pm2_5_atm is None:
                pm2_5_atm = _pms_value(pm, "pm_ug_per_m3", 2.5, atm_fallback=True)
            pm10_atm = _pms_value(pm, "pm_ug_per_m3_atm", 10)
            if pm10_atm is None:
                pm10_atm = _pms_value(pm, "pm_ug_per_m3", 10, atm_fallback=True)

            pm0_3_count = _pms_value(pm, "pm_per_1l_air", 0.3)
            pm0_5_count = _pms_value(pm, "pm_per_1l_air", 0.5)
            pm1_0_count = _pms_value(pm, "pm_per_1l_air", 1.0)
            pm2_5_count = _pms_value(pm, "pm_per_1l_air", 2.5)
            pm5_0_count = _pms_value(pm, "pm_per_1l_air", 5.0)
            pm10_count = _pms_value(pm, "pm_per_1l_air", 10)
        except Exception:
            pass

    imu = mpu.read() if mpu is not None else None

    return {
        "lux": lux, "prox": prox,
        "temp": temp, "hum": hum, "pres": pres, "alt": alt,
        "cpu": cpu, "temp_comp": temp_comp,
        "ox": ox, "red": red, "nh3": nh3, "adc": adc,
        "nlow": nlow, "nmid": nmid, "nhigh": nhigh, "ntotal": ntotal,
        "pm1_0": pm1_0, "pm2_5": pm2_5, "pm10": pm10,
        "pm1_0_atm": pm1_0_atm, "pm2_5_atm": pm2_5_atm, "pm10_atm": pm10_atm,
        "pm0_3_count": pm0_3_count, "pm0_5_count": pm0_5_count,
        "pm1_0_count": pm1_0_count, "pm2_5_count": pm2_5_count,
        "pm5_0_count": pm5_0_count, "pm10_count": pm10_count,
        "imu_ax": (imu or {}).get("ax"), "imu_ay": (imu or {}).get("ay"),
        "imu_az": (imu or {}).get("az"), "imu_gx": (imu or {}).get("gx"),
        "imu_gy": (imu or {}).get("gy"), "imu_gz": (imu or {}).get("gz"),
        "imu_temp": (imu or {}).get("temp"),
    }


CSV_HEADER = [
    "timestamp_iso",
    "unix_time_s",
    "lux",
    "proximity",
    "temperature_C",
    "humidity_percent",
    "pressure_hPa",
    "altitude_m",
    "cpu_temperature_C",
    "temperature_compensated_C",
    "gas_oxidising_ohms",
    "gas_reducing_ohms",
    "gas_nh3_ohms",
    "gas_adc_raw",
    "noise_low",
    "noise_mid",
    "noise_high",
    "noise_total",
    "pm1_0_ug_m3",
    "pm2_5_ug_m3",
    "pm10_ug_m3",
    "pm1_0_atm_ug_m3",
    "pm2_5_atm_ug_m3",
    "pm10_atm_ug_m3",
    "pm0_3_count",
    "pm0_5_count",
    "pm1_0_count",
    "pm2_5_count",
    "pm5_0_count",
    "pm10_count",
    "gps_mode",
    "gps_lat",
    "gps_lon",
    "gps_alt_m",
    "gps_speed_m_s",
    "gps_track_deg",
    "gps_climb_m_s",
    "gps_eph_m",
    "gps_epv_m",
    "gps_time_utc",
    # MPU-6050 IMU
    "imu_accel_x_g",
    "imu_accel_y_g",
    "imu_accel_z_g",
    "imu_gyro_x_dps",
    "imu_gyro_y_dps",
    "imu_gyro_z_dps",
    "imu_temp_C",
]


class Recorder:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.fp = None
        self.writer = None
        self.path = None

    @property
    def is_recording(self) -> bool:
        return self.fp is not None

    def start(self):
        if self.is_recording:
            return
        self.path = new_csv_path(self.data_dir)
        self.fp = open(self.path, "w", newline="")
        self.writer = csv.writer(self.fp)
        self.writer.writerow(CSV_HEADER)
        self.fp.flush()
        print(f"[{now_iso_seconds()}] RECORDING STARTED -> {self.path}")

    def stop(self):
        if not self.is_recording:
            return
        try:
            self.fp.flush()
            self.fp.close()
        finally:
            print(f"[{now_iso_seconds()}] RECORDING STOPPED -> {self.path}")
            self.fp = None
            self.writer = None
            self.path = None

    def write_row(self, row):
        if not self.is_recording:
            return
        self.writer.writerow(row)
        self.fp.flush()


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = ensure_data_dir(script_dir)

    env_noise, bme, pms, mpu = init_sensors()
    display_ctx = init_display()

    recorder = Recorder(data_dir)
    sendjob = SendJob(SEND_SCRIPT, SEND_TIMEOUT_S)

    # Proximity control kept as a backup: tap = next page, hold 2-5s = toggle.
    ctl = ToggleController(time.time())
    last_ctrl_poll = 0.0
    TOGGLE_FILE = os.path.expanduser("~/.enviro_toggle")

    page = 0
    sv = read_display_sensors(bme) if display_ctx else {}
    pm_cache = {"pm1_0": None, "pm2_5": None, "pm10": None}
    last_pm_t = 0.0
    imu_cache = None
    hist = {k: deque(maxlen=HIST_MAXLEN) for k in HIST_KEYS}
    last_disp_refresh = 0.0

    toast_msg = ""
    toast_until = 0.0
    calib_status = "idle"   # idle | ok | fail (last IMU calibration result)
    calib_detail = ""

    def toast(msg):
        nonlocal toast_msg, toast_until
        toast_msg, toast_until = msg, time.time() + 1.3

    def do_toggle_recording(why):
        if recorder.is_recording:
            recorder.stop()
            toast("REC OFF")
            print(f"[{now_iso_seconds()}] {why} -> STOP recording")
        else:
            recorder.start()
            toast("REC ON")
            print(f"[{now_iso_seconds()}] {why} -> START recording")

    # GPS via bit-banged serial on GPIO16 (hardware UART is the PMS5003's)
    gpsbb = BitBangGPS(GPS_BB_GPIO, GPS_BB_BAUD)
    gps = gpsbb.fields()
    gps_state = "IDLE"

    # KY-040 rotary encoder
    rotary = RotaryControl(ENC_CLK_GPIO, ENC_DT_GPIO, ENC_SW_GPIO)
    shutdown_pending = False
    shutdown_deadline = 0.0
    shutdown_done = False

    next_sample = time.time()
    last_display_update = 0.0

    print(f"[{now_iso_seconds()}] Ready. Rotate = page; 1 click = activate; "
          f"2 clicks = record; {SHUTDOWN_CLICKS} clicks = shutdown.")
    print(f"[{now_iso_seconds()}] Data directory: {data_dir}")
    if PMS5003 is None:
        print(f"[{now_iso_seconds()}] PMS5003 module not installed. PM fields will remain blank.")
    elif pms is None:
        print(f"[{now_iso_seconds()}] PMS5003 init failed. PM fields will remain blank.")
    else:
        print(f"[{now_iso_seconds()}] PMS5003 active.")
    if mpu is not None and mpu.ok:
        print(f"[{now_iso_seconds()}] MPU-6050 IMU active (addr 0x{mpu.addr:02X}, WHO_AM_I=0x{mpu.who:02X}).")
    else:
        print(f"[{now_iso_seconds()}] MPU-6050 not found. IMU columns blank.")
    if rotary.ok:
        print(f"[{now_iso_seconds()}] KY-040 encoder ready (GPIO {ENC_CLK_GPIO}/{ENC_DT_GPIO}/{ENC_SW_GPIO}).")
    else:
        print(f"[{now_iso_seconds()}] KY-040 encoder not available (pigpio/pigpiod?). Gesture control still works.")

    try:
        while True:
            now = time.time()

            # ---- Proximity control (backup): tap = next page, 2-5s hold = toggle ----
            if (now - last_ctrl_poll) >= CTRL_POLL_S:
                last_ctrl_poll = now
                try:
                    c_lux = ltr559.get_lux()
                except Exception:
                    c_lux = None
                try:
                    c_prox = ltr559.get_proximity()
                except Exception:
                    c_prox = None

                action = ctl.update(now, c_lux, c_prox)

                if action is None and os.path.exists(TOGGLE_FILE):
                    try:
                        os.remove(TOGGLE_FILE)
                    except OSError:
                        pass
                    action = "toggle"

                if action == "tap":
                    page = (page + 1) % len(PAGES)
                    toast(PAGES[page]["title"])
                elif action == "toggle":
                    do_toggle_recording("gesture")

            # ---- KY-040 encoder ----
            step = rotary.take_rotation()
            clicks = rotary.take_clicks(now)

            if shutdown_pending:
                # Any input aborts the countdown.
                if step or clicks:
                    shutdown_pending = False
                    toast("shutdown cancelled")
                    print(f"[{now_iso_seconds()}] shutdown cancelled")
                elif now >= shutdown_deadline and not shutdown_done:
                    shutdown_done = True
                    print(f"[{now_iso_seconds()}] {SHUTDOWN_CLICKS} clicks -> SHUTDOWN")
                    recorder.stop()
                    st_off = {"page": page, "sv": sv, "pm": pm_cache, "gps": gps,
                              "gps_state": gps_state, "hist": hist, "is_recording": False,
                              "shutdown_remaining": 0.0,
                              "send_status": sendjob.status, "send_detail": sendjob.detail}
                    render_ui(display_ctx, st_off)
                    subprocess.Popen(["sudo", "shutdown", "-h", "now"])
            else:
                if step:
                    page = (page + step) % len(PAGES)
                    toast(PAGES[page]["title"])
                if clicks == 1:
                    p = PAGES[page]
                    if p["kind"] == "action" and p.get("action") == "send":
                        sendjob.start()
                        toast("sending CSVs...")
                        print(f"[{now_iso_seconds()}] send CSVs requested")
                    elif p["kind"] == "action" and p.get("action") == "calib":
                        if display_ctx:
                            _d = display_ctx
                            _d["draw"].rectangle((0, 0, _d["w"], _d["h"]), (0, 0, 0))
                            _d["draw"].text((6, 18), "HOLD STILL", font=_d["font"], fill=(255, 220, 0))
                            _d["draw"].text((6, 44), "calibrating IMU...", font=_d["fontsmall"], fill=(180, 180, 180))
                            _d["display"].display(_d["img"])
                        ok, msg = mpu.calibrate() if mpu is not None else (False, "no IMU")
                        calib_status = "ok" if ok else "fail"
                        calib_detail = msg
                        toast("IMU: " + msg)
                        print(f"[{now_iso_seconds()}] IMU calibrate ok={ok} ({msg})")
                    else:
                        toast("2 clicks = record")
                elif clicks == 2:
                    do_toggle_recording("double-click")
                elif clicks == SHUTDOWN_CLICKS:
                    shutdown_pending = True
                    shutdown_deadline = now + SHUTDOWN_CANCEL_S
                    print(f"[{now_iso_seconds()}] {SHUTDOWN_CLICKS} clicks -> shutdown in {SHUTDOWN_CANCEL_S:.0f}s")
                elif clicks:
                    toast(f"{clicks} clicks")

            sendjob.poll()

            # GPS (bit-banged): drain serial every loop, keep latest fields
            gpsbb.poll()
            gps = gpsbb.fields()
            gps_fix = (gps["mode"] is not None and gps["mode"] >= 2)
            if recorder.is_recording:
                gps_state = "WRITE" if gps_fix else ("NO FIX" if gpsbb.has_data else "NO DATA")
            else:
                gps_state = "NO DATA" if not gpsbb.has_data else "IDLE"

            # Throttled PMS read for the display/graph when not mid-sample
            if (pms is not None and (now - last_pm_t) >= PMS_DISP_INTERVAL):
                basic = read_pms_basic(pms)
                if basic is not None:
                    pm_cache = basic
                    last_pm_t = now

            # Refresh live sensor values + append graph history
            if (now - last_disp_refresh) >= DISP_REFRESH_S:
                last_disp_refresh = now
                sv = read_display_sensors(bme)
                if _is_num(sv.get("temp")): hist["temp"].append(sv["temp"])
                if _is_num(sv.get("hum")):  hist["hum"].append(sv["hum"])
                if _is_num(sv.get("pres")): hist["pres"].append(sv["pres"])
                if _is_num(sv.get("lux")):  hist["lux"].append(sv["lux"])
                if _is_num(sv.get("ox")):   hist["gas_ox"].append(sv["ox"] / 1000.0)
                if _is_num(pm_cache.get("pm2_5")): hist["pm2_5"].append(pm_cache["pm2_5"])
                if _is_num(gps.get("nsat")): hist["gps_nsat"].append(gps["nsat"])
                imu_cache = mpu.read() if mpu is not None else None
                if imu_cache is not None:
                    _amag = (imu_cache["ax"] ** 2 + imu_cache["ay"] ** 2 + imu_cache["az"] ** 2) ** 0.5
                    hist["imu_acc"].append(_amag)

            # Display update (faster during overlays for smooth bars)
            disp_interval = 0.15 if (ctl.cover_active or shutdown_pending) else 0.5
            if display_ctx and (now - last_display_update) >= disp_interval:
                st = {
                    "page": page, "sv": sv, "pm": pm_cache, "gps": gps, "gps_state": gps_state,
                    "hist": hist, "is_recording": recorder.is_recording, "imu": imu_cache,
                    "cover_active": ctl.cover_active, "hold_progress": ctl.hold_progress, "phase": ctl.phase,
                    "shutdown_remaining": (shutdown_deadline - now) if shutdown_pending else None,
                    "toast": toast_msg if now < toast_until else None,
                    "send_status": sendjob.status, "send_detail": sendjob.detail,
                    "calib_status": calib_status, "calib_detail": calib_detail,
                }
                render_ui(display_ctx, st)
                last_display_update = now

            # Sampling (only while recording)
            if recorder.is_recording and now >= next_sample:
                e = read_enviro(env_noise, bme, pms, mpu)
                pm_cache = {"pm1_0": e["pm1_0"], "pm2_5": e["pm2_5"], "pm10": e["pm10"]}
                last_pm_t = now

                row = [
                    now_iso_seconds(),
                    f"{now:.3f}",
                    safe_float(e["lux"]),
                    safe_float(e["prox"]),
                    safe_float(e["temp"]),
                    safe_float(e["hum"]),
                    safe_float(e["pres"]),
                    safe_float(e["alt"]),
                    safe_float(e["cpu"]),
                    safe_float(e["temp_comp"]),
                    safe_float(e["ox"]),
                    safe_float(e["red"]),
                    safe_float(e["nh3"]),
                    safe_float(e["adc"]),
                    safe_float(e["nlow"]),
                    safe_float(e["nmid"]),
                    safe_float(e["nhigh"]),
                    safe_float(e["ntotal"]),
                    safe_float(e["pm1_0"]),
                    safe_float(e["pm2_5"]),
                    safe_float(e["pm10"]),
                    safe_float(e["pm1_0_atm"]),
                    safe_float(e["pm2_5_atm"]),
                    safe_float(e["pm10_atm"]),
                    safe_float(e["pm0_3_count"]),
                    safe_float(e["pm0_5_count"]),
                    safe_float(e["pm1_0_count"]),
                    safe_float(e["pm2_5_count"]),
                    safe_float(e["pm5_0_count"]),
                    safe_float(e["pm10_count"]),
                    safe_int(gps["mode"]),
                    safe_float(gps["lat"]),
                    safe_float(gps["lon"]),
                    safe_float(gps["alt"]),
                    safe_float(gps["speed"]),
                    safe_float(gps["track"]),
                    safe_float(gps["climb"]),
                    safe_float(gps["eph"]),
                    safe_float(gps["epv"]),
                    gps["gpstime"] if gps["gpstime"] is not None else "",
                    safe_float(e["imu_ax"]),
                    safe_float(e["imu_ay"]),
                    safe_float(e["imu_az"]),
                    safe_float(e["imu_gx"]),
                    safe_float(e["imu_gy"]),
                    safe_float(e["imu_gz"]),
                    safe_float(e["imu_temp"]),
                ]

                recorder.write_row(row)
                next_sample = now + SAMPLE_INTERVAL_S

            time.sleep(0.05)

    except KeyboardInterrupt:
        pass
    finally:
        recorder.stop()
        try:
            rotary.stop()
        except Exception:
            pass
        print(f"[{now_iso_seconds()}] Exited.")


if __name__ == "__main__":
    main()
