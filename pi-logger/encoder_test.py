#!/usr/bin/env python3
"""Timed KY-040 test on GPIO5/6/13 (pins 29/31/33). No Ctrl-C needed.

Phase 1 (static, no touching): pull test on each line to check the wire is
  actually connected (module pull-ups on CLK/DT hold them high even against an
  internal pull-down; an OPEN wire follows the internal pull instead).
Phase 2 (dynamic): rotate + press for DUR seconds, then print a summary.

Usage: python -u encoder_test.py [seconds]   (default 20)
Requires pigpiod with the PWM clock (-t 0) -- already the permanent default.
"""
import sys, time

CLK, DT, SW = 5, 6, 13
DUR = float(sys.argv[1]) if len(sys.argv) > 1 else 20.0

import pigpio
pi = pigpio.pi()
if not pi.connected:
    raise SystemExit("pigpiod not connected (sudo systemctl status pigpiod)")


def pull_test(g, name):
    pi.set_mode(g, pigpio.INPUT)
    pi.set_pull_up_down(g, pigpio.PUD_DOWN); time.sleep(0.05); d = pi.read(g)
    pi.set_pull_up_down(g, pigpio.PUD_UP);   time.sleep(0.05); u = pi.read(g)
    if d == 1 and u == 1:
        verdict = "CONNECTED  (held high by module pull-up = wire good)"
    elif d == 0 and u == 1:
        verdict = "FLOATING   (open wire; normal for SW which has no module pull-up)"
    else:
        verdict = "driven LOW (d=%d u=%d -- unexpected)" % (d, u)
    print("  GPIO%-2d %-3s: pulldown=%d pullup=%d -> %s" % (g, name, d, u, verdict))


print("=== PHASE 1: STATIC PULL TEST (do not touch the encoder) ===")
pull_test(CLK, "CLK"); pull_test(DT, "DT"); pull_test(SW, "SW")
print("  (CLK & DT should read CONNECTED; SW typically FLOATING until pressed.)")

for g in (CLK, DT, SW):
    pi.set_pull_up_down(g, pigpio.PUD_UP)
pi.set_glitch_filter(CLK, 300); pi.set_glitch_filter(DT, 300); pi.set_glitch_filter(SW, 2000)

_CW  = {(0b11, 0b01), (0b01, 0b00), (0b00, 0b10), (0b10, 0b11)}
_CCW = {(0b11, 0b10), (0b10, 0b00), (0b00, 0b01), (0b01, 0b11)}
S = {"last": (pi.read(CLK) << 1) | pi.read(DT), "accum": 0,
     "cw": 0, "ccw": 0, "press": 0, "release": 0, "edges_clk": 0, "edges_dt": 0, "edges_sw": 0}
t0 = time.time()


def on_rot(gpio, level, tick):
    if gpio == CLK: S["edges_clk"] += 1
    else: S["edges_dt"] += 1
    code = (pi.read(CLK) << 1) | pi.read(DT)
    prev = S["last"]; S["last"] = code
    if code == prev: return
    if (prev, code) in _CW: S["accum"] += 1
    elif (prev, code) in _CCW: S["accum"] -= 1
    if code == 0b11:
        if S["accum"] >= 2:
            S["cw"] += 1; print("  [%5.1fs] CW  detent  (CW total=%d)" % (time.time() - t0, S["cw"]))
        elif S["accum"] <= -2:
            S["ccw"] += 1; print("  [%5.1fs] CCW detent  (CCW total=%d)" % (time.time() - t0, S["ccw"]))
        S["accum"] = 0


def on_btn(gpio, level, tick):
    S["edges_sw"] += 1
    if level == 0:
        S["press"] += 1; print("  [%5.1fs] BUTTON down (press #%d)" % (time.time() - t0, S["press"]))
    elif level == 1:
        S["release"] += 1; print("  [%5.1fs] BUTTON up" % (time.time() - t0))


cbs = [pi.callback(CLK, pigpio.EITHER_EDGE, on_rot),
       pi.callback(DT,  pigpio.EITHER_EDGE, on_rot),
       pi.callback(SW,  pigpio.EITHER_EDGE, on_btn)]

print("\n=== PHASE 2: rotate the knob both ways, then press it -- %.0f s window ===" % DUR)
sys.stdout.flush()
try:
    time.sleep(DUR)
finally:
    for cb in cbs:
        cb.cancel()

print("\n=== SUMMARY ===")
print("  raw edges seen:  CLK=%d  DT=%d  SW=%d   (0 on a pin = no signal = still open)"
      % (S["edges_clk"], S["edges_dt"], S["edges_sw"]))
print("  decoded detents: CW=%d  CCW=%d" % (S["cw"], S["ccw"]))
print("  button:          presses=%d  releases=%d" % (S["press"], S["release"]))
ok = (S["edges_clk"] > 0 and S["edges_dt"] > 0 and (S["cw"] + S["ccw"]) > 0 and S["press"] > 0)
print("  VERDICT: %s" % ("ENCODER FULLY WORKING" if ok else "INCOMPLETE -- see raw edges above"))
pi.stop()
print("Done.")
