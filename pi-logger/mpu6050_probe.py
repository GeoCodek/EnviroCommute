#!/usr/bin/env python3
"""Quick MPU-6050 (GY-521) check on I2C bus 1. Prints WHO_AM_I + one sample.

Run with the same Python that has smbus2 installed, e.g.:
    ~/.virtualenvs/pimoroni/bin/python mpu6050_probe.py
"""
import time
from smbus2 import SMBus

bus = SMBus(1)
found = None
for addr in (0x68, 0x69):
    try:
        who = bus.read_byte_data(addr, 0x75)  # WHO_AM_I
        print("addr 0x%02X responds, WHO_AM_I=0x%02X" % (addr, who))
        found = addr
        break
    except Exception as e:
        print("addr 0x%02X: no response (%s)" % (addr, e.__class__.__name__))

if found is None:
    raise SystemExit("MPU not found on 0x68/0x69 - check SDA/SCL/3V3/GND wiring")

addr = found
bus.write_byte_data(addr, 0x6B, 0x00)  # PWR_MGMT_1 = 0 -> wake from sleep
time.sleep(0.1)
d = bus.read_i2c_block_data(addr, 0x3B, 14)  # accel(6)+temp(2)+gyro(6)


def c(h, l):
    v = (h << 8) | l
    return v - 65536 if v >= 32768 else v


ax = c(d[0], d[1]) / 16384.0
ay = c(d[2], d[3]) / 16384.0
az = c(d[4], d[5]) / 16384.0
t = c(d[6], d[7]) / 340.0 + 36.53
gx = c(d[8], d[9]) / 131.0
gy = c(d[10], d[11]) / 131.0
gz = c(d[12], d[13]) / 131.0

print("accel g : x=%+.3f y=%+.3f z=%+.3f  |a|=%.3f" % (ax, ay, az, (ax * ax + ay * ay + az * az) ** 0.5))
print("gyro dps: x=%+.2f y=%+.2f z=%+.2f" % (gx, gy, gz))
print("die temp: %.1f C" % t)
print("(at rest & flat: |a| ~ 1.00, one axis ~ +/-1g, gyro ~ 0, temp ~ ambient+a few C)")
