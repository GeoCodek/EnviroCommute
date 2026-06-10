# Enviro+ Commute Logger (Raspberry Pi)

A mobile air-quality + GPS logger built on a Raspberry Pi with a Pimoroni **Enviro+**
HAT, a **PMS5003** particulate sensor, a **u-blox NEO-M8N** GPS, an **MPU-6050** IMU, and a **KY-040** rotary
encoder for on-device control. It records to timestamped CSVs, shows live colourful
graphs on the Enviro+ LCD, and can push recordings to a PC over SSH.

Part of the [EnviroCommute](../README.md) project.

## What it logs

- **Weather** — temperature, humidity, pressure, altitude (BME280)
- **Gas** — oxidising / reducing / NH3 (MICS6814)
- **Light** — lux (LTR559)
- **Noise** — low / mid / high / total bands (I2S MEMS mic)
- **Particulates** — PM1.0 / PM2.5 / PM10 + bin counts (PMS5003)
- **GPS** — fix mode, lat/lon/alt, speed/track, satellites, UTC time (NEO-M8N)
- **Motion** — 3-axis acceleration (g) + 3-axis rotation (°/s) + die temp (MPU-6050 IMU)

All to `~/data/enviro_log_YYYY-MM-DD_<unix>.csv` (see [CSV columns](#csv-columns)).

## Hardware

A 40-pin Raspberry Pi (developed on a Pi Zero 2 W running 32-bit Raspberry Pi OS /
Raspbian) + Pimoroni Enviro+ HAT. The Enviro+ provides the BME280, LTR559, MICS6814,
the I2S mic and a 0.96" ST7735 LCD, and brings out a PMS slot. The GPS and encoder are
soldered to spare GPIOs; the MPU-6050 IMU just shares the existing I2C bus.

### Wiring

| Device | Signal | BCM | Phys pin | Notes |
|---|---|---|---|---|
| **PMS5003** | TXD / RXD | GPIO14 / GPIO15 | 8 / 10 | via the Enviro+ PMS slot → hardware UART `/dev/ttyAMA0` |
| PMS5003 | enable / reset | GPIO22 / GPIO27 | 15 / 13 | driven by the Enviro+ — **never reuse these** |
| ST7735 LCD | DC / backlight | GPIO9 / GPIO12 | 21 / 32 | on the HAT (SPI0, CE1) |
| **GPS NEO-M8N** | **TX → Pi** | **GPIO16** | **36** | **bit-banged** software serial @ 9600 (module RX not used) |
| GPS NEO-M8N | VCC / GND | 3V3 / GND | 1 or 17 / any GND | 3.3 V logic |
| **KY-040** | **CLK** | **GPIO5** | **29** | rotary A |
| KY-040 | **DT** | **GPIO6** | **31** | rotary B |
| KY-040 | **SW** | **GPIO13** | **33** | push button |
| KY-040 | **+** | 3V3 | **17** | **power from 3V3, NOT 5V** — the onboard pull-ups would otherwise feed 5 V into the GPIOs |
| KY-040 | **GND** | GND | **39** | |
| **MPU-6050** | **SDA / SCL** | GPIO2 / GPIO3 | **3 / 5** | shares the Enviro+ I2C bus (addr **0x68**; AD0 left open). INT / XDA / XCL unused |
| MPU-6050 | VCC / GND | 3V3 / GND | 1 or 17 / any GND | **power from 3V3, NOT 5V** (keeps the I2C pull-up levels safe) |

> The Pi has only **one** usable hardware UART, and the Enviro+ claims it for the PMS
> slot. That is why the GPS is read over a **bit-banged** software serial instead.

## Key workarounds (read these before debugging)

These cost real time to discover; they are the reason the build works.

### 1. One UART → GPS is bit-banged, PMS keeps the hardware UART

The PMS5003 needs `/dev/ttyAMA0`. To make that the real PL011 UART and keep it free:

- `dtoverlay=pi3-miniuart-bt` — moves Bluetooth onto the mini-UART so the PL011 is free.
- remove any `console=serial0,...` from `cmdline.txt` — no serial console on the port.
- **`gpsd` is masked** (`systemctl mask gpsd gpsd.socket`) so it can't grab the UART.

The GPS module's TX is then read on **GPIO16** via pigpio's `bb_serial_read`, and the
NMEA sentences (RMC / GGA / GSA) are parsed in Python (`BitBangGPS`).

### 2. pigpiod MUST run with the PWM clock (`-t 0`) — the big one

pigpio's DMA timing uses the **PCM** peripheral by default. The Enviro+ I2S MEMS mic
(`dtoverlay=adau7002-simple` + `dtparam=audio=on`) **also** uses PCM. They collide, and
pigpio's sampling **silently dies**: `bb_serial_read` and GPIO callbacks return
**0 bytes** while plain `gpioread` still works — which looks exactly like a broken
solder joint.

**Fix:** run `pigpiod -t 0` (PWM clock). See
[`systemd/pigpiod.service.d/override.conf`](systemd/pigpiod.service.d/override.conf).
Both the GPS bit-bang and the encoder were "dead" until this was set.

> Debugging tip: never trust a single "0 bytes" reading as a wiring fault — cross-check
> against a GPIO you *know* is transmitting (e.g. the PMS TX on GPIO15).

### 3. KY-040 bring-up — read the symptoms

- a signal line reading **static / floating** (it just follows the internal pull) = an
  **open** wire.
- **CLK and DT clamped LOW and refusing to pull high** = **power and ground are
  swapped** (the module's pull-ups are pulling toward 0 V). We hit exactly this;
  swapping `+` / `GND` fixed it.
- `DT` shows edges on rotation but `CLK` stays flat = the `CLK` wire is open (or vice
  versa).
- avoid GPIO22 / GPIO27 (PMS enable/reset) and the LCD pins.

A standalone tester is included: [`encoder_test.py`](encoder_test.py) — a static pull
test plus a timed rotate/press window with a pass/fail summary (no Ctrl-C needed).

### 4. Misc

- **Temperature compensation:** the CPU warms the BME280; the logger reports
  `temp - (cpu - temp) / factor` (factor ≈ 2.25, empirical — tune for your enclosure).
- A **PMS5003 power glitch can brown-out / reset the Pi** — give it a solid 5 V feed.

## Controls (KY-040 encoder)

| Input | Action |
|---|---|
| **rotate** | move through the page carousel |
| **1 click** | activate the current page if it is an action (e.g. *Send CSVs*, *Calibrate IMU*) |
| **2 clicks** (< 1 s) | start / stop recording |
| **4 clicks** | shutdown (3 s cancellable countdown — rotate or click to abort) |

Clicks are accumulated and dispatched on the final count, so a double / quadruple click
never misfires as singles. Backups: a hand-cover gesture over the light sensor (tap =
next page, 2–5 s hold = toggle recording), and `touch ~/.enviro_toggle` to toggle
recording over SSH.

### Display

A carousel of pages, each with a **colourful scrolling graph**: temperature, humidity,
pressure, light, gas, a **PMS** page (PM1 / 2.5 / 10 + a PM2.5 graph), a **GPS** page
(mode / sats / lat-lon + a satellite-count graph), an **IMU** page (accel / gyro / `|a|` +
die temp, with an acceleration-magnitude graph), a **Calibrate IMU** action page, and a
**Send CSVs** action page. Top-right status dot: **red = recording, yellow = idle**.

### IMU calibration

The **Calibrate IMU** page does a one-click *at-rest* calibration: hold the unit still and
single-click. It averages ~1.2 s and then (a) subtracts the **gyro zero-rate bias** and
(b) scales the accelerometer so `|a|` reads 1 g. A **standard-deviation stillness guard**
rejects the calibration if it senses motion (any gyro axis std > 2.5 °/s, or `|a|` std >
0.04 g) — std rather than peak-to-peak, so the PMS fan's vibration is tolerated but real
motion isn't. The correction is applied to both the live display and the logged CSV.

Only the single-position calibration (gyro-zero + `|a|` normalisation) is done; full
6-position accelerometer calibration is skipped by choice. Gyro bias drifts a little with
temperature, so recalibrate after warm-up or whenever you're stopped and still.

## CSV columns

```
timestamp_iso, unix_time_s, lux, proximity, temperature_C, humidity_percent,
pressure_hPa, altitude_m, cpu_temperature_C, temperature_compensated_C,
gas_oxidising_ohms, gas_reducing_ohms, gas_nh3_ohms, gas_adc_raw,
noise_low, noise_mid, noise_high, noise_total,
pm1_0_ug_m3, pm2_5_ug_m3, pm10_ug_m3, pm1_0_atm_ug_m3, pm2_5_atm_ug_m3,
pm10_atm_ug_m3, pm0_3_count, pm0_5_count, pm1_0_count, pm2_5_count,
pm5_0_count, pm10_count,
gps_mode, gps_lat, gps_lon, gps_alt_m, gps_speed_m_s, gps_track_deg,
gps_climb_m_s, gps_eph_m, gps_epv_m, gps_time_utc,
imu_accel_x_g, imu_accel_y_g, imu_accel_z_g,
imu_gyro_x_dps, imu_gyro_y_dps, imu_gyro_z_dps, imu_temp_C
```

(The IMU columns are blank until an IMU is present; values are calibrated once you run
*Calibrate IMU*.)

## Setup

1. **OS + libs.** Raspberry Pi OS. Create a venv and install Pimoroni's stack:
   ```bash
   python3 -m venv ~/.virtualenvs/pimoroni
   ~/.virtualenvs/pimoroni/bin/pip install enviroplus pms5003 st7735 pimoroni-bme280 ltr559 pigpio smbus2
   ```
2. **`/boot/firmware/config.txt`** — the relevant lines from this build:
   ```ini
   dtparam=i2c_arm=on
   dtparam=spi=on
   dtparam=audio=on
   dtoverlay=adau7002-simple      # Enviro+ I2S mic (PCM)
   enable_uart=1
   dtoverlay=pi3-miniuart-bt      # frees the PL011 UART for the PMS
   ```
3. **`/boot/firmware/cmdline.txt`** — remove any `console=serial0,115200`.
4. **Free the UART for the PMS:** `sudo systemctl mask gpsd gpsd.socket`.
5. **pigpiod with the PWM clock** (critical): install
   [`systemd/pigpiod.service.d/override.conf`](systemd/pigpiod.service.d/override.conf),
   then `sudo systemctl daemon-reload && sudo systemctl enable --now pigpiod`.
6. **Logger service:** copy `enviro_logger.py` (and `send_csvs.sh`) to the Pi, edit
   [`systemd/enviro-logger.service`](systemd/enviro-logger.service) for your user / venv
   path, install it, then `sudo systemctl enable --now enviro-logger`.

## Off-loading CSVs to a PC over SSH (optional)

The *Send CSVs* page pushes recordings to a PC via `scp`, **incrementally**: it asks the
PC which CSVs it already has and sends only the missing ones, in a single connection
([`send_csvs.sh`](send_csvs.sh)).

**On the Pi:**

- key at `~/.ssh/pc_key` — `ssh-keygen -t ed25519 -f ~/.ssh/pc_key -N ''`
- destination in `~/.enviro_pc_dest` as one line `user@host:targetdir` (the target is
  relative to the PC user's home directory).

**On the PC (receiver):** install an OpenSSH **server**, authorize the Pi key, create the
target folder. A Windows helper is provided in
[`setup/pc_ssh_receiver_setup.ps1`](setup/pc_ssh_receiver_setup.ps1) (run elevated; paste
your Pi's public key into it first).

> Windows note: `Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0` can hang
> indefinitely when Windows Update / Features-on-Demand is policy-restricted; the helper
> installs the official **Win32-OpenSSH** release from GitHub instead, which is reliable.
> For an **admin** account the key must go in
> `C:\ProgramData\ssh\administrators_authorized_keys` (ACLs: SYSTEM + Administrators only).

## Files

- `enviro_logger.py` — the logger: sensors (incl. MPU-6050 IMU), display/UI, recorder, GPS bit-bang, encoder.
- `send_csvs.sh` — incremental CSV → PC sync.
- `encoder_test.py` — standalone KY-040 wiring tester.
- `mpu6050_probe.py` — standalone MPU-6050 I2C check (WHO_AM_I + one sample).
- `systemd/` — service unit + the essential pigpiod `-t 0` drop-in.
- `setup/pc_ssh_receiver_setup.ps1` — Windows OpenSSH receiver setup helper.

---

Hardware-specific values (pin assignments, the `-t 0` fix, the compensation factor)
reflect one particular build — adjust them for yours.
