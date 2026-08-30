#!/usr/bin/env python3
"""
zanderhbf2 — Axis P1357 Webcam FPS Research Probe
===================================================
Iteratively probes an MJPEG camera to model FPS as a function of:
  resolution, compression, time-of-day, concurrent connections, network RTT,
  and any other discoverable factors.

Designed to be called once per hour by a cron job. Each invocation:
  1. Reads history from data/history.jsonl
  2. Picks the next best test parameters (iterative experimental design)
  3. Captures a short MJPEG stream, measures actual FPS + frame sizes
  4. Optionally opens concurrent connections to measure contention
  5. Appends result to history.jsonl
  6. Rebuilds model summary (data/model_summary.json)
  7. Commits & pushes to git

Camera:  AXIS P1357 at http://mzinfo.dnshome.de:5010
API:     http://mzinfo.dnshome.de:5010/axis-cgi/mjpg/video.cgi
         ?resolution=WxH&compression=N&fps=30
"""

import json
import os
import subprocess
import sys
import time
import math
import hashlib
import datetime
import random
import threading
import urllib.request
import urllib.error
from pathlib import Path
from collections import defaultdict

# ─── CONFIGURATION ───────────────────────────────────────────────────────────

CAMERA_BASE = "http://mzinfo.dnshome.de:5010"
CAMERA_STREAM = f"{CAMERA_BASE}/axis-cgi/mjpg/video.cgi"
CAMERA_PARAMS = f"{CAMERA_BASE}/axis-cgi/param.cgi"

# All resolutions the camera supports
ALL_RESOLUTIONS = [
    "160x90", "160x120", "176x144",
    "240x180", "320x180", "320x240",
    "480x270", "480x360", "640x360",
    "640x480", "800x450", "800x600",
    "1024x768", "1280x720", "1280x960",
    "1280x1024", "1920x1080",
]

# Pixel counts for each resolution (used for megapixel classification)
RES_MP = {r: (int(r.split("x")[0]) * int(r.split("x")[1])) / 1e6 for r in ALL_RESOLUTIONS}

REPO_DIR = Path(__file__).parent
DATA_DIR = REPO_DIR / "data"
HISTORY_FILE = DATA_DIR / "history.jsonl"
MODEL_FILE = DATA_DIR / "model_summary.json"
PLAN_FILE = DATA_DIR / "experiment_plan.json"
GIT_CREDENTIALS_FILE = REPO_DIR / ".git-credentials"  # will be created at runtime, gitignored

CAPTURE_DURATION = 35  # seconds per probe (increased from 30 for better noise averaging)
MIN_FRAMES_FOR_VALID = 15  # minimum frames to consider a measurement valid
HIGH_JITTER_THRESHOLD_MS = 200  # if jitter exceeds this, flag measurement as noisy (short for hourly cron)
REQUESTED_FPS = 30

# ─── UTILITIES ────────────────────────────────────────────────────────────────

def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def load_history():
    """Load all historical measurements."""
    records = []
    if HISTORY_FILE.exists():
        with open(HISTORY_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return records


def append_record(record):
    """Append a measurement record to history."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(HISTORY_FILE, "a") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_plan():
    """Load the experiment plan."""
    if PLAN_FILE.exists():
        with open(PLAN_FILE, "r") as f:
            return json.load(f)
    return None


def save_plan(plan):
    """Save the experiment plan."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(PLAN_FILE, "w") as f:
        json.dump(plan, f, indent=2)


def measure_rtt():
    """Measure round-trip time to camera in ms."""
    try:
        start = time.monotonic()
        urllib.request.urlopen(CAMERA_PARAMS + "?action=list&group=Brand", timeout=10)
        elapsed = time.monotonic() - start
        return round(elapsed * 1000, 2)
    except Exception as e:
        log(f"RTT measurement failed: {e}")
        return None


def quick_jitter_check(duration_s=5):
    """Quick 5-second probe at smallest resolution to estimate network jitter.
    Returns jitter in ms, or None if the camera is unreachable.
    This is used as a pre-check before the full 35s probe."""
    try:
        url = (f"{CAMERA_STREAM}?resolution=160x90&compression=90&fps=30")
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "zanderhbf2-probe/1.0")
        resp = urllib.request.urlopen(req, timeout=15)
        boundary = resp.headers.get("Content-Type", "").split("boundary=")[-1].strip()
        if not boundary:
            return None

        intervals = []
        buf = b""
        last_frame_time = None
        start = time.monotonic()
        while time.monotonic() - start < duration_s:
            chunk = resp.read(4096)
            if not chunk:
                break
            buf += chunk
            while b"\xff\xd8" in buf and b"\xff\xd9" in buf:
                now = time.monotonic()
                if last_frame_time is not None:
                    intervals.append((now - last_frame_time) * 1000)
                last_frame_time = now
                idx = buf.index(b"\xff\xd9") + 2
                buf = buf[idx:]
        resp.close()

        if len(intervals) < 2:
            return None
        avg_interval = sum(intervals) / len(intervals)
        variance = sum((x - avg_interval) ** 2 for x in intervals) / len(intervals)
        jitter = math.sqrt(variance)
        return round(jitter, 1)
    except Exception as e:
        log(f"Quick jitter check failed: {e}")
        return None


def get_camera_temperature():
    """Try to read camera CPU temperature via VAPIX."""
    try:
        url = CAMERA_PARAMS + "?action=list&group=TemperatureControl"
        resp = urllib.request.urlopen(url, timeout=10).read().decode("utf-8", errors="replace")
        # The param CGI returns trigger thresholds but not actual temp.
        # Try the system health endpoint
        url2 = CAMERA_BASE + "/axis-cgi/operator/health.cgi"
        try:
            resp2 = urllib.request.urlopen(url2, timeout=5).read().decode("utf-8", errors="replace")
            return {"raw_health": resp2[:500]}
        except Exception:
            pass
        return {"temperature_params": resp[:500]}
    except Exception as e:
        return {"error": str(e)}


def get_camera_uptime():
    """Try to get camera uptime."""
    try:
        # The Axis param.cgi doesn't expose uptime directly on this firmware.
        # We can infer connection stability from our own measurements.
        return None
    except Exception:
        return None


def get_website_visitors():
    """
    Scrape the vnox.de visitor counter embedded on www.zander-info.de.
    Returns dict with total, today, yesterday, online_now.
    """
    import re
    try:
        url = "http://www.vnox.de/counter/counter.php?id=492&stats=j05"
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "zanderhbf2-probe/1.0")
        resp = urllib.request.urlopen(req, timeout=10).read().decode("utf-8", errors="replace")
        # The response is a document.write() with a table containing stats.
        # Extract all numbers from the left-aligned stats cells.
        nums = re.findall(r'align=left[^>]*><font[^>]*>(.*?)</font>', resp, re.S)
        values = []
        for block in nums:
            found = re.findall(r'\d+', block)
            values.extend(found)
        if len(values) >= 4:
            return {
                "total_visitors": int(values[0]),
                "visitors_today": int(values[1]),
                "visitors_yesterday": int(values[2]),
                "users_online": int(values[3]),
            }
        return {"raw_response": resp[:300], "values_found": values}
    except Exception as e:
        return {"error": str(e)}


# ─── CORE MJPEG PROBE ────────────────────────────────────────────────────────

def probe_mjpeg(resolution, compression, duration=CAPTURE_DURATION, extra_connections=0):
    """
    Stream MJPEG from camera, measure actual FPS, frame sizes, bandwidth.
    Returns a dict with all measurements.
    """
    url = (f"{CAMERA_STREAM}?resolution={resolution}&compression={compression}"
           f"&fps={REQUESTED_FPS}")

    frame_timestamps = []
    frame_sizes = []
    content_lengths = []  # from Content-Length headers
    total_bytes = 0
    first_frame_time = None
    last_frame_time = None
    errors = []
    boundary = None

    log(f"Probing: res={resolution} comp={compression} dur={duration}s extra_conn={extra_connections}")

    # Start extra connections if testing concurrent viewers
    extra_threads = []
    extra_results = []
    if extra_connections > 0:
        for i in range(extra_connections):
            t = threading.Thread(
                target=_dummy_stream,
                args=(resolution, compression, extra_results, i),
                daemon=True
            )
            t.start()
            extra_threads.append(t)
        time.sleep(1)  # let them establish

    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "zanderhbf2-probe/1.0")
        with urllib.request.urlopen(req, timeout=duration + 15) as resp:
            raw_headers = resp.headers
            content_type = raw_headers.get("Content-Type", "")
            # Extract boundary from multipart header
            if "boundary=" in content_type:
                boundary = content_type.split("boundary=")[1].strip()
            elif "myboundary" in str(resp.msg):
                boundary = "myboundary"

            buffer = b""
            start_time = time.monotonic()
            in_frame = False
            frame_start = None
            current_content_length = None

            while time.monotonic() - start_time < duration:
                try:
                    chunk = resp.read(8192)
                    if not chunk:
                        break
                    total_bytes += len(chunk)
                    buffer += chunk
                except Exception as e:
                    errors.append(f"read_error: {e}")
                    break

                # Parse JPEG frames from buffer using FFD8/FFD9 markers
                while True:
                    ffd8_pos = buffer.find(b"\xff\xd8")
                    ffd9_pos = buffer.find(b"\xff\xd9")

                    if ffd8_pos == -1:
                        # No start marker found, keep last few bytes in case marker is split
                        if len(buffer) > 4:
                            buffer = buffer[-4:]
                        break

                    if ffd9_pos == -1 or ffd9_pos <= ffd8_pos:
                        # No end marker or end before start - need more data
                        if len(buffer) > 65536:  # prevent unbounded growth
                            buffer = buffer[ffd8_pos:]
                        break

                    # Complete frame found
                    frame_size = ffd9_pos - ffd8_pos + 2
                    now = time.monotonic()

                    frame_timestamps.append(now)
                    frame_sizes.append(frame_size)

                    if first_frame_time is None:
                        first_frame_time = now
                    last_frame_time = now

                    # Advance buffer past this frame
                    buffer = buffer[ffd9_pos + 2:]

    except Exception as e:
        errors.append(f"stream_error: {e}")
    finally:
        # Wait for extra connections to finish
        for t in extra_threads:
            t.join(timeout=5)

    # Calculate metrics
    actual_duration = (last_frame_time - first_frame_time) if (first_frame_time and last_frame_time) else 0
    num_frames = len(frame_timestamps)
    measured_fps = (num_frames / actual_duration) if actual_duration > 0 else 0
    avg_frame_size = (sum(frame_sizes) / num_frames) if num_frames > 0 else 0
    bandwidth_bps = (total_bytes / actual_duration) if actual_duration > 0 else 0
    bandwidth_mbps = bandwidth_bps * 8 / 1e6

    # Inter-frame interval stats
    intervals = []
    for i in range(1, len(frame_timestamps)):
        intervals.append(frame_timestamps[i] - frame_timestamps[i - 1])
    avg_interval = (sum(intervals) / len(intervals)) if intervals else 0
    min_interval = min(intervals) if intervals else 0
    max_interval = max(intervals) if intervals else 0
    std_interval = (math.sqrt(sum((x - avg_interval) ** 2 for x in intervals) / len(intervals))) if intervals else 0
    # Jitter = standard deviation of inter-frame intervals
    jitter_ms = std_interval * 1000

    # Frame size stats
    min_frame_size = min(frame_sizes) if frame_sizes else 0
    max_frame_size = max(frame_sizes) if frame_sizes else 0
    std_frame_size = (math.sqrt(sum((x - avg_frame_size) ** 2 for x in frame_sizes) / len(frame_sizes))) if frame_sizes else 0

    return {
        "resolution": resolution,
        "compression": compression,
        "requested_fps": REQUESTED_FPS,
        "measured_fps": round(measured_fps, 4),
        "num_frames": num_frames,
        "duration_s": round(actual_duration, 2),
        "avg_frame_size_bytes": round(avg_frame_size, 1),
        "min_frame_size_bytes": min_frame_size,
        "max_frame_size_bytes": max_frame_size,
        "std_frame_size_bytes": round(std_frame_size, 1),
        "total_bytes": total_bytes,
        "bandwidth_mbps": round(bandwidth_mbps, 3),
        "avg_interval_ms": round(avg_interval * 1000, 2),
        "min_interval_ms": round(min_interval * 1000, 2),
        "max_interval_ms": round(max_interval * 1000, 2),
        "jitter_ms": round(jitter_ms, 2),
        "extra_connections": extra_connections,
        "extra_conn_fps": [r.get("fps", 0) for r in extra_results] if extra_results else [],
        "errors": errors,
        "boundary": boundary,
    }


def _dummy_stream(resolution, compression, results_out, idx):
    """Open a dummy MJPEG connection to simulate a concurrent viewer."""
    url = (f"{CAMERA_STREAM}?resolution={resolution}&compression={compression}"
           f"&fps={REQUESTED_FPS}")
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "zanderhbf2-probe-dummy/1.0")
        with urllib.request.urlopen(req, timeout=35) as resp:
            buffer = b""
            frame_count = 0
            start = time.monotonic()
            while time.monotonic() - start < 25:  # slightly less than main probe
                try:
                    chunk = resp.read(4096)
                    if not chunk:
                        break
                    buffer += chunk
                    while b"\xff\xd8" in buffer and b"\xff\xd9" in buffer:
                        ffd8 = buffer.index(b"\xff\xd8")
                        ffd9 = buffer.index(b"\xff\xd9", ffd8)
                        if ffd9 > ffd8:
                            frame_count += 1
                            buffer = buffer[ffd9 + 2:]
                        else:
                            break
                except Exception:
                    break
            elapsed = time.monotonic() - start
            results_out.append({"idx": idx, "frames": frame_count, "fps": round(frame_count / elapsed, 3) if elapsed > 0 else 0})
    except Exception as e:
        results_out.append({"idx": idx, "error": str(e)})


# ─── EXPERIMENTAL DESIGN ─────────────────────────────────────────────────────

def get_tested_combos(history):
    """Get set of (resolution, compression, extra_connections) already tested."""
    tested = set()
    for rec in history:
        key = (rec.get("resolution"), rec.get("compression"), rec.get("extra_connections", 0))
        tested.add(key)
    return tested


def design_next_test(history, plan):
    """
    Iteratively pick the next best test parameters.
    Strategy evolves based on how much data we have.
    """
    tested = get_tested_combos(history)
    n = len(history)

    now = datetime.datetime.now()
    hour = now.hour
    day_of_week = now.weekday()  # 0=Monday
    is_weekend = day_of_week >= 5

    # ── PHASE 1: Coarse grid (first ~50 runs) ──
    # Cover all resolutions × key compression levels
    key_compressions = list(range(0, 101, 25))  # 0, 25, 50, 75, 100

    if n < 50:
        # Prioritize untested combinations
        for res in ALL_RESOLUTIONS:
            for comp in key_compressions:
                if (res, comp, 0) not in tested:
                    return {"resolution": res, "compression": comp, "extra_connections": 0,
                            "phase": 1, "strategy": "coarse_grid_fill"}
        # If all combos tested, move on

    # ── PHASE 2: Untested resolution exploration + fine compression sweep (runs 50-120) ──
    if n < 120:
        # Build resolution stats
        res_fps_range = defaultdict(lambda: [100, 0])  # res -> [min_fps, max_fps]
        res_count = defaultdict(int)
        for rec in history:
            res = rec.get("resolution")
            fps = rec.get("measured_fps", 0)
            if res and rec.get("extra_connections", 0) == 0:
                res_fps_range[res][0] = min(res_fps_range[res][0], fps)
                res_fps_range[res][1] = max(res_fps_range[res][1], fps)
                res_count[res] += 1

        # PRIORITY 1: Explore UNTESTED resolutions first (critical for model coverage)
        # We need at least 1 baseline at every resolution before fine sweeps.
        # Start with lowest untested resolution (smaller = more likely to succeed on noisy network).
        for res in ALL_RESOLUTIONS:
            if res_count[res] == 0:
                return {"resolution": res, "compression": 50, "extra_connections": 0,
                        "phase": 2, "strategy": f"new_res_exploration_{res}"}

        # PRIORITY 2: Fill resolutions that have < 3 tests (need more baseline coverage)
        fine_compressions = list(range(0, 101, 5))
        MIN_BASELINE_PER_RES = 3
        for res in ALL_RESOLUTIONS:
            if 0 < res_count[res] < MIN_BASELINE_PER_RES:
                for comp in fine_compressions:
                    if (res, comp, 0) not in tested:
                        return {"resolution": res, "compression": comp, "extra_connections": 0,
                                "phase": 2, "strategy": f"baseline_fill_{res}"}

        # PRIORITY 3: Fine compression sweep at most interesting resolutions
        MAX_TOTAL_PER_RES_PHASE2 = 6
        MAX_FINE_PER_RES = 10
        candidates = sorted(res_fps_range.items(),
                            key=lambda x: x[1][1] - x[1][0], reverse=True)
        interesting = [(r, rng) for r, rng in candidates if res_count[r] < MAX_TOTAL_PER_RES_PHASE2][:5]
        interesting_res = [r for r, _ in interesting]

        for res in interesting_res:
            res_fine_count = sum(1 for r, c, e in tested
                                if r == res and e == 0 and c % 5 == 0)
            if res_fine_count >= MAX_FINE_PER_RES:
                continue
            for comp in fine_compressions:
                if (res, comp, 0) not in tested:
                    return {"resolution": res, "compression": comp, "extra_connections": 0,
                            "phase": 2, "strategy": f"fine_sweep_{res}"}

        # PRIORITY 4: Fill any remaining gaps at under-tested resolutions
        for res in ALL_RESOLUTIONS:
            if res_count[res] < MAX_TOTAL_PER_RES_PHASE2:
                for comp in fine_compressions:
                    if (res, comp, 0) not in tested:
                        return {"resolution": res, "compression": comp, "extra_connections": 0,
                                "phase": 2, "strategy": f"fill_sweep_{res}"}

    # ── PHASE 3: Concurrent connection tests (runs 120-160) ──
    if n < 160:
        # Pick 3 representative resolutions
        test_resolutions = ["640x480", "1280x720", "1920x1080"]
        for res in test_resolutions:
            for comp in [0, 50, 100]:
                for extra in [1, 2, 3, 5]:
                    if (res, comp, extra) not in tested:
                        return {"resolution": res, "compression": comp, "extra_connections": extra,
                                "phase": 3, "strategy": f"concurrency_extra{extra}"}

    # ── PHASE 4: Time-of-day variation (runs 160-300) ──
    if n < 300:
        # Re-test key combos at different hours
        # Track which hours we've tested which combos
        hour_combos = defaultdict(set)
        for rec in history:
            h = rec.get("timestamp", "").split(" ")[1].split(":")[0] if "timestamp" in rec else "?"
            key = (rec.get("resolution"), rec.get("compression"))
            hour_combos[h].add(key)

        key_res_comp = [
            ("640x480", 0), ("640x480", 50), ("640x480", 100),
            ("1280x720", 0), ("1280x720", 50), ("1280x720", 100),
            ("1920x1080", 0), ("1920x1080", 50), ("1920x1080", 100),
        ]
        current_hour = f"{hour:02d}"
        for res, comp in key_res_comp:
            if (res, comp) not in hour_combos.get(current_hour, set()):
                return {"resolution": res, "compression": comp, "extra_connections": 0,
                        "phase": 4, "strategy": f"timeofday_h{current_hour}"}

    # ── PHASE 5: Creative / exploratory tests (runs 300+) ──
    # Try unusual combinations, edge cases, etc.
    creative_tests = [
        # Smallest resolution at every compression
        *[{"resolution": "160x90", "compression": c, "extra_connections": 0,
           "phase": 5, "strategy": f"smallest_res_c{c}"} for c in range(0, 101, 10)
           if ("160x90", c, 0) not in tested],
        # Odd aspect ratios
        *[{"resolution": r, "compression": 50, "extra_connections": 0,
           "phase": 5, "strategy": "odd_aspect_ratio"}
          for r in ["1280x1024", "176x144", "800x450"]
          if (r, 50, 0) not in tested],
        # Re-test at different times with concurrent connections
        *[{"resolution": "640x480", "compression": c, "extra_connections": 2,
           "phase": 5, "strategy": f"retest_concurrent_c{c}"}
          for c in [0, 25, 50, 75, 100]
          if ("640x480", c, 2) not in tested],
        # Very high compression sweep at high res
        *[{"resolution": "1920x1080", "compression": c, "extra_connections": 0,
           "phase": 5, "strategy": f"highres_highcomp_c{c}"}
          for c in range(90, 101)
          if ("1920x1080", c, 0) not in tested],
    ]
    for test in creative_tests:
        return test  # return first untested creative test

    # ── PHASE 6: Repeat interesting measurements for statistical confidence ──
    # Find the measurement with highest variance and re-test it
    if history:
        # Group by (res, comp, extra_conn) and find which has highest FPS std
        grouped = defaultdict(list)
        for rec in history:
            key = (rec.get("resolution"), rec.get("compression"), rec.get("extra_connections", 0))
            grouped[key].append(rec.get("measured_fps", 0))

        most_variable = None
        highest_std = 0
        for key, fps_list in grouped.items():
            if len(fps_list) >= 2:
                mean = sum(fps_list) / len(fps_list)
                std = math.sqrt(sum((x - mean) ** 2 for x in fps_list) / len(fps_list))
                if std > highest_std:
                    highest_std = std
                    most_variable = key

        if most_variable:
            return {"resolution": most_variable[0], "compression": most_variable[1],
                    "extra_connections": most_variable[2],
                    "phase": 6, "strategy": "variance_repeat"}

    # Fallback: random untested combo
    for res in random.sample(ALL_RESOLUTIONS, min(5, len(ALL_RESOLUTIONS))):
        for comp in random.sample(range(0, 101, 5), min(5, 21)):
            if (res, comp, 0) not in tested:
                return {"resolution": res, "compression": comp, "extra_connections": 0,
                        "phase": 0, "strategy": "random_exploration"}

    # Everything tested — retest a random one
    res = random.choice(ALL_RESOLUTIONS)
    comp = random.randint(0, 100)
    return {"resolution": res, "compression": comp, "extra_connections": 0,
            "phase": 6, "strategy": "random_repeat"}


# ─── MODEL BUILDING ──────────────────────────────────────────────────────────

def predict_fps(coeffs, resolution, compression, rtt_ms, jitter_ms, hour):
    """
    Predict FPS using regression coefficients.
    Works with any model variant by checking which coefficient keys exist.
    """
    mp = RES_MP.get(resolution, 0.3)
    rtt = rtt_ms / 1000.0
    jitter = jitter_ms / 1000.0
    sin_hour = math.sin(2 * math.pi * hour / 24)
    fps = coeffs.get("intercept", 15)
    fps += coeffs.get("megapixels", 0) * mp
    fps += coeffs.get("compression", 0) * compression
    if "comp_squared" in coeffs:
        fps += coeffs["comp_squared"] * (compression * compression / 100)
    if "mp_x_compression" in coeffs:
        fps += coeffs["mp_x_compression"] * mp * compression
    if "rtt" in coeffs:
        fps += coeffs["rtt"] * rtt
    if "jitter" in coeffs:
        fps += coeffs["jitter"] * jitter
    if "sin_hour" in coeffs:
        fps += coeffs["sin_hour"] * sin_hour
    if "cos_hour" in coeffs:
        fps += coeffs["cos_hour"] * math.cos(2 * math.pi * hour / 24)
    if "log_jitter" in coeffs:
        fps += coeffs["log_jitter"] * math.log(1 + jitter * 10)
    if "mp_x_jitter" in coeffs:
        fps += coeffs["mp_x_jitter"] * mp * jitter
    return max(0, round(fps, 2))


def predict_fps_planning(coeffs, resolution, compression, hour):
    """
    Predict FPS using only knowable-ahead-of-time features:
    resolution, compression, and hour. No jitter/RTT needed.
    """
    mp = RES_MP.get(resolution, 0.3)
    sin_hour = math.sin(2 * math.pi * hour / 24)
    cos_hour = math.cos(2 * math.pi * hour / 24)
    fps = coeffs.get("intercept", 15)
    fps += coeffs.get("megapixels", 0) * mp
    fps += coeffs.get("compression", 0) * compression
    if "comp_squared" in coeffs:
        fps += coeffs["comp_squared"] * (compression * compression / 100)
    if "mp_x_compression" in coeffs:
        fps += coeffs["mp_x_compression"] * mp * compression
    if "sin_hour" in coeffs:
        fps += coeffs["sin_hour"] * sin_hour
    if "cos_hour" in coeffs:
        fps += coeffs["cos_hour"] * cos_hour
    return max(0, round(fps, 2))


def generate_planning_table(coeffs, label=""):
    """
    Hour-by-hour FPS table for practical streaming configs.
    Only uses resolution + compression + hour (no jitter/RTT).
    """
    configs = [
        ("160x90", 75, "160x90 c75"),
        ("320x240", 50, "320x240 c50"),
        ("640x480", 50, "640x480 c50"),
        ("1280x720", 75, "1280x720 c75"),
    ]
    table = []
    for res, comp, name in configs:
        row = {"config": name, "resolution": res, "compression": comp}
        # Key hours: 2 (night), 6 (dawn), 10 (morning peak), 14 (afternoon), 18 (evening), 22 (late night)
        for h in [2, 6, 10, 14, 18, 22]:
            pred = predict_fps_planning(coeffs, res, comp, h)
            row[f"{h:02d}utc"] = pred
        table.append(row)
    return {"planning_table": table, "model_label": label}


def build_model_summary(history):
    """
    Build a summary of all data with regression analysis.
    Attempt to find FPS = f(resolution, compression, connections, time, ...)
    """
    if not history:
        return {"status": "no_data", "total_measurements": 0}

    # Basic stats
    total = len(history)
    by_resolution = defaultdict(list)
    by_compression = defaultdict(list)
    by_phase = defaultdict(list)
    by_hour = defaultdict(list)
    by_extra_conn = defaultdict(list)
    by_visitors_online = defaultdict(list)

    for rec in history:
        res = rec.get("resolution", "unknown")
        comp = rec.get("compression", -1)
        fps = rec.get("measured_fps", 0)
        phase = rec.get("phase", -1)
        extra = rec.get("extra_connections", 0)
        ts = rec.get("timestamp", "")

        by_resolution[res].append(fps)
        by_compression[comp].append(fps)
        by_phase[phase].append(fps)
        by_extra_conn[extra].append(fps)

        # Website visitors online (from vnox.de counter)
        visitors = rec.get("website_visitors", {})
        if isinstance(visitors, dict) and "users_online" in visitors:
            online = visitors["users_online"]
            if isinstance(online, int):
                # Bucket into ranges for grouping
                bucket = online
                by_visitors_online[bucket].append(fps)

        if ts:
            try:
                hour = int(ts.split(" ")[1].split(":")[0])
                by_hour[hour].append(fps)
            except (IndexError, ValueError):
                pass

    def stats_list(lst):
        if not lst:
            return {"count": 0}
        s = sorted(lst)
        n = len(s)
        mean = sum(s) / n
        std = math.sqrt(sum((x - mean) ** 2 for x in s) / n) if n > 1 else 0
        return {
            "count": n,
            "mean": round(mean, 4),
            "std": round(std, 4),
            "min": round(min(s), 4),
            "max": round(max(s), 4),
            "median": round(s[n // 2], 4),
        }

    summary = {
        "status": "ok",
        "total_measurements": total,
        "last_updated": datetime.datetime.now().isoformat(),
        "global_fps": stats_list([r.get("measured_fps", 0) for r in history]),
        "by_resolution": {k: stats_list(v) for k, v in sorted(by_resolution.items())},
        "by_compression": {str(k): stats_list(v) for k, v in sorted(by_compression.items())},
        "by_hour": {str(k): stats_list(v) for k, v in sorted(by_hour.items())},
        "by_extra_connections": {str(k): stats_list(v) for k, v in sorted(by_extra_conn.items())},
        "by_visitors_online": {str(k): stats_list(v) for k, v in sorted(by_visitors_online.items())},
        "by_phase": {str(k): stats_list(v) for k, v in sorted(by_phase.items())},
    }

    # ── Simple linear regression: FPS vs pixel count ──
    # For each compression level, try FPS = a * megapixels + b
    regression_by_comp = {}
    for comp, recs in by_compression.items():
        if comp < 0:
            continue
        # Get (mp, fps) pairs for this compression
        pairs = []
        for rec in history:
            if rec.get("compression") == comp and rec.get("extra_connections", 0) == 0:
                res = rec.get("resolution")
                if res in RES_MP:
                    pairs.append((RES_MP[res], rec.get("measured_fps", 0)))

        if len(pairs) >= 3:
            regression = simple_linear_regression(pairs)
            if regression:
                regression_by_comp[str(comp)] = regression

    summary["regression_fps_vs_megapixels"] = regression_by_comp

    # ── Multi-factor: FPS vs (mp, compression, connections, users_online, rtt, jitter, hour) ──
    # Build two datasets: all data, and clean data (non-noisy)
    multi_data = []
    multi_data_clean = []
    for rec in history:
        res = rec.get("resolution")
        if res in RES_MP and rec.get("compression") is not None:
            visitors = rec.get("website_visitors", {})
            users_online = visitors.get("users_online", 0)
            if not isinstance(users_online, int):
                users_online = 0
            
            entry = {
                "mp": RES_MP[res],
                "comp": rec["compression"],
                "conn": rec.get("extra_connections", 0),
                "online": users_online,
                "rtt": (rec.get("network_rtt_ms", 0) or 0) / 1000.0,  # convert to seconds
                "jitter": (rec.get("jitter_ms", 0) or 0) / 1000.0,  # convert to seconds
                "hour": rec.get("utc_hour", rec.get("local_hour", 12)),
                "fps": rec.get("measured_fps", 0),
            }
            multi_data.append(entry)
            
            # Clean dataset: exclude noisy/invalid measurements
            quality = rec.get("quality_flags", {})
            jitter_raw = rec.get("jitter_ms", 0) or 0
            # For noisy: use quality_flags if available, else infer from jitter
            is_noisy_record = quality.get("is_noisy", jitter_raw > HIGH_JITTER_THRESHOLD_MS)
            # For valid: use quality_flags if available, else check frame count directly
            if quality:
                is_invalid_record = not quality.get("is_valid", True)
            else:
                is_invalid_record = (rec.get("num_frames", 999) or 999) < MIN_FRAMES_FOR_VALID
            if not is_noisy_record and not is_invalid_record:
                multi_data_clean.append(entry)

    # Run regression on all data — this is the PRIMARY predictive model.
    # It includes noisy/congested measurements so it can predict real-world FPS
    # including daytime congestion. Clean model is kept as a "best case" reference.
    if len(multi_data) >= 8:
        summary["multifactor_regression"] = multi_linear_regression(multi_data, label="all_data")
    # Generate planning table (no jitter/RTT needed — usable for advance planning)
    planning_model = summary.get("planning_model")
    if planning_model and planning_model.get("status") == "ok" and planning_model.get("coefficients"):
        summary["planning_table"] = generate_planning_table(
            planning_model["coefficients"],
            label=planning_model.get("label", "")
        )
    # Run regression on clean data — "best case" reference model
    if len(multi_data_clean) >= 6:
        summary["multifactor_regression_clean"] = multi_linear_regression(multi_data_clean, label="clean_data")
    # Run PLANNING model on clean data — no jitter/RTT features
    # Only uses resolution, compression, time-of-day (knowable ahead of time)
    if len(multi_data_clean) >= 7:
        summary["planning_model"] = multi_linear_regression(multi_data_clean, label="planning", force_variant="planning_tod")

    # ── Bottleneck analysis ──
    # Identify whether encoder or bandwidth is the bottleneck per measurement
    bottleneck_analysis = analyze_bottlenecks(history)
    summary["bottleneck_analysis"] = bottleneck_analysis

    # ── Quality metrics ──
    # Track how many measurements are noisy/invalid
    # For old records without quality_flags, infer from jitter directly
    noisy_count = 0
    invalid_count = 0
    noisy_indices = []
    for idx, r in enumerate(history):
        qf = r.get("quality_flags", {})
        if qf:
            if qf.get("is_noisy", False):
                noisy_count += 1
                noisy_indices.append(idx + 1)
            if not qf.get("is_valid", True):
                invalid_count += 1
        else:
            # Infer from raw jitter for records before quality_flags existed
            jitter_val = r.get("jitter_ms", 0) or 0
            if jitter_val > HIGH_JITTER_THRESHOLD_MS:
                noisy_count += 1
                noisy_indices.append(idx + 1)
    summary["quality_metrics"] = {
        "total_measurements": total,
        "noisy_count": noisy_count,
        "invalid_count": invalid_count,
        "clean_count": total - noisy_count,
        "noise_rate": round(noisy_count / total, 3) if total > 0 else 0,
        "noisy_run_numbers": noisy_indices,
    }
    
    # ── RTT and jitter stats ──
    rtts = [r.get("network_rtt_ms", 0) or 0 for r in history]
    jitters = [r.get("jitter_ms", 0) or 0 for r in history]
    summary["network_stats"] = {
        "rtt_ms": stats_list(rtts),
        "jitter_ms": stats_list(jitters),
    }
    
    # ── Key findings ──
    summary["key_findings"] = generate_findings(summary)

    # ── Diurnal FPS profile ──
    # Hour-by-hour expected FPS at a common streaming config (640x480 c50)
    # using the all-data model if available
    all_model = summary.get("multifactor_regression", {})
    if all_model.get("status") == "ok" and all_model.get("coefficients"):
        coeffs = all_model["coefficients"]
        diurnal = []
        for h in range(24):
            # Use median RTT and jitter for this hour from actual data
            hour_fps = [r.get("measured_fps", 0) for r in history if r.get("utc_hour") == h]
            hour_rtt = [r.get("network_rtt_ms", 0) or 0 for r in history if r.get("utc_hour") == h]
            hour_jitter = [r.get("jitter_ms", 0) or 0 for r in history if r.get("utc_hour") == h]
            median_rtt = sorted(hour_rtt)[len(hour_rtt) // 2] if hour_rtt else 650
            median_jitter = sorted(hour_jitter)[len(hour_jitter) // 2] if hour_jitter else 50
            # Predict for 640x480 c50 (common streaming config)
            pred = predict_fps(coeffs, "640x480", 50, median_rtt, median_jitter, h)
            diurnal.append({
                "utc_hour": h,
                "median_rtt_ms": round(median_rtt, 1),
                "median_jitter_ms": round(median_jitter, 1),
                "n_measurements": len(hour_fps),
                "predicted_fps_640x480_c50": pred,
                "actual_mean_fps": round(sum(hour_fps) / len(hour_fps), 2) if hour_fps else None,
            })
        summary["diurnal_profile"] = diurnal

    return summary


def simple_linear_regression(pairs):
    """Y = a*X + b. Returns {slope, intercept, r_squared}."""
    n = len(pairs)
    sum_x = sum(p[0] for p in pairs)
    sum_y = sum(p[1] for p in pairs)
    sum_xy = sum(p[0] * p[1] for p in pairs)
    sum_x2 = sum(p[0] ** 2 for p in pairs)

    denom = n * sum_x2 - sum_x ** 2
    if abs(denom) < 1e-10:
        return None

    slope = (n * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - slope * sum_x) / n

    # R-squared
    mean_y = sum_y / n
    ss_tot = sum((p[1] - mean_y) ** 2 for p in pairs)
    ss_res = sum((p[1] - (slope * p[0] + intercept)) ** 2 for p in pairs)
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0

    return {
        "slope": round(slope, 6),
        "intercept": round(intercept, 4),
        "r_squared": round(r_squared, 6),
        "n": n,
        "equation": f"FPS = {slope:.4f} * MP + {intercept:.2f} (R²={r_squared:.4f})",
    }


def multi_linear_regression(data, label="", force_variant=None):
    """
    Multiple regression with several feature sets.
    force_variant: if set, return only that variant (e.g. "planning_tod") instead of picking best R².
    """
    n = len(data)
    if n < 6:
        return {"status": "insufficient_data", "label": label}

    results = {}
    fail_info = {}
    
    # Try full 9-feature model first (needs n >= 10 for stability)
    if n >= 10:
        full_result = _fit_regression(data, features="full")
        if full_result.get("status") == "ok":
            full_result["label"] = label + "_9feat"
            results["full_9feat"] = full_result
        else:
            fail_info["full_9feat"] = full_result.get("status", "unknown")
    else:
        fail_info["full_9feat"] = f"need n>=10, have n={n}"
    
    # Try reduced 6-feature model (always if n >= k=6)
    if n >= 6:
        reduced_result = _fit_regression(data, features="reduced")
        if reduced_result.get("status") == "ok":
            reduced_result["label"] = label + "_6feat"
            results["reduced_6feat"] = reduced_result
        else:
            fail_info["reduced_6feat"] = reduced_result.get("status", "unknown")
    else:
        fail_info["reduced_6feat"] = f"need n>=6, have n={n}"
    
    # Try minimal 4-feature model (mp, comp, rtt, jitter)
    minimal_result = _fit_regression(data, features="minimal")
    if minimal_result.get("status") == "ok":
        minimal_result["label"] = label + "_4feat"
        results["minimal_4feat"] = minimal_result
    else:
        fail_info["minimal_4feat"] = minimal_result.get("status", "unknown")

    # Try minimal 5-feature model (mp, comp, mp*comp, rtt, jitter)
    # Key interaction: compression alleviates the resolution penalty
    minimal5_result = _fit_regression(data, features="minimal5")
    if minimal5_result.get("status") == "ok":
        minimal5_result["label"] = label + "_5feat"
        results["minimal_5feat"] = minimal5_result
    else:
        fail_info["minimal_5feat"] = minimal5_result.get("status", "unknown")

    # Try 7-feature model with quadratic compression (captures non-linear comp response)
    if n >= 7:
        quad7_result = _fit_regression(data, features="quad7")
        if quad7_result.get("status") == "ok":
            quad7_result["label"] = label + "_7feat_quad"
            results["quad_7feat"] = quad7_result
        else:
            fail_info["quad_7feat"] = quad7_result.get("status", "unknown")

    # Try 9-feature model with time-of-day (sin+cos hour) — the PREDICTIVE model
    # This tells users what FPS to expect at any given hour, including daytime
    if n >= 9:
        tod_result = _fit_regression(data, features="tod_quad9")
        if tod_result.get("status") == "ok":
            tod_result["label"] = label + "_tod_quad9"
            results["tod_quad9"] = tod_result
        else:
            fail_info["tod_quad9"] = tod_result.get("status", "unknown")

    # Try 11-feature model with log-jitter and mp×jitter interaction
    # Captures the non-linear (threshold-like) effect of jitter on FPS,
    # and the fact that high-res streams suffer more from jitter than low-res ones.
    if n >= 11:
        tod11_result = _fit_regression(data, features="tod_quad11")
        if tod11_result.get("status") == "ok":
            tod11_result["label"] = label + "_tod_quad11"
            results["tod_quad11"] = tod11_result
        else:
            fail_info["tod_quad11"] = tod11_result.get("status", "unknown")

    # Try 7-feature PLANNING model: resolution + compression + time-of-day only.
    # No jitter/RTT — these can't be known ahead of time.
    # Usable for "what fps at 10pm tonight?" without measuring network first.
    if n >= 7:
        plan_result = _fit_regression(data, features="planning_tod")
        if plan_result.get("status") == "ok":
            plan_result["label"] = label + "_planning_tod"
            results["planning_tod"] = plan_result
        else:
            fail_info["planning_tod"] = plan_result.get("status", "unknown")

    if not results:
        return {"status": "all_models_failed", "n": n, "label": label, "fail_reasons": fail_info}

    # If force_variant is set, return only that variant
    if force_variant:
        if force_variant in results:
            forced = results[force_variant]
            forced["best_model"] = force_variant
            forced["label"] = label
            forced["n"] = n
            # Include all model comparison
            all_models = {}
            for k, v in results.items():
                all_models[k] = {"r_squared": v.get("r_squared", 0), "status": "ok"}
            for k, reason in fail_info.items():
                all_models[k] = {"status": reason}
            forced["all_models"] = all_models
            return forced
        else:
            return {"status": "forced_variant_failed", "variant": force_variant, "reason": fail_info.get(force_variant, "not attempted"), "label": label}

    # Pick best model by R²
    best_key = max(results, key=lambda k: results[k].get("r_squared", -999))
    best = results[best_key]
    best["best_model"] = best_key
    best["label"] = label
    best["n"] = n
    # Keep all models for comparison, include failures with reasons
    all_models = {}
    for k, v in results.items():
        all_models[k] = {"r_squared": v.get("r_squared", 0), "equation": v.get("equation", ""), "status": "ok"}
    for k, reason in fail_info.items():
        all_models[k] = {"status": reason}
    best["all_models"] = all_models
    return best


def _fit_regression(data, features="reduced"):
    """
    Fit a specific regression model configuration.
    feature sets:
      - "full": [1, mp, comp, conn, mp*comp, online, rtt, jitter, sin(hour)]
      - "reduced": [1, mp, comp, conn, mp*comp, online]
      - "minimal": [1, mp, comp, rtt, jitter]
    """
    n = len(data)
    X = []
    Y = []
    feature_names = []
    
    for d in data:
        mp = d["mp"]
        comp = d["comp"]
        conn = d["conn"]
        online = d.get("online", 0)
        rtt = d.get("rtt", 0)
        jitter = d.get("jitter", 0)
        hour = d.get("hour", 12)
        # Sinusoidal hour feature to capture time-of-day periodicity
        sin_hour = math.sin(2 * math.pi * hour / 24)
        
        if features == "full":
            row = [1, mp, comp, conn, mp * comp, online, rtt, jitter, sin_hour]
            feature_names = ["intercept", "megapixels", "compression", "connections",
                            "mp_x_compression", "users_online", "rtt_s", "jitter_s", "sin_hour"]
        elif features == "minimal":
            row = [1, mp, comp, rtt, jitter]
            feature_names = ["intercept", "megapixels", "compression", "rtt_s", "jitter_s"]
        elif features == "minimal5":
            row = [1, mp, comp, mp * comp, rtt, jitter]
            feature_names = ["intercept", "megapixels", "compression", "mp_x_compression", "rtt_s", "jitter_s"]
        elif features == "quad7":
            # 7-feat: adds compression² to capture non-linear compression response
            row = [1, mp, comp, comp * comp / 100, mp * comp, rtt, jitter]
            feature_names = ["intercept", "megapixels", "compression", "comp_squared", "mp_x_compression", "rtt_s", "jitter_s"]
        elif features == "tod_quad9":
            # 9-feat: quad7 + sin(hour) + cos(hour) for time-of-day diurnal pattern
            # This is the PREDICTIVE model — tells you what FPS to expect at any hour
            cos_hour = math.cos(2 * math.pi * hour / 24)
            row = [1, mp, comp, comp * comp / 100, mp * comp, rtt, jitter, sin_hour, cos_hour]
            feature_names = ["intercept", "megapixels", "compression", "comp_squared", "mp_x_compression", "rtt_s", "jitter_s", "sin_hour", "cos_hour"]
        elif features == "tod_quad11":
            # 11-feat: tod_quad9 + log(1+jitter*10) + mp*jitter
            # log_jitter captures the non-linear threshold effect of jitter on FPS
            # (high jitter doesn't just linearly reduce FPS — it crushes it)
            # mp_x_jitter captures that high-res + high-jitter is worse than additive
            cos_hour = math.cos(2 * math.pi * hour / 24)
            log_jitter = math.log(1 + jitter * 10)
            mp_jitter = mp * jitter
            row = [1, mp, comp, comp * comp / 100, mp * comp, rtt, jitter, sin_hour, cos_hour, log_jitter, mp_jitter]
            feature_names = ["intercept", "megapixels", "compression", "comp_squared", "mp_x_compression", "rtt_s", "jitter_s", "sin_hour", "cos_hour", "log_jitter", "mp_x_jitter"]
        elif features == "planning_tod":
            # 7-feat PLANNING model: no jitter/RTT — only knowable-ahead-of-time features
            # resolution (mp), compression, comp^2, mp*comp, sin(hour), cos(hour)
            cos_hour = math.cos(2 * math.pi * hour / 24)
            row = [1, mp, comp, comp * comp / 100, mp * comp, sin_hour, cos_hour]
            feature_names = ["intercept", "megapixels", "compression", "comp_squared", "mp_x_compression", "sin_hour", "cos_hour"]
        else:  # reduced
            row = [1, mp, comp, conn, mp * comp, online]
            feature_names = ["intercept", "megapixels", "compression", "connections",
                            "mp_x_compression", "users_online"]
        
        X.append(row)
        Y.append(d["fps"])

    k = len(X[0])
    if n < k:
        return {"status": f"underdetermined (n={n} < k={k})"}

    # Normal equation: beta = (X^T X)^-1 X^T Y
    XtX = [[0.0] * k for _ in range(k)]
    XtY = [0.0] * k

    for i in range(n):
        for j in range(k):
            XtY[j] += X[i][j] * Y[i]
            for l in range(k):
                XtX[j][l] += X[i][j] * X[i][l]

    # Invert XtX using Gauss-Jordan elimination
    try:
        inv = matrix_invert(XtX)
    except Exception:
        return {"status": "singular_matrix", "n": n, "k": k}

    # beta = inv * XtY
    beta = [0.0] * k
    for i in range(k):
        for j in range(k):
            beta[i] += inv[i][j] * XtY[j]

    # Calculate R-squared
    y_mean = sum(Y) / n
    ss_tot = sum((y - y_mean) ** 2 for y in Y)
    ss_res = 0
    for i in range(n):
        y_pred = sum(X[i][j] * beta[j] for j in range(k))
        ss_res += (Y[i] - y_pred) ** 2
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0

    # Predicted vs actual error stats
    errors = []
    for i in range(n):
        y_pred = sum(X[i][j] * beta[j] for j in range(k))
        errors.append(abs(Y[i] - y_pred))
    mae = sum(errors) / n
    rmse = math.sqrt(sum(e ** 2 for e in errors) / n)

    # Build coefficient dict
    coefficients = {name: round(beta[i], 6) for i, name in enumerate(feature_names)}
    
    # Build equation string
    terms = []
    for i, name in enumerate(feature_names):
        if name == "intercept":
            terms.append(f"{beta[i]:.3f}")
        else:
            short_name = name.replace("_s", "").replace("mp_x_compression", "MP*C")
            terms.append(f"{beta[i]:.4f}*{short_name}")
    equation = "FPS = " + " + ".join(terms) + f" (R²={r_squared:.4f}, RMSE={rmse:.3f})"

    return {
        "status": "ok",
        "n": n,
        "k": k,
        "coefficients": coefficients,
        "r_squared": round(r_squared, 6),
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "equation": equation,
    }


def matrix_invert(matrix):
    """Invert a square matrix using Gauss-Jordan elimination."""
    n = len(matrix)
    # Create augmented matrix [M | I]
    aug = [row[:] + [1.0 if i == j else 0.0 for j in range(n)] for i, row in enumerate(matrix)]

    for col in range(n):
        # Find pivot
        max_val = abs(aug[col][col])
        max_row = col
        for row in range(col + 1, n):
            if abs(aug[row][col]) > max_val:
                max_val = abs(aug[row][col])
                max_row = row

        if max_val < 1e-12:
            raise ValueError("Matrix is singular or nearly singular")

        # Swap rows
        aug[col], aug[max_row] = aug[max_row], aug[col]

        # Scale pivot row
        pivot = aug[col][col]
        for j in range(2 * n):
            aug[col][j] /= pivot

        # Eliminate column
        for row in range(n):
            if row != col:
                factor = aug[row][col]
                for j in range(2 * n):
                    aug[row][j] -= factor * aug[col][j]

    # Extract inverse
    return [row[n:] for row in aug]


def analyze_bottlenecks(history):
    """
    Analyze whether each measurement was bottlenecked by:
    - Network noise (high jitter/RTT causing frame delivery issues)
    - Encoder (FPS limited by JPEG encoding speed) — low compression + low FPS relative to expectation
    - Bandwidth (FPS limited by network) — high frame size * high FPS exceeds capacity
    - Sensor (FPS limited by capture hardware) — very high compression still low FPS
    - Normal (measurement matches expected range from the clean regression model)
    
    Uses the clean-data regression model to compute expected FPS and classify
    based on residuals. This replaces the old fixed-threshold approach that
    left most records as "unknown/mixed".
    """
    analysis = {
        "encoder_bottleneck_count": 0,
        "bandwidth_bottleneck_count": 0,
        "sensor_bottleneck_count": 0,
        "network_noise_count": 0,
        "unknown_count": 0,
    }

    # Build a quick expected-FPS lookup from clean data grouped by compression
    # For each compression level, compute the median FPS of clean measurements
    clean_by_comp = defaultdict(list)
    for rec in history:
        jitter = rec.get("jitter_ms", 0)
        if jitter <= 200:
            clean_by_comp[rec.get("compression", 50)].append(rec.get("measured_fps", 0))
    
    # Expected FPS per compression (median of clean data)
    expected_fps_by_comp = {}
    for comp, fps_list in clean_by_comp.items():
        if fps_list:
            expected_fps_by_comp[comp] = sorted(fps_list)[len(fps_list) // 2]

    for rec in history:
        comp = rec.get("compression", 50)
        fps = rec.get("measured_fps", 0)
        avg_size = rec.get("avg_frame_size_bytes", 0)
        res = rec.get("resolution", "")
        mp = RES_MP.get(res, 1)
        jitter = rec.get("jitter_ms", 0)
        rtt = rec.get("network_rtt_ms", 0) or 0

        # First check for network noise: high jitter or very high RTT
        if jitter > 200 or rtt > 1500:
            rec["_bottleneck"] = "network_noise"
            analysis["network_noise_count"] += 1
            continue

        effective_bandwidth_demand = avg_size * fps * 8 / 1e6  # Mbps

        # Get expected FPS for this compression level
        expected = expected_fps_by_comp.get(comp, 15)  # default 15 if no clean data
        fps_ratio = fps / expected if expected > 0 else 1.0

        # Sensor bottleneck: very high compression but still very low FPS
        if comp >= 80 and fps < 2.0:
            rec["_bottleneck"] = "sensor"
            analysis["sensor_bottleneck_count"] += 1
        # Encoder bottleneck: low compression, low FPS relative to what this compression should achieve
        elif comp <= 30 and fps_ratio < 0.3 and fps < 8.0:
            rec["_bottleneck"] = "encoder"
            analysis["encoder_bottleneck_count"] += 1
        # Bandwidth bottleneck: high bandwidth demand with suppressed FPS
        elif effective_bandwidth_demand > 3.0 and fps_ratio < 0.5:
            rec["_bottleneck"] = "bandwidth"
            analysis["bandwidth_bottleneck_count"] += 1
        # Network noise (moderate): jitter between 100-200 with degraded FPS
        elif jitter > 100 and fps_ratio < 0.6:
            rec["_bottleneck"] = "network_noise"
            analysis["network_noise_count"] += 1
        # Normal: FPS is within reasonable range of expectation
        else:
            rec["_bottleneck"] = "normal"
            analysis["unknown_count"] += 1

    return analysis


def generate_findings(summary):
    """Generate human-readable key findings from the data."""
    findings = []
    total = summary.get("total_measurements", 0)

    if total < 5:
        findings.append(f"Only {total} measurements so far. Need more data for reliable findings.")
        return findings

    # Highest FPS achieved
    by_res = summary.get("by_resolution", {})
    best_res = max(by_res.items(), key=lambda x: x[1].get("max", 0))
    findings.append(
        f"Best FPS: {best_res[1]['max']} at resolution {best_res[0]} "
        f"(mean: {best_res[1]['mean']}, {best_res[1]['count']} tests)"
    )

    # Worst resolution
    worst_res = min(by_res.items(), key=lambda x: x[1].get("mean", 999))
    findings.append(
        f"Worst mean FPS: {worst_res[1]['mean']} at resolution {worst_res[0]}"
    )

    # Compression effect
    by_comp = summary.get("by_compression", {})
    if len(by_comp) >= 3:
        low_comp = by_comp.get("0", {}).get("mean", 0)
        high_comp = by_comp.get("100", {}).get("mean", 0)
        mid_comp = by_comp.get("50", {}).get("mean", 0)
        findings.append(
            f"Compression effect: c0={low_comp}fps, c50={mid_comp}fps, c100={high_comp}fps"
        )

    # Multi-factor model
    multi = summary.get("multifactor_regression", {})
    if multi.get("status") == "ok":
        findings.append(f"Model: {multi.get('equation', 'N/A')}")
        r2 = multi.get("r_squared", 0)
        best_model = multi.get("best_model", "unknown")
        findings.append(f"Best model variant: {best_model}")
        if r2 > 0.8:
            findings.append(f"Strong model fit (R²={r2}). Variables explain most FPS variance.")
        elif r2 > 0.5:
            findings.append(f"Moderate model fit (R²={r2}). Missing factors (time-of-day, network load?).")
        else:
            findings.append(f"Weak model fit (R²={r2}). FPS depends on unmeasured factors (network noise dominant?).")
    # Check clean model
    multi_clean = summary.get("multifactor_regression_clean", {})
    if multi_clean.get("status") == "ok":
        r2_clean = multi_clean.get("r_squared", 0)
        if r2_clean > r2 + 0.1:
            findings.append(f"Clean model (excluding noisy measurements) fits much better: R²={r2_clean} vs {r2}")

    # Planning model (no jitter/RTT — usable for advance scheduling)
    plan_model = summary.get("planning_model")
    if plan_model and plan_model.get("status") == "ok":
        plan_r2 = plan_model.get("r_squared", 0)
        plan_label = plan_model.get("best_model", "unknown")
        findings.append(f"Planning model (no jitter/RTT): R²={plan_r2} — predict fps from resolution+compression+hour only")
        # Show a quick example: 640x480 c50 at night vs afternoon
        plan_coeffs = plan_model.get("coefficients", {})
        night_fps = predict_fps_planning(plan_coeffs, "640x480", 50, 2)
        afternoon_fps = predict_fps_planning(plan_coeffs, "640x480", 50, 14)
        findings.append(f"  640x480 c50 example: 02:00 UTC ≈ {night_fps} fps, 14:00 UTC ≈ {afternoon_fps} fps")

    # Concurrent connection effect
    by_conn = summary.get("by_extra_connections", {})
    if "0" in by_conn and "3" in by_conn:
        base = by_conn["0"].get("mean", 0)
        with3 = by_conn["3"].get("mean", 0)
        if base > 0:
            drop_pct = round((1 - with3 / base) * 100, 1)
            findings.append(f"3 concurrent connections cause ~{drop_pct}% FPS drop (baseline: {base}, with 3: {with3})")

    # Bottleneck analysis
    ba = summary.get("bottleneck_analysis", {})
    if ba:
        findings.append(
            f"Bottleneck distribution: encoder={ba.get('encoder_bottleneck_count', 0)}, "
            f"bandwidth={ba.get('bandwidth_bottleneck_count', 0)}, "
            f"sensor={ba.get('sensor_bottleneck_count', 0)}, "
            f"network_noise={ba.get('network_noise_count', 0)}, "
            f"normal={ba.get('unknown_count', 0)}"
        )

    # Time-of-day effect
    by_hour = summary.get("by_hour", {})
    if len(by_hour) >= 3:
        best_hour = max(by_hour.items(), key=lambda x: x[1].get("mean", 0))
        worst_hour = min(by_hour.items(), key=lambda x: x[1].get("mean", 999))
        findings.append(
            f"Time-of-day: best hour {best_hour[0]}:00 (mean {best_hour[1]['mean']}fps), "
            f"worst hour {worst_hour[0]}:00 (mean {worst_hour[1]['mean']}fps)"
        )
    
    # Quality/noise findings
    qm = summary.get("quality_metrics", {})
    if qm.get("noise_rate", 0) > 0.2:
        findings.append(
            f"High noise rate: {qm['noise_rate']*100:.0f}% of measurements are noisy (jitter > {HIGH_JITTER_THRESHOLD_MS}ms). "
            f"The all-data model uses jitter + time-of-day as features so it predicts FPS "
            f"under ALL network conditions. The clean model (R²={summary.get('multifactor_regression_clean', {}).get('r_squared', 0):.3f}) "
            f"is more precise for good-network predictions."
        )
    
    # Network stats findings
    ns = summary.get("network_stats", {})
    rtt_stats = ns.get("rtt_ms", {})
    if rtt_stats.get("count", 0) >= 3:
        findings.append(
            f"Network RTT: mean={rtt_stats.get('mean', 0):.0f}ms, "
            f"range=[{rtt_stats.get('min', 0):.0f}-{rtt_stats.get('max', 0):.0f}]ms"
        )

    # Website visitors online effect
    by_online = summary.get("by_visitors_online", {})
    if len(by_online) >= 2:
        online_sorted = sorted(by_online.items(), key=lambda x: int(x[0]))
        low_online = online_sorted[0]
        high_online = online_sorted[-1]
        findings.append(
            f"Website visitors online: {low_online[0]} users → {low_online[1]['mean']}fps, "
            f"{high_online[0]} users → {high_online[1]['mean']}fps"
        )

    return findings


# ─── GIT OPERATIONS ───────────────────────────────────────────────────────────

def git_commit_push(repo_dir, message):
    """Add, commit, and push changes to the git repo."""
    try:
        subprocess.run(["git", "-C", str(repo_dir), "add", "-A"],
                       capture_output=True, timeout=30)
        result = subprocess.run(["git", "-C", str(repo_dir), "commit", "-m", message],
                                capture_output=True, timeout=30)
        if result.returncode == 0:
            push_result = subprocess.run(["git", "-C", str(repo_dir), "push"],
                                         capture_output=True, timeout=60)
            if push_result.returncode == 0:
                log("Git commit + push successful")
            else:
                log(f"Git push failed: {push_result.stderr.decode()[:200]}")
        elif b"nothing to commit" in result.stdout:
            log("No changes to commit")
        else:
            log(f"Git commit failed: {result.stderr.decode()[:200]}")
    except subprocess.TimeoutExpired:
        log("Git operation timed out")
    except Exception as e:
        log(f"Git error: {e}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

MAX_RETRIES = 1  # retry once if measurement is noisy
RETRY_WAIT_S = 30  # seconds to wait before retry (let network conditions change)

# Fast-skip window: only the most extreme congestion where probes yield ~0 fps.
# We DO record all other noisy data — daytime measurements are VALUABLE for
# predicting real-world FPS. The regression uses jitter + time-of-day as features.
# Evidence: 07:30-09:00 UTC consistently gives <1 fps with >500ms jitter.
FAST_SKIP_START_UTC = 7.5   # 07:30 UTC
FAST_SKIP_END_UTC = 9      # 09:00 UTC
QUICK_JITTER_ABORT_MS = 800  # only skip if pre-check jitter is truly catastrophic
# JITTER_HARD_ABORT_MS removed — ALL data gets recorded

def main():
    log("=== zanderhbf2 probe starting ===")

    history = load_history()
    plan = load_plan()
    log(f"History: {len(history)} measurements")

    # Fast-skip only the worst 1.5h window (07:30-09:00 UTC) where fps is reliably ~0
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    utc_frac_hour = utc_now.hour + utc_now.minute / 60.0
    if FAST_SKIP_START_UTC <= utc_frac_hour < FAST_SKIP_END_UTC:
        log(f"FAST SKIP: UTC {utc_now.strftime('%H:%M')} in extreme congestion window ({FAST_SKIP_START_UTC}:00-{FAST_SKIP_END_UTC}:00 UTC). Saving 70s.")
        git_commit_push(REPO_DIR, f"skip #{len(history)+1}: fast-skip (UTC {utc_now.strftime('%H:%M')})")
        log("=== zanderhbf2 probe complete (skipped) ===")
        return None

    # Design next test
    test = design_next_test(history, plan)
    log(f"Test plan: phase={test.get('phase')} strategy={test.get('strategy')} "
        f"res={test['resolution']} comp={test['compression']} conn={test['extra_connections']}")

    # Measure environmental factors
    rtt = measure_rtt()
    log(f"RTT: {rtt}ms")

    # Quick jitter pre-check: 5-second probe at 160x90 to estimate network conditions
    log("Running quick jitter pre-check (5s at 160x90 c90)...")
    pre_jitter = quick_jitter_check(duration_s=5)
    if pre_jitter is not None:
        log(f"Pre-check jitter: {pre_jitter:.1f}ms")
        if pre_jitter > QUICK_JITTER_ABORT_MS:
            log(f"FAST SKIP: Pre-check jitter {pre_jitter:.0f}ms is catastrophic (> {QUICK_JITTER_ABORT_MS}ms).")
            git_commit_push(REPO_DIR, f"skip #{len(history)+1}: catastrophic jitter ({pre_jitter:.0f}ms)")
            log("=== zanderhbf2 probe complete (skipped) ===")
            return None
    else:
        log("Pre-check failed (camera unreachable?), proceeding with full probe anyway")

    camera_info = get_camera_temperature()
    website_visitors = get_website_visitors()
    log(f"Website visitors online: {website_visitors.get('users_online', '?')}")

    # Run the probe (with retry on noisy measurement)
    result = None
    is_noisy = False
    is_valid = False
    attempt = 0
    while attempt <= MAX_RETRIES:
        if attempt > 0:
            log(f"Retry #{attempt}: waiting {RETRY_WAIT_S}s before re-probing...")
            time.sleep(RETRY_WAIT_S)
            # Re-measure RTT and visitors for the retry
            rtt = measure_rtt()
            log(f"RTT before retry: {rtt}ms")
            website_visitors = get_website_visitors()
            log(f"Website visitors before retry: {website_visitors.get('users_online', '?')}")
        
        result = probe_mjpeg(
            resolution=test["resolution"],
            compression=test["compression"],
            duration=CAPTURE_DURATION,
            extra_connections=test["extra_connections"],
        )
        
        is_noisy = result.get("jitter_ms", 0) > HIGH_JITTER_THRESHOLD_MS
        is_valid = result.get("num_frames", 0) >= MIN_FRAMES_FOR_VALID
        
        if is_noisy and attempt < MAX_RETRIES:
            log(f"  WARNING: Measurement is noisy (jitter={result.get('jitter_ms', 0):.0f}ms > {HIGH_JITTER_THRESHOLD_MS}ms threshold)")
            attempt += 1
            continue
        
        if not is_valid and attempt < MAX_RETRIES:
            log(f"  WARNING: Only {result.get('num_frames', 0)} frames captured (< {MIN_FRAMES_FOR_VALID} minimum)")
            attempt += 1
            continue
        
        break  # measurement is clean or out of retries

    # Build full record
    now = datetime.datetime.now()
    # Compute local hour (CST = UTC+8, server is in UTC+8)
    local_hour = now.hour
    # Also compute true UTC hour for consistency
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    utc_hour = utc_now.hour
    
    # Flag noisy measurements based on jitter
    is_noisy = result.get("jitter_ms", 0) > HIGH_JITTER_THRESHOLD_MS
    is_valid = result.get("num_frames", 0) >= MIN_FRAMES_FOR_VALID
    
    record = {
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "utc_hour": utc_hour,
        "local_hour": local_hour,
        "utc_day_of_week": utc_now.weekday(),
        "utc_is_weekend": utc_now.weekday() >= 5,
        **result,
        "network_rtt_ms": rtt,
        "camera_info": camera_info,
        "website_visitors": website_visitors,
        "phase": test.get("phase"),
        "strategy": test.get("strategy"),
        "run_number": len(history) + 1,
        "hostname": "zai-2-cron",
        "quality_flags": {
            "is_noisy": is_noisy,
            "is_valid": is_valid,
            "jitter_ms": result.get("jitter_ms", 0),
            "rtt_ms": rtt,
            "attempt": attempt,  # 0 = first try, 1 = retry
        },
    }
    
    if is_noisy:
        log(f"  FINAL WARNING: Measurement still noisy after {attempt} attempt(s)")
    if not is_valid:
        log(f"  FINAL WARNING: Measurement still invalid after {attempt} attempt(s)")

    # ALWAYS record data — even noisy measurements are informative for predicting real-world FPS.
    # The regression uses jitter and time-of-day as features, so noisy data
    # helps the model learn the diurnal congestion pattern.
    append_record(record)
    log(f"Result: {record['num_frames']} frames, {record['measured_fps']} fps, "
        f"{record['bandwidth_mbps']} Mbps, avg_frame={record['avg_frame_size_bytes']} bytes")

    # Rebuild model summary
    history = load_history()  # re-read to include new record
    summary = build_model_summary(history)
    with open(MODEL_FILE, "w") as f:
        json.dump(summary, f, indent=2)
    log(f"Model summary updated: {summary['total_measurements']} total measurements")

    # Log key findings
    for finding in summary.get("key_findings", []):
        log(f"  FINDING: {finding}")

    # Git commit & push
    git_commit_push(
        REPO_DIR,
        f"probe #{record['run_number']}: {test['resolution']} c{test['compression']} "
        f"conn={test['extra_connections']} → {record['measured_fps']}fps "
        f"({test.get('strategy', '?')})"
    )

    log("=== zanderhbf2 probe complete ===")
    return record


if __name__ == "__main__":
    main()
