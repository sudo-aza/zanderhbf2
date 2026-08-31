# zanderhbf2

Automated research into the FPS behavior of an Axis P1357 network camera.

## Goal

**Core deliverable: a planning model that predicts FPS from resolution, compression, and time-of-day ONLY.**

These are parameters you can know completely independent of runtime — no real-time network measurements needed. The model lets you answer "if I set the camera to 1280×720 compression 50 at 14:00 UTC, what FPS will I get?" before deploying.

The planning model uses 7 features: intercept, megapixels, compression, compression², MP×compression, sin(hour), cos(hour). No jitter, no RTT, no concurrent viewers — those are runtime variables.

A secondary diagnostic model (tod_quad11) includes jitter/RTT features and achieves higher R² (~0.80), but it can only be used for post-hoc analysis — you can't predict jitter in advance.

Full equation: **FPS = f(resolution, compression, time_of_day)** — with concurrent_viewers, network_conditions as secondary factors explored in later phases.

## Camera

- **Model**: AXIS P1357 Network Camera
- **Firmware**: 6.50.3 (Oct 16 2018)
- **URL**: `http://mzinfo.dnshome.de:5010/axis-cgi/mjpg/video.cgi`
- **Sensor**: 16:9 aspect ratio
- **Supported resolutions**: 160x90, 160x120, 176x144, 240x180, 320x180, 320x240, 480x270, 480x360, 640x360, 640x480, 800x450, 800x600, 1024x768, 1280x720, 1280x960, 1280x1024, 1920x1080
- **Parameters**: `?resolution=WxH&compression=0-100&fps=30`
- **Rotation**: 180°

## How it works

An hourly cron job runs `probe.py` which:

1. Reads all historical data from `data/history.jsonl`
2. Picks the next best test parameters using iterative experimental design
3. Streams MJPEG for 30 seconds, measuring actual FPS, frame sizes, bandwidth, jitter
4. Optionally opens concurrent connections to test viewer contention
5. Appends results to `data/history.jsonl`
6. Rebuilds `data/model_summary.json` with regression analysis
7. Git commits and pushes to this repository

## Experiment Phases

| Phase | Runs | Strategy |
|-------|------|----------|
| 1 | 0-49 | Coarse grid: all resolutions × [0,25,50,75,100] compression |
| 2 | 50-119 | Fine compression sweep at most interesting resolutions |
| 3 | 120-159 | Concurrent connection tests (1-5 extra viewers) |
| 4 | 160-299 | Time-of-day variation at key parameter combos |
| 5 | 300+ | Creative/exploratory: edge cases, unusual combos, re-tests |
| 6 | Ongoing | Repeat most variable measurements for statistical confidence |

## Data Format

Each line in `history.jsonl` is a JSON object:
```json
{
  "timestamp": "2026-08-24 10:00:00",
  "resolution": "640x480",
  "compression": 50,
  "requested_fps": 30,
  "measured_fps": 4.32,
  "num_frames": 129,
  "duration_s": 29.87,
  "avg_frame_size_bytes": 12345.6,
  "bandwidth_mbps": 0.41,
  "jitter_ms": 231.2,
  "extra_connections": 0,
  "network_rtt_ms": 45.2,
  "phase": 1,
  "strategy": "coarse_grid_fill",
  "run_number": 1
}
```

## Regression Models

### Planning Model (core deliverable)

**FPS = a + b×MP + c×compression + d×compression² + e×(MP×compression) + f×sin(hour) + g×cos(hour)**

- Features: resolution (megapixels), compression (0-100), time-of-day (sin/cos encoding)
- No jitter, no RTT, no concurrent viewers — these aren't knowable in advance
- Current R² ≈ 0.63 — improves as more hours get coverage
- Use case: "What FPS will I get at 1280×720 c50 at 14:00 UTC?"

### Diagnostic Model (tod_quad11)

Adds jitter, log(jitter), RTT, MP×jitter, and website visitors as features.
- Current R² ≈ 0.80 — higher because it uses real-time network info
- Use case: post-hoc analysis of "why did I get this FPS?"
- Cannot be used for advance planning (you can't predict jitter beforehand)

Both models use normal equation regression (no external dependencies). Results in `data/model_summary.json`.

## Key Hypotheses

1. **Encoder bottleneck**: At low compression (0-20), JPEG encoding speed limits FPS, especially at high resolutions
2. **Bandwidth bottleneck**: At mid compression, network bandwidth becomes the limiting factor
3. **Sensor bottleneck**: At high compression (80-100), the sensor/capture hardware limits FPS
4. **Viewer contention**: More concurrent viewers reduce per-viewer FPS
5. **Time-of-day**: Network conditions vary by hour, affecting FPS

## Repository Structure

```
zanderhbf2/
├── README.md           # This file
├── .gitignore          # Ignores mp4, cgi, etc.
├── probe.py            # Main research script (called by cron)
├── analyze.py          # Offline analysis / visualization script
└── data/
    ├── history.jsonl   # All measurements (append-only)
    ├── model_summary.json  # Regression + stats summary
    └── experiment_plan.json # Current experiment state
```