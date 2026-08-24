#!/usr/bin/env python3
"""
zanderhbf2 — Offline analysis script
=================================
Run locally to visualize data, test hypotheses, export plots.
Reads from data/history.jsonl and produces analysis.

Usage:
  python3 analyze.py              # Print summary
  python3 analyze.py plot           # Generate PNG plots (requires matplotlib)
  python3 analyze.py export csv     # Export to CSV
"""

import json
import sys
import math
from pathlib import Path
from collections import defaultdict

REPO_DIR = Path(__file__).parent
DATA_DIR = REPO_DIR / "data"
HISTORY_FILE = DATA_DIR / "history.jsonl"
MODEL_FILE = DATA_DIR / "model_summary.json"
OUTPUT_DIR = DATA_DIR / "plots"


RES_MP = {}
ALL_RESOLUTIONS = [
    "160x90", "160x120", "176x144",
    "240x180", "320x180", "320x240",
    "480x270", "480x360", "640x360",
    "640x480", "800x450", "800x600",
    "1024x768", "1280x720", "1280x960",
    "1280x1024", "1920x1080",
]
for r in ALL_RESOLUTIONS:
    RES_MP[r] = (int(r.split("x")[0]) * int(r.split("x")[1])) / 1e6


def load_history():
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


def print_summary(history):
    print(f"\n{'='*60}")
    print(f"  zanderhbf2 — Axis P1357 FPS Research Dashboard")
    print(f"{'='*60}")
    print(f"  Total measurements: {len(history)}")

    if not history:
        print("  No data yet.\n")
        return

    # Load model summary if it exists
    if MODEL_FILE.exists():
        with open(MODEL_FILE, "r") as f:
            model = json.load(f)
        print(f"\n  Model: {model.get('multifactor_regression', {}).get('equation', 'N/A')}")
        print(f"  R²: {model.get('multifactor_regression', {}).get('r_squared', 'N/A')}")

        print(f"\n  Key Findings:")
        for finding in model.get("key_findings", []):
            print(f"    • {finding}")

    # FPS by resolution (most recent measurement per res)
    latest_by_res = {}
    for rec in history:
        res = rec.get("resolution")
        if res:
            latest_by_res[res] = rec

    print(f"\n  Latest FPS by Resolution (sorted by measured FPS):")
    print(f"  {'Resolution':<14} {'Comp':>5} {'FPS':>8} {'BW(Mbps)':>10} {'AvgSize(B)':>11} {'Jitter(ms)':>11}")
    print(f"  {'-'*14} {'-'*5} {'-'*8} {'-'*10} {'-'*11} {'-'*11}")

    sorted_res = sorted(latest_by_res.items(), key=lambda x: x[1].get("measured_fps", 0), reverse=True)
    for res, rec in sorted_res:
        print(f"  {res:<14} {rec.get('compression', '?'):>5} {rec.get('measured_fps', 0):>8.3f} "
              f"{rec.get('bandwidth_mbps', 0):>10.3f} {rec.get('avg_frame_size_bytes', 0):>11.1f} "
              f"{rec.get('jitter_ms', 0):>11.2f}")

    # FPS by compression (aggregated)
    by_comp = defaultdict(list)
    for rec in history:
        if rec.get("extra_connections", 0) == 0:
            by_comp[rec.get("compression")].append(rec.get("measured_fps", 0))

    print(f"\n  Mean FPS by Compression (0-extra-conn only):")
    print(f"  {'Comp':>5} {'Mean FPS':>10} {'Std':>8} {'N':>5}")
    print(f"  {'-'*5} {'-'*10} {'-'*8} {'-'*5}")
    for comp in sorted(by_comp.keys()):
        fps_list = by_comp[comp]
        mean = sum(fps_list) / len(fps_list)
        std = math.sqrt(sum((x - mean) ** 2 for x in fps_list) / len(fps_list)) if len(fps_list) > 1 else 0
        print(f"  {comp:>5} {mean:>10.3f} {std:>8.3f} {len(fps_list):>5}")

    # Concurrent connection effect
    by_conn = defaultdict(list)
    for rec in history:
        by_conn[rec.get("extra_connections", 0)].append(rec.get("measured_fps", 0))

    if len(by_conn) > 1:
        print(f"\n  Concurrent Connection Effect:")
        print(f"  {'Conns':>6} {'Mean FPS':>10} {'Std':>8} {'N':>5}")
        print(f"  {'-'*6} {'-'*10} {'-'*8} {'-'*5}")
        for conn in sorted(by_conn.keys()):
            fps_list = by_conn[conn]
            mean = sum(fps_list) / len(fps_list)
            std = math.sqrt(sum((x - mean) ** 2 for x in fps_list) / len(fps_list)) if len(fps_list) > 1 else 0
            print(f"  {conn:>6} {mean:>10.3f} {std:>8.3f} {len(fps_list):>5}")

    # Time of day
    by_hour = defaultdict(list)
    for rec in history:
        ts = rec.get("timestamp", "")
        if ts:
            try:
                hour = int(ts.split(" ")[1].split(":")[0])
                by_hour[hour].append(rec.get("measured_fps", 0))
            except (IndexError, ValueError):
                pass

    if len(by_hour) >= 2:
        print(f"\n  Mean FPS by Hour (UTC):")
        print(f"  {'Hour':>5} {'Mean FPS':>10} {'N':>5}")
        print(f"  {'-'*5} {'-'*10} {'-'*5}")
        for hour in sorted(by_hour.keys()):
            fps_list = by_hour[hour]
            mean = sum(fps_list) / len(fps_list)
            print(f"  {hour:>5} {mean:>10.3f} {len(fps_list):>5}")

    print(f"\n{'='*60}\n")


def export_csv(history):
    """Export all data to CSV."""
    if not history:
        print("No data to export.")
        return

    csv_path = DATA_DIR / "export.csv"
    # Collect all unique keys
    all_keys = []
    seen = set()
    for rec in history:
        for k in rec.keys():
            if k not in seen and k != "_bottleneck":  # skip internal field
                seen.add(k)
                all_keys.append(k)

    with open(csv_path, "w") as f:
        f.write(",".join(all_keys) + "\n")
        for rec in history:
            row = []
            for k in all_keys:
                v = rec.get(k, "")
                if isinstance(v, str) and ("," in v or "\n" in v):
                    v = f'"{v}"'
                row.append(str(v))
            f.write(",".join(row) + "\n")

    print(f"Exported {len(history)} records to {csv_path}")


def generate_plots(history):
    """Generate analysis plots."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.font_manager as fm
    except ImportError:
        print("matplotlib not available. Install with: pip install matplotlib")
        return

    # Font setup
    fm.fontManager.addfont('/usr/share/fonts/truetype/chinese/NotoSansSC-Regular.ttf')
    fm.fontManager.addfont('/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf')
    plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Noto Sans SC']
    plt.rcParams['axes.unicode_minus'] = False

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not history:
        print("No data to plot.")
        return

    # Plot 1: FPS vs Compression for each resolution
    fig, ax = plt.subplots(figsize=(12, 7), constrained_layout=True)
    by_res_comp = defaultdict(dict)
    for rec in history:
        if rec.get("extra_connections", 0) == 0:
            res = rec.get("resolution")
            comp = rec.get("compression")
            fps = rec.get("measured_fps", 0)
            # Keep latest measurement per (res, comp)
            by_res_comp[res][comp] = fps

    for res in ALL_RESOLUTIONS:
        if res in by_res_comp:
            comps = sorted(by_res_comp[res].keys())
            fps_vals = [by_res_comp[res][c] for c in comps]
            mp = RES_MP.get(res, 0)
            ax.plot(comps, fps_vals, "o-", label=f"{res} ({mp:.2f}MP)", markersize=3, linewidth=1)

    ax.set_xlabel("Compression Level")
    ax.set_ylabel("Measured FPS")
    ax.set_title("Axis P1357: FPS vs Compression by Resolution")
    ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=7)
    ax.grid(True, alpha=0.3)
    fig.savefig(OUTPUT_DIR / "fps_vs_compression.png", dpi=150)
    plt.close(fig)
    print(f"Saved: {OUTPUT_DIR / 'fps_vs_compression.png'}")

    # Plot 2: FPS vs Megapixels (scatter, colored by compression)
    fig, ax = plt.subplots(figsize=(10, 7), constrained_layout=True)
    for rec in history:
        if rec.get("extra_connections", 0) == 0:
            res = rec.get("resolution")
            mp = RES_MP.get(res)
            if mp:
                ax.scatter(mp, rec.get("measured_fps", 0),
                           c=rec.get("compression", 50), cmap="RdYlGn_r",
                           s=20, alpha=0.7, vmin=0, vmax=100)

    ax.set_xlabel("Megapixels")
    ax.set_ylabel("Measured FPS")
    ax.set_title("Axis P1357: FPS vs Resolution (color = compression)")
    sm = plt.cm.ScalarMappable(cmap="RdYlGn_r", norm=plt.Normalize(0, 100))
    sm.set_array([])
    fig.colorbar(sm, ax=ax, label="Compression")
    ax.grid(True, alpha=0.3)
    fig.savefig(OUTPUT_DIR / "fps_vs_resolution.png", dpi=150)
    plt.close(fig)
    print(f"Saved: {OUTPUT_DIR / 'fps_vs_resolution.png'}")

    # Plot 3: Time series of FPS
    fig, ax = plt.subplots(figsize=(14, 5), constrained_layout=True)
    timestamps = []
    fps_vals = []
    for rec in history:
        ts = rec.get("timestamp", "")
        if ts:
            try:
                from datetime import datetime
                dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                timestamps.append(dt)
                fps_vals.append(rec.get("measured_fps", 0))
            except ValueError:
                pass

    if timestamps:
        ax.plot(timestamps, fps_vals, "o-", markersize=2, linewidth=0.5, alpha=0.7)
        ax.set_xlabel("Time")
        ax.set_ylabel("Measured FPS")
        ax.set_title("Axis P1357: FPS Over Time")
        ax.grid(True, alpha=0.3)
        fig.autofmt_xdate()
        fig.savefig(OUTPUT_DIR / "fps_timeseries.png", dpi=150)
        plt.close(fig)
        print(f"Saved: {OUTPUT_DIR / 'fps_timeseries.png'}")

    # Plot 4: Concurrent connections effect
    by_conn_data = defaultdict(list)
    for rec in history:
        conn = rec.get("extra_connections", 0)
        by_conn_data[conn].append(rec.get("measured_fps", 0))

    if len(by_conn_data) > 1:
        fig, ax = plt.subplots(figsize=(8, 5), constrained_layout=True)
        conns = sorted(by_conn_data.keys())
        means = [sum(by_conn_data[c]) / len(by_conn_data[c]) for c in conns]
        stds = [math.sqrt(sum((x - sum(by_conn_data[c]) / len(by_conn_data[c])) ** 2
                           for x in by_conn_data[c]) / len(by_conn_data[c]))
                for c in conns]
        ax.bar(conns, means, yerr=stds, capsize=5, color="steelblue", alpha=0.7)
        ax.set_xlabel("Concurrent Connections")
        ax.set_ylabel("Mean FPS")
        ax.set_title("Axis P1357: FPS vs Concurrent Viewers")
        ax.set_xticks(conns)
        ax.grid(True, alpha=0.3, axis="y")
        fig.savefig(OUTPUT_DIR / "fps_vs_connections.png", dpi=150)
        plt.close(fig)
        print(f"Saved: {OUTPUT_DIR / 'fps_vs_connections.png'}")

    # Plot 5: Bandwidth vs FPS
    fig, ax = plt.subplots(figsize=(10, 7), constrained_layout=True)
    for rec in history:
        ax.scatter(rec.get("bandwidth_mbps", 0), rec.get("measured_fps", 0),
                   c=rec.get("compression", 50), cmap="RdYlGn_r",
                   s=20, alpha=0.7, vmin=0, vmax=100)
    ax.set_xlabel("Bandwidth (Mbps)")
    ax.set_ylabel("Measured FPS")
    ax.set_title("Axis P1357: FPS vs Bandwidth (color = compression)")
    sm = plt.cm.ScalarMappable(cmap="RdYlGn_r", norm=plt.Normalize(0, 100))
    sm.set_array([])
    fig.colorbar(sm, ax=ax, label="Compression")
    ax.grid(True, alpha=0.3)
    fig.savefig(OUTPUT_DIR / "fps_vs_bandwidth.png", dpi=150)
    plt.close(fig)
    print(f"Saved: {OUTPUT_DIR / 'fps_vs_bandwidth.png'}")

    print(f"\nAll plots saved to {OUTPUT_DIR}/")


def main():
    history = load_history()
    cmd = sys.argv[1].lower() if len(sys.argv) > 1 else "summary"

    if cmd == "summary":
        print_summary(history)
    elif cmd == "plot":
        generate_plots(history)
    elif cmd == "export":
        export_csv(history)
    elif cmd == "csv":
        export_csv(history)
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: python3 analyze.py [summary|plot|export|csv]")


if __name__ == "__main__":
    main()
