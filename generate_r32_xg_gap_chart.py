"""
Render a grouped bar chart of the two FEATURE-008 gaps (model-vs-xG, xG-vs-actual)
for each Round of 32 match with external xG on file -- a visual companion to
external_xg_calibration.py's per-stage summary.

Rendering matches the house style (roi_history.py / generate_scoreline_heatmap.py /
generate_feature009_chart.py): a pure-stdlib SVG rasterized to PNG via headless
Chrome -- no matplotlib/Pillow.

Usage:
    python generate_r32_xg_gap_chart.py
    python generate_r32_xg_gap_chart.py --stage R16 --out r16_xg_gaps.png
"""

import argparse
import os
import shutil
import subprocess
import sqlite3

from core.sports_db import DATABASE_PATH
from external_xg_calibration import build_rows

# Dark tweet palette (matches the other house charts).
BG, PANEL, MUTED, ACCENT = "#15202b", "#22303c", "#8899a6", "#1d9bf0"
MODEL_VS_XG_COLOR = ACCENT       # blue -- is the MODEL's own read off?
XG_VS_ACTUAL_COLOR = "#2e8b57"   # house "win" green -- did the RESULT diverge from the process?

SOCIALS = [("x:", "@minvest__", "#ffffff"),
           ("bluesky:", "@minvest-picks", "#ffffff"),
           ("web:", "https://minvest.tech", ACCENT)]

CHROME_PATHS = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
]


def parse_args():
    ap = argparse.ArgumentParser(description="Chart per-match model-vs-xG and xG-vs-actual gaps.")
    ap.add_argument("--stage", default="R32", help="Stage to chart (default R32).")
    ap.add_argument("--out", default="r32_xg_gaps.png", help="Output PNG path.")
    ap.add_argument("--svg", action="store_true", help="Only write the SVG (skip Chrome PNG).")
    return ap.parse_args()


def _esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_svg(rows, stage):
    n = len(rows)
    W = max(900, 140 * n + 200)
    H = 980
    chart_top, zero_y, chart_bottom = 240, 480, 700
    max_val = max(max(abs(r["model_vs_xg"]), abs(r["xg_vs_actual"])) for r in rows) * 1.2
    scale = (zero_y - chart_top) / max_val   # px per goal, symmetric up/down

    group_w = (W - 160) / n
    bar_w = min(38, group_w * 0.32)
    gap = 10

    e = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
         f'viewBox="0 0 {W} {H}" font-family="Helvetica,Arial,sans-serif">',
         f'<rect width="{W}" height="{H}" fill="{BG}"/>']

    e.append(f'<text x="{W/2:.0f}" y="56" fill="#ffffff" font-size="34" '
             f'font-weight="bold" text-anchor="middle">Model vs. xG vs. Actual — {_esc(stage)}</text>')
    e.append(f'<text x="{W/2:.0f}" y="90" fill="{MUTED}" font-size="20" '
             f'text-anchor="middle">Per-match goal gaps ({n} matches with external xG on file)</text>')

    # zero baseline
    e.append(f'<line x1="80" y1="{zero_y}" x2="{W-80}" y2="{zero_y}" stroke="{MUTED}" stroke-width="2"/>')

    for i, r in enumerate(rows):
        cx = 80 + group_w * (i + 0.5)
        mvx, xva = r["model_vs_xg"], r["xg_vs_actual"]

        for offset, val, color, label in (
            (-(bar_w + gap / 2), mvx, MODEL_VS_XG_COLOR, "model-xG"),
            (+(bar_w / 2 + gap / 2), xva, XG_VS_ACTUAL_COLOR, "xG-actual"),
        ):
            x = cx + offset
            bh = abs(val) * scale
            y = zero_y - bh if val >= 0 else zero_y
            e.append(f'<rect x="{x:.0f}" y="{y:.0f}" width="{bar_w:.0f}" height="{bh:.0f}" '
                     f'rx="4" fill="{color}"/>')
            ty = y - 8 if val >= 0 else y + bh + 20
            e.append(f'<text x="{x + bar_w/2:.0f}" y="{ty:.0f}" fill="#ffffff" font-size="16" '
                     f'font-weight="bold" text-anchor="middle">{val:+.2f}</text>')

        # match label, rotated to fit under a narrow group (extends down-left from
        # the anchor at 35 degrees, so it needs real clearance below chart_bottom)
        label = f"{_esc(r['home'])} v {_esc(r['away'])}"
        lx, ly = cx, chart_bottom + 30
        e.append(f'<text x="{lx:.0f}" y="{ly:.0f}" fill="#ffffff" font-size="14" '
                 f'text-anchor="end" transform="rotate(-35 {lx:.0f} {ly:.0f})">{label}</text>')

    # legend (own line, well clear of the subtitle above it)
    ly = 160
    legend = [(MODEL_VS_XG_COLOR, "Model − xG  (is the model's own read off?)"),
              (XG_VS_ACTUAL_COLOR, "xG − Actual  (did the result diverge from the process?)")]
    lx = W / 2 - 300
    for col, lab in legend:
        e.append(f'<rect x="{lx:.0f}" y="{ly-18:.0f}" width="22" height="22" fill="{col}"/>')
        e.append(f'<text x="{lx+30:.0f}" y="{ly:.0f}" fill="{MUTED}" font-size="18">{lab}</text>')
        lx += 40 + len(lab) * 8.2

    # socials / branding footer
    sep = "      "
    spans = []
    for k, (label, handle, col) in enumerate(SOCIALS):
        lead = sep if k else ""
        spans.append(f'<tspan fill="{MUTED}">{lead}{label} </tspan>'
                     f'<tspan fill="{col}">{_esc(handle)}</tspan>')
    e.append(f'<text x="{W/2:.0f}" y="{H - 30:.0f}" font-size="20" '
             f'text-anchor="middle" xml:space="preserve">{"".join(spans)}</text>')

    e.append('</svg>')
    return "\n".join(e), W, H


def rasterize(svg_path, png_path, w, h, scale=2):
    chrome = next((p for p in CHROME_PATHS if os.path.exists(p)), None) \
        or shutil.which("google-chrome") or shutil.which("chromium")
    if not chrome:
        print(f"Wrote {svg_path} — no Chrome found to make a PNG; open the SVG in a "
              f"browser and screenshot it.")
        return
    subprocess.run([chrome, "--headless", "--disable-gpu", f"--screenshot={png_path}",
                    f"--window-size={w},{h}", f"--force-device-scale-factor={scale}",
                    "--hide-scrollbars", f"--default-background-color={BG.lstrip('#')}ff",
                    f"file://{os.path.abspath(svg_path)}"], capture_output=True)
    print(f"Wrote {png_path}" if os.path.exists(png_path)
          else f"Wrote {svg_path} — Chrome render failed; screenshot the SVG instead.")


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    rows = build_rows(conn, args.stage)
    conn.close()

    if not rows:
        print(f"No matches with external xG on file for stage={args.stage!r}.")
        return

    svg, W, H = build_svg(rows, args.stage)
    svg_path = args.out.rsplit(".", 1)[0] + ".svg"
    with open(svg_path, "w", encoding="utf-8") as fh:
        fh.write(svg)

    if args.svg:
        print(f"Wrote {svg_path}")
    else:
        rasterize(svg_path, args.out, W, H)


if __name__ == "__main__":
    main()
