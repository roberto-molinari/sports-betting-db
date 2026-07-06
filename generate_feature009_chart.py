"""
Render the FEATURE-009 backtest comparison chart: units returned by the actual
historical system (EV-only, no two-step) vs. the FEATURE-009 two-step selection
(value / prediction / fallback modes), both run against the same graded picks.
For the minvest.tech/data report card.

Both series are computed live from the DB (not hardcoded), so the chart stays
correct as more picks get graded:
  - "Actual" reads soccer_wc_picks.result/odds directly -- what really happened.
  - "FEATURE-009" re-runs feature009_backtest.load_games/simulate at the locked
    bars (VALUE_MODE_MIN_PROBABILITY / PREDICTION_MODE_MIN_IMPLIED_PROBABILITY
    in generate_wc_card.py) -- what the two-step selection would have returned
    on the exact same set of matches.

Rendering matches the house style (roi_history.py / generate_scoreline_heatmap.py):
a pure-stdlib SVG rasterized to PNG via headless Chrome -- no matplotlib/Pillow.

Usage:
    python generate_feature009_chart.py
    python generate_feature009_chart.py --out feature009_backtest.png
"""

import argparse
import os
import shutil
import subprocess
import sqlite3

from core.sports_db import DATABASE_PATH
from core.poisson_model import american_to_decimal
from generate_wc_card import VALUE_MODE_MIN_PROBABILITY, PREDICTION_MODE_MIN_IMPLIED_PROBABILITY
from feature009_backtest import load_games, simulate

# Dark tweet palette (matches roi_history.py / generate_scoreline_heatmap.py).
BG, PANEL, MUTED, ACCENT = "#15202b", "#22303c", "#8899a6", "#1d9bf0"
BASELINE_COLOR = "#56687a"   # muted slate -- the current/actual (EV-only) system
FEATURE_COLOR = "#2e8b57"    # house "win" green -- the FEATURE-009 backtest

# Branding footer: (label, handle, colour) shown along the bottom of every graphic.
SOCIALS = [("x:", "@minvest__", "#ffffff"),
           ("bluesky:", "@minvest-picks", "#ffffff"),
           ("web:", "https://minvest.tech", ACCENT)]

CHROME_PATHS = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
]


def parse_args():
    ap = argparse.ArgumentParser(description="Render the FEATURE-009 backtest comparison chart.")
    ap.add_argument("--out", default="feature009_backtest.png", help="Output PNG path.")
    ap.add_argument("--svg", action="store_true", help="Only write the SVG (skip Chrome PNG).")
    return ap.parse_args()


def actual_record(conn):
    """What the system's actual, already-graded picks did (odds/result as stored)."""
    rows = conn.execute("""
        SELECT p.odds, p.result FROM soccer_wc_picks p
        JOIN soccer_wc_matches m ON m.match_id = p.match_id
        WHERE p.result IS NOT NULL
    """).fetchall()
    w = loss_n = pu = 0
    units = 0.0
    for odds, result in rows:
        if result == "win":
            w += 1
            units += american_to_decimal(odds) - 1
        elif result == "loss":
            loss_n += 1
            units -= 1
        else:
            pu += 1
    return len(rows), w, loss_n, pu, units


def feature009_record(conn):
    """What FEATURE-009's two-step selection (at the locked bars) would have
    returned on the exact same graded matches."""
    games = load_games(conn)
    modes = simulate(games, VALUE_MODE_MIN_PROBABILITY, PREDICTION_MODE_MIN_IMPLIED_PROBABILITY)
    w = loss_n = pu = 0
    units = 0.0
    n = 0
    for rows in modes.values():
        n += len(rows)
        for _, c, u in rows:
            units += u
            if c["result"] == "win":
                w += 1
            elif c["result"] == "loss":
                loss_n += 1
            else:
                pu += 1
    return n, w, loss_n, pu, units


def _esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_svg(actual, feature):
    n_a, w_a, l_a, pu_a, u_a = actual
    n_f, w_f, l_f, pu_f, u_f = feature
    hit_a = 100 * w_a / (w_a + l_a) if (w_a + l_a) else 0.0
    hit_f = 100 * w_f / (w_f + l_f) if (w_f + l_f) else 0.0

    W, H = 900, 700
    chart_top, chart_bottom = 220, 560
    chart_h = chart_bottom - chart_top
    bar_w = 220
    gap = 140
    total_w = 2 * bar_w + gap
    left = (W - total_w) / 2

    max_u = max(u_a, u_f, 1.0) * 1.25
    bar_h_a = chart_h * (u_a / max_u)
    bar_h_f = chart_h * (u_f / max_u)

    bars = [
        (left, bar_h_a, BASELINE_COLOR, "Actual system", u_a, w_a, l_a, pu_a, hit_a, n_a),
        (left + bar_w + gap, bar_h_f, FEATURE_COLOR, "FEATURE-009", u_f, w_f, l_f, pu_f, hit_f, n_f),
    ]

    e = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
         f'viewBox="0 0 {W} {H}" font-family="Helvetica,Arial,sans-serif">',
         f'<rect width="{W}" height="{H}" fill="{BG}"/>']

    # title block
    e.append(f'<text x="{W/2:.0f}" y="66" fill="#ffffff" font-size="38" '
             f'font-weight="bold" text-anchor="middle">Two-Step Selection Backtest</text>')
    e.append(f'<text x="{W/2:.0f}" y="104" fill="{MUTED}" font-size="24" '
             f'text-anchor="middle">Units returned on {n_a} graded 2026 World Cup picks</text>')
    delta = u_f - u_a
    e.append(f'<text x="{W/2:.0f}" y="144" fill="{ACCENT}" font-size="22" font-weight="bold" '
             f'text-anchor="middle">{delta:+.2f}u more on the same picks</text>')

    # baseline (zero line)
    e.append(f'<line x1="{left - 40:.0f}" y1="{chart_bottom:.0f}" '
             f'x2="{left + total_w + 40:.0f}" y2="{chart_bottom:.0f}" '
             f'stroke="{MUTED}" stroke-width="2"/>')

    for x, bh, color, label, units, w, loss_n, pu, hit, n in bars:
        y = chart_bottom - bh
        e.append(f'<rect x="{x:.0f}" y="{y:.0f}" width="{bar_w}" height="{bh:.0f}" '
                 f'rx="8" fill="{color}"/>')
        # units value on top of the bar
        e.append(f'<text x="{x + bar_w/2:.0f}" y="{y - 20:.0f}" fill="#ffffff" '
                 f'font-size="34" font-weight="bold" text-anchor="middle">{units:+.2f}u</text>')
        # label below the axis
        e.append(f'<text x="{x + bar_w/2:.0f}" y="{chart_bottom + 42:.0f}" fill="#ffffff" '
                 f'font-size="24" font-weight="bold" text-anchor="middle">{_esc(label)}</text>')
        # record + hit rate
        e.append(f'<text x="{x + bar_w/2:.0f}" y="{chart_bottom + 72:.0f}" fill="{MUTED}" '
                 f'font-size="20" text-anchor="middle">{w}-{loss_n}-{pu}  ({hit:.1f}% hit)</text>')

    # socials / branding footer
    sep = "      "
    spans = []
    for k, (label, handle, col) in enumerate(SOCIALS):
        lead = sep if k else ""
        spans.append(f'<tspan fill="{MUTED}">{lead}{label} </tspan>'
                     f'<tspan fill="{col}">{_esc(handle)}</tspan>')
    e.append(f'<text x="{W/2:.0f}" y="{H - 34:.0f}" font-size="20" '
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
    actual = actual_record(conn)
    feature = feature009_record(conn)
    conn.close()

    print(f"Actual:      n={actual[0]:<3} {actual[1]}-{actual[2]}-{actual[3]}  {actual[4]:+.2f}u")
    print(f"FEATURE-009: n={feature[0]:<3} {feature[1]}-{feature[2]}-{feature[3]}  {feature[4]:+.2f}u")

    svg, W, H = build_svg(actual, feature)
    svg_path = args.out.rsplit(".", 1)[0] + ".svg"
    with open(svg_path, "w", encoding="utf-8") as fh:
        fh.write(svg)

    if args.svg:
        print(f"Wrote {svg_path}")
    else:
        rasterize(svg_path, args.out, W, H)


if __name__ == "__main__":
    main()
