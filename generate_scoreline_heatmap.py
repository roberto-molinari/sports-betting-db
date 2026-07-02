"""
Render a scoreline heat map for a World Cup match from the model's own numbers.

The grid is exactly ``scoreline_grid(lambda_H, lambda_A)`` — the Poisson joint
distribution over scorelines the card already prices from — annotated per cell,
coloured by outcome region (home win / draw / away win), with the modal scoreline
highlighted and a win/draw/win footer.

IMPORTANT — labelling. The per-team header number is the model's projected goals,
i.e. the combined Poisson rate **lambda (λ)**. It is goals-based, NOT expected
goals (xG); see DESIGN-001. The chart is labelled "λ" deliberately so the model's
output is never passed off as xG.

Rendering matches the house style (roi_history.py): a pure-stdlib SVG rasterized to
PNG via headless Chrome — no matplotlib/Pillow dependency. Without Chrome the SVG is
still written (open/screenshot it).

Usage:
    python generate_scoreline_heatmap.py --home Brazil --away Japan
    python generate_scoreline_heatmap.py --match-id 74 --out brazil_japan.png
    python generate_scoreline_heatmap.py --home Brazil --away Japan --max-goals 7 --svg
"""

import argparse
import os
import shutil
import subprocess
import sqlite3
import sys

from core.sports_db import DATABASE_PATH, get_latest_wc_strength
from core.poisson_model import (
    analyse_match_wc, scoreline_grid, outcome_probs, WC_MAX_GOALS,
)

# Dark tweet palette (matches roi_history.py / the group-stage report).
BG, PANEL, MUTED, ACCENT = "#15202b", "#22303c", "#8899a6", "#1d9bf0"
# Outcome-region colours (home win / draw / away win).
HOME_COLOR, DRAW_COLOR, AWAY_COLOR = "#2e8b57", "#b8a878", "#3b6fb5"

# Host venue edge, kept in sync with generate_wc_card so the heatmap's λ match the
# card's λ exactly (a host gets the boost on whichever side it is listed).
HOST_NATIONS = {"USA", "Mexico", "Canada"}
HOST_HOME_ADVANTAGE = 1.20

CHROME_PATHS = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
]

# Branding footer: (label, handle, colour) shown along the bottom of every graphic.
SOCIALS = [("x:", "@minvest__", "#ffffff"),
           ("bluesky:", "@minvest-picks", "#ffffff"),
           ("web:", "https://minvest.tech", ACCENT)]


def parse_args():
    ap = argparse.ArgumentParser(description="Render a scoreline heat map for a WC match.")
    ap.add_argument("--match-id", type=int, help="Match id (or use --home/--away).")
    ap.add_argument("--home", help="Home team name.")
    ap.add_argument("--away", help="Away team name.")
    ap.add_argument("--date", help="Override the date shown in the title (YYYY-MM-DD).")
    ap.add_argument("--max-goals", type=int, default=6,
                    help="Largest goal bucket shown per axis; the tail aggregates "
                         "into an 'N+' cell (default 6). W/D/L is always exact.")
    ap.add_argument("--out", help="Output PNG path (default: scoreline_<home>_<away>.png).")
    ap.add_argument("--svg", action="store_true",
                    help="Only write the SVG (skip the Chrome PNG rasterization).")
    return ap.parse_args()


def resolve_match(conn, args):
    """Return (home_id, home_name, away_id, away_name, date) for the requested match."""
    if args.match_id:
        row = conn.execute(
            """SELECT m.home_team_id, h.name, m.away_team_id, a.name, m.match_date
               FROM soccer_wc_matches m
               JOIN soccer_wc_teams h ON m.home_team_id = h.team_id
               JOIN soccer_wc_teams a ON m.away_team_id = a.team_id
               WHERE m.match_id = ?""", (args.match_id,)).fetchone()
        if not row:
            sys.exit(f"No match with id {args.match_id}.")
        return row
    if args.home and args.away:
        h = conn.execute("SELECT team_id FROM soccer_wc_teams WHERE name = ?", (args.home,)).fetchone()
        a = conn.execute("SELECT team_id FROM soccer_wc_teams WHERE name = ?", (args.away,)).fetchone()
        if not h or not a:
            sys.exit(f"Unknown team(s): {args.home!r} / {args.away!r}.")
        m = conn.execute(
            """SELECT match_date FROM soccer_wc_matches
               WHERE home_team_id = ? AND away_team_id = ? ORDER BY match_date LIMIT 1""",
            (h[0], a[0])).fetchone()
        return (h[0], args.home, a[0], args.away, m[0] if m else None)
    sys.exit("Provide --match-id, or both --home and --away.")


def model_lambdas(conn, home_id, home_name, away_id, away_name):
    """The combined Poisson rates λ_H / λ_A, computed exactly as the card does."""
    hs = get_latest_wc_strength(home_id, conn=conn)
    as_ = get_latest_wc_strength(away_id, conn=conn)
    if hs is None or as_ is None:
        sys.exit("Missing team strength for one side — cannot price the match.")
    h_att, h_def = hs
    a_att, a_def = as_
    home_adv = HOST_HOME_ADVANTAGE if home_name in HOST_NATIONS else 1.0
    away_adv = HOST_HOME_ADVANTAGE if away_name in HOST_NATIONS else 1.0
    r = analyse_match_wc(
        lambda_home_attack=h_att, lambda_away_attack=a_att,
        lambda_home_defense=h_def, lambda_away_defense=a_def,
        home_advantage=home_adv, away_advantage=away_adv)
    return r["lambda_H"], r["lambda_A"]


def display_matrix(grid, n):
    """Collapse the full grid into an (n+1)x(n+1) matrix whose last row/col is the
    'n+' tail bucket, so the heatmap conserves probability. Returns the matrix and
    the (home_goals, away_goals) of the modal scoreline (clamped into display)."""
    disp = [[0.0] * (n + 1) for _ in range(n + 1)]
    for i, row in enumerate(grid):
        di = min(i, n)
        for j, p in enumerate(row):
            disp[di][min(j, n)] += p
    bi = bj = 0
    best = -1.0
    for i, row in enumerate(grid):
        for j, p in enumerate(row):
            if p > best:
                best, bi, bj = p, i, j
    return disp, (min(bi, n), min(bj, n))


def _esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_svg(home, away, date, lam_h, lam_a, disp, modal, wdl):
    n = len(disp) - 1
    maxp = max(max(r) for r in disp) or 1.0
    cell, L, T, RM, BM = 96, 132, 196, 48, 288
    grid_w = (n + 1) * cell
    W, H = L + grid_w + RM, T + grid_w + BM
    labels = [str(k) for k in range(n)] + [f"{n}+"]

    def cx(j):
        return L + j * cell

    def cy(i):                      # home goals 0 at the BOTTOM
        return T + (n - i) * cell

    e = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
         f'viewBox="0 0 {W} {H}" font-family="Helvetica,Arial,sans-serif">',
         f'<rect width="{W}" height="{H}" fill="{BG}"/>']

    # title block
    e.append(f'<text x="{W/2:.0f}" y="64" fill="#ffffff" font-size="40" '
             f'font-weight="bold" text-anchor="middle">Scoreline Heat Map</text>')
    date_str = f"  |  {str(date)[:10]}" if date else ""
    e.append(f'<text x="{W/2:.0f}" y="110" fill="#ffffff" font-size="28" '
             f'text-anchor="middle">{_esc(home)} vs {_esc(away)}{date_str}</text>')
    e.append(f'<text x="{W/2:.0f}" y="150" fill="{ACCENT}" font-size="24" '
             f'text-anchor="middle">λ (projected goals): {lam_h:.2f} – {lam_a:.2f}</text>')

    # cells
    for i in range(n + 1):          # home goals (row)
        for j in range(n + 1):      # away goals (col)
            p = disp[i][j]
            color = HOME_COLOR if i > j else DRAW_COLOR if i == j else AWAY_COLOR
            op = 0.08 + 0.92 * (p / maxp)
            x, y = cx(j), cy(i)
            e.append(f'<rect x="{x:.0f}" y="{y:.0f}" width="{cell}" height="{cell}" '
                     f'fill="{color}" fill-opacity="{op:.3f}" stroke="{BG}" stroke-width="3"/>')
            tcol = "#ffffff" if (p / maxp) > 0.10 else "#56687a"
            e.append(f'<text x="{x + cell/2:.0f}" y="{y + cell/2 + 6:.0f}" fill="{tcol}" '
                     f'font-size="20" text-anchor="middle">{p*100:.1f}%</text>')

    # modal scoreline highlight
    mi, mj = modal
    e.append(f'<rect x="{cx(mj):.0f}" y="{cy(mi):.0f}" width="{cell}" height="{cell}" '
             f'fill="none" stroke="#ffd400" stroke-width="4"/>')

    # axis tick labels
    for j, lab in enumerate(labels):
        e.append(f'<text x="{cx(j) + cell/2:.0f}" y="{T + grid_w + 38:.0f}" fill="{MUTED}" '
                 f'font-size="22" text-anchor="middle">{lab}</text>')
    for i, lab in enumerate(labels):
        e.append(f'<text x="{L - 22:.0f}" y="{cy(i) + cell/2 + 7:.0f}" fill="{MUTED}" '
                 f'font-size="22" text-anchor="end">{lab}</text>')
    # axis titles
    e.append(f'<text x="{L + grid_w/2:.0f}" y="{T + grid_w + 76:.0f}" fill="#ffffff" '
             f'font-size="24" font-weight="bold" text-anchor="middle">'
             f'Goals — {_esc(away)}</text>')
    yt_x, yt_y = L - 78, T + grid_w / 2
    e.append(f'<text x="{yt_x:.0f}" y="{yt_y:.0f}" fill="#ffffff" font-size="24" '
             f'font-weight="bold" text-anchor="middle" '
             f'transform="rotate(-90 {yt_x:.0f} {yt_y:.0f})">Goals — {_esc(home)}</text>')

    # W/D/L footer + legend
    ph, pd, pa = wdl
    fy = T + grid_w + 120
    e.append(f'<rect x="{L}" y="{fy:.0f}" width="{grid_w}" height="52" rx="10" '
             f'fill="{PANEL}"/>')
    e.append(f'<text x="{L + grid_w/2:.0f}" y="{fy + 34:.0f}" fill="#ffffff" font-size="23" '
             f'font-weight="bold" text-anchor="middle">'
             f'{_esc(home)} win: {ph:.1%}     Draw: {pd:.1%}     {_esc(away)} win: {pa:.1%}</text>')
    ly = fy + 84
    legend = [(HOME_COLOR, f"{_esc(home)} win"), (DRAW_COLOR, "Draw"), (AWAY_COLOR, f"{_esc(away)} win")]
    lx = L + 30
    for col, lab in legend:
        e.append(f'<rect x="{lx:.0f}" y="{ly - 16:.0f}" width="22" height="22" fill="{col}"/>')
        e.append(f'<text x="{lx + 30:.0f}" y="{ly + 2:.0f}" fill="{MUTED}" font-size="20">{lab}</text>')
        lx += 60 + len(lab) * 12

    # socials / branding footer (one centered line; xml:space keeps the gaps)
    sep = "      "
    spans = []
    for k, (label, handle, col) in enumerate(SOCIALS):
        lead = sep if k else ""
        spans.append(f'<tspan fill="{MUTED}">{lead}{label} </tspan>'
                     f'<tspan fill="{col}">{_esc(handle)}</tspan>')
    e.append(f'<text x="{W/2:.0f}" y="{T + grid_w + 252:.0f}" font-size="20" '
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
    home_id, home, away_id, away, date = resolve_match(conn, args)
    if args.date:
        date = args.date
    lam_h, lam_a = model_lambdas(conn, home_id, home, away_id, away)
    conn.close()

    grid = scoreline_grid(lam_h, lam_a, max_goals=WC_MAX_GOALS)
    op = outcome_probs(grid)
    n = max(1, min(args.max_goals, WC_MAX_GOALS))
    disp, modal = display_matrix(grid, n)

    out = args.out or f"scoreline_{home}_{away}.png".replace(" ", "_")
    svg, W, H = build_svg(home, away, date, lam_h, lam_a, disp, modal,
                          (op["p_home"], op["p_draw"], op["p_away"]))
    svg_path = out.rsplit(".", 1)[0] + ".svg"
    with open(svg_path, "w", encoding="utf-8") as fh:
        fh.write(svg)

    print(f"λ (projected goals): {home} {lam_h:.2f}  {away} {lam_a:.2f}")
    print(f"W/D/L: {home} {op['p_home']:.1%}  Draw {op['p_draw']:.1%}  {away} {op['p_away']:.1%}")
    print(f"Modal scoreline: {home} {modal[0]}{'+' if modal[0]==n else ''}"
          f"-{modal[1]}{'+' if modal[1]==n else ''} {away}")
    if args.svg:
        print(f"Wrote {svg_path}")
    else:
        rasterize(svg_path, out, W, H)


if __name__ == "__main__":
    main()
