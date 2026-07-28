"""
Day-by-day cumulative ROI for the World Cup 2026 model.

Walks every graded pick in soccer_wc_picks, buckets by matchday (4am-ET broadcast
day), and reports each day's record + the running cumulative units and ROI%. Prints
a table and an ASCII chart by default; `--svg PATH` also writes a tweet-ready SVG
line chart (pure stdlib, no third-party deps). `--daily` overlays each day's own
ROI% as a bar alongside the cumulative line/bar. Stage-transition markers (Round of
32 / Round of 16 / Quarterfinals / Semifinals) are always shown, in every output
format, at the first graded-pick day of that stage.

Usage:
    python roi_history.py                  # table + ASCII chart
    python roi_history.py --daily          # ... plus each day's own ROI% bar
    python roi_history.py --svg roi.svg    # also write an SVG graphic
"""

import argparse
import os
import shutil
import sqlite3
import subprocess
from collections import OrderedDict

from core.sports_db import DATABASE_PATH
from core.poisson_model import american_to_decimal
from import_wc_odds import load_team_map, resolve_team

EASTERN_SQL_OFFSET = "-8 hours"   # matchday boundary (see generate_wc_card.py)

# A pick's market, inferred from its side text (soccer_wc_picks.side has no
# separate market column). "1x2" = moneyline/draw, "over_under" = any OVER/UNDER
# total line, "advance" = the knockout to-advance market.
MARKET_SQL = {
    "1x2": "p.side IN ('HOME', 'AWAY', 'DRAW')",
    "over_under": "(p.side LIKE 'OVER%' OR p.side LIKE 'UNDER%')",
    "advance": "p.side IN ('HOME ADVANCE', 'AWAY ADVANCE')",
}

# Knockout stages get a "started" marker on the timeline; Group is the series'
# own starting point so it's never marked. Order matters: it's the order stages
# are assumed to occur in, used to detect each one's first appearance.
STAGE_ORDER = ["Group", "R32", "R16", "QF", "SF", "3P", "Final"]
STAGE_LABELS = {
    "Group": "Group Stage", "R32": "Round of 32", "R16": "Round of 16",
    "QF": "Quarterfinals", "SF": "Semifinals", "3P": "3rd Place Match",
    "Final": "Final",
}

# Headless Chrome is the only no-extra-dependency way to rasterize SVG -> PNG at an
# exact aspect ratio on this setup (no matplotlib/Pillow/rsvg wheels on py3.14).
CHROME_PATHS = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
]


def units(odds, result):
    if result == "win":
        return american_to_decimal(odds) - 1
    if result == "loss":
        return -1.0
    return 0.0   # push: stake returned


def tournament_bounds(conn):
    """(earliest, latest) matchday among all graded picks -- the fallback
    endpoints when only one side of a --start/--end range is given."""
    row = conn.execute(
        """SELECT MIN(date(m.match_date, ?)), MAX(date(m.match_date, ?))
           FROM soccer_wc_picks p JOIN soccer_wc_matches m ON p.match_id = m.match_id
           WHERE p.result IS NOT NULL""", (EASTERN_SQL_OFFSET, EASTERN_SQL_OFFSET)).fetchone()
    return row


def _apply_filters(query, params, start, end, market, team_id):
    """Shared WHERE-clause builder for load_series/stage_totals: date range,
    market (see MARKET_SQL), and team (matches either side of the fixture)."""
    if start:
        query += " AND date(m.match_date, ?) >= ?"
        params += [EASTERN_SQL_OFFSET, start]
    if end:
        query += " AND date(m.match_date, ?) <= ?"
        params += [EASTERN_SQL_OFFSET, end]
    if market:
        query += f" AND {MARKET_SQL[market]}"
    if team_id:
        query += " AND (m.home_team_id = ? OR m.away_team_id = ?)"
        params += [team_id, team_id]
    return query, params


def load_series(conn, start=None, end=None, market=None, team_id=None):
    """Return an ordered list of per-day dicts with cumulative units + ROI%,
    optionally scoped to a matchday range, a market (MARKET_SQL), and/or a
    team (either side of the fixture). Everything -- bets count, cum_u, roi --
    is computed fresh from just the picks that match, not sliced off the
    full-tournament series.

    Each entry also carries that day's own ROI% (``day_roi``, for --daily) and
    ``stage_start`` -- the stage code (R32/R16/QF/SF) if this is the first
    graded-pick day of that stage IN THIS SCOPED RANGE, else None. Group is
    never flagged as a "start" since it's the series' own origin.
    """
    query = """SELECT date(m.match_date, ?) d, p.odds, p.result, m.stage
               FROM soccer_wc_picks p JOIN soccer_wc_matches m ON p.match_id = m.match_id
               WHERE p.result IS NOT NULL"""
    params = [EASTERN_SQL_OFFSET]
    query, params = _apply_filters(query, params, start, end, market, team_id)
    query += " ORDER BY d"
    rows = conn.execute(query, params).fetchall()
    days = OrderedDict()
    for d, o, r, stage in rows:
        days.setdefault(d, []).append((o, r, stage))

    series, cum_u, cum_n = [], 0.0, 0
    seen_stages = set()
    for d, bs in days.items():
        day_u = sum(units(o, r) for o, r, _ in bs)
        cum_u += day_u
        cum_n += len(bs)
        day_stages = {stage for _, _, stage in bs}
        new_stage = next((s for s in STAGE_ORDER
                           if s != "Group" and s in day_stages and s not in seen_stages), None)
        seen_stages |= day_stages
        series.append({
            "date": d,
            "w": sum(r == "win" for _, r, _ in bs),
            "l": sum(r == "loss" for _, r, _ in bs),
            "p": sum(r == "push" for _, r, _ in bs),
            "day_u": day_u, "cum_u": cum_u, "bets": cum_n,
            "roi": cum_u / cum_n * 100,
            "day_roi": day_u / len(bs) * 100,
            "stage_start": new_stage,
        })
    return series


def stage_totals(conn, start=None, end=None, market=None, team_id=None):
    """Return an OrderedDict of stage -> {w,l,p,n} across all graded picks,
    in STAGE_ORDER, omitting stages with no graded picks yet. Optionally
    scoped the same way as load_series (date range, market, team)."""
    query = """SELECT m.stage, p.result FROM soccer_wc_picks p
               JOIN soccer_wc_matches m ON p.match_id = m.match_id
               WHERE p.result IS NOT NULL"""
    params = []
    query, params = _apply_filters(query, params, start, end, market, team_id)
    rows = conn.execute(query, params).fetchall()
    totals = OrderedDict((s, {"w": 0, "l": 0, "p": 0, "n": 0}) for s in STAGE_ORDER)
    for stage, result in rows:
        t = totals.setdefault(stage, {"w": 0, "l": 0, "p": 0, "n": 0})
        t["n"] += 1
        if result in ("win", "loss", "push"):
            t[result[0]] += 1
    return OrderedDict((s, t) for s, t in totals.items() if t["n"] > 0)


def print_table(series):
    print(f"{'date':<12}{'W-L-P':>8}{'day u':>8}{'cum u':>8}{'bets':>6}{'cum ROI':>9}")
    for s in series:
        wlp = f"{s['w']}-{s['l']}-{s['p']}"
        print(f"{s['date']:<12}{wlp:>8}"
              f"{s['day_u']:>+8.2f}{s['cum_u']:>+8.2f}{s['bets']:>6}{s['roi']:>+8.1f}%")


def print_summary(series):
    last = series[-1]
    wins = sum(s["w"] for s in series)
    losses = sum(s["l"] for s in series)
    pushes = sum(s["p"] for s in series)
    print(f"\nOverall: {wins}-{losses}-{pushes} ({last['bets']} picks)  "
          f"·  Overall ROI: {last['roi']:+.1f}%")


def print_ascii(series, daily=False):
    label = "Cumulative ROI % (#)" + (" vs. that day's own ROI % (*)" if daily else "")
    print(f"\n{label}  (bars left of | = negative, right = positive; block ~2%)")
    vals = [s["roi"] for s in series] + ([s["day_roi"] for s in series] if daily else [])
    pad = int(round(abs(min(min(vals), 0)) / 2.0))
    for s in series:
        if s["stage_start"]:
            print(f"── {STAGE_LABELS[s['stage_start']]} starts ──")
        n = int(round(s["roi"] / 2.0))
        line = (" " * pad + "|" + "#" * n) if s["roi"] >= 0 else (" " * (pad + n) + "#" * (-n) + "|")
        out = f"{s['date'][5:]:<7}{s['roi']:>+6.1f}% {line}"
        if daily:
            dn = int(round(s["day_roi"] / 2.0))
            dline = (" " * pad + "|" + "*" * dn) if s["day_roi"] >= 0 else (" " * (pad + dn) + "*" * (-dn) + "|")
            out += f"   day{s['day_roi']:>+7.1f}% {dline}"
        print(out)


def write_svg(series, path, totals=None, daily=False):
    """Tweet-ready SVG line chart of cumulative ROI%, with stage-start markers.

    `totals` (from stage_totals()) drives the per-stage record line; when
    `daily` is set, each day's own ROI% is drawn as a bar behind the line.
    """
    W, H, L, R, T, B = 1200, 675, 90, 60, 150, 90
    pw, ph = W - L - R, H - T - B
    ys = [s["roi"] for s in series] + ([s["day_roi"] for s in series] if daily else [])
    ymin, ymax = min(min(ys), 0.0), max(max(ys), 0.0)
    span = (ymax - ymin) or 1.0
    ymin, ymax = ymin - span * 0.10, ymax + span * 0.12
    n = max(len(series) - 1, 1)

    def X(i):
        return L + pw * i / n

    def Y(v):
        return T + ph * (1 - (v - ymin) / (ymax - ymin))

    last = series[-1]
    tw = sum(s["w"] for s in series)
    tl = sum(s["l"] for s in series)
    tp = sum(s["p"] for s in series)
    rec = f"{tw}-{tl}-{tp}"
    accent = "#00ba7c" if last["roi"] >= 0 else "#f4212e"
    e = []
    e.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
             f'viewBox="0 0 {W} {H}" font-family="Helvetica,Arial,sans-serif">')
    e.append(f'<rect width="{W}" height="{H}" fill="#15202b"/>')
    e.append(f'<text x="{L}" y="56" fill="#ffffff" font-size="38" font-weight="bold">'
             f'World Cup 2026 — Model ROI</text>')
    e.append(f'<text x="{L}" y="86" fill="#8899a6" font-size="22">'
             f'{last["bets"]} picks · flat 1u · overall {rec} ({last["roi"]:+.1f}%)</text>')
    if totals:
        by_stage = " · ".join(f"{stage} {t['w']}-{t['l']}-{t['p']}"
                               for stage, t in totals.items())
        e.append(f'<text x="{L}" y="114" fill="#8899a6" font-size="20">{by_stage}</text>')

    # y gridlines at "nice" steps
    step = 25 if (ymax - ymin) > 60 else 10
    v = step * (int(ymin // step))
    while v <= ymax:
        if ymin <= v <= ymax:
            y = Y(v)
            zero = abs(v) < 1e-9
            e.append(f'<line x1="{L}" y1="{y:.1f}" x2="{L+pw}" y2="{y:.1f}" '
                     f'stroke="{"#3a4a5a" if zero else "#22303c"}" '
                     f'stroke-width="{2 if zero else 1}"/>')
            e.append(f'<text x="{L-12}" y="{y+6:.1f}" fill="#8899a6" font-size="20" '
                     f'text-anchor="end">{v:+.0f}%</text>')
        v += step

    # stage-start markers (dashed vertical + label), drawn under the data line
    for i, s in enumerate(series):
        if s["stage_start"]:
            x = X(i)
            e.append(f'<line x1="{x:.1f}" y1="{T}" x2="{x:.1f}" y2="{T+ph}" '
                     f'stroke="#546472" stroke-width="1.5" stroke-dasharray="5,4"/>')
            e.append(f'<text x="{x+5:.1f}" y="{T-8}" fill="#aab8c2" font-size="16" '
                     f'font-weight="bold">{s["stage_start"]}</text>')

    # daily ROI% bars (behind the cumulative line)
    if daily:
        bw = max(pw / n * 0.5, 3)
        for i, s in enumerate(series):
            x, y0, y1 = X(i), Y(0), Y(s["day_roi"])
            top, h = min(y0, y1), abs(y1 - y0)
            bar_color = "#00ba7c" if s["day_roi"] >= 0 else "#f4212e"
            e.append(f'<rect x="{x-bw/2:.1f}" y="{top:.1f}" width="{bw:.1f}" height="{h:.1f}" '
                     f'fill="{bar_color}" opacity="0.35"/>')

    # line + dots
    pts = " ".join(f"{X(i):.1f},{Y(s['roi']):.1f}" for i, s in enumerate(series))
    e.append(f'<polyline points="{pts}" fill="none" stroke="{accent}" stroke-width="4" '
             f'stroke-linejoin="round" stroke-linecap="round"/>')
    for i, s in enumerate(series):
        e.append(f'<circle cx="{X(i):.1f}" cy="{Y(s["roi"]):.1f}" r="4.5" fill="{accent}"/>')
        if i % 2 == 0 or i == len(series) - 1:
            e.append(f'<text x="{X(i):.1f}" y="{H-B+34:.1f}" fill="#8899a6" font-size="18" '
                     f'text-anchor="middle">{s["date"][5:]}</text>')

    # final value callout
    fx, fy = X(len(series) - 1), Y(last["roi"])
    e.append(f'<text x="{fx:.1f}" y="{fy-18:.1f}" fill="{accent}" font-size="30" '
             f'font-weight="bold" text-anchor="end">{last["roi"]:+.1f}%</text>')
    e.append('</svg>')
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(e))


def render_png(series, png_path, scale=2, totals=None, daily=False):
    """Write the SVG, then rasterize to PNG via headless Chrome (1200x675 * scale)."""
    svg_path = png_path.rsplit(".", 1)[0] + ".svg"
    write_svg(series, svg_path, totals=totals, daily=daily)
    chrome = next((p for p in CHROME_PATHS if os.path.exists(p)), None) \
        or shutil.which("google-chrome") or shutil.which("chromium")
    if not chrome:
        print(f"Wrote {svg_path} — no Chrome found to make a PNG; open the SVG in a "
              f"browser and screenshot it (or pass --svg and convert however you like).")
        return
    subprocess.run([chrome, "--headless", "--disable-gpu", f"--screenshot={png_path}",
                    "--window-size=1200,675", f"--force-device-scale-factor={scale}",
                    "--hide-scrollbars", "--default-background-color=15202bff",
                    f"file://{os.path.abspath(svg_path)}"], capture_output=True)
    print(f"Wrote {png_path}" if os.path.exists(png_path)
          else f"Wrote {svg_path} — Chrome render failed; screenshot the SVG instead.")


def main():
    ap = argparse.ArgumentParser(description="Day-by-day cumulative ROI for the WC model.")
    ap.add_argument("--svg", metavar="PATH", help="Also write a tweet-ready SVG chart.")
    ap.add_argument("--png", metavar="PATH", help="Write a tweet-ready PNG (via headless Chrome).")
    ap.add_argument("--daily", action="store_true",
                    help="Also show each day's own ROI% as a bar, alongside the cumulative line.")
    ap.add_argument("--start", metavar="YYYY-MM-DD",
                    help="Only include picks on/after this matchday. If --end is omitted, "
                         "defaults to the tournament's last graded matchday.")
    ap.add_argument("--end", metavar="YYYY-MM-DD",
                    help="Only include picks on/before this matchday. If --start is omitted, "
                         "defaults to the tournament's first graded matchday.")
    ap.add_argument("--market", choices=sorted(MARKET_SQL),
                    help="Only include picks in this market: 1x2, over_under, or advance.")
    ap.add_argument("--team", help="Only include picks in matches involving this team "
                                    "(either side of the fixture).")
    args = ap.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)

    team_id = None
    if args.team:
        team_id = resolve_team(args.team, load_team_map(conn))
        if team_id is None:
            conn.close()
            print(f"No team matching {args.team!r} in soccer_wc_teams.")
            return

    start, end = args.start, args.end
    if start or end:
        t_start, t_end = tournament_bounds(conn)
        start = start or t_start
        end = end or t_end
    if start or end or args.market or args.team:
        scope = []
        if start or end:
            scope.append(f"dates {start} .. {end}")
        if args.market:
            scope.append(f"market={args.market}")
        if args.team:
            scope.append(f"team={args.team}")
        print(f"Scoped to {', '.join(scope)}\n")

    series = load_series(conn, start, end, args.market, team_id)
    totals = stage_totals(conn, start, end, args.market, team_id)
    conn.close()
    if not series:
        print("No graded picks match that scope.")
        return
    print_table(series)
    print_summary(series)
    print_ascii(series, daily=args.daily)
    if args.svg:
        write_svg(series, args.svg, totals=totals, daily=args.daily)
        print(f"\nWrote {args.svg}")
    if args.png:
        render_png(series, args.png, totals=totals, daily=args.daily)


if __name__ == "__main__":
    main()
