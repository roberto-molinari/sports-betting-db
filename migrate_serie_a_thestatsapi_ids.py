"""
One-time migration step (BUGS.md, Serie A -> TheStatsAPI fast-follow): stamp
soccer_matches.thestatsapi_match_id onto every existing Serie A row, so
import_league_matches.py's dedup (which is keyed purely on that column, see
find_existing_match()) can recognize these rows going forward instead of
treating them as brand new.

DELIBERATELY does not touch core/leagues.py's registry -- Serie A's
thestatsapi_competition_id stays None (comp_5840 is hardcoded here instead)
until every existing row is safely stamped. Registering it early would be
the actual risk: import_league_matches.py --league "Serie A" (or an
automated call via season_kickoff.py / club_league_scorecard.py's refresh
step) would see thestatsapi_match_id IS NULL on every row and insert one
duplicate match per existing row. Stamp first, register second -- a
separate, deliberate follow-up step.

Matching logic (verified by hand against all 4 completed seasons + the
current in-progress one before this script was written -- see BUGS.md):
  - Team pairing after SERIE_A_NAME_MAP (our DB's longer names -> TheStatsAPI's
    shorter ones, e.g. "AC Milan" -> "Milan") -- 9 teams need this, the rest
    already match exactly.
  - Closest date within DATE_TOLERANCE_DAYS (some Serie A rows' stored dates
    drift up to 2 days from TheStatsAPI's real kickoff date -- a football-
    data.org quirk, not a TheStatsAPI or a real-fixture problem; verified
    score-identical in every case checked).
  - For a COMPLETED match, the score must also match before stamping -- a
    date/team match with a score disagreement is a genuine anomaly, not
    something to paper over, so it's reported and skipped, never stamped.
  - A row that finds no confident match is left alone, not necessarily a
    problem: TheStatsAPI publishes a season's fixtures progressively (every
    league shows this today, not just Serie A -- confirmed live against the
    4 already-migrated leagues' own partial current-season counts), so a
    late-season 2026-27 fixture may simply not exist there yet.

Usage:
    python migrate_serie_a_thestatsapi_ids.py              # dry-run, report only
    python migrate_serie_a_thestatsapi_ids.py --apply       # actually write
"""
import argparse
import sqlite3
from collections import defaultdict
from datetime import datetime

from core.sports_db import DATABASE_PATH, set_thestatsapi_match_id
from core.thestatsapi import Client

SERIE_A_COMPETITION_ID = "comp_5840"

# Our DB's name (football-data.org) -> TheStatsAPI's name. Verified complete
# for seasons 2022-2025 + the 2026 matches TheStatsAPI has published so far --
# a name showing up unmapped mid-run means a NEW team abbreviation, not
# necessarily an error, but worth a human look before assuming it's fine.
SERIE_A_NAME_MAP = {
    "AC Milan": "Milan", "AS Roma": "Roma", "Cagliari Calcio": "Cagliari",
    "Frosinone Calcio": "Frosinone", "Genoa CFC": "Genoa", "Como 1907": "Como",
    "Parma Calcio 1913": "Parma", "Venezia FC": "Venezia", "AC Pisa 1909": "Pisa",
}

DATE_TOLERANCE_DAYS = 3


def fetch_api_matches_by_season(client):
    """{start_year: [api_match, ...]} for every TheStatsAPI season under
    comp_5840 that overlaps a season we actually have Serie A rows for."""
    seasons = client.get_data(f"competitions/{SERIE_A_COMPETITION_ID}/seasons")
    by_season = {}
    for s in seasons:
        matches = list(client.paginate(
            "matches", {"competition_id": SERIE_A_COMPETITION_ID, "season_id": s["id"]}))
        by_season[s["start_year"]] = matches
    return by_season


def build_pair_index(api_matches):
    """{(home_name, away_name): [api_match, ...]} using TheStatsAPI's own names."""
    index = defaultdict(list)
    for m in api_matches:
        index[(m["home_team"]["name"], m["away_team"]["name"])].append(m)
    return index


def best_match(db_home, db_away, db_date, pair_index):
    """Closest-date candidate for this team pairing within DATE_TOLERANCE_DAYS,
    or None. db_date is a 'YYYY-MM-DD...' string; only the date part is used."""
    h_api = SERIE_A_NAME_MAP.get(db_home, db_home)
    a_api = SERIE_A_NAME_MAP.get(db_away, db_away)
    candidates = pair_index.get((h_api, a_api), [])
    if not candidates:
        return None
    target = datetime.fromisoformat(db_date[:10])
    best, best_diff = None, None
    for m in candidates:
        api_date = datetime.fromisoformat(m["utc_date"][:10])
        diff = abs((api_date - target).days)
        if best_diff is None or diff < best_diff:
            best, best_diff = m, diff
    if best_diff is not None and best_diff <= DATE_TOLERANCE_DAYS:
        return best
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true",
                        help="Actually write thestatsapi_match_id. Without this, report only.")
    args = parser.parse_args()

    client = Client()
    print(f"Fetching TheStatsAPI matches for Serie A ({SERIE_A_COMPETITION_ID})...")
    api_by_season = fetch_api_matches_by_season(client)
    for year, matches in sorted(api_by_season.items()):
        print(f"  season {year}: {len(matches)} matches")

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT m.match_id, ht.name AS home, at.name AS away, m.match_date,
               m.home_score, m.away_score, m.match_status, m.season
        FROM soccer_matches m
        JOIN soccer_teams ht ON ht.team_id = m.home_team_id
        JOIN soccer_teams at ON at.team_id = m.away_team_id
        WHERE m.league = 'Serie A' AND m.thestatsapi_match_id IS NULL
        ORDER BY m.season, m.match_date
    """)
    rows = cur.fetchall()
    conn.close()

    print(f"\n{len(rows)} Serie A row(s) with no thestatsapi_match_id yet.")

    pair_index_by_season = {year: build_pair_index(matches) for year, matches in api_by_season.items()}

    stamped, unmatched, score_mismatch, reused_id = [], [], [], []
    used_ids = {}

    for row in rows:
        pair_index = pair_index_by_season.get(row["season"])
        m = best_match(row["home"], row["away"], row["match_date"], pair_index) if pair_index else None
        if m is None:
            unmatched.append(row)
            continue

        if row["match_status"] == "completed":
            api_hs, api_aws = m["score"]["home"], m["score"]["away"]
            if row["home_score"] != api_hs or row["away_score"] != api_aws:
                score_mismatch.append((row, m, api_hs, api_aws))
                continue

        api_id = m["id"]
        if api_id in used_ids:
            reused_id.append((row, m, used_ids[api_id]))
            continue
        used_ids[api_id] = row["match_id"]
        stamped.append((row, api_id))

    print(f"\nWould stamp: {len(stamped)}")
    print(f"No confident match (likely a not-yet-published fixture): {len(unmatched)}")
    print(f"Score mismatch (NOT stamped, needs manual review): {len(score_mismatch)}")
    print(f"Same TheStatsAPI id matched twice (NOT stamped, needs manual review): {len(reused_id)}")

    if score_mismatch:
        print("\n=== SCORE MISMATCHES ===")
        for row, m, api_hs, api_aws in score_mismatch:
            print(f"  match_id={row['match_id']} {row['home']} v {row['away']} "
                  f"({row['match_date'][:10]}): db=({row['home_score']},{row['away_score']}) "
                  f"api=({api_hs},{api_aws}) [{m['id']}]")

    if reused_id:
        print("\n=== REUSED THESTATSAPI IDS ===")
        for row, m, other_match_id in reused_id:
            print(f"  match_id={row['match_id']} {row['home']} v {row['away']} "
                  f"({row['match_date'][:10]}) -> {m['id']} already used by match_id={other_match_id}")

    if not args.apply:
        print("\nDry-run only -- no writes made. Re-run with --apply to write.")
        return

    if score_mismatch or reused_id:
        print("\nRefusing to --apply while score mismatches or reused ids are unresolved. "
              "Fix or explicitly accept those first.")
        return

    conn = sqlite3.connect(DATABASE_PATH)
    for row, api_id in stamped:
        set_thestatsapi_match_id(row["match_id"], api_id, conn=conn)
    conn.close()
    print(f"\nStamped thestatsapi_match_id on {len(stamped)} row(s).")


if __name__ == "__main__":
    main()
