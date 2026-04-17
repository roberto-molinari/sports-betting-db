"""
Sports Betting Database - Schema and Core Functions
Supports Serie A (Soccer) and NHL (Hockey) with sport-specific tables.

Tables:
  soccer_teams, soccer_matches, soccer_betting_odds
  nhl_teams,    nhl_matches,    nhl_betting_odds
"""

import sqlite3
from pathlib import Path

DATABASE_PATH = Path(__file__).parent / "sports_betting.db"


# ── Schema ─────────────────────────────────────────────────────────────────────

def init_database():
    """Create all tables and indexes if they don't exist."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()

    cur.executescript('''
        CREATE TABLE IF NOT EXISTS soccer_teams (
            team_id    INTEGER PRIMARY KEY,
            name       TEXT NOT NULL UNIQUE,
            league     TEXT NOT NULL,
            country    TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS soccer_matches (
            match_id            INTEGER PRIMARY KEY,
            league              TEXT NOT NULL,
            season              INTEGER,
            home_team_id        INTEGER NOT NULL,
            away_team_id        INTEGER NOT NULL,
            match_date          TIMESTAMP NOT NULL,
            home_score          INTEGER,
            away_score          INTEGER,
            halftime_home_score INTEGER,
            halftime_away_score INTEGER,
            match_status        TEXT,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (home_team_id) REFERENCES soccer_teams(team_id),
            FOREIGN KEY (away_team_id) REFERENCES soccer_teams(team_id)
        );

        CREATE TABLE IF NOT EXISTS soccer_betting_odds (
            odds_id          INTEGER PRIMARY KEY,
            match_id         INTEGER NOT NULL,
            sportsbook       TEXT NOT NULL,
            odds_date        TIMESTAMP NOT NULL,
            home_moneyline   REAL,
            away_moneyline   REAL,
            spread_home      REAL,
            spread_away      REAL,
            spread_home_odds REAL,
            spread_away_odds REAL,
            over_under       REAL,
            over_odds        REAL,
            under_odds       REAL,
            notes            TEXT,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES soccer_matches(match_id)
        );

        CREATE TABLE IF NOT EXISTS nhl_teams (
            team_id    INTEGER PRIMARY KEY,
            name       TEXT NOT NULL UNIQUE,
            country    TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS nhl_matches (
            match_id     INTEGER PRIMARY KEY,
            season       INTEGER,
            home_team_id INTEGER NOT NULL,
            away_team_id INTEGER NOT NULL,
            match_date   TIMESTAMP NOT NULL,
            home_score   INTEGER,
            away_score   INTEGER,
            match_status TEXT,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (home_team_id) REFERENCES nhl_teams(team_id),
            FOREIGN KEY (away_team_id) REFERENCES nhl_teams(team_id)
        );

        CREATE TABLE IF NOT EXISTS nhl_betting_odds (
            odds_id          INTEGER PRIMARY KEY,
            match_id         INTEGER NOT NULL,
            sportsbook       TEXT NOT NULL,
            odds_date        TIMESTAMP NOT NULL,
            home_moneyline   REAL,
            away_moneyline   REAL,
            spread_home      REAL,
            spread_away      REAL,
            spread_home_odds REAL,
            spread_away_odds REAL,
            over_under       REAL,
            over_odds        REAL,
            under_odds       REAL,
            notes            TEXT,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES nhl_matches(match_id)
        );

        CREATE INDEX IF NOT EXISTS idx_soccer_match_date    ON soccer_matches(match_date);
        CREATE INDEX IF NOT EXISTS idx_soccer_league        ON soccer_matches(league);
        CREATE INDEX IF NOT EXISTS idx_soccer_season        ON soccer_matches(season);
        CREATE INDEX IF NOT EXISTS idx_soccer_team_name     ON soccer_teams(name);
        CREATE INDEX IF NOT EXISTS idx_soccer_odds_match    ON soccer_betting_odds(match_id);
        CREATE INDEX IF NOT EXISTS idx_nhl_match_date       ON nhl_matches(match_date);
        CREATE INDEX IF NOT EXISTS idx_nhl_season           ON nhl_matches(season);
        CREATE INDEX IF NOT EXISTS idx_nhl_team_name        ON nhl_teams(name);
        CREATE INDEX IF NOT EXISTS idx_nhl_odds_match       ON nhl_betting_odds(match_id);
    ''')

    conn.commit()
    conn.close()
    print(f"Database initialized at {DATABASE_PATH}")


# ── Soccer helpers ─────────────────────────────────────────────────────────────

def get_soccer_team_id(team_name):
    """Return team_id for a soccer team, or None if not found."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    cur.execute("SELECT team_id FROM soccer_teams WHERE name = ?", (team_name,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def ensure_soccer_team(name, league, country=None):
    """Insert a soccer team if it doesn't exist; return its team_id."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO soccer_teams (name, league, country) VALUES (?, ?, ?)",
            (name, league, country)
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        cur.execute("SELECT team_id FROM soccer_teams WHERE name = ?", (name,))
        return cur.fetchone()[0]
    finally:
        conn.close()


def add_soccer_match(league, season, home_team_id, away_team_id, match_date,
                     status="scheduled"):
    """Insert a soccer match; return its match_id."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO soccer_matches
               (league, season, home_team_id, away_team_id, match_date, match_status)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (league, season, home_team_id, away_team_id, match_date, status)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_soccer_match_result(match_id, home_score, away_score,
                                halftime_home=None, halftime_away=None):
    """Update final score (and optionally half-time scores) for a soccer match."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE soccer_matches
               SET home_score = ?, away_score = ?,
                   halftime_home_score = ?, halftime_away_score = ?,
                   match_status = 'completed'
               WHERE match_id = ?""",
            (home_score, away_score, halftime_home, halftime_away, match_id)
        )
        conn.commit()
    finally:
        conn.close()


def add_soccer_betting_odds(match_id, sportsbook, odds_date,
                             home_moneyline=None, away_moneyline=None,
                             spread_home=None, spread_away=None,
                             spread_home_odds=None, spread_away_odds=None,
                             over_under=None, over_odds=None, under_odds=None,
                             notes=None):
    """Insert betting odds for a soccer match; return odds_id."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO soccer_betting_odds
               (match_id, sportsbook, odds_date,
                home_moneyline, away_moneyline,
                spread_home, spread_away, spread_home_odds, spread_away_odds,
                over_under, over_odds, under_odds, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (match_id, sportsbook, odds_date,
             home_moneyline, away_moneyline,
             spread_home, spread_away, spread_home_odds, spread_away_odds,
             over_under, over_odds, under_odds, notes)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_soccer_matches(league=None, season=None, status=None):
    """Return soccer matches with team names, optionally filtered."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    clauses, params = [], []
    if league:
        clauses.append("sm.league = ?")
        params.append(league)
    if season:
        clauses.append("sm.season = ?")
        params.append(season)
    if status:
        clauses.append("sm.match_status = ?")
        params.append(status)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    cur.execute(f"""
        SELECT sm.*,
               h.name AS home_team_name,
               a.name AS away_team_name
        FROM soccer_matches sm
        JOIN soccer_teams h ON sm.home_team_id = h.team_id
        JOIN soccer_teams a ON sm.away_team_id = a.team_id
        {where}
        ORDER BY sm.match_date
    """, params)

    rows = cur.fetchall()
    conn.close()
    return rows


# ── NHL helpers ────────────────────────────────────────────────────────────────

def get_nhl_team_id(team_name):
    """Return team_id for an NHL team, or None if not found."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    cur.execute("SELECT team_id FROM nhl_teams WHERE name = ?", (team_name,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def ensure_nhl_team(name, country=None):
    """Insert an NHL team if it doesn't exist; return its team_id."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO nhl_teams (name, country) VALUES (?, ?)",
            (name, country)
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        cur.execute("SELECT team_id FROM nhl_teams WHERE name = ?", (name,))
        return cur.fetchone()[0]
    finally:
        conn.close()


def add_nhl_match(season, home_team_id, away_team_id, match_date,
                  status="scheduled"):
    """Insert an NHL match; return its match_id."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO nhl_matches
               (season, home_team_id, away_team_id, match_date, match_status)
               VALUES (?, ?, ?, ?, ?)""",
            (season, home_team_id, away_team_id, match_date, status)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_nhl_match_result(match_id, home_score, away_score):
    """Update final score for an NHL match."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE nhl_matches
               SET home_score = ?, away_score = ?, match_status = 'completed'
               WHERE match_id = ?""",
            (home_score, away_score, match_id)
        )
        conn.commit()
    finally:
        conn.close()


def add_nhl_betting_odds(match_id, sportsbook, odds_date,
                          home_moneyline=None, away_moneyline=None,
                          spread_home=None, spread_away=None,
                          spread_home_odds=None, spread_away_odds=None,
                          over_under=None, over_odds=None, under_odds=None,
                          notes=None):
    """Insert betting odds for an NHL match; return odds_id."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO nhl_betting_odds
               (match_id, sportsbook, odds_date,
                home_moneyline, away_moneyline,
                spread_home, spread_away, spread_home_odds, spread_away_odds,
                over_under, over_odds, under_odds, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (match_id, sportsbook, odds_date,
             home_moneyline, away_moneyline,
             spread_home, spread_away, spread_home_odds, spread_away_odds,
             over_under, over_odds, under_odds, notes)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_nhl_matches(season=None, status=None):
    """Return NHL matches with team names, optionally filtered."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    clauses, params = [], []
    if season:
        clauses.append("nm.season = ?")
        params.append(season)
    if status:
        clauses.append("nm.match_status = ?")
        params.append(status)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    cur.execute(f"""
        SELECT nm.*,
               h.name AS home_team_name,
               a.name AS away_team_name
        FROM nhl_matches nm
        JOIN nhl_teams h ON nm.home_team_id = h.team_id
        JOIN nhl_teams a ON nm.away_team_id = a.team_id
        {where}
        ORDER BY nm.match_date
    """, params)

    rows = cur.fetchall()
    conn.close()
    return rows


if __name__ == "__main__":
    init_database()
    print("Database setup complete!")
