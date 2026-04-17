"""
Schema migration: single-sport-agnostic tables → sport-specific tables.

Old:  teams, matches, betting_odds, match_outcomes, betting_analysis
New:  soccer_teams, soccer_matches, soccer_betting_odds,
      nhl_teams,   nhl_matches,   nhl_betting_odds

New fields added to soccer_matches: halftime_home_score, halftime_away_score
Dropped: match_outcomes, betting_analysis (both were empty)
"""

import sqlite3
import shutil
from pathlib import Path

DB_PATH = Path(__file__).parent / "sports_betting.db"
BACKUP_PATH = Path(__file__).parent / "sports_betting.db.backup_pre_migration"


def migrate():
    # ── Backup ────────────────────────────────────────────────────────────────
    print("Creating backup…")
    shutil.copy2(DB_PATH, BACKUP_PATH)
    print(f"  Backup → {BACKUP_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = OFF")
    cur = conn.cursor()

    # ── Verify source data ────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM teams WHERE sport='Soccer'")
    n_soccer_teams = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM teams WHERE sport='Hockey'")
    n_nhl_teams = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM matches WHERE sport='Soccer'")
    n_soccer_matches = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM matches WHERE sport='Hockey'")
    n_nhl_matches = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(*) FROM betting_odds bo
        JOIN matches m ON bo.match_id = m.match_id WHERE m.sport='Soccer'
    """)
    n_soccer_odds = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(*) FROM betting_odds bo
        JOIN matches m ON bo.match_id = m.match_id WHERE m.sport='Hockey'
    """)
    n_nhl_odds = cur.fetchone()[0]

    print(f"\nSource counts:")
    print(f"  soccer_teams:       {n_soccer_teams}")
    print(f"  nhl_teams:          {n_nhl_teams}")
    print(f"  soccer_matches:     {n_soccer_matches}")
    print(f"  nhl_matches:        {n_nhl_matches}")
    print(f"  soccer_betting_odds:{n_soccer_odds}")
    print(f"  nhl_betting_odds:   {n_nhl_odds}")

    # ── Rename old tables ─────────────────────────────────────────────────────
    print("\nRenaming old tables…")
    cur.execute("ALTER TABLE teams         RENAME TO _old_teams")
    cur.execute("ALTER TABLE matches       RENAME TO _old_matches")
    cur.execute("ALTER TABLE betting_odds  RENAME TO _old_betting_odds")
    # Drop the empty auxiliary tables
    cur.execute("DROP TABLE IF EXISTS match_outcomes")
    cur.execute("DROP TABLE IF EXISTS betting_analysis")

    # ── Create new tables ─────────────────────────────────────────────────────
    print("Creating new tables…")

    cur.execute('''
        CREATE TABLE soccer_teams (
            team_id   INTEGER PRIMARY KEY,
            name      TEXT NOT NULL UNIQUE,
            league    TEXT NOT NULL,
            country   TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cur.execute('''
        CREATE TABLE soccer_matches (
            match_id              INTEGER PRIMARY KEY,
            league                TEXT NOT NULL,
            season                INTEGER,
            home_team_id          INTEGER NOT NULL,
            away_team_id          INTEGER NOT NULL,
            match_date            TIMESTAMP NOT NULL,
            home_score            INTEGER,
            away_score            INTEGER,
            halftime_home_score   INTEGER,
            halftime_away_score   INTEGER,
            match_status          TEXT,
            created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (home_team_id) REFERENCES soccer_teams(team_id),
            FOREIGN KEY (away_team_id) REFERENCES soccer_teams(team_id)
        )
    ''')

    cur.execute('''
        CREATE TABLE soccer_betting_odds (
            odds_id         INTEGER PRIMARY KEY,
            match_id        INTEGER NOT NULL,
            sportsbook      TEXT NOT NULL,
            odds_date       TIMESTAMP NOT NULL,
            home_moneyline  REAL,
            away_moneyline  REAL,
            spread_home     REAL,
            spread_away     REAL,
            spread_home_odds REAL,
            spread_away_odds REAL,
            over_under      REAL,
            over_odds       REAL,
            under_odds      REAL,
            notes           TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES soccer_matches(match_id)
        )
    ''')

    cur.execute('''
        CREATE TABLE nhl_teams (
            team_id    INTEGER PRIMARY KEY,
            name       TEXT NOT NULL UNIQUE,
            country    TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cur.execute('''
        CREATE TABLE nhl_matches (
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
        )
    ''')

    cur.execute('''
        CREATE TABLE nhl_betting_odds (
            odds_id         INTEGER PRIMARY KEY,
            match_id        INTEGER NOT NULL,
            sportsbook      TEXT NOT NULL,
            odds_date       TIMESTAMP NOT NULL,
            home_moneyline  REAL,
            away_moneyline  REAL,
            spread_home     REAL,
            spread_away     REAL,
            spread_home_odds REAL,
            spread_away_odds REAL,
            over_under      REAL,
            over_odds       REAL,
            under_odds      REAL,
            notes           TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES nhl_matches(match_id)
        )
    ''')

    # ── Migrate data ──────────────────────────────────────────────────────────
    print("Migrating data…")

    cur.execute("""
        INSERT INTO soccer_teams (team_id, name, league, country, created_at)
        SELECT team_id, name, league, country, created_at
        FROM _old_teams WHERE sport = 'Soccer'
    """)

    cur.execute("""
        INSERT INTO nhl_teams (team_id, name, country, created_at)
        SELECT team_id, name, country, created_at
        FROM _old_teams WHERE sport = 'Hockey'
    """)

    cur.execute("""
        INSERT INTO soccer_matches
            (match_id, league, season, home_team_id, away_team_id,
             match_date, home_score, away_score,
             halftime_home_score, halftime_away_score,
             match_status, created_at)
        SELECT match_id, league, season, home_team_id, away_team_id,
               match_date, home_score, away_score,
               NULL, NULL,
               match_status, created_at
        FROM _old_matches WHERE sport = 'Soccer'
    """)

    cur.execute("""
        INSERT INTO nhl_matches
            (match_id, season, home_team_id, away_team_id,
             match_date, home_score, away_score, match_status, created_at)
        SELECT match_id, season, home_team_id, away_team_id,
               match_date, home_score, away_score, match_status, created_at
        FROM _old_matches WHERE sport = 'Hockey'
    """)

    cur.execute("""
        INSERT INTO soccer_betting_odds
            (odds_id, match_id, sportsbook, odds_date,
             home_moneyline, away_moneyline, spread_home, spread_away,
             spread_home_odds, spread_away_odds, over_under,
             over_odds, under_odds, notes, created_at)
        SELECT bo.odds_id, bo.match_id, bo.sportsbook, bo.odds_date,
               bo.home_moneyline, bo.away_moneyline, bo.spread_home, bo.spread_away,
               bo.spread_home_odds, bo.spread_away_odds, bo.over_under,
               bo.over_odds, bo.under_odds, bo.notes, bo.created_at
        FROM _old_betting_odds bo
        JOIN _old_matches m ON bo.match_id = m.match_id
        WHERE m.sport = 'Soccer'
    """)

    cur.execute("""
        INSERT INTO nhl_betting_odds
            (odds_id, match_id, sportsbook, odds_date,
             home_moneyline, away_moneyline, spread_home, spread_away,
             spread_home_odds, spread_away_odds, over_under,
             over_odds, under_odds, notes, created_at)
        SELECT bo.odds_id, bo.match_id, bo.sportsbook, bo.odds_date,
               bo.home_moneyline, bo.away_moneyline, bo.spread_home, bo.spread_away,
               bo.spread_home_odds, bo.spread_away_odds, bo.over_under,
               bo.over_odds, bo.under_odds, bo.notes, bo.created_at
        FROM _old_betting_odds bo
        JOIN _old_matches m ON bo.match_id = m.match_id
        WHERE m.sport = 'Hockey'
    """)

    # ── Create indexes ────────────────────────────────────────────────────────
    cur.execute("CREATE INDEX idx_soccer_match_date   ON soccer_matches(match_date)")
    cur.execute("CREATE INDEX idx_soccer_league       ON soccer_matches(league)")
    cur.execute("CREATE INDEX idx_soccer_season       ON soccer_matches(season)")
    cur.execute("CREATE INDEX idx_soccer_team_name    ON soccer_teams(name)")
    cur.execute("CREATE INDEX idx_soccer_odds_match   ON soccer_betting_odds(match_id)")
    cur.execute("CREATE INDEX idx_nhl_match_date      ON nhl_matches(match_date)")
    cur.execute("CREATE INDEX idx_nhl_season          ON nhl_matches(season)")
    cur.execute("CREATE INDEX idx_nhl_team_name       ON nhl_teams(name)")
    cur.execute("CREATE INDEX idx_nhl_odds_match      ON nhl_betting_odds(match_id)")

    # ── Verify destination counts ─────────────────────────────────────────────
    print("\nVerifying migrated counts…")
    checks = [
        ("soccer_teams",       n_soccer_teams),
        ("nhl_teams",          n_nhl_teams),
        ("soccer_matches",     n_soccer_matches),
        ("nhl_matches",        n_nhl_matches),
        ("soccer_betting_odds",n_soccer_odds),
        ("nhl_betting_odds",   n_nhl_odds),
    ]
    all_ok = True
    for table, expected in checks:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        actual = cur.fetchone()[0]
        status = "OK" if actual == expected else f"MISMATCH (expected {expected})"
        print(f"  {table:<25} {actual:>5}  {status}")
        if actual != expected:
            all_ok = False

    if not all_ok:
        conn.rollback()
        conn.close()
        raise RuntimeError("Count mismatch — rolled back. Old tables still intact.")

    # ── Drop old tables ───────────────────────────────────────────────────────
    print("\nDropping old tables…")
    cur.execute("DROP TABLE _old_betting_odds")
    cur.execute("DROP TABLE _old_matches")
    cur.execute("DROP TABLE _old_teams")

    conn.commit()
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("VACUUM")
    conn.close()

    print("\nMigration complete.")


if __name__ == "__main__":
    migrate()
