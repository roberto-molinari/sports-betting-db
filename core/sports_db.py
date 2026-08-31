"""
Sports Betting Database - Schema and Core Functions
Supports Serie A (Soccer) and NHL (Hockey) with sport-specific tables.

Tables:
  soccer_teams, soccer_matches, soccer_betting_odds, soccer_model_predictions,
  soccer_market_odds
  soccer_players, soccer_player_stats, soccer_player_match_lineups,
  soccer_player_team_strength  (FEATURE-011)
  nhl_teams,    nhl_matches,    nhl_betting_odds
  soccer_wc_teams, soccer_wc_players, soccer_wc_player_stats,
  soccer_wc_matches, soccer_wc_odds, soccer_wc_team_strength, soccer_wc_picks
"""

import sqlite3
from pathlib import Path

DATABASE_PATH = Path(__file__).resolve().parent.parent / "sports_betting.db"


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
            -- Kept LAST to match the migrated column order (see soccer_wc_matches note
            -- on this same pattern). FEATURE-011: TheStatsAPI's match id, resolved by
            -- team+date matching, so per-match player-stats/lineups calls can be made.
            -- Named for the specific source (renamed from api_match_id 2026-08-10,
            -- multi-league expansion) now that a second source (football-data.co.uk)
            -- is also in play for these same tables.
            thestatsapi_match_id TEXT,
            FOREIGN KEY (home_team_id) REFERENCES soccer_teams(team_id),
            FOREIGN KEY (away_team_id) REFERENCES soccer_teams(team_id)
        );

        CREATE TABLE IF NOT EXISTS soccer_betting_odds (
            odds_id          INTEGER PRIMARY KEY,
            match_id         INTEGER NOT NULL,
            sportsbook       TEXT NOT NULL,
            odds_date        TIMESTAMP NOT NULL,
            home_moneyline   REAL,
            draw_moneyline   REAL,
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

        -- ── World Cup 2026 tables ─────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS soccer_wc_teams (
            team_id       INTEGER PRIMARY KEY,
            name          TEXT NOT NULL UNIQUE,
            confederation TEXT,
            fifa_ranking  INTEGER,
            api_team_id   TEXT,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS soccer_wc_players (
            player_id     INTEGER PRIMARY KEY,
            team_id       INTEGER NOT NULL,
            name          TEXT NOT NULL,
            position      TEXT,
            club          TEXT,
            club_league   TEXT,
            api_player_id TEXT,
            api_club_id   TEXT,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (team_id) REFERENCES soccer_wc_teams(team_id)
        );

        CREATE TABLE IF NOT EXISTS soccer_wc_player_stats (
            stat_id        INTEGER PRIMARY KEY,
            player_id      INTEGER NOT NULL,
            season         INTEGER,
            minutes_played INTEGER,
            xg             REAL,
            xg_per90       REAL,
            goals          INTEGER,
            assists        INTEGER,
            club_xga_per90 REAL,
            club_ga_per90  REAL,
            source         TEXT,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (player_id) REFERENCES soccer_wc_players(player_id)
        );

        CREATE TABLE IF NOT EXISTS soccer_wc_matches (
            match_id     INTEGER PRIMARY KEY,
            match_date   TIMESTAMP NOT NULL,
            stage        TEXT,
            grp          TEXT,
            home_team_id INTEGER NOT NULL,
            away_team_id INTEGER NOT NULL,
            home_score   INTEGER,                  -- 90' regulation score (also grades 1X2 + O/U)
            away_score   INTEGER,
            match_status TEXT,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            -- Knockout-path columns kept LAST so a freshly-created DB matches the column
            -- order of a migrated one (ensure_wc_advance_schema adds these via ALTER ADD
            -- COLUMN, which appends to the end).
            extra_time_home_score INTEGER,         -- cumulative score at end of ET (NULL if no ET)
            extra_time_away_score INTEGER,
            shootout_home_score   INTEGER,         -- penalty shootout tally (NULL if no shootout)
            shootout_away_score   INTEGER,
            decided_by   TEXT,                     -- regulation|extra_time|shootout (NULL for group)
            FOREIGN KEY (home_team_id) REFERENCES soccer_wc_teams(team_id),
            FOREIGN KEY (away_team_id) REFERENCES soccer_wc_teams(team_id),
            CHECK (decided_by IN ('regulation', 'extra_time', 'shootout') OR decided_by IS NULL)
        );

        CREATE TABLE IF NOT EXISTS soccer_wc_odds (
            odds_id        INTEGER PRIMARY KEY,
            match_id       INTEGER NOT NULL,
            sportsbook     TEXT NOT NULL,
            odds_date      TIMESTAMP NOT NULL,
            home_moneyline REAL,
            draw_moneyline REAL,
            away_moneyline REAL,
            over_under     REAL,
            over_odds      REAL,
            under_odds     REAL,
            notes          TEXT,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            -- Kept LAST to match the migrated column order (see soccer_wc_matches note).
            home_advance_ml REAL,                  -- 2-way 'to advance' odds (knockouts; NULL otherwise)
            away_advance_ml REAL,
            FOREIGN KEY (match_id) REFERENCES soccer_wc_matches(match_id)
        );

        -- No UNIQUE on team_id: multiple lambda versions per team are allowed
        -- over time (e.g. updated after the group stage).
        CREATE TABLE IF NOT EXISTS soccer_wc_team_strength (
            strength_id    INTEGER PRIMARY KEY,
            team_id        INTEGER NOT NULL,
            lambda_attack  REAL,
            lambda_defense REAL,
            method         TEXT,
            computed_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes          TEXT,
            FOREIGN KEY (team_id) REFERENCES soccer_wc_teams(team_id)
        );

        CREATE TABLE IF NOT EXISTS soccer_wc_picks (
            pick_id        INTEGER PRIMARY KEY,
            match_id       INTEGER NOT NULL,
            generated_at   TIMESTAMP NOT NULL,
            side           TEXT,
            odds           REAL,
            model_prob     REAL,
            ev             REAL,
            stars          INTEGER,
            result         TEXT,
            selection_mode TEXT,   -- FEATURE-009: 'value' | 'prediction' | 'fallback'
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES soccer_wc_matches(match_id)
        );

        -- FEATURE-008: external (non-model) per-match xG, for POST-HOC COMPARISON ONLY.
        -- HARD CONSTRAINT: never joined into or confused with the model's own xG
        -- (soccer_wc_player_stats.xg_per90) or any core-workflow table -- kept in this
        -- separate table so the two sources can never be mixed.
        CREATE TABLE IF NOT EXISTS soccer_wc_external_xg (
            external_xg_id INTEGER PRIMARY KEY,
            match_id       INTEGER NOT NULL,
            source         TEXT NOT NULL,        -- e.g. 'thestatsapi'
            home_xg        REAL,
            away_xg        REAL,
            fetched_at     TIMESTAMP,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES soccer_wc_matches(match_id)
        );

        -- A tagged human deviation from the model's pick on a match. The model's
        -- pick (soccer_wc_picks) is left untouched (single source of truth); this
        -- records what the user took INSTEAD and WHY, so model-vs-user can be scored
        -- head-to-head and the reasons mined for a systematic model improvement.
        CREATE TABLE IF NOT EXISTS soccer_wc_pick_overrides (
            override_id INTEGER PRIMARY KEY,
            match_id    INTEGER NOT NULL,
            model_side  TEXT,                 -- snapshot of the model's pick at deviation time
            model_odds  REAL,
            user_side   TEXT NOT NULL,        -- the side the user actually took
            user_odds   REAL,
            category    TEXT,                 -- short tag: form/injury/lineup/motivation/market/...
            reason      TEXT NOT NULL,        -- free-text rationale (the systematize-later signal)
            result      TEXT,                 -- graded on user_side: win/loss/push
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES soccer_wc_matches(match_id)
        );

        -- Individual penalty-shootout kicks (player-level), for PK analytics. Tournament-
        -- agnostic name (no `wc`) to seed future reuse; its match_id/team_id/player_id still
        -- reference the soccer_wc_* tables in this DB for now (see REFACTOR-001).
        CREATE TABLE IF NOT EXISTS soccer_penalty_kicks (
            penalty_kick_id INTEGER PRIMARY KEY,
            match_id    INTEGER NOT NULL,
            team_id     INTEGER NOT NULL,
            player_id   INTEGER,
            player_name TEXT,
            kick_order  INTEGER,
            result      TEXT,                       -- 'goal' | 'miss' | 'saved'
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES soccer_wc_matches(match_id),
            FOREIGN KEY (team_id)  REFERENCES soccer_wc_teams(team_id),
            CHECK (result IN ('goal', 'miss', 'saved') OR result IS NULL)
        );

        CREATE TABLE IF NOT EXISTS soccer_extra_time_goals (
            extra_time_goal_id INTEGER PRIMARY KEY,
            match_id    INTEGER NOT NULL,
            team_id     INTEGER NOT NULL,
            player_id   INTEGER,
            player_name TEXT,
            minute      INTEGER,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES soccer_wc_matches(match_id),
            FOREIGN KEY (team_id)  REFERENCES soccer_wc_teams(team_id)
        );

        -- Model output (1X2 + O/U probabilities) for a club-league match, independent of
        -- whether it was ever surfaced as a "pick". League-agnostic (soccer_matches already
        -- keys club-league rows by `league`), so this covers Serie A now and any club league
        -- added later without a schema change. `method` tags the model/version that produced
        -- the row (e.g. 'poisson_v1') so re-runs after a model change don't get confused with
        -- older rows for the same match.
        CREATE TABLE IF NOT EXISTS soccer_model_predictions (
            prediction_id   INTEGER PRIMARY KEY,
            match_id        INTEGER NOT NULL,
            league          TEXT NOT NULL,
            match_date      TIMESTAMP NOT NULL,
            generated_at    TIMESTAMP NOT NULL,
            method          TEXT,
            lambda_home     REAL,
            lambda_away     REAL,
            p_home          REAL,
            p_draw          REAL,
            p_away          REAL,
            over_under_line REAL,
            p_over          REAL,
            p_under         REAL,
            home_moneyline  REAL,
            draw_moneyline  REAL,
            away_moneyline  REAL,
            over_odds       REAL,
            under_odds      REAL,
            ev_home         REAL,
            ev_draw         REAL,
            ev_away         REAL,
            ev_over         REAL,
            ev_under        REAL,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES soccer_matches(match_id)
        );

        -- FEATURE-016 (2026-08-19): generate_club_league_card.py's picks, so a live
        -- card can be scored later -- same purpose as soccer_wc_picks below, one level
        -- up (club leagues, not the World Cup). side covers both 1X2 ('HOME'/'DRAW'/
        -- 'AWAY') and totals ('OVER 2.5'/'UNDER 2.5') since one match can produce up to
        -- MAX_PICKS_PER_MATCH picks across different markets. league is denormalized
        -- (also reachable via match_id -> soccer_matches.league) matching every other
        -- multi-league table in this schema (soccer_model_predictions, soccer_market_
        -- odds) -- cheap and avoids a join for the common "picks for this league" query.
        -- method (2026-08-21): the live pipeline's model/guardrail version tag
        -- (generate_club_league_card.CARD_MODEL_VERSION) at the moment this pick was
        -- generated -- so post-matchday performance-over-time analysis can tell model
        -- changes apart instead of silently blending picks made under different configs.
        -- Nullable: rows from before this column existed have no way to know retroactively.
        CREATE TABLE IF NOT EXISTS soccer_club_league_picks (
            pick_id      INTEGER PRIMARY KEY,
            match_id     INTEGER NOT NULL,
            league       TEXT NOT NULL,
            generated_at TIMESTAMP NOT NULL,
            side         TEXT,
            odds         REAL,
            model_prob   REAL,
            ev           REAL,
            stars        INTEGER,
            result       TEXT,
            method       TEXT,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES soccer_matches(match_id)
        );

        -- 1X2 odds from an external book (sharp: Pinnacle, Betfair Exchange; soft:
        -- Bet365), for measuring the model against a market read. `source` distinguishes
        -- the book, `line_type` distinguishes opening vs closing. `p_*_fair` is the
        -- devigged (overround-removed) implied probability, comparable directly against
        -- soccer_model_predictions.p_*.
        CREATE TABLE IF NOT EXISTS soccer_market_odds (
            market_odds_id  INTEGER PRIMARY KEY,
            match_id        INTEGER NOT NULL,
            league          TEXT NOT NULL,
            source          TEXT NOT NULL,
            line_type       TEXT NOT NULL,
            home_odds       REAL,
            draw_odds       REAL,
            away_odds       REAL,
            p_home_fair     REAL,
            p_draw_fair     REAL,
            p_away_fair     REAL,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES soccer_matches(match_id),
            CHECK (line_type IN ('opening', 'closing'))
        );

        -- FEATURE-011 prototype: player-level lambda inputs for club leagues (Serie A
        -- first). League-agnostic like soccer_model_predictions -- keys off the existing
        -- soccer_teams table (a club-league team IS the club, unlike soccer_wc_teams
        -- national teams, so no separate club table is needed).
        CREATE TABLE IF NOT EXISTS soccer_players (
            player_id     INTEGER PRIMARY KEY,
            team_id       INTEGER NOT NULL,
            name          TEXT NOT NULL,
            position      TEXT,
            api_player_id TEXT,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (team_id) REFERENCES soccer_teams(team_id)
        );

        -- Per-match, not per-season (FEATURE-011_REQUIREMENTS.md Persistence): a
        -- completed season and an in-progress season are stored identically -- one row
        -- per player per match played. Season/rolling-window totals are a query
        -- (aggregate rows over a date range), not a separate stored format. `venue`
        -- captures home/away per row from day one even though v1's lambda calculation
        -- doesn't split on it yet (Scenario 4 is deferred, the DATA isn't -- see Out of
        -- Scope for v1). Field names otherwise match soccer_wc_player_stats so the WC
        -- aggregation logic (compute_wc_team_strength.py) is reusable with minimal
        -- changes.
        CREATE TABLE IF NOT EXISTS soccer_player_stats (
            stat_id        INTEGER PRIMARY KEY,
            player_id      INTEGER NOT NULL,
            match_id       INTEGER NOT NULL,
            season         INTEGER,
            venue          TEXT,
            minutes_played INTEGER,
            xg             REAL,
            xg_per90       REAL,
            goals          INTEGER,
            assists        INTEGER,
            club_xga_per90 REAL,
            club_ga_per90  REAL,
            source         TEXT,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (player_id) REFERENCES soccer_players(player_id),
            FOREIGN KEY (match_id)  REFERENCES soccer_matches(match_id),
            CHECK (venue IN ('home', 'away') OR venue IS NULL)
        );

        -- New (FEATURE-011_REQUIREMENTS.md Persistence): real starting-lineup history,
        -- replacing the minutes-played proxy (Scenario 0). One row per player per match.
        -- Backfill depth intentionally differs from soccer_player_stats (1 season vs 3)
        -- -- rosters turn over enough that older lineups aren't a useful "who starts
        -- next" signal. Code aggregating "all per-match player data for a team" must not
        -- assume these two tables share the same date range.
        CREATE TABLE IF NOT EXISTS soccer_player_match_lineups (
            lineup_id   INTEGER PRIMARY KEY,
            player_id   INTEGER NOT NULL,
            match_id    INTEGER NOT NULL,
            team_id     INTEGER NOT NULL,
            started     BOOLEAN NOT NULL,
            position    TEXT,
            formation   TEXT,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (player_id) REFERENCES soccer_players(player_id),
            FOREIGN KEY (match_id)  REFERENCES soccer_matches(match_id),
            FOREIGN KEY (team_id)   REFERENCES soccer_teams(team_id)
        );

        -- Prototype-stage strength row: keeps the pure player-aggregation lambda, the
        -- team-level equivalent it was blended against, the resulting blend, and the
        -- weight used -- so a prototype run is self-documenting (no need to re-derive
        -- "what would team-level alone have said" later). No UNIQUE on team_id, same
        -- reasoning as soccer_wc_team_strength (multiple versions over time).
        CREATE TABLE IF NOT EXISTS soccer_player_team_strength (
            strength_id           INTEGER PRIMARY KEY,
            team_id                INTEGER NOT NULL,
            league                 TEXT NOT NULL,
            lambda_attack_player   REAL,
            lambda_defense_player  REAL,
            lambda_attack_team     REAL,
            lambda_defense_team    REAL,
            lambda_attack_blend    REAL,
            lambda_defense_blend   REAL,
            weight_attack          REAL,
            weight_defense         REAL,
            basis                  TEXT,
            computed_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes                  TEXT,
            FOREIGN KEY (team_id) REFERENCES soccer_teams(team_id)
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
        CREATE UNIQUE INDEX IF NOT EXISTS idx_nhl_match_unique
            ON nhl_matches(season, home_team_id, away_team_id, match_date);

        CREATE INDEX IF NOT EXISTS idx_wc_match_date      ON soccer_wc_matches(match_date);
        CREATE INDEX IF NOT EXISTS idx_wc_team_name       ON soccer_wc_teams(name);
        CREATE INDEX IF NOT EXISTS idx_wc_players_team    ON soccer_wc_players(team_id);
        CREATE INDEX IF NOT EXISTS idx_wc_stats_player    ON soccer_wc_player_stats(player_id);
        CREATE INDEX IF NOT EXISTS idx_wc_odds_match      ON soccer_wc_odds(match_id);
        CREATE INDEX IF NOT EXISTS idx_wc_strength_team   ON soccer_wc_team_strength(team_id);
        CREATE INDEX IF NOT EXISTS idx_wc_picks_match     ON soccer_wc_picks(match_id);
        CREATE INDEX IF NOT EXISTS idx_wc_overrides_match ON soccer_wc_pick_overrides(match_id);
        CREATE INDEX IF NOT EXISTS idx_wc_external_xg_match ON soccer_wc_external_xg(match_id);
        CREATE INDEX IF NOT EXISTS idx_penalty_kicks_match    ON soccer_penalty_kicks(match_id);
        CREATE INDEX IF NOT EXISTS idx_extra_time_goals_match ON soccer_extra_time_goals(match_id);
        CREATE INDEX IF NOT EXISTS idx_model_predictions_match ON soccer_model_predictions(match_id);
        CREATE INDEX IF NOT EXISTS idx_model_predictions_league ON soccer_model_predictions(league);
        CREATE INDEX IF NOT EXISTS idx_club_league_picks_match  ON soccer_club_league_picks(match_id);
        CREATE INDEX IF NOT EXISTS idx_club_league_picks_league ON soccer_club_league_picks(league);
        CREATE INDEX IF NOT EXISTS idx_market_odds_match  ON soccer_market_odds(match_id);
        CREATE INDEX IF NOT EXISTS idx_market_odds_source ON soccer_market_odds(source);
        CREATE INDEX IF NOT EXISTS idx_market_odds_type   ON soccer_market_odds(line_type);
        CREATE INDEX IF NOT EXISTS idx_players_team          ON soccer_players(team_id);
        CREATE INDEX IF NOT EXISTS idx_player_stats_player    ON soccer_player_stats(player_id);
        CREATE INDEX IF NOT EXISTS idx_player_stats_season    ON soccer_player_stats(season);
        CREATE INDEX IF NOT EXISTS idx_player_team_strength_team ON soccer_player_team_strength(team_id);
        CREATE INDEX IF NOT EXISTS idx_player_team_strength_league ON soccer_player_team_strength(league);
        CREATE INDEX IF NOT EXISTS idx_player_lineups_player  ON soccer_player_match_lineups(player_id);
        CREATE INDEX IF NOT EXISTS idx_player_lineups_match   ON soccer_player_match_lineups(match_id);
        CREATE INDEX IF NOT EXISTS idx_player_lineups_team    ON soccer_player_match_lineups(team_id);
    ''')

    ensure_soccer_betting_odds_schema(conn)
    ensure_wc_team_strength_schema(conn)
    ensure_wc_advance_schema(conn)
    ensure_wc_picks_schema(conn)
    ensure_player_stats_match_schema(conn)

    conn.commit()
    conn.close()
    print(f"Database initialized at {DATABASE_PATH}")


def ensure_club_league_picks_schema(conn=None):
    """Add the `method` column to soccer_club_league_picks on older databases
    (2026-08-21 -- see that column's comment in init_database)."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(soccer_club_league_picks)")
        existing = {row[1] for row in cur.fetchall()}
        if "method" not in existing:
            cur.execute("ALTER TABLE soccer_club_league_picks ADD COLUMN method TEXT")
        conn.commit()
    finally:
        if owns_connection:
            conn.close()


def ensure_wc_team_strength_schema(conn=None):
    """Add the `method` column to soccer_wc_team_strength on older databases."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(soccer_wc_team_strength)")
        existing = {row[1] for row in cur.fetchall()}
        if "method" not in existing:
            cur.execute("ALTER TABLE soccer_wc_team_strength ADD COLUMN method TEXT")
        conn.commit()
    finally:
        if owns_connection:
            conn.close()


def ensure_wc_advance_schema(conn=None):
    """Add to-advance / knockout-path columns to older databases (idempotent).

    The two new event tables (soccer_penalty_kicks, soccer_extra_time_goals) are created by
    the CREATE TABLE IF NOT EXISTS block in init_database, so only column adds are needed here.
    """
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(soccer_wc_matches)")
        match_cols = {row[1] for row in cur.fetchall()}
        for col in ("extra_time_home_score", "extra_time_away_score",
                    "shootout_home_score", "shootout_away_score"):
            if col not in match_cols:
                cur.execute(f"ALTER TABLE soccer_wc_matches ADD COLUMN {col} INTEGER")
        if "decided_by" not in match_cols:
            cur.execute("ALTER TABLE soccer_wc_matches ADD COLUMN decided_by TEXT "
                        "CHECK (decided_by IN ('regulation', 'extra_time', 'shootout') "
                        "OR decided_by IS NULL)")

        cur.execute("PRAGMA table_info(soccer_wc_odds)")
        odds_cols = {row[1] for row in cur.fetchall()}
        for col in ("home_advance_ml", "away_advance_ml"):
            if col not in odds_cols:
                cur.execute(f"ALTER TABLE soccer_wc_odds ADD COLUMN {col} REAL")
        conn.commit()
    finally:
        if owns_connection:
            conn.close()


def ensure_wc_picks_schema(conn=None):
    """Add the `selection_mode` column to soccer_wc_picks on older databases
    (FEATURE-009: which of value/prediction/fallback mode chose the pick)."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(soccer_wc_picks)")
        existing = {row[1] for row in cur.fetchall()}
        if "selection_mode" not in existing:
            cur.execute("ALTER TABLE soccer_wc_picks ADD COLUMN selection_mode TEXT")
        conn.commit()
    finally:
        if owns_connection:
            conn.close()


def ensure_soccer_betting_odds_schema(conn=None):
    """Add newer soccer odds columns to older databases if they are missing."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)

    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(soccer_betting_odds)")
        existing_columns = {row[1] for row in cur.fetchall()}

        if "draw_moneyline" not in existing_columns:
            cur.execute("ALTER TABLE soccer_betting_odds ADD COLUMN draw_moneyline REAL")

        conn.commit()
    finally:
        if owns_connection:
            conn.close()


def ensure_player_stats_match_schema(conn=None):
    """Add match_id/venue to soccer_player_stats and thestatsapi_match_id to
    soccer_matches on databases created before per-match player stats existed.

    Existing rows from the season-total prototype are left in place with match_id/venue
    NULL -- they predate this schema and are superseded by re-running the per-match
    import, not migrated in place."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()

        cur.execute("PRAGMA table_info(soccer_player_stats)")
        existing = {row[1] for row in cur.fetchall()}
        if "match_id" not in existing:
            cur.execute("ALTER TABLE soccer_player_stats ADD COLUMN match_id INTEGER")
        if "venue" not in existing:
            cur.execute("ALTER TABLE soccer_player_stats ADD COLUMN venue TEXT")

        cur.execute("PRAGMA table_info(soccer_matches)")
        existing = {row[1] for row in cur.fetchall()}
        if "thestatsapi_match_id" not in existing:
            if "api_match_id" in existing:
                # 2026-08-10 rename (multi-league expansion) -- preserve already-
                # populated values (e.g. Serie B's TheStatsAPI-sourced matches)
                # instead of adding a second, empty column.
                cur.execute("ALTER TABLE soccer_matches RENAME COLUMN api_match_id TO thestatsapi_match_id")
            else:
                cur.execute("ALTER TABLE soccer_matches ADD COLUMN thestatsapi_match_id TEXT")

        # Created here, not in the main executescript, because on a migrated (not
        # freshly-created) database the match_id column doesn't exist until the ALTER
        # TABLE above runs.
        cur.execute("CREATE INDEX IF NOT EXISTS idx_player_stats_match ON soccer_player_stats(match_id)")
        cur.execute("DROP INDEX IF EXISTS idx_matches_api_match_id")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_matches_thestatsapi_match_id ON soccer_matches(thestatsapi_match_id)")

        conn.commit()
    finally:
        if owns_connection:
            conn.close()


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
    """Insert a soccer team if it doesn't exist; return its team_id.

    `name` alone is the unique key (not `name`+`league`) -- deliberate, so a
    promoted team (e.g. Cremonese, Serie B -> Serie A) keeps the SAME team_id
    across a division change, which cross-league player history (BUG-010) depends
    on. Raises if `country` conflicts with an existing same-named team's country --
    that's two different clubs sharing a name, not one club changing division, so
    this refuses to silently merge them."""
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
        cur.execute("SELECT team_id, country FROM soccer_teams WHERE name = ?", (name,))
        existing_id, existing_country = cur.fetchone()
        # Both sides must be known to raise -- if either is None there's nothing to
        # compare, so this falls through and reuses the row (the old, pre-check
        # behavior) rather than raising on incomplete information. Keeps any caller
        # that doesn't pass country working unchanged; the tradeoff is a real
        # cross-country collision on a None-country row would still silently merge.
        if country is not None and existing_country is not None and country != existing_country:
            raise ValueError(
                f"ensure_soccer_team: {name!r} already exists as team_id={existing_id} "
                f"with country={existing_country!r}, but this call passed country={country!r} "
                f"-- looks like two different clubs sharing a name, not the same club "
                f"changing division. Not silently merging; resolve the name collision explicitly."
            )
        return existing_id
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


def update_soccer_match_date(match_id, match_date):
    """Correct a soccer match's kickoff date/time -- e.g. a source-side
    reschedule (TV move, postponement) caught by import_league_matches.py's
    conflict detection (BUG-024, 2026-08-31: this had no write path at all --
    a flagged match_date conflict never actually got applied, even under
    --allow-overwrite, because the only apply path was update_soccer_match_result,
    which only ever touches score/status)."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE soccer_matches SET match_date = ? WHERE match_id = ?",
            (match_date, match_id)
        )
        conn.commit()
    finally:
        conn.close()


def add_soccer_betting_odds(match_id, sportsbook, odds_date,
                             home_moneyline=None, draw_moneyline=None,
                             away_moneyline=None,
                             spread_home=None, spread_away=None,
                             spread_home_odds=None, spread_away_odds=None,
                             over_under=None, over_odds=None, under_odds=None,
                             notes=None):
    """Insert betting odds for a soccer match; return odds_id."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        ensure_soccer_betting_odds_schema(conn)
        cur.execute(
            """INSERT INTO soccer_betting_odds
               (match_id, sportsbook, odds_date,
            home_moneyline, draw_moneyline, away_moneyline,
                spread_home, spread_away, spread_home_odds, spread_away_odds,
                over_under, over_odds, under_odds, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (match_id, sportsbook, odds_date,
             home_moneyline, draw_moneyline, away_moneyline,
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


def clear_soccer_model_predictions(league, season, method, conn=None):
    """Delete prior prediction rows for a league/season/method so a re-run of the
    same backfill doesn't accumulate duplicates. season is matched via soccer_matches."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """DELETE FROM soccer_model_predictions
               WHERE league = ? AND method = ?
                 AND match_id IN (
                     SELECT match_id FROM soccer_matches WHERE league = ? AND season = ?
                 )""",
            (league, method, league, season)
        )
        conn.commit()
    finally:
        if owns_connection:
            conn.close()


def add_soccer_model_prediction(match_id, league, match_date, generated_at, method=None,
                                 lambda_home=None, lambda_away=None,
                                 p_home=None, p_draw=None, p_away=None,
                                 over_under_line=None, p_over=None, p_under=None,
                                 home_moneyline=None, draw_moneyline=None, away_moneyline=None,
                                 over_odds=None, under_odds=None,
                                 ev_home=None, ev_draw=None, ev_away=None,
                                 ev_over=None, ev_under=None, conn=None):
    """Insert a model-prediction row (1X2 + O/U probabilities) for a soccer match;
    return prediction_id."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO soccer_model_predictions
               (match_id, league, match_date, generated_at, method,
                lambda_home, lambda_away, p_home, p_draw, p_away,
                over_under_line, p_over, p_under,
                home_moneyline, draw_moneyline, away_moneyline, over_odds, under_odds,
                ev_home, ev_draw, ev_away, ev_over, ev_under)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (match_id, league, match_date, generated_at, method,
             lambda_home, lambda_away, p_home, p_draw, p_away,
             over_under_line, p_over, p_under,
             home_moneyline, draw_moneyline, away_moneyline, over_odds, under_odds,
             ev_home, ev_draw, ev_away, ev_over, ev_under)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        if owns_connection:
            conn.close()


def clear_soccer_market_odds(league, season, source, line_type, conn=None):
    """Delete prior market-odds rows for a league/season/source/line_type so a
    re-run of an import doesn't accumulate duplicates."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """DELETE FROM soccer_market_odds
               WHERE league = ? AND source = ? AND line_type = ?
                 AND match_id IN (
                     SELECT match_id FROM soccer_matches WHERE league = ? AND season = ?
                 )""",
            (league, source, line_type, league, season)
        )
        conn.commit()
    finally:
        if owns_connection:
            conn.close()


def add_soccer_market_odds(match_id, league, source, line_type,
                            home_odds=None, draw_odds=None, away_odds=None,
                            p_home_fair=None, p_draw_fair=None, p_away_fair=None,
                            conn=None):
    """Insert a market-odds row (opening or closing, devigged fair probabilities)
    for a soccer match; return market_odds_id."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO soccer_market_odds
               (match_id, league, source, line_type, home_odds, draw_odds, away_odds,
                p_home_fair, p_draw_fair, p_away_fair)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (match_id, league, source, line_type, home_odds, draw_odds, away_odds,
             p_home_fair, p_draw_fair, p_away_fair)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        if owns_connection:
            conn.close()


# ── World Cup 2026 helpers ──────────────────────────────────────────────────────

def ensure_wc_team(name, confederation=None, fifa_ranking=None, api_team_id=None):
    """Insert a World Cup national team if it doesn't exist; return its team_id.

    If the team already exists, fill in api_team_id when it was previously unset.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO soccer_wc_teams (name, confederation, fifa_ranking, api_team_id)
               VALUES (?, ?, ?, ?)""",
            (name, confederation, fifa_ranking, api_team_id)
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        cur.execute("SELECT team_id FROM soccer_wc_teams WHERE name = ?", (name,))
        team_id = cur.fetchone()[0]
        # Backfill api id / ranking / confederation if they were previously unset.
        cur.execute(
            """UPDATE soccer_wc_teams
               SET api_team_id = COALESCE(api_team_id, ?),
                   fifa_ranking = COALESCE(fifa_ranking, ?),
                   confederation = COALESCE(confederation, ?)
               WHERE team_id = ?""",
            (api_team_id, fifa_ranking, confederation, team_id)
        )
        conn.commit()
        return team_id
    finally:
        conn.close()


def get_wc_team_id(name):
    """Return team_id for a World Cup team, or None if not found."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    cur.execute("SELECT team_id FROM soccer_wc_teams WHERE name = ?", (name,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def add_wc_player(team_id, name, position=None, club=None, club_league=None,
                  api_player_id=None, api_club_id=None):
    """Insert a squad player if not already present for this team; return player_id.

    If the player already exists, refresh club/position/api fields (a squad
    re-import may carry newer club info).
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT player_id FROM soccer_wc_players WHERE team_id = ? AND name = ?",
            (team_id, name)
        )
        existing = cur.fetchone()
        if existing:
            cur.execute(
                """UPDATE soccer_wc_players
                   SET position = COALESCE(?, position),
                       club = COALESCE(?, club),
                       club_league = COALESCE(?, club_league),
                       api_player_id = COALESCE(?, api_player_id),
                       api_club_id = COALESCE(?, api_club_id)
                   WHERE player_id = ?""",
                (position, club, club_league, api_player_id, api_club_id, existing[0])
            )
            conn.commit()
            return existing[0]
        cur.execute(
            """INSERT INTO soccer_wc_players
               (team_id, name, position, club, club_league, api_player_id, api_club_id)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (team_id, name, position, club, club_league, api_player_id, api_club_id)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def upsert_wc_player_stats(player_id, season=None, minutes_played=None,
                           xg=None, xg_per90=None, goals=None, assists=None,
                           club_xga_per90=None, club_ga_per90=None, source=None):
    """Insert or replace a player's club stat line for a season; return stat_id."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT stat_id FROM soccer_wc_player_stats WHERE player_id = ? AND season IS ?",
            (player_id, season)
        )
        existing = cur.fetchone()
        if existing:
            # COALESCE: only overwrite a column when a new (non-None) value is
            # given, so a partial update doesn't wipe previously stored fields.
            cur.execute(
                """UPDATE soccer_wc_player_stats
                   SET minutes_played = COALESCE(?, minutes_played),
                       xg             = COALESCE(?, xg),
                       xg_per90       = COALESCE(?, xg_per90),
                       goals          = COALESCE(?, goals),
                       assists        = COALESCE(?, assists),
                       club_xga_per90 = COALESCE(?, club_xga_per90),
                       club_ga_per90  = COALESCE(?, club_ga_per90),
                       source         = COALESCE(?, source)
                   WHERE stat_id = ?""",
                (minutes_played, xg, xg_per90, goals, assists,
                 club_xga_per90, club_ga_per90, source, existing[0])
            )
            conn.commit()
            return existing[0]
        cur.execute(
            """INSERT INTO soccer_wc_player_stats
               (player_id, season, minutes_played, xg, xg_per90, goals, assists,
                club_xga_per90, club_ga_per90, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (player_id, season, minutes_played, xg, xg_per90, goals, assists,
             club_xga_per90, club_ga_per90, source)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def ensure_wc_match(match_date, home_team_id, away_team_id,
                    stage=None, grp=None, status="scheduled"):
    """Insert or reuse a World Cup match; return its match_id.

    Matches are keyed on (home_team_id, away_team_id, match_date) so re-imports
    do not create duplicates.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT match_id FROM soccer_wc_matches
               WHERE home_team_id = ? AND away_team_id = ? AND match_date = ?""",
            (home_team_id, away_team_id, match_date)
        )
        existing = cur.fetchone()
        if existing:
            return existing[0]
        cur.execute(
            """INSERT INTO soccer_wc_matches
               (match_date, stage, grp, home_team_id, away_team_id, match_status)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (match_date, stage, grp, home_team_id, away_team_id, status)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_wc_match_result(match_id, home_score, away_score):
    """Update final score for a World Cup match."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE soccer_wc_matches
               SET home_score = ?, away_score = ?, match_status = 'completed'
               WHERE match_id = ?""",
            (home_score, away_score, match_id)
        )
        conn.commit()
    finally:
        conn.close()


def upsert_wc_odds(match_id, sportsbook, odds_date,
                   home_moneyline=None, draw_moneyline=None, away_moneyline=None,
                   over_under=None, over_odds=None, under_odds=None,
                   home_advance_ml=None, away_advance_ml=None, notes=None):
    """Insert or update odds for a World Cup match + sportsbook; return odds_id.

    home_advance_ml/away_advance_ml carry the 2-way knockout 'to advance' market (NULL for
    group games / books that don't post it); the rest is the 90' 1X2 + O/U as before.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT odds_id FROM soccer_wc_odds WHERE match_id = ? AND sportsbook = ?",
            (match_id, sportsbook)
        )
        existing = cur.fetchone()
        if existing:
            cur.execute(
                """UPDATE soccer_wc_odds
                   SET odds_date = ?, home_moneyline = ?, draw_moneyline = ?,
                       away_moneyline = ?, over_under = ?, over_odds = ?,
                       under_odds = ?, home_advance_ml = ?, away_advance_ml = ?, notes = ?
                   WHERE odds_id = ?""",
                (odds_date, home_moneyline, draw_moneyline, away_moneyline,
                 over_under, over_odds, under_odds, home_advance_ml, away_advance_ml,
                 notes, existing[0])
            )
            conn.commit()
            return existing[0]
        cur.execute(
            """INSERT INTO soccer_wc_odds
               (match_id, sportsbook, odds_date, home_moneyline, draw_moneyline,
                away_moneyline, over_under, over_odds, under_odds,
                home_advance_ml, away_advance_ml, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (match_id, sportsbook, odds_date, home_moneyline, draw_moneyline,
             away_moneyline, over_under, over_odds, under_odds,
             home_advance_ml, away_advance_ml, notes)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def set_wc_match_advance_result(match_id, regulation_home, regulation_away,
                                extra_time_home=None, extra_time_away=None,
                                shootout_home=None, shootout_away=None, decided_by=None):
    """Record a knockout match's full path and mark it completed.

    The 90' regulation score goes in home_score/away_score (so 1X2 + O/U grade unchanged);
    extra-time / shootout tallies and decided_by capture the rest. Who advanced is DERIVED
    from these fields (see core.grading.advancing_side), not stored.
    """
    if decided_by not in (None, "regulation", "extra_time", "shootout"):
        raise ValueError(f"invalid decided_by: {decided_by!r}")
    conn = sqlite3.connect(DATABASE_PATH)
    try:
        conn.execute(
            """UPDATE soccer_wc_matches
               SET home_score = ?, away_score = ?,
                   extra_time_home_score = ?, extra_time_away_score = ?,
                   shootout_home_score = ?, shootout_away_score = ?,
                   decided_by = ?, match_status = 'completed'
               WHERE match_id = ?""",
            (regulation_home, regulation_away, extra_time_home, extra_time_away,
             shootout_home, shootout_away, decided_by, match_id))
        conn.commit()
    finally:
        conn.close()


def add_penalty_kick(match_id, team_id, kick_order=None, result=None,
                     player_id=None, player_name=None):
    """Record one penalty-shootout kick; return penalty_kick_id."""
    if result not in (None, "goal", "miss", "saved"):
        raise ValueError(f"invalid penalty result: {result!r}")
    conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.execute(
            """INSERT INTO soccer_penalty_kicks
               (match_id, team_id, player_id, player_name, kick_order, result)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (match_id, team_id, player_id, player_name, kick_order, result))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def add_extra_time_goal(match_id, team_id, minute=None, player_id=None, player_name=None):
    """Record one extra-time goal; return extra_time_goal_id."""
    conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.execute(
            """INSERT INTO soccer_extra_time_goals
               (match_id, team_id, player_id, player_name, minute)
               VALUES (?, ?, ?, ?, ?)""",
            (match_id, team_id, player_id, player_name, minute))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def set_wc_team_strength(team_id, lambda_attack, lambda_defense,
                         method=None, notes=None):
    """Persist a new lambda version for a team; return strength_id.

    `method` records how the lambdas were derived ('player_aggregation' or
    'fifa_ranking') so we can track, per team, which approach is in use and flip
    data-quality-poor teams to the FIFA fallback over time.

    Always inserts a new row (no UNIQUE on team_id) so prior versions are kept.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO soccer_wc_team_strength
               (team_id, lambda_attack, lambda_defense, method, notes)
               VALUES (?, ?, ?, ?, ?)""",
            (team_id, lambda_attack, lambda_defense, method, notes)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_latest_wc_strength(team_id, conn=None):
    """Return the most recent (lambda_attack, lambda_defense) for a team, or None."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT lambda_attack, lambda_defense
               FROM soccer_wc_team_strength
               WHERE team_id = ?
               ORDER BY computed_at DESC, strength_id DESC
               LIMIT 1""",
            (team_id,)
        )
        row = cur.fetchone()
        return (row[0], row[1]) if row else None
    finally:
        if owns_connection:
            conn.close()


def add_wc_pick(match_id, generated_at, side, odds, model_prob, ev, stars,
                result=None, selection_mode=None):
    """Store a generated pick for later scoring; return pick_id."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO soccer_wc_picks
               (match_id, generated_at, side, odds, model_prob, ev, stars, result,
                selection_mode)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (match_id, generated_at, side, odds, model_prob, ev, stars, result,
             selection_mode)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def replace_wc_pick(match_id, generated_at, side, odds, model_prob, ev, stars,
                    result=None, selection_mode=None):
    """Store a pick for a match, replacing any prior *ungraded* pick for that
    match so re-running the card supersedes (rather than stacks) picks. Already
    graded picks (result IS NOT NULL) are left intact — once a pick is settled
    it's locked history. Returns the new pick_id.

    The DB is the record of what the model picked, one current pick per match;
    re-running after a model improvement should overwrite the prior pick, not
    create a duplicate."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM soccer_wc_picks WHERE match_id = ? AND result IS NULL",
            (match_id,)
        )
        cur.execute(
            """INSERT INTO soccer_wc_picks
               (match_id, generated_at, side, odds, model_prob, ev, stars, result,
                selection_mode)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (match_id, generated_at, side, odds, model_prob, ev, stars, result,
             selection_mode)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def set_wc_pick_result(pick_id, result):
    """Grade a stored pick: result is 'win', 'loss', or 'push'."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE soccer_wc_picks SET result = ? WHERE pick_id = ?",
            (result, pick_id)
        )
        conn.commit()
    finally:
        conn.close()


def replace_club_league_picks_for_match(match_id, league, generated_at, picks, method=None, conn=None):
    """Store generate_club_league_card.py's picks for one match, replacing any
    prior *ungraded* picks for that match (result IS NULL) so re-running the card
    supersedes rather than stacks -- same "DB is the current record, already-
    graded picks are locked history" contract as replace_wc_pick (FEATURE-016).

    Deletes/inserts for the WHOLE match in one call (not per-pick) because one
    match can produce up to MAX_PICKS_PER_MATCH picks across different markets
    (e.g. HOME and OVER 2.5 together) -- a per-pick delete-then-insert would wipe
    out a same-match pick just inserted moments earlier.

    picks: list of dicts, each with side/odds/prob/ev and optionally stars --
    matches the candidate dicts generate_club_league_card.py already builds.
    method: the model/guardrail version tag in effect for this run (2026-08-21,
    generate_club_league_card.CARD_MODEL_VERSION) -- None if the caller doesn't
    track one. Returns the list of new pick_ids, in the same order as `picks`."""
    owns_conn = conn is None
    if owns_conn:
        conn = sqlite3.connect(DATABASE_PATH)
    ensure_club_league_picks_schema(conn=conn)
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM soccer_club_league_picks WHERE match_id = ? AND result IS NULL",
            (match_id,)
        )
        pick_ids = []
        for p in picks:
            cur.execute(
                """INSERT INTO soccer_club_league_picks
                   (match_id, league, generated_at, side, odds, model_prob, ev, stars, method)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (match_id, league, generated_at, p["side"], p["odds"], p["prob"],
                 p["ev"], p.get("stars"), method)
            )
            pick_ids.append(cur.lastrowid)
        if owns_conn:
            conn.commit()
        return pick_ids
    finally:
        if owns_conn:
            conn.close()


def set_club_league_pick_result(pick_id, result, conn=None):
    """Grade a stored club-league pick: result is 'win', 'loss', or 'push'."""
    owns_conn = conn is None
    if owns_conn:
        conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE soccer_club_league_picks SET result = ? WHERE pick_id = ?",
            (result, pick_id)
        )
        if owns_conn:
            conn.commit()
    finally:
        if owns_conn:
            conn.close()


def add_wc_pick_override(match_id, user_side, user_odds, reason,
                         category=None, model_side=None, model_odds=None):
    """Record a tagged human deviation from the model's pick on a match. Leaves
    soccer_wc_picks untouched (the model's record stays the source of truth); this
    captures what the user took instead and WHY, so the two can be scored
    head-to-head and the reasons mined for a systematic model fix. Returns the
    override_id. Re-recording for a match supersedes any ungraded prior override."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM soccer_wc_pick_overrides WHERE match_id = ? AND result IS NULL",
            (match_id,)
        )
        cur.execute(
            """INSERT INTO soccer_wc_pick_overrides
               (match_id, model_side, model_odds, user_side, user_odds, category, reason)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (match_id, model_side, model_odds, user_side, user_odds, category, reason)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def set_wc_override_result(override_id, result):
    """Grade a user override on its user_side: result is 'win', 'loss', or 'push'."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE soccer_wc_pick_overrides SET result = ? WHERE override_id = ?",
            (result, override_id)
        )
        conn.commit()
    finally:
        conn.close()


def upsert_wc_external_xg(match_id, source, home_xg, away_xg, fetched_at):
    """Insert or update external (non-model) per-match xG for FEATURE-008 comparison
    only; return external_xg_id. Kept in its own table so it can never be confused
    with the model's own xG (see soccer_wc_external_xg's schema comment)."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT external_xg_id FROM soccer_wc_external_xg WHERE match_id = ? AND source = ?",
            (match_id, source)
        )
        existing = cur.fetchone()
        if existing:
            cur.execute(
                """UPDATE soccer_wc_external_xg
                   SET home_xg = ?, away_xg = ?, fetched_at = ?
                   WHERE external_xg_id = ?""",
                (home_xg, away_xg, fetched_at, existing[0])
            )
            conn.commit()
            return existing[0]
        cur.execute(
            """INSERT INTO soccer_wc_external_xg (match_id, source, home_xg, away_xg, fetched_at)
               VALUES (?, ?, ?, ?, ?)""",
            (match_id, source, home_xg, away_xg, fetched_at)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


# ── NHL helpers ────────────────────────────────────────────────────────────────

def canonical_nhl_team_name(team_name):
    """Normalize known NHL naming variants used by external feeds."""
    aliases = {
        "Montreal Canadiens": "Montréal Canadiens",
    }
    return aliases.get(team_name, team_name)

def get_nhl_team_id(team_name):
    """Return team_id for an NHL team, or None if not found."""
    canonical_name = canonical_nhl_team_name(team_name)
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    cur.execute("SELECT team_id FROM nhl_teams WHERE name = ?", (canonical_name,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def ensure_nhl_team(name, country=None):
    """Insert an NHL team if it doesn't exist; return its team_id."""
    canonical_name = canonical_nhl_team_name(name)
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO nhl_teams (name, country) VALUES (?, ?)",
            (canonical_name, country)
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        cur.execute("SELECT team_id FROM nhl_teams WHERE name = ?", (canonical_name,))
        return cur.fetchone()[0]
    finally:
        conn.close()


def add_nhl_match(season, home_team_id, away_team_id, match_date,
                  status="scheduled"):
    """Insert or reuse an NHL match; return its match_id."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT match_id, match_status FROM nhl_matches
               WHERE season = ? AND home_team_id = ? AND away_team_id = ?
                 AND match_date = ?""",
            (season, home_team_id, away_team_id, match_date)
        )
        existing = cur.fetchone()
        if existing:
            match_id, existing_status = existing
            if existing_status != status:
                cur.execute(
                    """UPDATE nhl_matches
                       SET match_status = ?
                       WHERE match_id = ?""",
                    (status, match_id)
                )
                conn.commit()
            return match_id

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


def add_player(team_id, name, position=None, api_player_id=None, conn=None, set_team_id=True):
    """Insert a club-league player if not already present; return player_id.

    Looks up by api_player_id FIRST when given -- it's the one globally stable
    identity TheStatsAPI gives us across a player's whole career, unlike (team_id,
    name), which breaks for a player encountered via a historical match played for a
    DIFFERENT team than they're on now (a transfer). Falls back to (team_id, name)
    only when no api_player_id is available. team_id is treated as "most recently
    seen team", not history -- per-match team_id on soccer_player_stats /
    soccer_player_match_lineups is the source of truth for who played for whom when.

    set_team_id: when False, an EXISTING player's team_id is left untouched (still
    used, along with name/position, to create a brand-new player row). Pass False
    from any historical/backfill call site (a past-season or lower-division import,
    e.g. import_club_player_stats.py backfilling prior Serie B seasons) -- those
    calls process matches out of real-world chronological order relative to data
    already in the DB, so "most recently written wins" no longer means "most
    recently played for". Without this, backfilling e.g. a promoted team's PRIOR
    Serie B season after their current Serie A season is already imported would
    silently stomp team_id back to the old club, corrupting the live
    current_roster_player_ids() signal that resolve_blend_weight depends on for real
    predictions (found 2026-08-03 while designing the cross-league player-history
    ingestion, FEATURE-011). import_club_squads.py's live current-roster pull is the
    one call site that should keep set_team_id=True (default) -- it's the sole
    authority for "who's on the roster right now".
    """
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()
        existing = None
        if api_player_id:
            cur.execute(
                "SELECT player_id FROM soccer_players WHERE api_player_id = ?",
                (api_player_id,)
            )
            existing = cur.fetchone()
        if not existing:
            cur.execute(
                "SELECT player_id FROM soccer_players WHERE team_id = ? AND name = ?",
                (team_id, name)
            )
            existing = cur.fetchone()
        if existing:
            if set_team_id:
                cur.execute(
                    """UPDATE soccer_players
                       SET team_id = ?,
                           position = COALESCE(?, position),
                           api_player_id = COALESCE(?, api_player_id)
                       WHERE player_id = ?""",
                    (team_id, position, api_player_id, existing[0])
                )
            else:
                cur.execute(
                    """UPDATE soccer_players
                       SET position = COALESCE(?, position),
                           api_player_id = COALESCE(?, api_player_id)
                       WHERE player_id = ?""",
                    (position, api_player_id, existing[0])
                )
            conn.commit()
            return existing[0]
        cur.execute(
            """INSERT INTO soccer_players (team_id, name, position, api_player_id)
               VALUES (?, ?, ?, ?)""",
            (team_id, name, position, api_player_id)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        if owns_connection:
            conn.close()


def add_player_match_stats(player_id, match_id, season=None, venue=None, minutes_played=None,
                           xg=None, xg_per90=None, goals=None, assists=None,
                           club_xga_per90=None, club_ga_per90=None, source=None, conn=None):
    """Insert or update a player's stat line for ONE MATCH; return stat_id.

    Replaces the old season-scoped upsert_player_stats (FEATURE-011_REQUIREMENTS.md
    Persistence: per-match, not per-season -- season totals become a query over these
    rows, not a separately stored/upserted format). Idempotent on (player_id, match_id)
    so a re-run doesn't duplicate; COALESCE on update so a partial re-fetch doesn't
    wipe previously stored fields."""
    if venue not in (None, "home", "away"):
        raise ValueError(f"invalid venue: {venue!r}")
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT stat_id FROM soccer_player_stats WHERE player_id = ? AND match_id = ?",
            (player_id, match_id)
        )
        existing = cur.fetchone()
        if existing:
            cur.execute(
                """UPDATE soccer_player_stats
                   SET season         = COALESCE(?, season),
                       venue          = COALESCE(?, venue),
                       minutes_played = COALESCE(?, minutes_played),
                       xg             = COALESCE(?, xg),
                       xg_per90       = COALESCE(?, xg_per90),
                       goals          = COALESCE(?, goals),
                       assists        = COALESCE(?, assists),
                       club_xga_per90 = COALESCE(?, club_xga_per90),
                       club_ga_per90  = COALESCE(?, club_ga_per90),
                       source         = COALESCE(?, source)
                   WHERE stat_id = ?""",
                (season, venue, minutes_played, xg, xg_per90, goals, assists,
                 club_xga_per90, club_ga_per90, source, existing[0])
            )
            conn.commit()
            return existing[0]
        cur.execute(
            """INSERT INTO soccer_player_stats
               (player_id, match_id, season, venue, minutes_played, xg, xg_per90, goals,
                assists, club_xga_per90, club_ga_per90, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (player_id, match_id, season, venue, minutes_played, xg, xg_per90, goals,
             assists, club_xga_per90, club_ga_per90, source)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        if owns_connection:
            conn.close()


def add_player_match_lineup(player_id, match_id, team_id, started, position=None,
                            formation=None, conn=None):
    """Insert or update a player's lineup row for one match; return lineup_id.
    Idempotent on (player_id, match_id), same reasoning as add_player_match_stats."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT lineup_id FROM soccer_player_match_lineups WHERE player_id = ? AND match_id = ?",
            (player_id, match_id)
        )
        existing = cur.fetchone()
        if existing:
            cur.execute(
                """UPDATE soccer_player_match_lineups
                   SET team_id = ?, started = ?,
                       position = COALESCE(?, position), formation = COALESCE(?, formation)
                   WHERE lineup_id = ?""",
                (team_id, started, position, formation, existing[0])
            )
            conn.commit()
            return existing[0]
        cur.execute(
            """INSERT INTO soccer_player_match_lineups
               (player_id, match_id, team_id, started, position, formation)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (player_id, match_id, team_id, started, position, formation)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        if owns_connection:
            conn.close()


def set_thestatsapi_match_id(match_id, thestatsapi_match_id, conn=None):
    """Store TheStatsAPI's match id for one of our soccer_matches rows, resolved by
    team+date matching (see the club-league import scripts)."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        conn.execute(
            "UPDATE soccer_matches SET thestatsapi_match_id = ? WHERE match_id = ?",
            (thestatsapi_match_id, match_id)
        )
        conn.commit()
    finally:
        if owns_connection:
            conn.close()


def set_player_team_strength(team_id, league, lambda_attack_player=None, lambda_defense_player=None,
                             lambda_attack_team=None, lambda_defense_team=None,
                             lambda_attack_blend=None, lambda_defense_blend=None,
                             weight_attack=None, weight_defense=None, basis=None,
                             notes=None, conn=None):
    """Persist a new player/team/blend lambda row for a club-league team; return
    strength_id. Always inserts (no UNIQUE on team_id) so prior runs are kept,
    same reasoning as set_wc_team_strength."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO soccer_player_team_strength
               (team_id, league, lambda_attack_player, lambda_defense_player,
                lambda_attack_team, lambda_defense_team,
                lambda_attack_blend, lambda_defense_blend,
                weight_attack, weight_defense, basis, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (team_id, league, lambda_attack_player, lambda_defense_player,
             lambda_attack_team, lambda_defense_team,
             lambda_attack_blend, lambda_defense_blend,
             weight_attack, weight_defense, basis, notes)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        if owns_connection:
            conn.close()


def get_latest_player_team_strength(team_id, conn=None):
    """Return the most recent soccer_player_team_strength row for a team as a
    dict, or None."""
    owns_connection = conn is None
    if owns_connection:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """SELECT * FROM soccer_player_team_strength
               WHERE team_id = ?
               ORDER BY computed_at DESC, strength_id DESC
               LIMIT 1""",
            (team_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        if owns_connection:
            conn.close()


if __name__ == "__main__":
    init_database()
    print("Database setup complete!")
