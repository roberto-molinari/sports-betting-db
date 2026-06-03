"""
Generate Poisson model picks for Matchday 37 (2026-05-17).

Analyzes all 10 matches with full 1X2 + O/U coverage.
Runs Poisson model with cutoff at match_date (no data leakage).
Calculates EV for every outcome and ranks by edge.
"""

import sqlite3
from datetime import datetime, timedelta
from core.poisson_model import analyse_match, american_to_decimal

DB_PATH = 'sports_betting.db'
MATCH_DATE = '2026-05-17'
EV_THRESHOLD = 0.0  # Show all EV > 0%
SPORTSBOOK = 'User Book'


def load_matches(conn):
    """Load all 10 scheduled matchday 37 matches with odds."""
    cur = conn.cursor()
    cur.execute("""
        SELECT m.match_id, m.home_team_id, m.away_team_id,
               ht.name, at.name,
               o.home_moneyline, o.draw_moneyline, o.away_moneyline, o.over_under
        FROM soccer_matches m
        JOIN soccer_teams ht ON ht.team_id = m.home_team_id
        JOIN soccer_teams at ON at.team_id = m.away_team_id
        LEFT JOIN soccer_betting_odds o ON o.match_id = m.match_id AND o.sportsbook = ?
        WHERE m.league = 'Serie A' AND m.season = 2025 
          AND DATE(m.match_date) = ?
        ORDER BY m.match_date
    """, (SPORTSBOOK, MATCH_DATE))
    
    return cur.fetchall()


def generate_picks(conn, matches):
    """Run Poisson model on each match and generate ranked picks."""
    
    all_picks = []
    
    print("\n" + "="*120)
    print(f"MATCHDAY 37 PICK GENERATION — {MATCH_DATE}")
    print("="*120)
    
    for match_row in matches:
        match_id, home_id, away_id, home_name, away_name, hml, dml, aml, ou = match_row
        
        if hml is None:
            print(f"\n✗ {home_name} vs {away_name}: NO ODDS")
            continue
        
        # Run model with today as cutoff (no future data leakage)
        result = analyse_match(
            home_id, away_id, 
            MATCH_DATE,
            home_moneyline=hml,
            draw_moneyline=dml,
            away_moneyline=aml,
            league="Serie A",
            conn=conn
        )
        
        p_home = result['p_home']
        p_draw = result['p_draw']
        p_away = result['p_away']
        p_over25 = result['p_over25']
        p_under25 = 1.0 - p_over25
        
        # Calculate EVs
        ev_home = result.get('ev_home', None)
        ev_draw = result.get('ev_draw', None)
        ev_away = result.get('ev_away', None)
        
        # O/U EV
        if ou is not None:
            # Assume -110 for both sides (typical)
            ou_odds = -110.0
            ev_over = p_over25 * american_to_decimal(ou_odds) - 1
            ev_under = p_under25 * american_to_decimal(ou_odds) - 1
        else:
            ev_over = ev_under = None
        
        # Collect all positive-EV picks
        picks_this_match = []
        
        if ev_home is not None and ev_home >= EV_THRESHOLD:
            picks_this_match.append({
                'match_id': match_id,
                'home': home_name,
                'away': away_name,
                'outcome': 'HOME',
                'model_prob': p_home,
                'market_prob': result.get('implied_home', 0),
                'odds': hml,
                'decimal_odds': american_to_decimal(hml),
                'ev': ev_home,
            })
        
        if ev_draw is not None and ev_draw >= EV_THRESHOLD:
            picks_this_match.append({
                'match_id': match_id,
                'home': home_name,
                'away': away_name,
                'outcome': 'DRAW',
                'model_prob': p_draw,
                'market_prob': result.get('implied_draw', 0),
                'odds': dml,
                'decimal_odds': american_to_decimal(dml),
                'ev': ev_draw,
            })
        
        if ev_away is not None and ev_away >= EV_THRESHOLD:
            picks_this_match.append({
                'match_id': match_id,
                'home': home_name,
                'away': away_name,
                'outcome': 'AWAY',
                'model_prob': p_away,
                'market_prob': result.get('implied_away', 0),
                'odds': aml,
                'decimal_odds': american_to_decimal(aml),
                'ev': ev_away,
            })
        
        if ev_over is not None and ev_over >= EV_THRESHOLD:
            picks_this_match.append({
                'match_id': match_id,
                'home': home_name,
                'away': away_name,
                'outcome': f'OVER {ou}',
                'model_prob': p_over25,
                'market_prob': 0.5,  # Placeholder for -110 odds
                'odds': ou_odds,
                'decimal_odds': american_to_decimal(ou_odds),
                'ev': ev_over,
            })
        
        if ev_under is not None and ev_under >= EV_THRESHOLD:
            picks_this_match.append({
                'match_id': match_id,
                'home': home_name,
                'away': away_name,
                'outcome': f'UNDER {ou}',
                'model_prob': p_under25,
                'market_prob': 0.5,
                'odds': ou_odds,
                'decimal_odds': american_to_decimal(ou_odds),
                'ev': ev_under,
            })
        
        all_picks.extend(picks_this_match)
        
        # Match summary
        print(f"\n  {home_name:25s} vs {away_name:25s}")
        print(f"    Model probs: H={p_home*100:5.1f}% D={p_draw*100:5.1f}% A={p_away*100:5.1f}%  |  O2.5={p_over25*100:5.1f}% U2.5={p_under25*100:5.1f}%")
        if picks_this_match:
            for pick in picks_this_match:
                print(f"      ✓ {pick['outcome']:12s}  EV={pick['ev']:+.2%}  Model={pick['model_prob']*100:5.1f}%  Odds={pick['odds']:7.0f}")
        else:
            print(f"      (no picks above EV threshold)")
    
    # Sort all picks by EV descending
    all_picks.sort(key=lambda x: x['ev'], reverse=True)
    
    # Print ranked pick card
    print("\n" + "="*120)
    print("RANKED PICKS BY EXPECTED VALUE")
    print("="*120)
    print(f"{'Rank':<6} {'Match':<50} {'Bet Type':<15} {'Odds':<8} {'EV':<8} {'Model %':<10} {'Decimal':<8}")
    print("-"*120)
    
    for rank, pick in enumerate(all_picks, 1):
        matchup = f"{pick['home'][:12]:12s} vs {pick['away'][:12]:12s}"
        print(f"{rank:<6} {matchup:<50} {pick['outcome']:<15} {pick['odds']:>7.0f}  {pick['ev']:>+6.2%}  {pick['model_prob']*100:>8.1f}%  {pick['decimal_odds']:>7.2f}")
    
    print("-"*120)
    print(f"Total picks: {len(all_picks)}")
    
    return all_picks


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    try:
        matches = load_matches(conn)
        print(f"Loaded {len(matches)} matches for {MATCH_DATE}")
        picks = generate_picks(conn, matches)
    finally:
        conn.close()
