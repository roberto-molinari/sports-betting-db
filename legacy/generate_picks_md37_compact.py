"""
Generate Poisson model picks for Matchday 37 — one pick per match.

Shows picks at two EV thresholds (0% and 5%) with value classification.
High Value: EV ≥ 5%
Low Value: 0% ≤ EV < 5%
"""

import sqlite3
from datetime import datetime
from poisson_model import analyse_match, american_to_decimal

DB_PATH = 'sports_betting.db'
MATCH_DATE = '2026-05-17'
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


def find_best_pick(result, hml, dml, aml, ou):
    """Find the single highest-EV outcome for a match."""
    
    p_home = result['p_home']
    p_draw = result['p_draw']
    p_away = result['p_away']
    p_over25 = result['p_over25']
    p_under25 = 1.0 - p_over25
    
    candidates = []
    
    # 1X2 EVs
    if hml is not None:
        ev_home = result.get('ev_home', None)
        if ev_home is not None:
            candidates.append({
                'outcome': 'HOME',
                'prob': p_home,
                'odds': hml,
                'decimal': american_to_decimal(hml),
                'ev': ev_home,
            })
    
    if dml is not None:
        ev_draw = result.get('ev_draw', None)
        if ev_draw is not None:
            candidates.append({
                'outcome': 'DRAW',
                'prob': p_draw,
                'odds': dml,
                'decimal': american_to_decimal(dml),
                'ev': ev_draw,
            })
    
    if aml is not None:
        ev_away = result.get('ev_away', None)
        if ev_away is not None:
            candidates.append({
                'outcome': 'AWAY',
                'prob': p_away,
                'odds': aml,
                'decimal': american_to_decimal(aml),
                'ev': ev_away,
            })
    
    # O/U EVs (assuming -110 odds for both sides)
    if ou is not None:
        ou_odds = -110.0
        ev_over = p_over25 * american_to_decimal(ou_odds) - 1
        ev_under = p_under25 * american_to_decimal(ou_odds) - 1
        
        candidates.append({
            'outcome': f'OVER {ou}',
            'prob': p_over25,
            'odds': ou_odds,
            'decimal': american_to_decimal(ou_odds),
            'ev': ev_over,
        })
        
        candidates.append({
            'outcome': f'UNDER {ou}',
            'prob': p_under25,
            'odds': ou_odds,
            'decimal': american_to_decimal(ou_odds),
            'ev': ev_under,
        })
    
    # Find best
    if not candidates:
        return None
    
    return max(candidates, key=lambda x: x['ev'])


def generate_picks(conn, matches):
    """Generate one pick per match at 0% and 5% EV thresholds."""
    
    print("\n" + "="*130)
    print(f"MATCHDAY 37 PICKS — {MATCH_DATE} (One Pick Per Match)")
    print("="*130)
    
    picks_data = []
    
    for match_row in matches:
        match_id, home_id, away_id, home_name, away_name, hml, dml, aml, ou = match_row
        
        if hml is None:
            print(f"✗ {home_name} vs {away_name}: NO ODDS\n")
            continue
        
        # Run model with today as cutoff
        result = analyse_match(
            home_id, away_id, 
            MATCH_DATE,
            home_moneyline=hml,
            draw_moneyline=dml,
            away_moneyline=aml,
            league="Serie A",
            conn=conn
        )
        
        best_pick = find_best_pick(result, hml, dml, aml, ou)
        
        if best_pick:
            ev = best_pick['ev']
            value_0pct = "High" if ev >= 0.05 else "Low"
            value_5pct = "High" if ev >= 0.05 else ""
            
            show_at_5pct = ev >= 0.05
            
            picks_data.append({
                'matchup': f"{home_name} vs {away_name}",
                'pick_0pct': best_pick['outcome'],
                'value_0pct': value_0pct,
                'pick_5pct': best_pick['outcome'] if show_at_5pct else "—",
                'value_5pct': value_5pct if show_at_5pct else "—",
                'odds': best_pick['odds'],
                'ev': ev,
                'prob': best_pick['prob'],
            })
    
    # Print consolidated table
    print(f"\n{'Match':<45} {'Pick (0%)':<15} {'Value':<8} {'Pick (5%)':<15} {'Value':<8} {'Odds':<8} {'EV':<8}")
    print("-"*130)
    
    for pick in picks_data:
        print(f"{pick['matchup']:<45} {pick['pick_0pct']:<15} {pick['value_0pct']:<8} {pick['pick_5pct']:<15} {pick['value_5pct']:<8} {pick['odds']:>7.0f}  {pick['ev']:>+6.2%}")
    
    print("-"*130)
    
    # Summary stats
    high_value_at_0pct = sum(1 for p in picks_data if p['value_0pct'] == 'High')
    high_value_at_5pct = sum(1 for p in picks_data if p['value_5pct'] == 'High')
    
    print(f"Total matches: {len(picks_data)}")
    print(f"High-value picks (EV ≥ 5%) at 0% threshold: {high_value_at_0pct}/10")
    print(f"High-value picks (EV ≥ 5%) at 5% threshold: {high_value_at_5pct}/10")
    print()
    
    return picks_data


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    try:
        matches = load_matches(conn)
        picks = generate_picks(conn, matches)
    finally:
        conn.close()
