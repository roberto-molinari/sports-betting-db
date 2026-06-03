"""
Serie A value card generator.

Builds a ranked card for the next Serie A match window with:
- one best pick per match
- optional secondary and tertiary picks for the same match
- all printed picks staked at 1u
"""

import sqlite3
from datetime import datetime, timezone, timedelta
from core.sports_db import DATABASE_PATH
from core.poisson_model import analyse_match

# Card rules
EXTRA_PICK_EV = 0.08


def dec_from_american(a):
    a = float(a)
    return 1 + (a/100.0 if a > 0 else 100.0/abs(a))

def ev(prob, american_odds):
    return prob * dec_from_american(american_odds) - 1


def build_pick(match_row, side, odds, prob, edge):
    return {
        'match_id': match_row['match_id'],
        'match_date': match_row['match_date'],
        'home': match_row['home'],
        'away': match_row['away'],
        'side': side,
        'odds': odds,
        'prob': prob,
        'ev': edge,
    }


def main():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    now = datetime.now(timezone.utc)
    start = (now.date() + timedelta(days=1)).isoformat()
    end = (now.date() + timedelta(days=4)).isoformat()

    cur.execute('''
    SELECT
        m.match_id,
        m.match_date,
        ht.name AS home,
        at.name AS away,
        m.home_team_id,
        m.away_team_id,
        o.sportsbook,
        o.home_moneyline,
        o.draw_moneyline,
        o.away_moneyline,
        o.over_under,
        o.over_odds,
        o.under_odds
    FROM soccer_matches m
    JOIN soccer_teams ht ON ht.team_id = m.home_team_id
    JOIN soccer_teams at ON at.team_id = m.away_team_id
    JOIN soccer_betting_odds o ON o.match_id = m.match_id
    WHERE m.league = 'Serie A'
      AND date(m.match_date) >= date(?)
      AND date(m.match_date) <= date(?)
    ORDER BY m.match_date, m.match_id
    ''', (start, end))
    rows = cur.fetchall()

    ranked_matches = []


    for m in rows:
        r = analyse_match(
            m['home_team_id'],
            m['away_team_id'],
            m['match_date'],
            m['home_moneyline'],
            m['draw_moneyline'],
            m['away_moneyline'],
            conn=conn,
        )

        # 1X2 EVs
        e_home = ev(r['p_home'], m['home_moneyline']) if m['home_moneyline'] is not None else None
        e_draw = ev(r['p_draw'], m['draw_moneyline']) if m['draw_moneyline'] is not None else None
        e_away = ev(r['p_away'], m['away_moneyline']) if m['away_moneyline'] is not None else None

        # Totals EVs only when the line is 2.5 (model outputs p_over25)
        e_over = e_under = None
        if m['over_under'] is not None and abs(float(m['over_under']) - 2.5) < 1e-9 and m['over_odds'] is not None and m['under_odds'] is not None:
            e_over = ev(r['p_over25'], m['over_odds'])
            e_under = ev(1 - r['p_over25'], m['under_odds'])

        picks_for_match = []

        for side, edge, odds, prob in [
            ('HOME', e_home, m['home_moneyline'], r['p_home']),
            ('DRAW', e_draw, m['draw_moneyline'], r['p_draw']),
            ('AWAY', e_away, m['away_moneyline'], r['p_away']),
            ('OVER 2.5', e_over, m['over_odds'], r['p_over25']),
            ('UNDER 2.5', e_under, m['under_odds'], 1 - r['p_over25'] if e_under is not None else None),
        ]:
            if edge is not None and odds is not None and prob is not None:
                picks_for_match.append(build_pick(m, side, odds, prob, edge))

        if not picks_for_match:
            continue

        picks_for_match.sort(key=lambda item: item['ev'], reverse=True)
        best_two = picks_for_match[:2]

        ranked_matches.append(best_two)


    # Sort matches by the EV of the best pick
    ranked_matches.sort(key=lambda picks: picks[0]['ev'], reverse=True)

    def fmt_pick(rank_label, pick):
        return (
            f"{rank_label:>3} {pick['home']} vs {pick['away']} | {pick['side']:<9} "
            f"| odds {pick['odds']:+.0f} | model p {pick['prob']:.3f} | EV {pick['ev']:+.1%} | stake 1.00u"
        )

    print('WINDOW', start, 'to', end)
    print('MATCHES', len(rows))
    print('')

    print('=== TOP 2 PICKS PER MATCH (RANKED BY BEST PICK EV) ===')
    if ranked_matches:
        for index, picks in enumerate(ranked_matches, 1):
            print(fmt_pick(f'{index}.', picks[0]))
            if len(picks) > 1:
                print(fmt_pick(f'{index}b', picks[1]))
            print('')
    else:
        print('No matches with priced markets')

    conn.close()

if __name__ == "__main__":
    main()
