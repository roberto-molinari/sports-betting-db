"""Moneyline ROI analysis for NHL favorites/underdogs.

Usage examples:
  python calculate_moneyline_roi.py --side favorite
  python calculate_moneyline_roi.py --side underdog
  python calculate_moneyline_roi.py --side both
"""

import argparse
import sqlite3

from core.sports_db import DATABASE_PATH


def calculate_payout(bet_amount: float, moneyline: float) -> float:
    if moneyline < 0:
        return bet_amount * (100 / abs(moneyline))
    return bet_amount * (moneyline / 100)


def _select_side(match: sqlite3.Row, side: str):
    home_ml = match["home_moneyline"]
    away_ml = match["away_moneyline"]
    home_score = match["home_score"]
    away_score = match["away_score"]

    if side == "favorite":
        if home_ml < away_ml:
            return home_ml, match["home_team"], home_score > away_score
        return away_ml, match["away_team"], away_score > home_score

    if home_ml > away_ml:
        return home_ml, match["home_team"], home_score > away_score
    return away_ml, match["away_team"], away_score > home_score


def run_roi(side: str, bet_amount: float = 100.0):
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = """
        SELECT
            nm.match_id,
            h.name  as home_team,
            a.name  as away_team,
            nm.home_score,
            nm.away_score,
            bo.home_moneyline,
            bo.away_moneyline,
            nm.match_date
        FROM nhl_matches nm
        JOIN nhl_teams h  ON nm.home_team_id = h.team_id
        JOIN nhl_teams a  ON nm.away_team_id = a.team_id
        JOIN nhl_betting_odds bo ON nm.match_id = bo.match_id
        WHERE nm.match_status = 'completed'
          AND bo.home_moneyline IS NOT NULL
          AND bo.away_moneyline IS NOT NULL
        ORDER BY nm.match_date ASC
    """

    cursor.execute(query)
    matches = cursor.fetchall()
    conn.close()

    total_wagered = 0.0
    total_profit = 0.0
    wins = 0
    losses = 0
    details = []

    for match in matches:
        selected_ml, selected_team, won = _select_side(match, side)

        total_wagered += bet_amount
        if won:
            payout = calculate_payout(bet_amount, selected_ml)
            profit = payout - bet_amount
            total_profit += profit
            wins += 1
            outcome = "WIN"
        else:
            profit = -bet_amount
            total_profit += profit
            losses += 1
            outcome = "LOSS"

        details.append(
            {
                "date": match["match_date"],
                "matchup": f"{match['home_team']} vs {match['away_team']}",
                "selection": f"{selected_team} ({selected_ml})",
                "score": f"{match['home_score']}-{match['away_score']}",
                "outcome": outcome,
                "profit": profit,
            }
        )

    print("=" * 70)
    print(f"NHL MONEYLINE {side.upper()} BETTING ANALYSIS")
    print("=" * 70)
    print(f"\nBet Amount Per Game: ${bet_amount:.2f}")
    print(f"Total Games Played: {len(matches)}")
    print(f"Total Amount Wagered: ${total_wagered:,.2f}")
    print(f"\nWins: {wins} ({100*wins/len(matches):.1f}%)")
    print(f"Losses: {losses} ({100*losses/len(matches):.1f}%)")
    print(f"\nTotal Profit/Loss: ${total_profit:,.2f}")
    print(f"ROI: {100*total_profit/total_wagered:.2f}%")

    if total_profit > 0:
        print(f"\nYou would have MADE ${total_profit:,.2f}")
    else:
        print(f"\nYou would have LOST ${abs(total_profit):,.2f}")

    print("\n" + "=" * 70)
    print("RECENT GAMES (Last 10):")
    print("=" * 70)
    for row in details[-10:]:
        status = "+" if row["outcome"] == "WIN" else "-"
        profit_str = (
            f"+${row['profit']:.2f}" if row["profit"] > 0 else f"-${abs(row['profit']):.2f}"
        )
        print(
            f"{status} {row['date'][:10]} | {row['matchup']:40} | "
            f"{row['outcome']:4} | {profit_str:>10}"
        )


def main():
    parser = argparse.ArgumentParser(description="Calculate NHL moneyline ROI.")
    parser.add_argument(
        "--side",
        choices=["favorite", "underdog", "both"],
        default="both",
        help="Which side to evaluate (default: both).",
    )
    parser.add_argument(
        "--bet-amount",
        type=float,
        default=100.0,
        help="Flat stake per game (default: 100).",
    )
    args = parser.parse_args()

    if args.side == "both":
        run_roi("favorite", bet_amount=args.bet_amount)
        print("\n")
        run_roi("underdog", bet_amount=args.bet_amount)
        return

    run_roi(args.side, bet_amount=args.bet_amount)


if __name__ == "__main__":
    main()