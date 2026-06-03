#!/usr/bin/env python3
"""Compatibility entrypoint for advanced soccer betting analysis."""

from betting_analyzer import BettingAnalyzer


if __name__ == "__main__":
    BettingAnalyzer().generate_advanced_report(sport="soccer", league="Serie A")
