#!/usr/bin/env python3
"""Compatibility entrypoint for NHL betting analysis summary."""

from betting_analyzer import BettingAnalyzer


if __name__ == "__main__":
    BettingAnalyzer().generate_summary_report("hockey")
