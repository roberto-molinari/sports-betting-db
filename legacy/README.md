# legacy/

These scripts are **broken** as of the April 2026 schema migration.

They were written against the old single-table schema (`matches`, `teams`, `betting_odds`) and have not been updated. They are kept here for reference only — do not run them against the current database.

| File | Original purpose |
|------|-----------------|
| `check_odds.py` | Spot-check odds values in the old `betting_odds` table |
| `check_odds_data.py` | Data quality checks on odds coverage |
| `check_api_field_coverage.py` | Diagnostic: what % of API fields are populated (free tier) |
| `deep_dive_underdogs.py` | Deep analysis of underdog betting patterns |
| `import_serie_a_csv.py` | One-time import of Serie A odds from football-data.co.uk CSVs |
| `import_csv_odds.py` | Generic CSV odds importer |
| `import_odds_from_csv.py` | Alternative CSV odds importer |
| `import_betting_odds.py` | Original betting odds import script |
