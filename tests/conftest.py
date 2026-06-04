"""
Shared pytest fixtures.

The functions in ``core.sports_db`` each open their own connection to the
module-level ``DATABASE_PATH`` (they don't take a ``conn`` argument), so a true
``:memory:`` database can't be shared across calls — every new connection would
see an empty DB. Instead we point ``DATABASE_PATH`` at a throwaway file under
pytest's ``tmp_path`` for the duration of a test. Same isolation and speed, but
it works with the code exactly as written.
"""

import sqlite3

import pytest

import core.sports_db as sports_db
import core.poisson_model as poisson_model


@pytest.fixture
def db_path(tmp_path, monkeypatch):
    """A fresh, schema-initialised sports DB in a temp file.

    Patches ``DATABASE_PATH`` everywhere it is consulted so the sports_db
    helpers and the poisson model both read/write the temp database.
    """
    path = tmp_path / "test_sports_betting.db"
    monkeypatch.setattr(sports_db, "DATABASE_PATH", path)
    # poisson_model did `from core.sports_db import DATABASE_PATH`, binding the
    # value into its own namespace, so patch that copy too.
    monkeypatch.setattr(poisson_model, "DATABASE_PATH", path)
    sports_db.init_database()
    return path


@pytest.fixture
def conn(db_path):
    """An open connection to the temp DB (for passing into poisson functions
    and for direct seeding/inspection in tests)."""
    connection = sqlite3.connect(db_path)
    try:
        yield connection
    finally:
        connection.close()
