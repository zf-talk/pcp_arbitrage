import sqlite3
import tempfile
import os
from pcp_arbitrage import db as _db


def test_positions_exit_columns():
    """Test that positions table has the exit tracking columns after init_db."""
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    try:
        # Initialize the database
        _db.init_db(db_path)

        # Connect and check columns
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        cols = {row[1] for row in con.execute("PRAGMA table_info(positions)")}
        con.close()

        assert "exit_attempt_count" in cols, f"Missing exit_attempt_count. Columns: {cols}"
        assert "exit_started_at" in cols, f"Missing exit_started_at. Columns: {cols}"
        assert "exit_last_attempt_at" in cols, f"Missing exit_last_attempt_at. Columns: {cols}"
    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)
