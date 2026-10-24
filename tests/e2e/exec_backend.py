"""Select exec in private databases for fixtures using fake codex commands."""

import sqlite3
from pathlib import Path

from app.state import SQLiteStateStore


def use_exec_backend(db_path: Path) -> None:
    SQLiteStateStore(str(db_path))
    with sqlite3.connect(db_path) as connection:
        # Set the choice before assignment applies the default. Some fixtures
        # receive handler-generated item IDs that are not known in advance.
        connection.executescript(
            """
            CREATE TRIGGER test_select_exec AFTER INSERT ON work_items BEGIN
                INSERT OR IGNORE INTO conversation_executors
                VALUES (NEW.work_item_id, 'exec');
            END;
            """
        )
