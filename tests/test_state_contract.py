import tempfile
import unittest
from pathlib import Path

from app.state import SQLiteStateStore, StateStore
class StateStoreContractTests(unittest.TestCase):
    def test_sqlite_store_implements_state_store_protocol(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)

            self.assertIsInstance(store, StateStore)
if __name__ == "__main__":
    unittest.main()
