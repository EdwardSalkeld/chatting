import json
import tempfile
import unittest
from pathlib import Path

from app.worker.main import _load_config, _resolve_non_negative_int


class MainWorkerTests(unittest.TestCase):
    def test_load_config_accepts_activity_port(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(
                json.dumps({"bbmb_address": "127.0.0.1:9876", "activity_port": 0}),
                encoding="utf-8",
            )

            payload = _load_config(str(config_path))

        self.assertEqual(payload["activity_port"], 0)

    def test_resolve_non_negative_int_accepts_zero(self) -> None:
        resolved = _resolve_non_negative_int(
            None,
            0,
            default_value=9465,
            setting_name="activity_port",
        )

        self.assertEqual(resolved, 0)

    def test_resolve_non_negative_int_rejects_negative_values(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "config activity_port must be a non-negative integer"
        ):
            _resolve_non_negative_int(
                None,
                -1,
                default_value=9465,
                setting_name="activity_port",
            )


if __name__ == "__main__":
    unittest.main()
