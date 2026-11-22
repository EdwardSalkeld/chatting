import json
import tempfile
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

from app.worker.activity import start_worker_activity_server
from app.worker.secret_files import Unavailable, save_secret


class SecretFileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "item_1").mkdir()
        self.payload = {
            "workspace_root": str(self.root),
            "work_item_id": "item_1",
            "request_id": "abc123",
            "secret": "sensitive value",
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        }

    def test_save_recovery_permissions_and_expiry(self):
        saved = save_secret(self.root, self.payload)
        path = Path(str(saved["path"]))
        self.assertFalse(saved["existing"])
        self.assertEqual(path.read_text(), "sensitive value")
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        recovered = save_secret(self.root, {**self.payload, "secret": None})
        self.assertTrue(recovered["existing"])
        self.assertEqual(path.read_text(), "sensitive value")
        expired = {
            **self.payload,
            "request_id": "expired",
            "expires_at": "2000-01-01T00:00:00Z",
        }
        with self.assertRaises(Unavailable):
            save_secret(self.root, expired)

    def test_rejects_workspace_symlink_and_traversal(self):
        (self.root / "item_1").rmdir()
        outside = self.root / "outside"
        outside.mkdir()
        (self.root / "item_1").symlink_to(outside)
        with self.assertRaises(OSError):
            save_secret(self.root, self.payload)
        self.assertEqual(list(outside.iterdir()), [])
        with self.assertRaises(Unavailable):
            save_secret(self.root, {**self.payload, "work_item_id": "../outside"})

    def test_loopback_endpoint_rejects_browser_and_accepts_handler(self):
        server = start_worker_activity_server(
            host="127.0.0.1", port=0, monitor=Mock(), secret_workspace_root=self.root
        )
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server.server_port}/internal/secret-file"
        data = json.dumps(self.payload).encode()
        request = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Origin": "https://attacker.example",
            },
        )
        with self.assertRaises(urllib.error.HTTPError) as raised:
            urllib.request.urlopen(request)
        self.assertEqual(raised.exception.code, 404)
        request = urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(request) as response:
            result = json.load(response)
        self.assertEqual(Path(result["path"]).read_text(), "sensitive value")


if __name__ == "__main__":
    unittest.main()
