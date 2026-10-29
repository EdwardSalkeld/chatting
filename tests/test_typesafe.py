import io
import json
import sqlite3
import unittest
import urllib.error
from unittest.mock import patch
from types import SimpleNamespace

from app import typesafe
from app.worker.personal_inbox import process_one
from tests.test_personal_inbox import jev_answer, mail_task


def rate_limit(retry_after=None):
    return urllib.error.HTTPError(
        typesafe.ENDPOINT,
        429,
        "limited",
        {} if retry_after is None else {"Retry-After": retry_after},
        io.BytesIO(b"{}"),
    )


class TypeSafeTransportTests(unittest.TestCase):
    def test_recovered_429_is_cached_before_ack_replay(self):
        outputs = []

        class Broker:
            failed_ack = False

            def pickup_json(self, *args, **kwargs):
                return SimpleNamespace(guid="same", payload=mail_task())

            def publish_json(self, queue, result):
                outputs.append(result)

            def ack(self, *args):
                if not self.failed_ack:
                    self.failed_ack = True
                    raise OSError("lost acknowledgement")

        broker = Broker()
        with sqlite3.connect(":memory:") as cache:
            cache.execute(
                "CREATE TABLE classifications (id TEXT PRIMARY KEY, fingerprint TEXT, result TEXT)"
            )
            with (
                patch("app.typesafe.urllib.request.build_opener") as build,
                patch("app.typesafe.time.sleep"),
            ):
                build.return_value.open.side_effect = [
                    rate_limit(),
                    io.BytesIO(json.dumps(jev_answer("news")).encode()),
                ]
                with self.assertRaises(OSError):
                    process_one(broker, cache, jev_key="test", summary_model="unused")
                process_one(broker, cache, jev_key="test", summary_model="unused")
                self.assertEqual(build.return_value.open.call_count, 2)
        self.assertEqual(len(outputs), 2)
        self.assertEqual(outputs[0], outputs[1])
        self.assertEqual(outputs[0]["classification"]["category"], "news")
        self.assertFalse(outputs[0]["classification"]["trace"]["triage_failed"])

    def test_429_recovers_before_returning_a_final_result(self):
        with (
            patch("app.typesafe.urllib.request.build_opener") as build,
            patch("app.typesafe.time.sleep") as sleep,
        ):
            build.return_value.open.side_effect = [
                rate_limit(),
                io.BytesIO(b'{"answers": {}}'),
            ]
            self.assertEqual(
                typesafe.post(typesafe.ENDPOINT, "secret", {"model": "test"}),
                {"answers": {}},
            )
            self.assertEqual(build.return_value.open.call_count, 2)
            sleep.assert_called_once_with(1)
            request = build.return_value.open.call_args.args[0]
            self.assertEqual(json.loads(request.data), {"model": "test"})

    def test_429_has_two_retries_and_three_seconds_max_wait(self):
        with (
            patch("app.typesafe.urllib.request.build_opener") as build,
            patch("app.typesafe.time.sleep") as sleep,
        ):
            build.return_value.open.side_effect = [
                rate_limit(),
                rate_limit(),
                rate_limit(),
            ]
            with self.assertRaises(urllib.error.HTTPError):
                typesafe.post(typesafe.ENDPOINT, "secret", {})
            self.assertEqual(build.return_value.open.call_count, 3)
            self.assertEqual([c.args[0] for c in sleep.call_args_list], [1, 2])

    def test_retry_after_is_respected_or_stops_without_early_retry(self):
        for header, calls, waits in (
            ("2", 2, [2]),
            ("60", 1, []),
            ("-1", 1, []),
            ("nan", 1, []),
            ("Thu, 29 Oct 2026 10:00:00 GMT", 1, []),
        ):
            with (
                self.subTest(header=header),
                patch("app.typesafe.urllib.request.build_opener") as build,
                patch("app.typesafe.time.sleep") as sleep,
            ):
                build.return_value.open.side_effect = [
                    rate_limit(header),
                    io.BytesIO(b"{}"),
                ]
                if calls == 1:
                    with self.assertRaises(urllib.error.HTTPError):
                        typesafe.post(typesafe.ENDPOINT, "secret", {})
                else:
                    typesafe.post(typesafe.ENDPOINT, "secret", {})
                self.assertEqual(build.return_value.open.call_count, calls)
                self.assertEqual([c.args[0] for c in sleep.call_args_list], waits)

    def test_router_can_disable_retries_and_other_failures_are_not_retried(self):
        for error, retry in (
            (rate_limit(), False),
            (TimeoutError(), True),
            (
                urllib.error.HTTPError(
                    typesafe.ENDPOINT, 401, "unauthorized", {}, None
                ),
                True,
            ),
            (
                urllib.error.HTTPError(typesafe.ENDPOINT, 529, "overloaded", {}, None),
                True,
            ),
        ):
            with (
                self.subTest(error=type(error).__name__, retry=retry),
                patch("app.typesafe.urllib.request.build_opener") as build,
                patch("app.typesafe.time.sleep") as sleep,
            ):
                build.return_value.open.side_effect = error
                with self.assertRaises(type(error)):
                    typesafe.post(typesafe.ENDPOINT, "secret", {}, retry_429=retry)
                self.assertEqual(build.return_value.open.call_count, 1)
                sleep.assert_not_called()

    def test_oversized_and_non_object_response_are_rejected(self):
        for body in (
            b" " * (typesafe.MAX_RESPONSE_BYTES + 1),
            b"[]",
            b"[" * 2000 + b"]" * 2000,
        ):
            with (
                self.subTest(size=len(body)),
                patch("app.typesafe.urllib.request.build_opener") as build,
            ):
                build.return_value.open.return_value = io.BytesIO(body)
                with self.assertRaises(ValueError):
                    typesafe.post(typesafe.ENDPOINT, "secret", {})

    def test_request_timeout_shrinks_with_retry_budget(self):
        with (
            patch("app.typesafe.urllib.request.build_opener") as build,
            patch("app.typesafe.time.sleep"),
            patch("app.typesafe.time.monotonic", side_effect=[0, 1, 2, 4]),
        ):
            build.return_value.open.side_effect = [rate_limit(), io.BytesIO(b"{}")]
            typesafe.post(typesafe.ENDPOINT, "secret", {}, timeout=5)
            self.assertEqual(
                [c.kwargs["timeout"] for c in build.return_value.open.call_args_list],
                [4, 1],
            )
