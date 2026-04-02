"""End-to-end auxiliary ingress test with separate auxiliary and main brokers."""

from __future__ import annotations

import http.client
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from app.state import SQLiteStateStore


def _is_port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.2)
        return probe.connect_ex((host, port)) == 0


def _wait_for_port(host: str, port: int, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if _is_port_open(host, port):
            return
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {host}:{port}")


def _reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        return int(listener.getsockname()[1])


def _resolve_bbmb_server_bin() -> Path:
    server_bin_raw = os.environ.get("CHATTING_BBMB_SERVER_BIN", "").strip()
    if not server_bin_raw:
        raise unittest.SkipTest("CHATTING_BBMB_SERVER_BIN is not set")
    server_bin = Path(server_bin_raw)
    if not server_bin.exists():
        raise unittest.SkipTest(f"bbmb server binary not found: {server_bin}")
    return server_bin


def _start_bbmb_server(
    *,
    server_bin: Path,
    repo_root: Path,
    port: int,
    metrics_port: int,
    log_file,
) -> subprocess.Popen[str]:
    process = subprocess.Popen(
        [
            str(server_bin),
            f"--port={port}",
            f"--metrics-port={metrics_port}",
        ],
        cwd=repo_root,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        _wait_for_port("127.0.0.1", port, timeout_seconds=5.0)
    except Exception:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        raise
    return process


def _post_json(host: str, port: int, path: str, payload: object) -> dict[str, object]:
    connection = http.client.HTTPConnection(host, port, timeout=5)
    try:
        body = json.dumps(payload).encode("utf-8")
        connection.request(
            "POST",
            path,
            body=body,
            headers={"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        response_body = response.read().decode("utf-8")
    finally:
        connection.close()
    if response.status != 200:
        raise RuntimeError(f"unexpected status {response.status}: {response_body}")
    return json.loads(response_body)


class AuxiliaryIngressE2ETests(unittest.TestCase):
    def test_auxiliary_ingress_post_reaches_worker_and_completes(self) -> None:
        repo_root = Path(__file__).resolve().parent.parent.parent
        server_bin = _resolve_bbmb_server_bin()
        fake_codex = str(repo_root / "tests" / "e2e" / "fake_codex.py")
        main_bbmb_port = _reserve_port()
        main_bbmb_metrics_port = _reserve_port()
        auxiliary_bbmb_port = _reserve_port()
        auxiliary_bbmb_metrics_port = _reserve_port()
        auxiliary_port = _reserve_port()
        handler_metrics_port = _reserve_port()
        worker_activity_port = _reserve_port()
        main_bbmb_address = f"127.0.0.1:{main_bbmb_port}"
        auxiliary_bbmb_address = f"127.0.0.1:{auxiliary_bbmb_port}"

        main_bbmb_proc: subprocess.Popen[str] | None = None
        auxiliary_bbmb_proc: subprocess.Popen[str] | None = None
        auxiliary_proc: subprocess.Popen[str] | None = None
        worker_proc: subprocess.Popen[str] | None = None
        handler_proc: subprocess.Popen[str] | None = None

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_root = Path(tmpdir)
            handler_db_path = temp_root / "handler.db"
            worker_db_path = temp_root / "worker.db"
            prompt_dir = temp_root / "prompts"
            prompt_dir.mkdir()

            handler_log = temp_root / "handler.log"
            worker_log = temp_root / "worker.log"
            auxiliary_log = temp_root / "auxiliary.log"
            main_bbmb_log = temp_root / "main-bbmb.log"
            auxiliary_bbmb_log = temp_root / "auxiliary-bbmb.log"

            handler_config_path = temp_root / "handler.json"
            worker_config_path = temp_root / "worker.json"
            auxiliary_config_path = temp_root / "auxiliary-ingress.json"

            handler_config_path.write_text(
                json.dumps(
                    {
                        "db_path": str(handler_db_path),
                        "bbmb_address": main_bbmb_address,
                        "poll_interval_seconds": 0.1,
                        "poll_timeout_seconds": 1,
                        "max_loops": 60,
                        "allowed_egress_channels": ["log"],
                        "metrics_port": handler_metrics_port,
                        "auxiliary_ingress_enabled": True,
                        "auxiliary_ingress_bbmb_address": auxiliary_bbmb_address,
                        "auxiliary_ingress_routes": ["generic-post:12334"],
                    }
                ),
                encoding="utf-8",
            )
            worker_config_path.write_text(
                json.dumps(
                    {
                        "db_path": str(worker_db_path),
                        "bbmb_address": main_bbmb_address,
                        "max_attempts": 2,
                        "poll_timeout_seconds": 1,
                        "sleep_seconds": 0.05,
                        "max_loops": 60,
                        "activity_port": worker_activity_port,
                        "codex_command": f"{sys.executable} {fake_codex}",
                    }
                ),
                encoding="utf-8",
            )
            auxiliary_config_path.write_text(
                json.dumps(
                    {
                        "bbmb_address": auxiliary_bbmb_address,
                        "listen_host": "127.0.0.1",
                        "listen_port": auxiliary_port,
                        "ingress_routes": ["generic-post:12334"],
                    }
                ),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["FAKE_CODEX_PROMPT_DIR"] = str(prompt_dir)

            handler_log_fh = open(handler_log, "w", encoding="utf-8")
            worker_log_fh = open(worker_log, "w", encoding="utf-8")
            auxiliary_log_fh = open(auxiliary_log, "w", encoding="utf-8")
            main_bbmb_log_fh = open(main_bbmb_log, "w", encoding="utf-8")
            auxiliary_bbmb_log_fh = open(auxiliary_bbmb_log, "w", encoding="utf-8")

            try:
                main_bbmb_proc = _start_bbmb_server(
                    server_bin=server_bin,
                    repo_root=repo_root,
                    port=main_bbmb_port,
                    metrics_port=main_bbmb_metrics_port,
                    log_file=main_bbmb_log_fh,
                )
                auxiliary_bbmb_proc = _start_bbmb_server(
                    server_bin=server_bin,
                    repo_root=repo_root,
                    port=auxiliary_bbmb_port,
                    metrics_port=auxiliary_bbmb_metrics_port,
                    log_file=auxiliary_bbmb_log_fh,
                )

                auxiliary_proc = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "app.main_auxiliary_ingress",
                        "--config",
                        str(auxiliary_config_path),
                    ],
                    cwd=repo_root,
                    stdout=auxiliary_log_fh,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=env,
                )
                _wait_for_port("127.0.0.1", auxiliary_port, timeout_seconds=5.0)

                worker_proc = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "app.main_worker",
                        "--config",
                        str(worker_config_path),
                    ],
                    cwd=repo_root,
                    stdout=worker_log_fh,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=env,
                )
                handler_proc = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "app.main_message_handler",
                        "--config",
                        str(handler_config_path),
                    ],
                    cwd=repo_root,
                    stdout=handler_log_fh,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=env,
                )

                time.sleep(1)

                request_body = {
                    "kind": "generic-post",
                    "message": "Please do the auxiliary ingress e2e thing",
                }
                response_payload = _post_json(
                    "127.0.0.1",
                    auxiliary_port,
                    "/12334",
                    request_body,
                )
                self.assertEqual(response_payload["status"], "queued")

                prompt_file = prompt_dir / "prompt.json"
                prompt = self._wait_for_prompt(
                    prompt_file=prompt_file,
                    timeout_seconds=30,
                    main_bbmb_proc=main_bbmb_proc,
                    auxiliary_bbmb_proc=auxiliary_bbmb_proc,
                    handler_proc=handler_proc,
                    worker_proc=worker_proc,
                    auxiliary_proc=auxiliary_proc,
                    main_bbmb_log=main_bbmb_log,
                    auxiliary_bbmb_log=auxiliary_bbmb_log,
                    handler_log=handler_log,
                    worker_log=worker_log,
                    auxiliary_log=auxiliary_log,
                    main_bbmb_log_fh=main_bbmb_log_fh,
                    auxiliary_bbmb_log_fh=auxiliary_bbmb_log_fh,
                    handler_log_fh=handler_log_fh,
                    worker_log_fh=worker_log_fh,
                    auxiliary_log_fh=auxiliary_log_fh,
                )

                task = prompt["task"]
                task_id = task["task_id"]
                expected_event_id = f"evt:{task_id}:0:completion:internal"
                self._wait_for_completion(
                    handler_db_path=handler_db_path,
                    task_id=task_id,
                    event_id=expected_event_id,
                    timeout_seconds=30,
                    main_bbmb_proc=main_bbmb_proc,
                    auxiliary_bbmb_proc=auxiliary_bbmb_proc,
                    handler_proc=handler_proc,
                    worker_proc=worker_proc,
                    auxiliary_proc=auxiliary_proc,
                    main_bbmb_log=main_bbmb_log,
                    auxiliary_bbmb_log=auxiliary_bbmb_log,
                    handler_log=handler_log,
                    worker_log=worker_log,
                    auxiliary_log=auxiliary_log,
                    main_bbmb_log_fh=main_bbmb_log_fh,
                    auxiliary_bbmb_log_fh=auxiliary_bbmb_log_fh,
                    handler_log_fh=handler_log_fh,
                    worker_log_fh=worker_log_fh,
                    auxiliary_log_fh=auxiliary_log_fh,
                )
                expected_envelope_id = task_id.removeprefix("task:")
                self._wait_for_successful_worker_run(
                    worker_db_path=worker_db_path,
                    expected_envelope_id=expected_envelope_id,
                    timeout_seconds=30,
                    main_bbmb_proc=main_bbmb_proc,
                    auxiliary_bbmb_proc=auxiliary_bbmb_proc,
                    handler_proc=handler_proc,
                    worker_proc=worker_proc,
                    auxiliary_proc=auxiliary_proc,
                    main_bbmb_log=main_bbmb_log,
                    auxiliary_bbmb_log=auxiliary_bbmb_log,
                    handler_log=handler_log,
                    worker_log=worker_log,
                    auxiliary_log=auxiliary_log,
                    main_bbmb_log_fh=main_bbmb_log_fh,
                    auxiliary_bbmb_log_fh=auxiliary_bbmb_log_fh,
                    handler_log_fh=handler_log_fh,
                    worker_log_fh=worker_log_fh,
                    auxiliary_log_fh=auxiliary_log_fh,
                )

                auxiliary_proc.terminate()
                auxiliary_proc.wait(timeout=5)
                auxiliary_proc = None

                handler_out = ""
                worker_out = ""
                handler_err = ""
                worker_err = ""
                if handler_proc is not None:
                    handler_out, handler_err = handler_proc.communicate(timeout=45)
                if worker_proc is not None:
                    worker_out, worker_err = worker_proc.communicate(timeout=45)

                self.assertEqual(
                    handler_proc.returncode,
                    0,
                    msg=(
                        "message-handler exited non-zero\n"
                        f"stdout:\n{handler_out}\n"
                        f"stderr:\n{handler_err}"
                    ),
                )
                self.assertEqual(
                    worker_proc.returncode,
                    0,
                    msg=(
                        "worker exited non-zero\n"
                        f"stdout:\n{worker_out}\n"
                        f"stderr:\n{worker_err}"
                    ),
                )

                self.assertEqual(task["source"], "webhook")
                self.assertEqual(json.loads(task["content"]), request_body)
                self.assertEqual(task["reply_channel"]["type"], "webhook")
                self.assertEqual(task["reply_channel"]["target"], "generic-post")

                worker_store = SQLiteStateStore(str(worker_db_path))
                worker_runs = worker_store.list_runs()
                self.assertTrue(
                    any(
                        run.envelope_id == expected_envelope_id
                        and run.result_status == "success"
                        for run in worker_runs
                    ),
                    msg=(
                        "missing successful worker run for "
                        f"envelope_id={expected_envelope_id!r}"
                    ),
                )

                handler_store = SQLiteStateStore(str(handler_db_path))
                self.assertTrue(
                    handler_store.has_dispatched_event_id(
                        task_id=task_id,
                        event_id=expected_event_id,
                    )
                )
            finally:
                for proc in (
                    handler_proc,
                    worker_proc,
                    auxiliary_proc,
                    main_bbmb_proc,
                    auxiliary_bbmb_proc,
                ):
                    if proc is None:
                        continue
                    if proc.poll() is None:
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=5)
                    if proc.stdout is not None:
                        proc.stdout.close()
                    if proc.stderr is not None:
                        proc.stderr.close()
                if not handler_log_fh.closed:
                    handler_log_fh.close()
                if not worker_log_fh.closed:
                    worker_log_fh.close()
                if not auxiliary_log_fh.closed:
                    auxiliary_log_fh.close()
                if not main_bbmb_log_fh.closed:
                    main_bbmb_log_fh.close()
                if not auxiliary_bbmb_log_fh.closed:
                    auxiliary_bbmb_log_fh.close()

    def _wait_for_prompt(
        self,
        *,
        prompt_file: Path,
        timeout_seconds: float,
        main_bbmb_proc: subprocess.Popen[str] | None,
        auxiliary_bbmb_proc: subprocess.Popen[str] | None,
        handler_proc: subprocess.Popen[str] | None,
        worker_proc: subprocess.Popen[str] | None,
        auxiliary_proc: subprocess.Popen[str] | None,
        main_bbmb_log: Path,
        auxiliary_bbmb_log: Path,
        handler_log: Path,
        worker_log: Path,
        auxiliary_log: Path,
        main_bbmb_log_fh,
        auxiliary_bbmb_log_fh,
        handler_log_fh,
        worker_log_fh,
        auxiliary_log_fh,
    ) -> dict[str, object]:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if prompt_file.exists():
                return json.loads(prompt_file.read_text(encoding="utf-8"))
            time.sleep(1)
        diagnostics = self._dump_diagnostics(
            main_bbmb_proc=main_bbmb_proc,
            auxiliary_bbmb_proc=auxiliary_bbmb_proc,
            handler_proc=handler_proc,
            worker_proc=worker_proc,
            auxiliary_proc=auxiliary_proc,
            main_bbmb_log=main_bbmb_log,
            auxiliary_bbmb_log=auxiliary_bbmb_log,
            handler_log=handler_log,
            worker_log=worker_log,
            auxiliary_log=auxiliary_log,
            main_bbmb_log_fh=main_bbmb_log_fh,
            auxiliary_bbmb_log_fh=auxiliary_bbmb_log_fh,
            handler_log_fh=handler_log_fh,
            worker_log_fh=worker_log_fh,
            auxiliary_log_fh=auxiliary_log_fh,
        )
        self.fail(f"prompt file never appeared\n\n{diagnostics}")

    def _wait_for_completion(
        self,
        *,
        handler_db_path: Path,
        task_id: str,
        event_id: str,
        timeout_seconds: float,
        main_bbmb_proc: subprocess.Popen[str] | None,
        auxiliary_bbmb_proc: subprocess.Popen[str] | None,
        handler_proc: subprocess.Popen[str] | None,
        worker_proc: subprocess.Popen[str] | None,
        auxiliary_proc: subprocess.Popen[str] | None,
        main_bbmb_log: Path,
        auxiliary_bbmb_log: Path,
        handler_log: Path,
        worker_log: Path,
        auxiliary_log: Path,
        main_bbmb_log_fh,
        auxiliary_bbmb_log_fh,
        handler_log_fh,
        worker_log_fh,
        auxiliary_log_fh,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            handler_store = SQLiteStateStore(str(handler_db_path))
            if handler_store.has_dispatched_event_id(
                task_id=task_id, event_id=event_id
            ):
                return
            time.sleep(1)
        diagnostics = self._dump_diagnostics(
            main_bbmb_proc=main_bbmb_proc,
            auxiliary_bbmb_proc=auxiliary_bbmb_proc,
            handler_proc=handler_proc,
            worker_proc=worker_proc,
            auxiliary_proc=auxiliary_proc,
            main_bbmb_log=main_bbmb_log,
            auxiliary_bbmb_log=auxiliary_bbmb_log,
            handler_log=handler_log,
            worker_log=worker_log,
            auxiliary_log=auxiliary_log,
            main_bbmb_log_fh=main_bbmb_log_fh,
            auxiliary_bbmb_log_fh=auxiliary_bbmb_log_fh,
            handler_log_fh=handler_log_fh,
            worker_log_fh=worker_log_fh,
            auxiliary_log_fh=auxiliary_log_fh,
        )
        self.fail(
            f"completion event was not recorded for task_id={task_id!r}\n\n{diagnostics}"
        )

    def _wait_for_successful_worker_run(
        self,
        *,
        worker_db_path: Path,
        expected_envelope_id: str,
        timeout_seconds: float,
        main_bbmb_proc: subprocess.Popen[str] | None,
        auxiliary_bbmb_proc: subprocess.Popen[str] | None,
        handler_proc: subprocess.Popen[str] | None,
        worker_proc: subprocess.Popen[str] | None,
        auxiliary_proc: subprocess.Popen[str] | None,
        main_bbmb_log: Path,
        auxiliary_bbmb_log: Path,
        handler_log: Path,
        worker_log: Path,
        auxiliary_log: Path,
        main_bbmb_log_fh,
        auxiliary_bbmb_log_fh,
        handler_log_fh,
        worker_log_fh,
        auxiliary_log_fh,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            worker_store = SQLiteStateStore(str(worker_db_path))
            worker_runs = worker_store.list_runs()
            if any(
                run.envelope_id == expected_envelope_id
                and run.result_status == "success"
                for run in worker_runs
            ):
                return
            time.sleep(1)
        diagnostics = self._dump_diagnostics(
            main_bbmb_proc=main_bbmb_proc,
            auxiliary_bbmb_proc=auxiliary_bbmb_proc,
            handler_proc=handler_proc,
            worker_proc=worker_proc,
            auxiliary_proc=auxiliary_proc,
            main_bbmb_log=main_bbmb_log,
            auxiliary_bbmb_log=auxiliary_bbmb_log,
            handler_log=handler_log,
            worker_log=worker_log,
            auxiliary_log=auxiliary_log,
            main_bbmb_log_fh=main_bbmb_log_fh,
            auxiliary_bbmb_log_fh=auxiliary_bbmb_log_fh,
            handler_log_fh=handler_log_fh,
            worker_log_fh=worker_log_fh,
            auxiliary_log_fh=auxiliary_log_fh,
        )
        worker_store = SQLiteStateStore(str(worker_db_path))
        worker_runs = worker_store.list_runs()
        worker_run_summary = [
            {
                "run_id": run.run_id,
                "envelope_id": run.envelope_id,
                "result_status": run.result_status,
                "workflow": run.workflow,
            }
            for run in worker_runs
        ]
        self.fail(
            "successful worker run was not recorded for "
            f"envelope_id={expected_envelope_id!r}\n\n"
            f"worker_runs={worker_run_summary}\n\n{diagnostics}"
        )

    def _dump_diagnostics(
        self,
        *,
        main_bbmb_proc: subprocess.Popen[str] | None,
        auxiliary_bbmb_proc: subprocess.Popen[str] | None,
        handler_proc: subprocess.Popen[str] | None,
        worker_proc: subprocess.Popen[str] | None,
        auxiliary_proc: subprocess.Popen[str] | None,
        main_bbmb_log: Path,
        auxiliary_bbmb_log: Path,
        handler_log: Path,
        worker_log: Path,
        auxiliary_log: Path,
        main_bbmb_log_fh,
        auxiliary_bbmb_log_fh,
        handler_log_fh,
        worker_log_fh,
        auxiliary_log_fh,
    ) -> str:
        for proc in (
            handler_proc,
            worker_proc,
            auxiliary_proc,
            main_bbmb_proc,
            auxiliary_bbmb_proc,
        ):
            if proc is not None and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
        for file_handle in (
            handler_log_fh,
            worker_log_fh,
            auxiliary_log_fh,
            main_bbmb_log_fh,
            auxiliary_bbmb_log_fh,
        ):
            if not file_handle.closed:
                file_handle.close()

        diagnostics = []
        for name, path in (
            ("main bbmb", main_bbmb_log),
            ("auxiliary bbmb", auxiliary_bbmb_log),
            ("handler", handler_log),
            ("worker", worker_log),
            ("auxiliary", auxiliary_log),
        ):
            if path.exists():
                diagnostics.append(
                    f"== {name} log ==\n{path.read_text(encoding='utf-8')}"
                )
        return "\n\n".join(diagnostics)
