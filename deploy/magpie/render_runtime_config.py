from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


BLINK_WORKSPACE_PREFIX = "repo:/workspace"
MAGPIE_WORKSPACE_PREFIX = "repo:/srv/chatting/workspace"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rewrite Blink chatting configs into Magpie host paths."
    )
    parser.add_argument(
        "--source-root",
        required=True,
        help="Directory containing handler/ and worker/ subdirectories from Blink.",
    )
    parser.add_argument(
        "--output-root",
        required=True,
        help="Directory to write handler.json, worker.json, env files, and schedule files.",
    )
    return parser.parse_args()


def _rewrite_context_refs(values: object) -> object:
    if not isinstance(values, list):
        return values
    rewritten: list[object] = []
    for value in values:
        if isinstance(value, str) and value.startswith(BLINK_WORKSPACE_PREFIX):
            suffix = value[len(BLINK_WORKSPACE_PREFIX) :]
            rewritten.append(f"{MAGPIE_WORKSPACE_PREFIX}{suffix}")
            continue
        rewritten.append(value)
    return rewritten


def _rewrite_handler_config(payload: dict[str, object]) -> dict[str, object]:
    updated = dict(payload)
    updated["db_path"] = "/var/lib/handler/chatting-message-handler.db"
    updated["bbmb_address"] = "127.0.0.1:9876"
    updated["metrics_host"] = "127.0.0.1"
    updated["telegram_attachment_dir"] = "/var/lib/handler/telegram-attachments"
    schedule_file = updated.get("schedule_file")
    if isinstance(schedule_file, str) and schedule_file.strip():
        updated["schedule_file"] = f"/etc/chatting/{Path(schedule_file).name}"
    for key in (
        "context_refs",
        "telegram_context_refs",
        "auxiliary_ingress_context_refs",
        "github_context_refs",
    ):
        updated[key] = _rewrite_context_refs(updated.get(key))
    return updated


def _rewrite_worker_config(payload: dict[str, object]) -> dict[str, object]:
    updated = dict(payload)
    updated["db_path"] = "/var/lib/worker/chatting-worker.db"
    updated["bbmb_address"] = "127.0.0.1:9876"
    updated["codex_working_dir"] = "/srv/chatting/workspace"
    return updated


def _copy_if_present(source: Path, target: Path) -> None:
    if source.exists():
        shutil.copy2(source, target)


def render_runtime_config(source_root: Path, output_root: Path) -> None:
    handler_dir = source_root / "handler"
    worker_dir = source_root / "worker"
    output_root.mkdir(parents=True, exist_ok=True)

    handler_payload = json.loads((handler_dir / "handler.json").read_text(encoding="utf-8"))
    worker_payload = json.loads((worker_dir / "worker.json").read_text(encoding="utf-8"))

    (output_root / "handler.json").write_text(
        json.dumps(_rewrite_handler_config(handler_payload), indent=2) + "\n",
        encoding="utf-8",
    )
    (output_root / "worker.json").write_text(
        json.dumps(_rewrite_worker_config(worker_payload), indent=2) + "\n",
        encoding="utf-8",
    )

    _copy_if_present(handler_dir / "handler.env", output_root / "handler.env")
    _copy_if_present(worker_dir / "worker.env", output_root / "worker.env")
    _copy_if_present(
        handler_dir / "live-schedule.local.json",
        output_root / "live-schedule.local.json",
    )


def main() -> int:
    args = _parse_args()
    render_runtime_config(
        source_root=Path(args.source_root).resolve(),
        output_root=Path(args.output_root).resolve(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
