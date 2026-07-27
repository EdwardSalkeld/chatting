from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


DOCKER_WORKSPACE_PREFIX = "repo:/workspace"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rewrite chatting runtime configs for a host deployment."
    )
    parser.add_argument(
        "--source-root",
        required=True,
        help="Directory containing handler/ and worker/ config subdirectories.",
    )
    parser.add_argument(
        "--output-root",
        required=True,
        help="Directory to write handler.json, worker.json, env files, and schedule files.",
    )
    parser.add_argument(
        "--workspace-dir",
        required=True,
        help="Absolute host workspace path to use for repo:/ context refs and codex_working_dir.",
    )
    parser.add_argument(
        "--handler-state-dir",
        default="/var/lib/handler",
        help="Directory that should own handler persistent state.",
    )
    parser.add_argument(
        "--worker-state-dir",
        default="/var/lib/worker",
        help="Directory that should own worker persistent state.",
    )
    parser.add_argument(
        "--config-dir",
        default="/etc/chatting",
        help="Directory where rendered host config files will be installed.",
    )
    parser.add_argument(
        "--bbmb-address",
        default="127.0.0.1:9876",
        help="BBMB address for the rendered host runtime.",
    )
    parser.add_argument(
        "--metrics-host",
        default="127.0.0.1",
        help="Metrics bind host for the rendered handler config.",
    )
    return parser.parse_args()


def _rewrite_context_refs(values: object, workspace_dir: str) -> object:
    if not isinstance(values, list):
        return values
    rewritten: list[object] = []
    for value in values:
        if isinstance(value, str) and value.startswith(DOCKER_WORKSPACE_PREFIX):
            suffix = value[len(DOCKER_WORKSPACE_PREFIX) :]
            rewritten.append(f"repo:{workspace_dir}{suffix}")
            continue
        rewritten.append(value)
    return rewritten


def _rewrite_handler_config(
    payload: dict[str, object],
    *,
    workspace_dir: str,
    handler_state_dir: str,
    config_dir: str,
    bbmb_address: str,
    metrics_host: str,
) -> dict[str, object]:
    updated = dict(payload)
    updated["db_path"] = f"{handler_state_dir}/chatting-message-handler.db"
    updated["bbmb_address"] = bbmb_address
    updated["metrics_host"] = metrics_host
    updated["telegram_attachment_dir"] = f"{handler_state_dir}/telegram-attachments"
    schedule_file = updated.get("schedule_file")
    if isinstance(schedule_file, str) and schedule_file.strip():
        updated["schedule_file"] = f"{config_dir}/{Path(schedule_file).name}"
    for key in (
        "context_refs",
        "telegram_context_refs",
        "auxiliary_ingress_context_refs",
        "github_context_refs",
    ):
        updated[key] = _rewrite_context_refs(updated.get(key), workspace_dir)
    return updated


def _rewrite_worker_config(
    payload: dict[str, object],
    *,
    worker_state_dir: str,
    bbmb_address: str,
    workspace_dir: str,
) -> dict[str, object]:
    updated = dict(payload)
    updated["db_path"] = f"{worker_state_dir}/chatting-worker.db"
    updated["bbmb_address"] = bbmb_address
    updated["codex_working_dir"] = workspace_dir
    return updated


def _copy_if_present(source: Path, target: Path) -> None:
    if source.exists():
        shutil.copy2(source, target)


def render_runtime_config(
    source_root: Path,
    output_root: Path,
    *,
    workspace_dir: str,
    handler_state_dir: str,
    worker_state_dir: str,
    config_dir: str,
    bbmb_address: str,
    metrics_host: str,
) -> None:
    handler_dir = source_root / "handler"
    worker_dir = source_root / "worker"
    output_root.mkdir(parents=True, exist_ok=True)

    handler_payload = json.loads((handler_dir / "handler.json").read_text(encoding="utf-8"))
    worker_payload = json.loads((worker_dir / "worker.json").read_text(encoding="utf-8"))

    (output_root / "handler.json").write_text(
        json.dumps(
            _rewrite_handler_config(
                handler_payload,
                workspace_dir=workspace_dir,
                handler_state_dir=handler_state_dir,
                config_dir=config_dir,
                bbmb_address=bbmb_address,
                metrics_host=metrics_host,
            ),
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (output_root / "worker.json").write_text(
        json.dumps(
            _rewrite_worker_config(
                worker_payload,
                worker_state_dir=worker_state_dir,
                bbmb_address=bbmb_address,
                workspace_dir=workspace_dir,
            ),
            indent=2,
        )
        + "\n",
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
        workspace_dir=args.workspace_dir,
        handler_state_dir=args.handler_state_dir,
        worker_state_dir=args.worker_state_dir,
        config_dir=args.config_dir,
        bbmb_address=args.bbmb_address,
        metrics_host=args.metrics_host,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
