"""Inspect and explicitly resume tasks interrupted after possible side effects."""

import argparse
import json
import os

from app.state import SQLiteStateStore
from app.worker.main import WORKER_CONFIG_PATH_ENV_VAR, _load_config, worker_db_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument(
        "--resume",
        metavar="TASK_ID",
        help="Resume one task after checking its prior activity",
    )
    args = parser.parse_args()
    config = (
        _load_config(None, os.environ)
        if os.environ.get(WORKER_CONFIG_PATH_ENV_VAR)
        else {}
    )
    path = args.db or worker_db_path(config)
    if not path:
        parser.error("worker database is required via --db or worker config")
    store = SQLiteStateStore(path)
    if args.resume:
        if not store.resume_interrupted_quota_task(args.resume):
            print(json.dumps({"status": "not_interrupted_or_still_running"}))
            return 1
        print(json.dumps({"status": "queued", "task_id": args.resume}))
    else:
        print(json.dumps({"interrupted_tasks": store.interrupted_quota_tasks()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
