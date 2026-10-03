"""Record executor intervals for the parallel worker integration test."""

import json
import os
import sys
import time


def main() -> None:
    task = json.load(sys.stdin)["task"]
    task_id = task["task_id"]
    duration = float(task["content"].removeprefix("sleep:"))
    log_path = sys.argv[1]
    for phase in ("start", "end"):
        with open(log_path, "a", encoding="utf-8") as log:
            log.write(
                json.dumps({"task_id": task_id, "phase": phase, "at": time.time()})
                + "\n"
            )
            log.flush()
            os.fsync(log.fileno())
        if phase == "start":
            time.sleep(duration)


if __name__ == "__main__":
    main()
