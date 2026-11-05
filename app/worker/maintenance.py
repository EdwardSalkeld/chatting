"""Built-in maintenance tasks issued by the handler's cron scheduler."""

import json
import logging
import sqlite3
from pathlib import Path

from app.models import TaskEnvelope
from app.state.maintenance import maintain_worker

TARGET = "database-maintenance"


def is_maintenance(envelope: TaskEnvelope) -> bool:
    return (
        envelope.source == "internal"
        and envelope.reply_channel.type == "internal"
        and envelope.reply_channel.target == TARGET
    )


def run_maintenance(db_path: str, content: str) -> dict[str, int]:
    policy = json.loads(content)
    if not isinstance(policy, dict):
        raise ValueError("maintenance policy must be an object")
    values: list[int] = []
    for key in ("retention_days", "delivery_retention_days"):
        value = policy.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError("maintenance retention days must be positive integers")
        values.append(value)
    connection = sqlite3.connect(
        Path(db_path).resolve().as_uri() + "?mode=rw", uri=True, timeout=60
    )
    try:
        counts = maintain_worker(
            connection, retention_days=values[0], delivery_retention_days=values[1]
        )
    finally:
        connection.close()
    logging.getLogger(__name__).info("worker_database_maintenance counts=%s", counts)
    return counts
