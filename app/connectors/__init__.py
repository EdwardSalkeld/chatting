"""Connectors package."""

from app.connectors.base import Connector
from app.connectors.fake_cron_connector import CronTrigger, FakeCronConnector
from app.connectors.fake_email_connector import EmailMessage, FakeEmailConnector
from app.connectors.imap_email_connector import ImapEmailConnector
from app.connectors.interval_schedule_connector import (
    IntervalScheduleConnector,
    IntervalScheduleJob,
)

__all__ = [
    "Connector",
    "CronTrigger",
    "EmailMessage",
    "FakeCronConnector",
    "FakeEmailConnector",
    "ImapEmailConnector",
    "IntervalScheduleConnector",
    "IntervalScheduleJob",
]
